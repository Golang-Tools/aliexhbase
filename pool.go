// 连接池定义
package aliexhbase

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// PoolStatus 池状态
type PoolStatus uint32

const (
	PoolStatus_Stoped PoolStatus = iota
	PoolStatus_Open
)

// 连接池配置
type ThriftPoolConfig struct {
	// Thrfit Server端地址
	Addr   string
	User   string
	Passwd string
	// 最大连接数
	MaxConn int32
	// 创建连接超时时间
	ConnTimeout time.Duration
	// 空闲客户端超时时间，超时主动释放连接，关闭客户端
	IdleTimeout time.Duration
	// 获取Thrift客户端超时时间
	Timeout time.Duration
	// 获取Thrift客户端失败重试间隔
	Interval time.Duration
}

// Thrift客户端连接池
type ThriftPool struct {
	idle list.List
	// 同步锁，确保count/status/idle等公共数据并发操作安全
	lock *sync.Mutex
	// 记录当前已经创建的Thrift客户端,确保MaxConn配置
	count int32
	// Thrift客户端连接池状态,目前就open和stop两种
	status uint32
	// Thrift客户端连接池相关配置
	config *ThriftPoolConfig
}

var nowFunc = time.Now

// NewThriftPool 创建一个新的池对象
func NewThriftPool(config *ThriftPoolConfig) *ThriftPool {

	thriftPool := &ThriftPool{
		lock:   &sync.Mutex{},
		config: config,
		status: uint32(PoolStatus_Open),
		count:  0,
	}
	// 初始化空闲链接
	thriftPool.initConn()
	// 定期清理过期空闲连接
	go thriftPool.ClearConn()
	return thriftPool
}

// 链接池创建时先初始化一定数量的空闲连接数
func (p *ThriftPool) initConn() {
	initCount := p.config.MaxConn
	if initCount > MaxInitConnCount {
		initCount = MaxInitConnCount
	}
	wg := &sync.WaitGroup{}
	wg.Add(int(initCount))
	for i := int32(0); i < initCount; i++ {
		go p.createIdleConn(wg)
	}
	wg.Wait()
}

//获取一个空闲连接并放入池中
func (p *ThriftPool) createIdleConn(wg *sync.WaitGroup) {
	c, _ := p.Get()
	p.Put(c)
	wg.Done()
}

// ClearConn 定时清空闲置连接
func (p *ThriftPool) ClearConn() {
	sleepTime := DEFAULT_CHECKINTERVAL * time.Second
	if sleepTime < p.config.IdleTimeout {
		sleepTime = p.config.IdleTimeout
	}
	for {
		p.CheckTimeout()
		time.Sleep(sleepTime)
	}
}

// CheckTimeout 检查池中是否有超时的空闲连接,有的话关闭
func (p *ThriftPool) CheckTimeout() {
	p.lock.Lock()
	for p.idle.Len() != 0 {
		ele := p.idle.Back()
		if ele == nil {
			break
		}
		v := ele.Value.(*Conn)
		if v.t.Add(p.config.IdleTimeout).After(nowFunc()) {
			break
		}
		//timeout && clear
		p.idle.Remove(ele)
		p.lock.Unlock()
		v.Close() //close client connection
		atomic.AddInt32(&p.count, -1)
		p.lock.Lock()
	}
	p.lock.Unlock()
}

// 获取Thrift空闲连接
func (p *ThriftPool) Get() (*Conn, error) {
	return p.get(nowFunc().Add(p.config.Timeout))
}

// 获取连接的逻辑实现
// expire设定了一个超时时间点，当没有可用连接时，程序会休眠一小段时间后重试
// 如果一直获取不到连接，一旦到达超时时间点，则报ErrOverMax错误
func (p *ThriftPool) get(expire time.Time) (*Conn, error) {
	if atomic.LoadUint32(&p.status) == uint32(PoolStatus_Stoped) {
		return nil, ErrPoolClosed
	}

	// 判断是否超额
	p.lock.Lock()
	if p.idle.Len() == 0 && atomic.LoadInt32(&p.count) >= p.config.MaxConn {
		p.lock.Unlock()
		// 不采用递归的方式来实现重试机制，防止栈溢出，这里改用循环方式来实现重试
		for {
			// 休眠一段时间再重试
			time.Sleep(p.config.Interval)
			// 超时退出
			if nowFunc().After(expire) {
				return nil, ErrOverMax
			}
			p.lock.Lock()
			if p.idle.Len() == 0 && atomic.LoadInt32(&p.count) >= p.config.MaxConn {
				p.lock.Unlock()
			} else { // 有可用链接，退出for循环
				break
			}
		}
	}

	if p.idle.Len() == 0 {
		// 先加1，防止首次创建连接时，TCP握手太久，导致p.count未能及时+1，而新的请求已经到来
		// 从而导致短暂性实际连接数大于p.count（大部分链接由于无法进入空闲链接队列，而被关闭，处于TIME_WATI状态）
		atomic.AddInt32(&p.count, 1)
		p.lock.Unlock()
		client, err := NewConn(p.config.Addr, p.config.User, p.config.Passwd)
		if err != nil {
			atomic.AddInt32(&p.count, -1)
			return nil, err
		}
		// 检查连接是否有效
		if !client.Check() {
			atomic.AddInt32(&p.count, -1)
			return nil, ErrSocketDisconnect
		}

		return client, nil
	}

	// 从队头中获取空闲连接
	ele := p.idle.Front()
	idlec := ele.Value.(*Conn)
	p.idle.Remove(ele)
	p.lock.Unlock()

	// 连接从空闲队列获取，可能已经关闭了，这里再重新检查一遍
	if !idlec.Check() {
		atomic.AddInt32(&p.count, -1)
		return nil, ErrSocketDisconnect
	}
	return idlec, nil
}

//Put 归还Thrift客户端
func (p *ThriftPool) Put(client *Conn) error {
	if client == nil {
		return nil
	}

	if atomic.LoadUint32(&p.status) == uint32(PoolStatus_Stoped) {
		err := client.Close()
		client = nil
		return err
	}

	if atomic.LoadInt32(&p.count) > p.config.MaxConn || !client.Check() {
		atomic.AddInt32(&p.count, -1)
		err := client.Close()
		client = nil
		return err
	}

	p.lock.Lock()
	client.t = nowFunc()
	p.idle.PushFront(client)
	p.lock.Unlock()

	return nil
}

// 关闭有问题的连接，并创建新的连接
func (p *ThriftPool) Reconnect(client *Conn) (newClient *Conn, err error) {
	if client != nil {
		client.Close()
	}
	client = nil
	newClient, err = NewConn(p.config.Addr, p.config.User, p.config.Passwd)
	if err != nil {
		atomic.AddInt32(&p.count, -1)
		return
	}
	if !newClient.Check() {
		atomic.AddInt32(&p.count, -1)
		return nil, ErrSocketDisconnect
	}
	return
}

//CloseConn 关闭指定连接
func (p *ThriftPool) CloseConn(client *Conn) {
	if client != nil {
		client.Close()
	}
	atomic.AddInt32(&p.count, -1)
}

//GetIdleCount 获取现在闲置连接个数
func (p *ThriftPool) GetIdleCount() uint32 {
	if p != nil {
		return uint32(p.idle.Len())
	}
	return 0
}

//GetConnCount 获取当前连接数
func (p *ThriftPool) GetConnCount() int32 {
	if p != nil {
		return atomic.LoadInt32(&p.count)
	}
	return 0
}

//Release 释放连接池
func (p *ThriftPool) Release() {
	atomic.StoreUint32(&p.status, uint32(PoolStatus_Stoped))
	atomic.StoreInt32(&p.count, 0)

	p.lock.Lock()
	idle := p.idle
	p.idle.Init()
	p.lock.Unlock()

	for iter := idle.Front(); iter != nil; iter = iter.Next() {
		iter.Value.(*Conn).Close()
	}
}

//Recover 恢复连接池
func (p *ThriftPool) Recover() {
	atomic.StoreUint32(&p.status, uint32(PoolStatus_Open))
}

// IsOpen 查看池是否开着
func (p *ThriftPool) IsOpen() bool {
	return atomic.LoadUint32(&p.status) != uint32(PoolStatus_Stoped)
}
