// hbase连接定义
package aliexhbase

import (
	"time"

	"github.com/Golang-Tools/aliexhbase/gen-go/hbase"
	"github.com/apache/thrift/lib/go/thrift"
)

type Conn struct {
	// 真正的Thrift客户端，业务创建传入
	*hbase.THBaseServiceClient
	// Thrift传输层，封装了底层连接建立、维护、关闭、数据读写等细节
	transport thrift.TTransport
	// 最近一次放入空闲队列的时间
	t time.Time
}

func NewConn(addr, user, passwd string) (*Conn, error) {
	conn := new(Conn)
	transport, err := thrift.NewTHttpClient(addr)
	// options := thrift.THttpClientOptions{
	// 	Client: &http.Client{

	// 	}
	// }
	// transport, err := thrift.NewTHttpClientWithOptions()
	if err != nil {
		return nil, err
	}
	if err := transport.Open(); err != nil {
		return nil, err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	// 设置用户名密码
	httClient := transport.(*thrift.THttpClient)
	httClient.SetHeader("ACCESSKEYID", user)
	httClient.SetHeader("ACCESSSIGNATURE", passwd)
	conn.transport = transport
	conn.THBaseServiceClient = hbase.NewTHBaseServiceClientFactory(transport, protocolFactory)
	return conn, nil
}

// 检测连接是否有效
func (c *Conn) Check() bool {
	if c.transport == nil || c.THBaseServiceClient == nil {
		return false
	}
	return c.IsOpen()
}

func (c *Conn) Close() error {
	return c.transport.Close()
}
func (c *Conn) Open() error {
	return c.transport.Open()
}
func (c *Conn) IsOpen() bool {
	return c.transport.IsOpen()
}
