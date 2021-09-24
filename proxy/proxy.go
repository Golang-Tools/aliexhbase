//hbase客户端代理
package proxy

import (
	"github.com/Golang-Tools/aliexhbase"
)

//Callback redis操作的回调函数
type Callback func(cli *aliexhbase.Client) error

//Proxy bun客户端的代理
type Proxy struct {
	*aliexhbase.Client
	callBacks []Callback
}

// New 创建一个新的数据库客户端代理
func New() *Proxy {
	proxy := new(Proxy)
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *Proxy) IsOk() bool {
	return proxy.Client != nil
}

// Init 初始化代理对象
func (proxy *Proxy) Init(opts ...aliexhbase.Option) error {
	o := aliexhbase.DefaultOptions
	for _, opt := range opts {
		opt.Apply(&o)
	}
	c, err := aliexhbase.New(aliexhbase.WithOptions(&o))
	if err != nil {
		return err
	}
	proxy.Client = c
	if o.Parallelcallback {
		for _, cb := range proxy.callBacks {
			go func(cb Callback) {
				err := cb(proxy.Client)
				if err != nil {
					o.Logger.WithError(err).Error("regist callback get error")
				} else {
					o.Logger.Debug("regist callback done")
				}
			}(cb)
		}
	} else {
		for _, cb := range proxy.callBacks {
			err := cb(proxy.Client)
			if err != nil {
				o.Logger.WithError(err).Error("regist callback get error")
			} else {
				o.Logger.Debug("regist callback done")
			}
		}
	}
	return nil
}

// Regist 注册回调函数,在init执行后执行回调函数
//如果对象已经设置了被代理客户端则无法再注册回调函数
func (proxy *Proxy) Regist(cb Callback) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedUniversalClient
	}
	proxy.callBacks = append(proxy.callBacks, cb)
	return nil
}

//DB 默认的数据库代理对象
var DB = New()
