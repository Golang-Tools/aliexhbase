// 初始化配置项
package aliexhbase

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	logrus "github.com/sirupsen/logrus"
)

//Options 客户端配置
type Options struct {
	// Thrfit Server端地址
	Poolconfig       *ThriftPoolConfig
	QueryTimeout     time.Duration
	Logger           logrus.FieldLogger
	Parallelcallback bool
}

var DefaultOptions = Options{
	Poolconfig: &ThriftPoolConfig{
		MaxConn: 60,
		// 创建连接超时时间
		ConnTimeout: time.Second * 2,
		// 空闲客户端超时时间,超时主动释放连接,关闭客户端
		IdleTimeout: time.Minute * 15,
		// 获取Thrift客户端超时时间
		Timeout: time.Second * 5,
		// 获取Thrift客户端失败重试间隔
		Interval: time.Millisecond * 50,
	},
	Logger: logrus.New().WithField("logger", "aliexhbase"),
}

// Option configures how we set up the connection.
type Option interface {
	Apply(*Options)
}

// func (emptyOption) apply(*Options) {}
type funcOption struct {
	f func(*Options)
}

func (fo *funcOption) Apply(do *Options) {
	fo.f(do)
}

func newFuncOption(f func(*Options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

//WithOption 设置option
func WithOptions(opts *Options) Option {
	return newFuncOption(func(o *Options) {
		o.Poolconfig = opts.Poolconfig
		o.QueryTimeout = opts.QueryTimeout
		o.Logger = opts.Logger
		o.Parallelcallback = opts.Parallelcallback
	})
}

//WithParallelCallback 只对proxy有效,设置初始化后回调并行执行而非串行执行
func WithParallelCallback() Option {
	return newFuncOption(func(o *Options) {
		o.Parallelcallback = true
	})
}

//WithQueryTimeoutMS 设置最大请求超时,单位ms
func WithQueryTimeoutMS(QueryTimeout int) Option {
	return newFuncOption(func(o *Options) {
		o.QueryTimeout = time.Duration(QueryTimeout) * time.Millisecond
	})
}

//WithURL 使用要连接的数据库管理系统的url,注意必须有用户名和密码,也就是说形式为`http:\\user:pwd@host:port`
func WithURL(URL string) Option {
	return newFuncOption(func(o *Options) {
		if o.Poolconfig == nil {
			o.Poolconfig = &ThriftPoolConfig{
				MaxConn: 60,
				// 创建连接超时时间
				ConnTimeout: time.Second * 2,
				// 空闲客户端超时时间，超时主动释放连接，关闭客户端
				IdleTimeout: time.Minute * 15,
				// 获取Thrift客户端超时时间
				Timeout: time.Second * 5,
				// 获取Thrift客户端失败重试间隔
				Interval: time.Millisecond * 50,
			}
		}
		U, err := url.Parse(URL)
		if err != nil {
			panic(err)
		}
		username := U.User.Username()
		pwd, ok := U.User.Password()
		if !ok {
			panic(ErrClientPWDNotSet)
		}
		address := strings.ReplaceAll(URL, fmt.Sprintf("%s:%s@", username, pwd), "")
		o.Poolconfig.Addr = address
		o.Poolconfig.User = username
		o.Poolconfig.Passwd = pwd
	})
}

//WithMaxOpenConns 设置连接池的最大连接数
func WithMaxConns(MaxConns int32) Option {
	return newFuncOption(func(o *Options) {
		if o.Poolconfig == nil {
			o.Poolconfig = &ThriftPoolConfig{
				MaxConn: 60,
				// 创建连接超时时间
				ConnTimeout: time.Second * 2,
				// 空闲客户端超时时间，超时主动释放连接，关闭客户端
				IdleTimeout: time.Minute * 15,
				// 获取Thrift客户端超时时间
				Timeout: time.Second * 5,
				// 获取Thrift客户端失败重试间隔
				Interval: time.Millisecond * 50,
			}
		}
		o.Poolconfig.MaxConn = MaxConns
	})
}

//WithConnTimeoutS 创建连接超时时间,单位s
func WithConnTimeoutS(ConnTimeoutS int) Option {
	return newFuncOption(func(o *Options) {
		if o.Poolconfig == nil {
			o.Poolconfig = &ThriftPoolConfig{
				MaxConn: 60,
				// 创建连接超时时间
				ConnTimeout: time.Second * 2,
				// 空闲客户端超时时间，超时主动释放连接，关闭客户端
				IdleTimeout: time.Minute * 15,
				// 获取Thrift客户端超时时间
				Timeout: time.Second * 5,
				// 获取Thrift客户端失败重试间隔
				Interval: time.Millisecond * 50,
			}
		}
		o.Poolconfig.ConnTimeout = time.Duration(ConnTimeoutS) * time.Second
	})
}

//WithIdleTimeoutS 空闲客户端超时时间,超时主动释放连接,关闭客户端,单位s
func WithIdleTimeoutS(IdleTimeoutS int) Option {
	return newFuncOption(func(o *Options) {
		if o.Poolconfig == nil {
			o.Poolconfig = &ThriftPoolConfig{
				MaxConn: 60,
				// 创建连接超时时间
				ConnTimeout: time.Second * 2,
				// 空闲客户端超时时间，超时主动释放连接，关闭客户端
				IdleTimeout: time.Minute * 15,
				// 获取Thrift客户端超时时间
				Timeout: time.Second * 5,
				// 获取Thrift客户端失败重试间隔
				Interval: time.Millisecond * 50,
			}
		}
		o.Poolconfig.IdleTimeout = time.Duration(IdleTimeoutS) * time.Second
	})
}

// WithTimeoutS 获取Thrift客户端超时时间,单位s
func WithTimeoutS(TimeoutS int) Option {
	return newFuncOption(func(o *Options) {
		if o.Poolconfig == nil {
			o.Poolconfig = &ThriftPoolConfig{
				MaxConn: 60,
				// 创建连接超时时间
				ConnTimeout: time.Second * 2,
				// 空闲客户端超时时间，超时主动释放连接，关闭客户端
				IdleTimeout: time.Minute * 15,
				// 获取Thrift客户端超时时间
				Timeout: time.Second * 5,
				// 获取Thrift客户端失败重试间隔
				Interval: time.Millisecond * 50,
			}
		}
		o.Poolconfig.Timeout = time.Duration(TimeoutS) * time.Second
	})
}

//WithIntervalMS 获取Thrift客户端失败重试间隔,单位ms
func WithIntervalMS(IntervalMS int) Option {
	return newFuncOption(func(o *Options) {
		if o.Poolconfig == nil {
			o.Poolconfig = &ThriftPoolConfig{
				MaxConn: 60,
				// 创建连接超时时间
				ConnTimeout: time.Second * 2,
				// 空闲客户端超时时间，超时主动释放连接，关闭客户端
				IdleTimeout: time.Minute * 15,
				// 获取Thrift客户端超时时间
				Timeout: time.Second * 5,
				// 获取Thrift客户端失败重试间隔
				Interval: time.Millisecond * 50,
			}
		}
		o.Poolconfig.Interval = time.Duration(IntervalMS) * time.Millisecond
	})
}

//WithLogger 指定使用logger
func WithLogger(logger logrus.FieldLogger) Option {
	return newFuncOption(func(o *Options) {
		o.Logger = logger
	})
}
