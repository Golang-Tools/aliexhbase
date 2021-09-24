// 常量定义
package aliexhbase

import "time"

const (
	DEFAULT_CHECKINTERVAL      = 120 //清除超时连接间隔
	MaxInitConnCount           = 10
	DEFAULT_POOL_CLOSE_TIMEOUT = time.Duration(1) * time.Second
)
