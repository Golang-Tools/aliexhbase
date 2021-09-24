// 异常定义
package aliexhbase

import "errors"

//error
var (
	//ErrOverMax ThriftPool 连接超过设置的最大连接数
	ErrOverMax = errors.New("ThriftPool 连接超过设置的最大连接数")
	//ErrInvalidConn ThriftPool Client回收时变成nil
	ErrInvalidConn = errors.New("ThriftPool Client回收时变成nil")
	//ErrPoolClosed ThriftPool 连接池已经被关闭
	ErrPoolClosed = errors.New("ThriftPool 连接池已经被关闭")
	//ErrPoolAlreadyOpened ThriftPool 连接池已经被打开
	ErrPoolAlreadyOpened = errors.New("ThriftPool 连接池已经被打开")
	//ErrSocketDisconnect ThriftPool 客户端socket连接已断开
	ErrSocketDisconnect = errors.New("ThriftPool 客户端socket连接已断开")
	//ErrClientPWDNotSet Client 未设置密码
	ErrClientPWDNotSet = errors.New("Client 未设置密码")
	//ErrClientCreateParamsNotEnough Client 对象创建参数不全
	ErrClientCreateParamsNotEnough = errors.New("Client 对象创建参数不全")
	//ErrClientPoolNotSet Client 未设置连接池
	ErrClientPoolNotSet = errors.New("Client 未设置连接池")
)
