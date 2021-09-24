# aliexhbase

阿里的增强版hbase客户端,使用thrift连接,提供连接池

本项目提供了两个工具

+ 客户端对象,需要导入`github.com/Golang-Tools/aliexhbase`,使用`New`函数创建一个客户端,其参数可以参考`options`中的定义.
+ 代理对象,需要导入`github.com/Golang-Tools/aliexhbase/proxy`,使用`New`函数创建,用对象的`Init`方法初始化,其参数可以参考`options`中的定义.同时提供对象`DB`作为默认的代理对象.

除了上面得对象外还提供了接口`UniversalClient`用于描述上面的2个对象.
