源码编译
1. 源码下载地址：[https://github.com/alibaba/nacos/][https://github.com/alibaba/nacos/]，选择版本，下载zip
2. 解压，进入目录后执行编译命令：mvn -Prelease-nacos -DskipTests clean install -U
3. 导入idea，配置spring boot启动参数。nacos 支持单机和集群两种模式，单机模式执行参数：nacos.standalone=true 表示单机模式，nacos.home：是工程的项目目录
``` 
-Dnacos.standalone=true -Dnacos.home=/Users/hr/nacos-2.2.4
``` 
4. 启动服务，console 包下的 com.alibaba.nacos.Nacos#main
5. 访问地址：http://localhost:8848/nacos

官网地址：https://nacos.io/zh-cn/