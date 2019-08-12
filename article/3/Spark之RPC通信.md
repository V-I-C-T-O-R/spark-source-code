###### Spark rpc模块依赖
![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/3/pic/1.jpg)
##### Spark Rpc启动端口监听过程
最顶层的抽象类RpcEnv，其伴生对象包含两个create方法，通过NettyRpcEnvFactory来生成RpcEnv对象，而内部是实例化了RpcEnv的实现类NettyRpcEnv。当判定当前启动的不是client环境时，会额外的去调用Core包的org.apache.spark.util.Utils.startServiceOnPort启动监听端口，内部会调用之前定义好的函数来启动。该函数传入Int值，返回(NettyRpcEnv, Int)。
```
val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
}
```
随后调用NettyRpcEnv的startServer方法，每一个实例化的NettyRpcEnv对象都包含有如下属性：transportConf，dispatcher，streamManager，transportContext，clientFactory。其中dispatcher为Dispatcher类根据spark.conf的配置实例化出，transportContext为实例化的TransportContext对象(内部包含有transportConf的引用，新的NettyRpcHandler对象)。
```
def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
        java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        java.util.Collections.emptyList()
      }
    server = transportContext.createServer(bindAddress, port, bootstraps)
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
}
```
transportContext.createServer在内部返回了TransportServer对象，它在实例化TransportServer对象的过程中，底层调用了Netty库的IO操作。首先，传入配置信息选择是netty的NIO模式还是EPOLL模式，并实例化线程池(根据线程名前缀实例化DefaultThreadFactory)，从而根据配置的netty模式实例化NioEventLoopGroup，EpollEventLoopGroup。其次，根据配置实例化netty缓冲资源分配器PooledByteBufAllocator来构建ServerBootstrap启动器，初始化通道处理器ChannelHandler(具体实现是NettyRpcHandler)，并为通道设置处理响应的处理器。最后，执行bootstrap.bind(address)来使用Netty监听本地socket端口(实际上是调用netty库的doBind0方法执行线程，并返回ChannelFuture)。
dispatcher.registerRpcEndpoint方法在endpoints集合中注册保存了名为"endpoint-verifier"的EndpointData实例(EndpointData中持有RpcEndpoint、NettyRpcEndpointRef、Inbox的引用)，在endpointRefs集合中保存了RpcEndpoint对应的NettyRpcEndpointRef对象，最后返回该RpcEndpoint对应的NettyRpcEndpointRef的引用。
```
IOMode ioMode = IOMode.valueOf(conf.ioMode());
    EventLoopGroup bossGroup =
      NettyUtils.createEventLoop(ioMode, conf.serverThreads(), conf.getModuleName() + "-server");
    EventLoopGroup workerGroup = bossGroup;
 　 ......省略......
    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NettyUtils.getServerChannelClass(ioMode))
      .option(ChannelOption.ALLOCATOR, allocator)
      .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS)
      .childOption(ChannelOption.ALLOCATOR, allocator);
    ......省略......
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        RpcHandler rpcHandler = appRpcHandler;
        for (TransportServerBootstrap bootstrap : bootstraps) {
          rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
        }
        context.initializePipeline(ch, rpcHandler);
      }
    });

    InetSocketAddress address = hostToBind == null ?
        new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
    channelFuture = bootstrap.bind(address);
    channelFuture.syncUninterruptibly();

    port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
}
```
<div align=center>![2.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/3/pic/2.jpg)</div>

##### Spark rpc注册endpoint
在Spark中的注册方法为`NettyRpcEnv.setupEndpoint`：
```
override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
}
```
随后也会调用dispatcher.registerRpcEndpoint(name, endpoint)来注册endpoint，会将endpointName和`RpcEndpoint`的映射关系保存到endpoints中，同时`Dispatcher`会为`RpcEndpoint`创建对应的`NettyRpcEndpointRef`实例，并保存到endpointRefs中。

##### Spark rpc消息发送
首先客户端会向服务端申请获取需要通信的`RpcEndpoint`的`RpcEndpointRef`。客户端会调用`RpcEnv`中的方法来获取`RpcEndpointRef`，其中`RpcEnv`提供了三种调用方法，即asyncSetupEndpointRefByURI，setupEndpointRefByURI，setupEndpointRef这三个方法最终会调用`NettyRpcEnv.asyncSetupEndpointRefByURI`方法：
```
def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
}
```
在客户端会构造`RpcEndpointVerifier`对应的`NettyRpcEndpointRef`，并发送给服务端，询问请求的endpoint是否已经注册。如果服务端返回true，则将endpoint对应的`NettyRpcEndpointRef`返回给调用者，之后的请求都用它与服务端对应的`RpcEndpoint`进行通信。
```
private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
      ......省略......
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        postToOutbox(message.receiver, rpcMessage)
        promise.future.failed.foreach {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
        ......省略......
}
```
##### Spark rpc消息接收
Spark接收来自客户端的消息最终会调用`RpcHandler`来处理，其具体实现为NettyRpcHandler。它有两个个接收方法：
```
override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit
override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit
```
这两个方法中都是调用internalReceive方法来组装消息，然后调用`Dispatcher`的对应方法，将反序列化后的消息发送给`Dispatcher`进一步处理。
```
private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
}
```
Dispatcher会根据调用的方法是否是需要返回值来调用处理：postRemoteMessage和postOneWayMessage，最后都是通过postMessage方法发送到队列里进行不同的操作。
```
private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
    error.foreach(callbackIfStopped)
}
```
