重读Spark~任务提交准备2
---------------------------------------
以经典的WordCount程序提交到yarn client方式集群运行为例，读取参数路径中的文本内容，通过Spark算子按统计单词逻辑顺序处理，之后打印出处理好的单词统计结果。编译打包之后，命令行执行如下指令即可。
```
spark-submit \
--name WordCount \
--class com.example.WordCount \
--master yarn \
--deploy-mode client \
--driver-memory 3g \
--executor-memory 2G \
--total-executor-cores 2 \
/home/spark/lib/spark.jar \
hdfs://192.172.1.100:8020/hello.txt

object WordCount {
	//此方法由app.start方法内部反射触发
	def main(args: Array[String]): Unit = {
		//实例化spark运行默认配置
		var conf = new SparkConf().setAppName("WordCount")
		var sc = new SparkContext(conf)

		//args(0)传参文件路径
		var result = sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2)
	    //打印结果
	    result.collect().foreach(println)
	    sc.stop()
	}
}
```
在SparkConf实例化过程中，包含了很多复杂的过程，下面具体来看一下。在SparkContext构造方法中，首先会通过SparkConf对象的validateSettings来对运行参数进行验证与设置。
```
//SparkContext类
//设置启动Driver的ip地址
_conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
//设置启动Driver的端口
_conf.setIfMissing("spark.driver.port", "0")
//在实例化LiveListenerBus对象时，会遇到private[spark]语法。该语法表示private[包名]，包名可以是父包名或当前包名，如果是父包名，则父包和子包都可以访问。
//LiveListenerBus相当于Spark事件监听总线，当调用其start方法后，监听总线会广播所有的事件消息给对应的监听器。
_listenerBus = new LiveListenerBus(_conf)
//初始化Application状态存储单元和监视器
_statusStore = AppStatusStore.createLiveStore(conf)
//将_statusStore添加到监听总线中的appStatus队列
listenerBus.addToStatusQueue(_statusStore.listener.get)
//按照字面意思是创建Spark环境，那么具体是什么呢？
_env = createSparkEnv(_conf, isLocal, listenerBus)
```
SparkContext的createSparkEnv方法内部调用SparkEnv的createDriverEnv方法，同时传入SparkConf、listenerBus等参数。如果不继续往看的话，肯定这只是一个实例配置初始化过程。但是事实上，它做的比这要多的多。
```
//SparkEnv类
//关键代码部分
private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    //获取Driver绑定地址与端口
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }
```
看到这儿，肯定有点清楚了，这是要开启Socket服务了。没错，这里最终是调用了NettyRpcEnv类的create方法来完成Driver的远程rpc调用，并返回启动程序的上下文与访问方式——柳暗花明，原来在这里启动Driver角色。
```
//NettyRpcEnv类
def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    //序列化配置信息
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager, config.numUsableCores)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
      	//rpc调用
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
```
接下来就是Driver部分的一些配置，比如Block管理、MapOutPut配置、shuffleManager配置、OutPutCommit均衡配置、Driver临时文件夹配置等等，再回到SparkContext类createSparkEnv之后分析。
```
//SparkContext类，关键代码
//假如配置了开始spark ui，则初始化
_ui = if (conf.getBoolean("spark.ui.enabled", true)) {
        Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "",
          startTime))
}
//启动ui程式
_ui.foreach(_.bind())
//设定心跳检查点
heartbeatReceiver = env.rpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))
//创建task scheduler
val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
//实例化DAGScheduler
_dagScheduler = new DAGScheduler(this)
_taskScheduler = ts
_taskScheduler.start()
//从这里可以看出，上面的操作肯定跟yarn有关
_applicationId = _taskScheduler.applicationId()
```
下面开始分析createTaskScheduler部分，这里会根据调度模式FIFO或FAIR申请Task scheduler调度池
```
private def createTaskScheduler(
  sc: SparkContext,
  master: String,
  deployMode: String): (SchedulerBackend, TaskScheduler) = {
	import SparkMasterRegex._
	val MAX_LOCAL_TASK_FAILURES = 1

	master match {
	  //master等于Local走这里
	  case "local" =>
		......
	  //master等于包含local\[([0-9]+|\*)\]走这里
	  case LOCAL_N_REGEX(threads) =>
		......
	  //master等于包含local\[([0-9]+|\*)\s*,\s*([0-9]+)\]走这里

	  case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
		......
	  //master等于包含spark://(.*)走这里
	  case SPARK_REGEX(sparkUrl) =>
		......
	  //master等于包含local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]走这里
	  case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
	  	......
	  //master其他情况走这里，示例的yarn就是走这里
	  case masterUrl =>
	    val cm = getClusterManager(masterUrl) match {
	      case Some(clusterMgr) => clusterMgr
	      case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
	    }
	    try {
	      val scheduler = cm.createTaskScheduler(sc, masterUrl)
	      val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
	      cm.initialize(scheduler, backend)
	      (backend, scheduler)
	    } catch {
	      case se: SparkException => throw se
	      case NonFatal(e) =>
	        throw new SparkException("External scheduler cannot be instantiated", e)
	    }
	}
}
```
这里getClusterManager的作用是在众多ExternalClusterManager继承者中来根据传入的master方式选定实现类，实现类有YarnClusterManager、MesosClusterManager、KubernetesClusterManager，比如说示例中是yarn，那么其实现方式如下：
```
private def getClusterManager(url: String): Option[ExternalClusterManager] = {
    val loader = Utils.getContextOrSparkClassLoader
    //类加载器加载所有ExternalClusterManager的继承者，然后根据canCreate方法中的匹配来做筛选
    val serviceLoaders = ServiceLoader.load(classOf[ExternalClusterManager], loader).asScala.filter(_.canCreate(url))
    serviceLoaders.headOption
 }
override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    sc.deployMode match {
      case "cluster" => new YarnClusterScheduler(sc)
      //示例yarn client
      case "client" => new YarnScheduler(sc)
    }
}
override def createSchedulerBackend(sc: SparkContext,masterURL: String,scheduler: TaskScheduler): SchedulerBackend = {
	sc.deployMode match {
	  case "cluster" =>
	    new YarnClusterSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sc)
	  //示例yarn client
	  case "client" =>
	    new YarnClientSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sc)
	}
}
```
创建好TaskScheduler后，将实例化DAGScheduler对象并绑定当前上下文与TaskScheduler，且创建DAGScheduler内部线程池和事件处理器。而上面的_taskScheduler.start()方法实际上调用的是TaskSchedulerImpl的start方法，该方法内部如下：
```
override def start() {
	//这里实际上调用的是YarnClientSchedulerBackend的start方法，该方法中会启动一个yarn client来提交Application申请和Application上下文信息，返回appId，且将attemptId、appId赋值到_schedulerBackend内部属性
	//按照配置的休眠时间死循环监控Application任务在yarn中的申请运行状态，一旦它的运行状态在FINISHED、FAILED、KILLED、RUNNING中的一个则退出循环，继续下一步操作
	//建立异步Daemon线程来监控YARN application的运行状态
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }
```
而backend.start()内部很重要的一部分便是与yarn通信交互部分，具体来说就是在org.apache.spark.deploy.yarn.client的submitApplication()方法中的内容。
```
//org.apache.spark.deploy.yarn.client类
//关键代码
def submitApplication(): ApplicationId = {
    var appId: ApplicationId = null
    //启动并连接LauncherBackend socket
	launcherBackend.connect()
	//申明并初始化yarn client
	yarnClient.init(hadoopConf)
	//yarn client连接yarn
	yarnClient.start()

	//从ResourceManager中申请一个新的application
	val newApp = yarnClient.createApplication()
	val newAppResponse = newApp.getNewApplicationResponse()
	//获取application id
	appId = newAppResponse.getApplicationId()

	new CallerContext("CLIENT", sparkConf.get(APP_CALLER_CONTEXT),
	    Option(appId.toString)).setCurrentContext()

	// 验证集群是否有足够的资源来启动Applicationmaster
	verifyClusterResources(newAppResponse)

	//发起ResourceManager分配container去启动ApplicationMaster,并上传资源文件与依赖，返回AM container
	val containerContext = createContainerLaunchContext(newAppResponse)
	val appContext = createApplicationSubmissionContext(newApp, containerContext)

	// yarn提交application任务到container并持续监控
	yarnClient.submitApplication(appContext)
	launcherBackend.setAppId(appId.toString)
	// 标记application状态为submited
	reportLauncherState(SparkAppHandle.State.SUBMITTED)

	appId
}
```
当ApplicatMaster被创建后，ApplicatMaster会请求ResourceManager分配资源去启动Executor。很显然，启动Executor是在master上做的，这里具体是org.apache.spark.deploy.master.Master类的schedule方法，后再调用org.apache.spark.deploy.worker.Worker类的receive时间接收类做具体启动。
```
private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers

      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }
    startExecutorsOnWorkers()
  }
```
流程所涉及的主要模块与方法如下图：  
![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/restudy/3/pic/1.jpg)  
继续主流程走向走完，资源部分准备完成.
```
//SparkContext类，关键代码
//为上面所有ListenerBus中注册好监听器每一个都创建一个线程
setupAndStartListenerBus()
//貌似是分发队列事件
postEnvironmentUpdate()
//推送application start事件
postApplicationStart()
//钩子，用于停止释放资源
_shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
  logInfo("Invoking stop() from shutdown hook")
  try {
    stop()
  } catch {
    case e: Throwable =>
      logWarning("Ignoring Exception while stopping SparkContext from shutdown hook", e)
  }
}

```