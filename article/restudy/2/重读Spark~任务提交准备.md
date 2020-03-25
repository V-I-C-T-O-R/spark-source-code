重读Spark~任务提交准备
---------------------------------------

成功调用spark-shell脚本之后，spark提交机制开始正式运行。启动Java虚拟机环境之后，类加载机制会去找SparkSubmit伴生对象的main方法来执行，而main方法中重写了SparkSubmit伴生类的日志输出方法以及相关的异常捕获，类似面向切面编程记录输出日志信息，之后再调用伴生类的doSubmit()方法执行下一步。doSubmit方法中首先会根据日志配置进行初始化Logging模块
```
private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    // 如果配置的日志类名等于org.slf4j.impl.Log4jLoggerFactory，则进行日志初始化
    if (Logging.isLog4j12()) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // 如果没有初始化，则加载默认配置文件
      if (!log4j12Initialized) {
        Logging.defaultSparkLog4jConfig = true
        val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
        ......
      }
      // 设置日志等级
      val rootLogger = LogManager.getRootLogger()
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel()
      }
      ......
    }
    Logging.initialized = true
    log
  }
```
加载配置完Log之后，对传递过来的命令参数进行解析映射，这里解析参数使用的是SparkSubmitArguments构造函数中的parse方法。parse方法将args中的键值对解析到SparkSubmitArguments的私有属性中，将额外的配置暂存在childArgs属性中。
```
override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      //赋值应用程序名称
      case NAME =>
        name = value
      //赋值master地址
      case MASTER =>
        master = value
      case CLASS =>
      //赋值主类
        mainClass = value
      //赋值部署模式
      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          error("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value
       ......
    }
    action != SparkSubmitAction.PRINT_VERSION
}

def doSubmit(args: Array[String]): Unit = {
    ......
    //appArgs.action从哪儿来？
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
      case SparkSubmitAction.PRINT_VERSION => printVersion()
    }
}
```
下面就到了提交指令的一个小拐点，这里有一个判断——当前提交的指令的意图。根据代码中的展示是通过appArgs.action枚举值来判断具体后续操作，那么appArgs.action是哪里设置的呢？是的，就在之前解析参数之后，在构造SparkSubmitArguments实例的时候在loadEnvironmentArguments方法中给该属性赋值。
```
private def loadEnvironmentArguments(): Unit = {
	......
    // 获取action方式
    action = Option(action).getOrElse(SUBMIT)
}
```
当然这里我们是提交任务，那么接下来就很关键了。先思考一下，当我们编写的程序编译打包好之后，我们需要指定哪些参数？没错，有--class、--master、
--deploy-mode、--driver-memory、--executor-memory等等。有没有想到什么呢？对的，我们以Clinet on Yarn任务提交方式为例，分以下执行流程  
![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/restudy/2/pic/1.jpg)  
- 客户端提交一个 Application, 在客户端启动一个Driver 进程
- 应用程序启动后会向RM(ResourceManager) 发送请求, 启动AM(ApplicationMaster)
- RM收到请求, 随机选择一台NM(NodeManager)启动AM
- AM启动后, 会向ResourceManager请求一批container资源, 用于启动Executor
- RM会找到一批NM返回给AM, 用于启动Executor
- AM会向NM发送命令启动Executor
- Executor启动后, 会反向注册给Driver，Driver发送任务到Executor, Executor最后将执行情况和结果返回给Driver端  

在上部分代码中，具体的submit action会交给submit方法来做，submit方法隐藏了构造提交信息的prepareSubmitEnvironment操作。prepareSubmitEnvironment方法中根据传入参数的不同会生成几个关键属性：childArgs, childClasspath, sparkConf, childMainClass。其中childMainClass属性代表接下来将要执行的主类，而不同的主类对应着不同运行的模式。
```
private[deploy] def prepareSubmitEnvironment(args: SparkSubmitArguments,conf: Option[HadoopConfiguration] = None): (Seq[String], Seq[String], SparkConf, String) = {
	......
	//根据传递的master参数选择clusterManager所属枚举值，这里是YARN，也就是1
	val clusterManager: Int = args.master match {
	      case "yarn" => YARN
	      case "yarn-client" | "yarn-cluster" =>
	        logWarning(s"Master ${args.master} is deprecated since 2.0." +
	          " Please use master \"yarn\" with specified deploy mode instead.")
	        YARN
	      ......
	//根据传递的deploy-mode参数选择deployMode所属枚举值，这里是CLIENT，也是1
	var deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      ......省略各种加载依赖文件的过程......

	//如果deployMode为Client，则childMainClass为参数列表中--class属性的值。
	if (deployMode == CLIENT) {
	  childMainClass = args.mainClass
	//如果deployMode为Cluster，则分情况设定。
	//假设模式是yarn-cluster，则childMainClass为org.apache.spark.deploy.yarn.YarnClusterApplication
	if (isYarnCluster) {
      childMainClass = YARN_CLUSTER_SUBMIT_CLASS
    //假设模式是standalone-cluster，则childMainClass为org.apache.spark.deploy.rest.RestSubmissionClientApp或org.apache.spark.deploy.ClientApp
	if (args.isStandaloneCluster){
       if (args.useRest) {
        childMainClass = REST_CLUSTER_SUBMIT_CLASS
        childArgs += (args.primaryResource, args.mainClass)
      } 
    else {
    	childMainClass = STANDALONE_CLUSTER_SUBMIT_CLASS
    }
    //假设模式是k8s-cluster，则childMainClass为org.apache.spark.deploy.k8s.submit.KubernetesClientApplication
    if (isKubernetesCluster) {
      childMainClass = KUBERNETES_CLUSTER_SUBMIT_CLASS
    ......
}
```
由于以Yarn-Client模式做示例，所以接下来详细介绍该模式下的执行流程。当解析出上面步骤中解析出来的必要参数后实际开始调用runMain方法。
```
private def runMain(
    childArgs: Seq[String],
    childClasspath: Seq[String],
    sparkConf: SparkConf,
    childMainClass: String,
    verbose: Boolean): Unit = {
  ......

  var mainClass: Class[_] = null
  ......
  //实例化指定的主类,如果mainClass继承于SparkApplication，则实例化
  val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
    mainClass.newInstance().asInstanceOf[SparkApplication]
  } else {
    ......
    //如果mainClass不继承于SparkApplication，则实例化JavaMainApplication类
    new JavaMainApplication(mainClass)
  }
  ......
  try {
    //开始执行对应实例化对象中的start方法
    app.start(childArgs.toArray, sparkConf)
  } catch {
    case t: Throwable =>
      throw findCause(t)
  }
}
```
这里我们一般调用的是对应Application类的start方法，进而调用自己编写的实现类中的Main方法，完成预加载。
```
override def start(args: Array[String], conf: SparkConf): Unit = {
    val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
    ......
    //反射机制唤醒main方法
    mainMethod.invoke(null, args)
}
```
走到这里，cluster模式的运行就比较明了了，即执行对应的YarnClusterApplication、RestSubmissionClientApp、ClientApp、KubernetesClientApplication中的main方法继续具体的操作。而示例Yarn-Client方式的具体提交在于用户自定义的任务类，具体说来就是在用户实例化SparkContext对象的途中，完成与Yarn集群的交互。  

总结：  
yarn-client和yarn-cluster的唯一区别在于，yarn-client的Driver运行在本地，而yarn-cluster的Driver则运行在ApplicationMaster所在的container里，Driver和AppMaster是同一个进程的两个不同线程，它们之间也会进行通信，ApplicationMaster同样等待Driver的完成，从而释放资源。  
在yarn-client模式里，优先运行的是Driver，然后在初始化SparkContext的时候，会作为client端向yarn申请ApplicationMaster资源。而在yarn-cluster模式里，本地进程则仅仅只是一个client，它会优先向yarn申请ApplicationMaster资源运行ApplicationMaster，在运行ApplicationMaster的时候通过反射启动Driver，在SparkContext初始化成功后，再向yarn注册自己并申请Executor资源。
