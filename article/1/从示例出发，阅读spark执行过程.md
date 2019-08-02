从示例出发，阅读spark执行过程
---------------------------------------
故事的开篇需要一段摸索，摸索过后就是一顿胖揍！没错，选了个examples的JavaLogQuery作为起点，希望有个好的开端-_-。
程序开头便是Driver程序初始化生成SparkSession，内部包含多个同步逻辑，判断当前ThreadLocal对象池中是否包含有存活的session，如果包含且session内部的sparkContext尚且没有关闭，则使用存在的session。
```
var session = activeThreadSession.get()
if ((session ne null) && !session.sparkContext.isStopped) {
options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
if (options.nonEmpty) {
  logWarning("Using an existing SparkSession; some configuration may not take effect.")
}
return session
}
```
如果不存在session则从默认的session中获取。假如获取的默认session存活，则将基础的config和自定义设置的config键值对遍历存到SessionState的settings对象(也就是HashMap的线程安全模式)，再返回session。
```
@transient protected[spark] val settings = java.util.Collections.synchronizedMap(new java.util.HashMap[String, String]())
```
如果上述情况下的session都不存在的话，那就先生成sparkContext。
```
val sparkContext = userSuppliedContext.getOrElse {
  val sparkConf = new SparkConf()
  options.foreach { case (k, v) => sparkConf.set(k, v) }

  // set a random app name if not given.
  if (!sparkConf.contains("spark.app.name")) {
    sparkConf.setAppName(java.util.UUID.randomUUID().toString)
  }

  SparkContext.getOrCreate(sparkConf)
  // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
}
```
检查extensions之后实例化出SparkSession，后续对它进行监听。
```
session = new SparkSession(sparkContext, None, None, extensions)
options.foreach { case (k, v) => session.initialSessionOptions.put(k, v) }
setDefaultSession(session)
setActiveSession(session)

// Register a successfully instantiated context to the singleton. This should be at the
// end of the class definition so that the singleton is updated only if there is no
// exception in the construction of the instance.
sparkContext.addSparkListener(new SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    defaultSession.set(null)
  }
})
```



