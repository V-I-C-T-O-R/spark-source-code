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
如果上述情况下的session都不存在的话，那就先生成SparkContext。生成SparkContext的过程中，会实例化各种基础参数。例如：

* 基础配置对象_conf
* jar包对象_jars
* 文件对象_files
* 时间日志_eventLogDir
* SparkEnv
* 状态追踪_statusTracker
* SparkUI
* Hadoop配置
* Rpc心跳RpcEndpointRef
* ConsoleProgressBar
* DAGScheduler
* TaskScheduler
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
接下来便是生成RDD，这里的示例是使用定义好的日志和默认的分区数来实例化。parallelize过程中会遇见withScope函数，这里会用到scala的两个概念：柯里化概念；在scala中，当方法只有一个参数时， 后面可以用花括号代替圆括号。需要理解概念之后才知道具体的调用逻辑。
```
private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

def parallelize[T: ClassTag](
seq: Seq[T],
numSlices: Int = defaultParallelism): RDD[T] = withScope {
assertNotStopped()
new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
}
```
从源码上看，当走到代码段方法parallelize，mapToPair，reduceByKey的时候，并没有对数据立刻作出分布式数据集处理，而是通过上下文的不同Rdd类型来创建出依赖关系(也就是所谓的血统)。只有当做走到collect这种action动作的时候，真正的处理才开始执行。每种Rdd都会带有基础属性: 

* 分区列表，Partition List。这里的分区概念类似hadoop中的split切片概念，即数据的逻辑切片  
* 针对每个split(切片)的计算函数，即同一个RDD的每个切片的数据使用相同的计算函数  
* 对其他rdd的依赖列表  
* 可选，如果是（Key，Value）型的RDD，可以带分区类  
* 可选，首选块位置列表(hdfs block location)
```
def collect(): Array[T] = withScope {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
}
  
def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.length)
} 
```
此处的partitions方法才会去按照分区数划分并生成partition。
```
final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
        partitions_.zipWithIndex.foreach { case (partition, index) =>
          require(partition.index == index,
            s"partitions($index).partition == ${partition.index}, but it should equal $index")
        }
      }
      partitions_
   }
}
override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
}
  
 def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[T]]]
      case nr: NumericRange[_] =>
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices
      case _ =>
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toSeq
        }.toSeq
    }
}
```
接下来开始执行任务，分别启动dagScheduler.runJob,progressBar.foreach,rdd.doCheckpoint进程。
```
def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    ......省略......
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    rdd.doCheckpoint()
}
```
DagScheduler.runJob是用来对Rdd根据血统生成的Dag图进行任务切分别分发的过程；progressBar.foreach从现有stage状态中获取运行动态；rdd.doCheckpoint则对必要的数据集进行持久化保存，便于容错和恢复；
DagScheduler.runJob会向spark集群提交分布式任务，并异步获取线程运行的状态。内部创建新的jobId，发送JobSubmitted对象事件线程安全队列，并返回线程执行反馈的引用。同时DagScheduler本身实现了EventLoop的onReceive方法，用来针对不同的Event做处理。
```
private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, workerLost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }
```
最后提交整个解析出来的stage，并将task解析交给TaskScheduler执行。
