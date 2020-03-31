重读Spark~任务提交
---------------------------------------

Spark的算子基本分为两类：Transformation变换/转换算子、Action算子。Trasformtion算子主要有union、reduceByKey、groupBy、join、map、mapPartition、cogroup、parallelize、textFile、leftoutJoin、flatMap、coalesce、Repartition。Action算子主要有count、take、collect、foreach、foreachPartition、saveAsTextFile、ditinct、first、reduce。
其中，Transformation变换/转换算子并不触发提交作业，完成作业中间过程处理。Transformation操作是延迟计算的，也就是说从一个RDD转换生成另一个RDD的转换操作不是马上执行，需要等到有Action操作的时候才会真正触发运算，而Action算子则会触发SparkContext提交Job作业。
示例WordCount触发提交作业的便是collect算子，而collect方法又属于RDD类。RDD（Resilient Distributed Dataset）：弹性分布式数据集，Spark计算的基石，为用户屏蔽了底层对数据的复杂抽象和处理，为用户提供了一组方便的数据转换与求值方法。
```
def collect(): Array[T] = withScope {
	//内部调用SparkContext类的runJob方法
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
}
//实际调用
def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    //开始构造DAG图
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    //执行RDD checkpoint
    rdd.doCheckpoint()
}
```
DagScheduler实际上会执行submitJob方法来提交任务，该方法做了哪些事呢？首先它判断RDD分区是否正常，如果partitions下标值大于集合的长度或小于0，则直接抛出异常；如果partitions的个数等于0，则直接返回Job包含0个任务；提交JobSubmit事件；发送事件之后，DAGSchedulerEventProcessLoop事件总线会调用onReceive来接收相关事件。
```
private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      //接收事件后的具体执行
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)
}
private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      //划分stage
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      ......
    }
    //清理中间数据
    barrierJobIdToNumTasksCheckFailures.remove(jobId)

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    
    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    //提交Stage监听事件
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
  }
```
handleJobSubmitted方法主要做了三件事：划分Stage、清除中间数据、提交Stage事件，其中，我们熟知的划分宽窄依赖的过程也在该方法里。
```
private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    ......
    //创建stage依赖关系，调用getShuffleDependencies得到ResultStage
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
}
private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ArrayStack[RDD[_]]
    //将RDD集合加入栈
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      //从栈中取出
      val toVisit = waitingForVisit.pop()
      //如果没有访问过
      if (!visited(toVisit)) {
        //将该RDD放入已访问的列表
        visited += toVisit
        //遍历该RDD的依赖
        toVisit.dependencies.foreach {
          //如果该依赖属于shuffle依赖则将该RDD加入parents
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          //如果不属于ShuffleDependency，则将依赖压入栈
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }
```
我们知道Spark各个RDD间的依赖关系分为宽依赖ShuffleDependency和窄依赖NarrowDependency。getShuffleDependencies方法中，将RDD集合压入ArrayStack集合，该集合从名称上来看都是属于栈，而栈结构取出元素的原则是先入后出。那么最后一个操作算子构成的RDD成为第一个访问节点(每个算子在生成RDD定义的时候就已经分配了对应的宽窄RDD类型)，按照宽窄依赖判断是否分割成一个stage，最后将该stage返回。接下来会调用submitStage方法，划分之前没有分出来的stage，最后再提交所有的stage任务。
```
private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      //判断该stage状态，需要既不是等待状态，也不是运行和失败状态
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        //找到其他的stage并按照id排序
        val missing = getMissingParentStages(stage).sortBy(_.id)
        //假如该stage没有父依赖则直接提交
        if (missing.isEmpty) {
          submitMissingTasks(stage, jobId.get)
        } else {
        //否则遍历提交所有stage
          for (parent <- missing) {
            //递归调用，也就是说会从初始算子到最后的算子方向提交任务
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } 
}

```
接下来就到了任务提交的submitMissingTasks方法部分，下面是关键代码
```
//发出提交任务事件
listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
//生成Task任务列表
val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            stage.pendingPartitions += id
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId, stage.rdd.isBarrier())
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptNumber,
              taskBinary, part, locs, id, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier())
          }
      }
}
//taskScheduler提交task集合
taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties))
```
```
override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets = taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])

      stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true
      }
      stageTaskSets(taskSet.stageAttemptId) = manager
      //提交任务到schedule队列，发送到对应的executor
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
}
```