Spark依赖解析与提交
----------------------------
要理解Spark任务的提交过程，不可避免的需要了解Spark任务的划分与提交。SparkContext实例化工程中，会对DagSchuler和TaskSchuler进行初始化。在执行action算子的时候，会执行dagScheduler.runJob来对所有的DAG血统进行依赖切分宽窄依赖，从而划分到不同的Task中。
那故事就从上一节的dagScheduler.handleJobSubmitted讲起。DAGScheduler通过LinkedBlockingDeque队列获取任务后进行该处理，算是任务分发处理的开端，handleJobSubmitted则会实例化一个ResultStage对象作为结果输出。
```
private[scheduler] def handleJobSubmitted(jobId: Int,finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,partitions: Array[Int],
      callSite: CallSite,listener: JobListener,properties: Properties) {
    var finalStage: ResultStage = null
    try {
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
     ......省略.......
    }
    // Job submitted, clear internal data.
    barrierJobIdToNumTasksCheckFailures.remove(jobId)

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
　　　　......省略.......
    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
  }
```
handleJobSubmitted中会首选调用createResultStage方法。
```
private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
}
```
如上的方法中，对整个任务进行动态分配与自定义模式的冲突检查，对整个任务的FinalRdd的分区数进行检查，对任务的Rdd依赖屏障模式数量进行检查(与spark新特性中的barrier stage有关)。检查完之后，会调用getOrCreateParentStages方法来找出依赖的父Rdd，通过父子Rdd的对应关系生成ResultStage。
```
private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    getShuffleDependencies(rdd).map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
}
/**
 *此方法通过两个HashSet做遍历，循环递归遍历FinalRdd每一个依赖对应的Rdd。当遍历Rdd的依赖发现该依赖属于窄依赖，则把该ShuffleDependency添加到parents集合中；当遍历Rdd的依赖发现该依赖属于宽依赖，则将该依赖的Rdd添加到待访问列表；仅当待访问列表为空时终止遍历，返回parents集合。
 **/
private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ArrayStack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
}
/**
 *此方法从shuffleIdToMapStage集合中取jobid和对应的ShuffleMapStage，如果存在则直接返回该ShuffleMapStage，如果不存在，调用下面的方法进行操作。
 **/
private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage
      case None =>
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }
  /**
 *此方法用于找出祖父辈窄依赖，如果shuffleIdToMapStage不存在，即添加到该集合中返回。
 **/
 private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): ArrayStack[ShuffleDependency[_, _, _]] = {
    val ancestors = new ArrayStack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ArrayStack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.push(shuffleDep)
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
}
  /**
 *此方法针相当于对shuffleIdToMapStage、stageIdToStage进行初始化，实例化ShuffleMapStage并返回。
 **/
def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd)
    checkBarrierStageWithRDDChainPattern(rdd, rdd.getNumPartitions)
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)
    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
}
```
生成最后一个ResultStage对象之后，创建一个ActiveJob示例，提交Stage，提交阶段的重头戏重这里开始。
```
private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
    //检查stage是否处于等待列表、运行列表、失败列表
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        if (missing.isEmpty) {
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
}
```
submitStage内部包含两个主要操作：getMissingParentStages和submitMissingTasks，用于按照顺序提交stage，主要的流程如下：  
![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/2/pic/1.jpg)

getMissingParentStages根据stage的rdd依赖来进行处理，若是窄依赖则继续递归处理该Rdd；若是宽依赖则获取该shuffleMapStage,判断该stage的状态是否是ready，不是则添加到待提交列表。
```
private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
   
    val waitingForVisit = new ArrayStack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
}
```
判断上一步返回的missingList是否为空，空则执行submitMissingTasks操作。不为空则遍历missingList列表递归调用submitStage方法，且将当前stage加入waitingStages集合中。submitMissingTasks中核心的执行操作如下：  
```
private def submitMissingTasks(stage: Stage, jobId: Int) {
    ......省略.......
    //将stage添加到runningStages集合中
    runningStages += stage
    //TaskLocation对象中只有一个属性 host: String，taskIdToLocations记录计算partition所在的本地host信息
   val taskIdToLocations: Map[Int, Seq[TaskLocation]] = 
   stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
   //发送SparkListenerStageSubmitted时间到事件总线
   listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
   
   //task任务的具体生成细节，根据ShuffleMapTask和ResultTask两种task类型来生成对应的task list。
   RDDCheckpointData.synchronized {
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }

        partitions = stage.rdd.partitions
      }
      
      taskBinary = sc.broadcast(taskBinaryBytes)
   
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
    
    //检查当前task数量大于0，提交task任务
    if (tasks.size > 0) {
      logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) (first 15 " +
        s"tasks are for partitions ${tasks.take(15).map(_.partitionId)})")
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties))
}
```
submitTasks方法用于具体的任务提交(实际上是调用了RPC主键去发送task)
```
override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
        stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true
      }
　　stageTaskSets(taskSet.stageAttemptId) = manager
　　schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
     ........省略........
    
    backend.reviveOffers()
}
//RpcEndpointRef实现
override def reviveOffers() {
    localEndpoint.send(ReviveOffers)
  }
```