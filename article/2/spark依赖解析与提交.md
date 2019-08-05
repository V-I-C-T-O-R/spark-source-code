Spark依赖解析与提交
----------------------------
要理解Spark任务的提交过程，不可避免的需要了解Spark任务的划分与提交。SparkContext实例化工程中，会对DagSchuler和TaskSchuler进行初始化。在执行action算子的时候，会执行dagScheduler.runJob来对所有的DAG血统进行依赖切分宽窄依赖，从而划分到不同的Task中。
那故事就从上一节的dagScheduler.handleJobSubmitted讲起。DAGScheduler通过LinkedBlockingDeque队列获取任务后进行该处理，算是任务分发处理的开端，handleJobSubmitted则会实例化一个ResultStage对象作为结果输出。
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
如上，