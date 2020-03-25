Kafka启动时会对ReplicaManager进行初始化，这里会在一个新的KafkaScheduler调度池中，并发循环调用两个ISR副本同步相关的函数:maybeShrinkIsr和maybePropagateIsrChanges
> 记录scala重要语法:Scala中方法和函数是两个不同的概念，方法无法作为参数进> 行传递，也无法赋值给变量，但是函数是可以的。在Scala中，利用下划线可以将> 方法转换成函数：
> //将println方法转换成函数，并赋值给p
> val p = println _

Kafka-0.9.0.0之前，Kafka支持两种副本同步列表检查方式:replica.lag.time.max.ms和replica.lag.max.messages。replica.lag.max.messages表示当同步副本与leader副本的数据量差额大于了该参数则可以认为该同步副本处于不同步状态，同理，replica.lag.time.max.ms则表示在该参数时间内同步副本没有从leader副本中fetch消息可以认为该同步副本处于不同步状态。当前最新版本的Kafka只支持replica.lag.time.max.ms这种方式。maybeShrinkIsr函数:  
```
def startup() {
    /*
    *isr-expiration:任务名
    *maybeShrinkIsr:每次调度执行的函数
    *period:下一次调度之前所等待的时长
    *unit:period的单位
    */
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    /*
    *同上
    */
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    //新建分区副本清理更新处理器
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
}
```
```
private def maybeShrinkIsr(): Unit = {
    //评估ISR列表中的分区副本是否该移除
    nonOfflinePartitionsIterator.foreach(_.maybeShrinkIsr(config.replicaLagTimeMaxMs))
}
//此方法传递的便是replica.lag.time.max.ms值
def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    //这里运用到了柯里化，def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.writeLock)(fun)　先后传入锁和要执行的函数，然后进行运算
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
    //过滤判断当前broker中有哪些leader副本，包含的就做处理，不包含就continue
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            //从同步列表中去除找出的疑似不同步的副本
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))
            // 更新ISR列表到zookeeper
            updateIsr(newInSyncReplicas)
            replicaManager.isrShrinkRate.mark()
            //判断是否需要更新leader副本的HW
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }
        case None => false // do nothing if no longer leader
      }
    }
   
    if (leaderHWIncremented)
      //尝试更新leader的HW位置
      tryCompleteDelayedRequests()
}
//找出疑似不同步的副本
def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
　　　　//找出除了leader的同步副本列表
    val candidateReplicas = inSyncReplicas - leaderReplica
    //找出当前时间(毫秒级别)与最近一次从leader副本fetch成功的时间差大于replica.lag.time.max.ms的同步副本
    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))
    laggingReplicas
}
//判断是否需要更新leader副本的HW
private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
　　　 //找出所有分区副本中与当前时间差小于replica.lag.time.max.ms和属于ISR列表中的副本
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)
    //去所有满足上述条件的副本的LEO的最小值作为新的HW
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark
    //当新HW大于旧leader的HW时或新HW的Segment的偏移量大于旧leader的Segment的偏移量，则标记为更新leader的HW
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      leaderReplica.highWatermark = newHighWatermark
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else  {
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark." +
        s"All LEOs are ${allLogEndOffsets.mkString(",")}")
      false
    }
}
```
maybePropagateIsrChanges函数，该函数的功能是广播ISR列表，包含两个条件：  

* 当ISR不为空，变更且没有广播
* 当前ISR列表在最新5s内没有发生变化，或自从上次广播之后与当前时间之差大于60s
```
def maybePropagateIsrChanges() {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
          //更新zk中ISR节点信息
        ReplicationUtils.propagateIsrChanges(zkUtils, isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }
