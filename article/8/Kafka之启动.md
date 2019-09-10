Kafka Server启动过程  
1.启动KafkaScheduler
```
override def startup() {
    debug("Initializing task scheduler.")
    this synchronized {
      if(isStarted)
        throw new IllegalStateException("This scheduler has already been started!")
      #申明调度器线程池，设置关闭时的清理策略
      executor = new ScheduledThreadPoolExecutor(threads)
      executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
      executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      executor.setThreadFactory(new ThreadFactory() {
                                  def newThread(runnable: Runnable): Thread = 
                                    new KafkaThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon)
                                })
    }
}
```
2.初始化zookeeper连接配置，确定配置的zk节点存在，没有就创建，生成clusterId并创建对应的zk节点
3.产生当前节点的brokerId与数据存放目录，创建metrics收集器
4.启动SocketServer
```
def startup() {
    this.synchronized {
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      val sendBufferSize = config.socketSendBufferBytes
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId
      var processorBeginIndex = 0
      config.listeners.foreach { endpoint =>
        val listenerName = endpoint.listenerName
        val securityProtocol = endpoint.securityProtocol
        val processorEndIndex = processorBeginIndex + numProcessorThreads
        #定义numProcessorThreads个处理器来处理网络请求
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, listenerName, securityProtocol, memoryPool)
        #使用Reactor模式创建一个Acceptor来接收所有的网络请求
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        #启动监听线程
        KafkaThread.nonDaemon(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor).start()
        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        private val ioWaitRatioMetricNames = processors.map { p =>
          metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
        }

        def value = ioWaitRatioMetricNames.map { metricName =>
          Option(metrics.metric(metricName)).fold(0.0)(_.value)
        }.sum / totalProcessorThreads
      }
    )
    newGauge("MemoryPoolAvailable",
      new Gauge[Long] {
        def value = memoryPool.availableMemory()
      }
    )
    newGauge("MemoryPoolUsed",
      new Gauge[Long] {
        def value = memoryPool.size() - memoryPool.availableMemory()
      }
    )
    info("Started " + acceptors.size + " acceptor threads")
  }
```
5.创建并启动副本管理器replicaManager
```
def startup() {
　　 #设置ISR策略,定时扫描在线副本，发现同步副本延时超过指定时间即更新ISR列表
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    #设置ISR策略,定时发现ISR副本不同步即通知zk
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
}

def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }
```
6.创建并启动KafkaController，用于进程内事件传递与通知
```
override def process(): Unit = {
      #订阅zk上的节点变化
      registerSessionExpirationListener()
      registerControllerChangeListener()
      elect()
}

def onControllerFailover() {
    info("Starting become controller state transition")
    readControllerEpochFromZookeeper()
    incrementControllerEpoch()
    LogDirUtils.deleteLogDirEvents(zkUtils)

    #监听各种状态变化
    registerPartitionReassignmentListener()
    registerIsrChangeNotificationListener()
    registerPreferredReplicaElectionListener()
    registerTopicChangeListener()
    registerTopicDeletionListener()
    registerBrokerChangeListener()
    registerLogDirEventNotificationListener()

    initializeControllerContext()
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    replicaStateMachine.startup()
    partitionStateMachine.startup()

    // register the partition change listeners for all existing topics on failover
    controllerContext.allTopics.foreach(topic => registerPartitionModificationsListener(topic))
    info(s"Ready to serve as the new controller with epoch $epoch")
    maybeTriggerPartitionReassignment()
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onPreferredReplicaElection(pendingPreferredReplicaElections)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }
  }
```
7.