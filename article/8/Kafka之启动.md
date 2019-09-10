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
　　 #设置ISR策略,发现ISR副本不同步即通知zk
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
}

def schedule(name: String, fun: ()=>Unit, delay: Long, period: Long, unit: TimeUnit) {
    debug("Scheduling task %s with initial delay %d ms and period %d ms."
        .format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
    this synchronized {
      ensureRunning
      val runnable = CoreUtils.runnable {
        try {
          trace("Beginning execution of scheduled task '%s'.".format(name))
          fun()
        } catch {
          case t: Throwable => error("Uncaught exception in scheduled task '" + name +"'", t)
        } finally {
          trace("Completed execution of scheduled task '%s'.".format(name))
        }
      }
      if(period >= 0)
        executor.scheduleAtFixedRate(runnable, delay, period, unit)
      else
        executor.schedule(runnable, delay, unit)
  }
}
```
