重读Spark~CheckPoint
---------------------------------------
主要应用场景：对RDD做CheckPoint，切断做CheckPoint的RDD与父RDD的依赖关系，将RDD数据保存到分布式存储(如HDFS)以便数据恢复；Spark Streaming中使用CheckPoint用来保存DStreamGraph以及相关配置信息，以便在Driver崩溃后重启能够接着之前进度继续进行处理。  
主要来看看Spark RDD做CheckPoint的过程，SparkContext.runJob来提交任务，内部调用了doCheckpoint，可以看到下面包含了checkpoint操作。  
```
def runJob[T, U: ClassTag](
      rdd: RDD[T],func: (TaskContext, Iterator[T]) => U,partitions: Seq[Int],resultHandler: (Int, U) => Unit): Unit = {
    ......
    //RDD执行doCheckpoint操作
    rdd.doCheckpoint()
}
private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      ......
      //遍历各个依赖
      dependencies.foreach(_.rdd.doCheckpoint())
    }
}
final def dependencies: Seq[Dependency[_]] = {
    //根据配置将CheckPointRDD方式绑定到对应的Dependency中
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
}
```
首先RDD中checkpoint方法中只是新建了一个ReliableRDDCheckpintData对象，并没有做真正的写入工作。实际触发数据写入的是在runJob生成该RDD后，调用doCheckpoint方法来做的。RDDCheckpointData在Spark中的实现一共有两种：LocalRDDCheckpointData本地存储方式和ReliableRDDCheckpointData分布式高可用存储方式。这里一般会选择ReliableRDDCheckpointData方式来做CheckPoint，那么接着会调用其内部重写的doCheckpoint方法。  
```
protected override def doCheckpoint(): CheckpointRDD[T] = {
    //将RDD数据写入CheckPoint目录
    val newRDD = ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)
    //可选配置，引用超出范围时可否清除
    if (rdd.conf.getBoolean("spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }
    newRDD
}
```
![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/spark/article/restudy/6/pic/1.jpg)  
调用过程是RDD.doCheckpoint->RDDCheckpintData.checkpoint->ReliableRDDCheckpintData.doCheckpoint->ReliableRDDCheckpintData.writeRDDToCheckpointDirectory。在writeRDDToCheckpointDirectory方法中，RunJob任务将RDD中每个parition的数据依次写入到CheckPoint目录(writePartitionToCheckpointFile)，如果该RDD中的partitioner不为空，也会将该对象序列化后存储到checkpoint目录。  
因此，CheckPoint写入hdfs的数据主要包括：RDD每个parition的实际数据以及存在的partitioner对象(writePartitionerToCheckpointDir)。
做完CheckPoint操作之后，会调用RDD的markCheckpoined方法斩断该RDD对上游的依赖以及将paritions置空等操作。
```
def writeRDDToCheckpointDirectory[T: ClassTag](originalRDD: RDD[T],checkpointDir: String,blockSize: Int = -1): ReliableCheckpointRDD[T] = {
    //获取系统纳秒时间
    val checkpointStartTimeNs = System.nanoTime()

    val sc = originalRDD.sparkContext

    val checkpointDirPath = new Path(checkpointDir)
    //构建hdfs配置目录引用
    val fs = checkpointDirPath.getFileSystem(sc.hadoopConfiguration)
    //广播hdfs配置信息
    val broadcastedConf = sc.broadcast(
      new SerializableConfiguration(sc.hadoopConfiguration))
    //写入RDD数据到checkpoint目录
    sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)
    //检查partitioner是否存在
    if (originalRDD.partitioner.nonEmpty) {
      writePartitionerToCheckpointDir(sc, originalRDD.partitioner.get, checkpointDirPath)
    }
    //计算CheckPoint耗时
    val checkpointDurationMs =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - checkpointStartTimeNs)
    //返回操作好的新RDD对象
    val newRDD = new ReliableCheckpointRDD[T](
      sc, checkpointDirPath.toString, originalRDD.partitioner)
    
    newRDD
  }
```
读取CheckPoint数据是在DAGSchedulor提交任务之后，当TaskSchedulor分配好ShuffleMapTask与ResultTask，执行runTask方法，其内部开始执行rdd.iterator(partition, context)。
```
final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    //是否有缓存，有则从缓存状态读取
    if (storageLevel != StorageLevel.NONE) {
      getOrCompute(split, context)
    } else {
      //没有缓存则重新计算或读取checkpoint
      computeOrReadCheckpoint(split, context)
    }
}
private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    //是否做过CheckPoint处理，有则从CheckpointRDD读取
    if (isCheckpointedAndMaterialized) {
      firstParent[T].iterator(split, context)
    } else {
      //没有CheckPoint则重新计算
      compute(split, context)
    }
}
```
![2.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/spark/article/restudy/6/pic/1.jpg)  
完整的调用过程是：  
ShuffleTask/ResultTask.runTask->RDD.iterator->RDD.computeOrReadCheckpoint->CheckpointRDD.iterator->ReliableCheckpointRDD.iterator->ReliableCheckpointRDD.compute->ReliableCheckpointRDD.readCheckpointFile  
```
def readCheckpointFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableConfiguration],
      context: TaskContext): Iterator[T] = {
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    //从文件系统读去数据
    val fileInputStream = {
      val fileStream = fs.open(path, bufferSize)
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedInputStream(fileStream)
      } else {
        fileStream
      }
    }
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    context.addTaskCompletionListener[Unit](context => deserializeStream.close())
    //反序列化数据后转换成一个Iterator
    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }
```
