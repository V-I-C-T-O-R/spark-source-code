重读Spark~RDD
--------------------------------------- 

RDD(resilient distributed dataset，弹性分布式数据集)，是Spark框架的基本计算单元，是一个不可修改的分布式对象集合。每个RDD由多个分区组成，本身不存放数据，只存放数据的引用，操作 RDD 就像操作本地集合一样，而每个分区可以在不同的节点上做计算。RDD是只读的，只能由一个RDD转换到另一个RDD，可以通过Spark丰富的算子函数实现。  
RDD特点：
* 有一个分片列表。只有能够切分的数据才能并行计算。
* 有一个函数计算每一个分片。
* 对其他的RDD的可能有依赖列表，依赖还具体分为宽依赖和窄依赖，但并不是所有的RDD都有依赖。
* 可选：key-value型的RDD是根据哈希来分区的，类似于mapreduce当中的Paritioner接口，控制key分到哪个reduce。
* 可选：每一个分片的优先计算位置(preferred locations)，比如HDFS的block的所在位置应该是优先计算的位置。
##### 分区  
RDD逻辑上是分区的，是抽象的概念，在开始计算的时候会通过compute函数来得到对应每个分区的数据。如果RDD是通过文件系统中的文件构建出的，那么compute函数会读取指定文件系统路径中的数据；如果RDD是通过其他RDD转换过来的，则将其他RDD处理好的数据进行转换处理；
##### 依赖  
RDD通过算子操作进行转换数据，转换得到的新RDD包含了从其他RDD衍生过来的必要内容，RDD之间维护着血缘关系。
##### 缓存  
如果多次使用同一个RDD，则可以将其缓存起来。那么该RDD只有在第一次计算的时候才会根据血缘关系得到分区的数据，在后续步骤中会直接从缓存处取数据。
##### CheckPoint  
虽然RDD在数据丢失或任务失败时可以根据血缘关系进行回溯重建，但是对于一个长时间长流程的处理任务来说，随着时间的延长，RDD之间的血缘依赖关系也会越来越长。一旦出错，如果要从血缘关系进行恢复的话，系统性能肯定会受到影响。因此，RDD还支持CheckPoint功能将数据进行持久化保存，CheckPoint之后的RDD不再需要知道其父RDD的数据情况，当发生异常，可以直接从CheckPoint获取数据。

RDD主要有三种创建方式：从已经存在的集合创建、从文件系统创建、从已有的RDD转换。  
RDD算子主要分为两类：Transformation算子和Action算子。
- Transformation：根据RDD创建一个新的RDD，这类算子都是延迟加载的，它们不会直接执行计算，只是记住转换动作。只有要求返回结果给Driver时，这些转换才会真正运行。
- Action：对RDD计算后返回一个结果给Driver，会直接执行计算。

##### Transformation算子  
* map(f: T => U) 对每个元素执行函数计算，返回MappedRDD[U]。
* flatmap(f: T => TraversableOnce[U]) 首先对每个元素执行函数计算，然后将结果展平，返回FlatMappedRDD[U]。
* filter(f: T => Boolean) 保留函数计算结果为true的元素，返回FilteredRDD[T]。
* mapPartitions(Iterator[T] => Iterator[U]) 对每个分区执行函数计算，返回MapPartitionsRDD[U]。
* union(otherRdd[T]) 对源RDD和参数RDD求并集，返回UnionRDD[T]。
* distinct([numTasks]) 对源RDD进行去重后，返回新的RDD。numTasks表示并行任务的数量，默认为8。
* partitionBy() 对RDD进行分区操作，如果原有的partionRDD和现有的partionRDD数量一致就不进行分区，否则会生成ShuffledRDD。
* reduceByKey(f: (V, V) => V) 在(K,V)类型的RDD上调用，使用指定的reduce函数将key相同的value聚合到一起，返回ShuffledRDD[(K, V)]。同一台机器上的键值对在移动前会先组合，适合大数据复杂计算。
* groupByKey() 在(K,V)类型的RDD上调用，同样对每个key进行操作，但只生成一个sequence，返回ShuffledRDD[([K], Iterable[V])]。所有的键值对都会被移动，网络开销大，不适合大数据复杂计算。
* sortByKey([ascending], [numTasks]) 在(K,V)类型的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD。
* repartition(numPartitions) 将RDD数据重新混洗(reshuffle)并随机分布到新的分区中，使数据分布更均衡，新的分区个数取决于numPartitions。该算子总是需要通过网络混洗所有数据，底层是调用coalesce(numPartitions, shuffle = true)。

##### Action算子  
* reduce(func) 通过func函数聚合RDD中的所有元素，这个功能必须是可交换且可并联的。
* collect() 在Driver中，以数组的形式返回数据集的所有元素。
* count() 返回RDD的元素个数。
* saveAsTextFile(path) 将数据集中的元素以textfile的形式保存到指定的目录下，Spark会调用toString()方法转换每个元素。
* foreach(f: T => Unit) 为数据集的每一个元素调用func函数进行处理。
* foreachPartition(f: Iterator[T] => Unit) 为数据集的每一个分区调用func函数进行处理，可以用来处理数据库连接等操作。

子RDD与父RDD的血统依赖关系有两种：宽依赖和窄依赖。  
窄依赖(narrow dependency)指的是父RDD的每个Partition最多被子RDD的一个Partition使用，如map、filter、union等操作都会产生窄依赖。  
宽依赖(wide dependency)指的是子RDD的多个Partition依赖于父RDD的同一个Partition，如groupByKey、reduceByKey、sortByKey等操作都会产生宽依赖。  

DAG又称有向无环图，描述了多个RDD之间的转换关系，子RDD和父RDD之间存在引用依赖关系，最后的RDD会触发action操作——一个DAG构成一个Job任务。    
根据宽窄依赖划分DAG成不同的Stage，Spark划分Stage的整体思路是：各个RDD之间的依赖关系形成DAG（有向无环图），DAGScheduler对这些依赖关系形成的DAG进行Stage划分，从后往前回溯，遇到宽依赖就断开，划分为一个Stage，遇到窄依赖就将这个RDD加入该Stage中。完成了Stage的划分，DAGScheduler基于每个Stage生成TaskSet，并将TaskSet(一组Task)提交给TaskScheduler。TaskScheduler负责具体的任务调度，最后在Worker节点上启动Task。

##### RDD缓存方式  
RDD通过persist()方法或cache()方法可以缓存前面的计算结果，但并非这两个方法被调用时立即缓存，而要等触发后面的Action时才会将该RDD缓存在计算节点的内存中。其中缓存分为多个缓存级别，内存、磁盘或综合缓存。

##### CheckPoint
Checkpoint的产生就是为了相对而言更加可靠的持久化数据。Checkpoint可以指定把数据放在本地，并且是多副本的方式；在生产环境中可以把数据放在HDFS上，借助HDFS天然的高容错、高可靠的特征来实现可靠的持久化数据。CheckPoint 会斩断之前的血缘关系，CheckPoint 后的RDD不知道它的父RDD了，从CheckPoint处直接拿到数据。

RDD缓存和CheckPoint有区别：
* 存储位置不同：persist/cache 缓存的数据保存在BlockManager的内存或磁盘上，而CheckPoint的数据保存在HDFS上。
* 对血统的影响不同：persist/cache 缓存的RDD的不会丢掉血统，可以通过血缘关系重新计算。而CheckPoint执行完之后，已经将当前计算结果安全保存在HDFS上，会斩断血缘关系。
* 生命周期不同：persist/cache 缓存的RDD会在程序结束后被清除，Checkpoint保存的RDD在程序结束后会依然存在于HDFS。
