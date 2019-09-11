Kafka启动时会对ReplicaManager进行初始化，这里会在一个新的KafkaScheduler调度池中，并发循环调用两个ISR副本同步相关的函数:maybeShrinkIsr和maybePropagateIsrChanges
> 记录scala重要语法:Scala中方法和函数是两个不同的概念，方法无法作为参数进> 行传递，也无法赋值给变量，但是函数是可以的。在Scala中，利用下划线可以将> 方法转换成函数：
> //将println方法转换成函数，并赋值给p
> val p = println _

Kafka-0.9.0.0之前，Kafka支持两种副本同步列表检查方式:replica.lag.time.max.ms和replica.lag.max.messages，
