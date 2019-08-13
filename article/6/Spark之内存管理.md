MemoryManager，管理Spark在JVM中的总体内存使用情况。其有两种实现：StaticMemoryManager和UnifiedMemoryManager。  
TaskMemoryManager管理由各个任务分配的内存。

MemoryConsumers是TaskMemoryManager的客户端，对应于任务中的各个运算符和数据结构。TaskMemoryManager接收来自MemoryConsumers的内存分配请求，并向消费者发出回调，以便在内存不足时触发溢出。

MemoryPools是MemoryManager用来跟踪存储和执行之间内存划分的抽象。

源代码中的代码流程图：
![1.png](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/6/pic/1.png)

内存模式：主要分堆内内存和堆外内存，MemoryMode是一个枚举类，从本质上来说，ON_HEAP和OFF_HEAP都是MemoryMode的子类
下面的一张图，来说明一下Spark内存结构：  
系统内存：可以根据 spark.testing.memory 参数来配置（主要用于测试），默认是JVM 的可以使用的最大内存。

保留内存：可以根据 spark.testing.reservedMemory 参数来配置（主要用于测试）， 默认是 300M

最小系统内存：保留内存 * 1.5 后，再向下取整

系统内存的约束：系统内存必须大于最小保留内存，即 系统可用内存必须大于 450M， 可以通过 --driver-memory 或  spark.driver.memory 或 --executor-memory 或spark.executor.memory 来调节

可用内存 = 系统内存 - 保留内存

堆内内存占比默认是0.6， 可以根据 spark.memory.fraction 参数来调节

最大堆内内存 = 堆内可用内存 * 堆内内存占比

堆内内存存储池占比默认是 0.5 ，可以根据spark.memory.storageFraction 来调节。

默认堆内存储内存大小 = 最大堆内内存 * 堆内内存存储池占比。即堆内存储池内存大小默认是 （系统JVM最大可用内存 -  300M）* 0.6 * 0.5， 即约等于JVM最大可用内存的三分之一。

![2.png](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/6/pic/2.png)
图片来源[0x0fff.com](https://0x0fff.com/spark-memory-management/)  
文字来源[johnny666888](https://www.cnblogs.com/johnny666888/p/11197519.html)，很细，值得一看
