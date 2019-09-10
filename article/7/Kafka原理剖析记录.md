#### 1.控制器的产生
控制器本身也是一个broker，负责分区首领的选举。集群中第一个启动的broker通过zookeeper创建一个临时节点/controller让自己成为控制器。其他broker启动时也会去尝试创建这个节点，在收到“节点已存在”的异常后，创建Zookeeper Watch对象接收zookeeper节点的变更通知，确保集群只有一个控制器存在。
如果控制器被关闭或者与zookeeper断开连接，那么zookeeper上的临时节点就会消失。集群里的其他broker会收到控制器节点消失的通知，它们会主动尝试让自己成为控制器，第一个在zookeeper成功创建控制器节点的broker就会成为新的控制器。
#### 2.复制
首领副本，每个分区都有一个首领副本，所有的生产者请求和消费请求都会经过这里。跟随者副本，除开首领副本意外的副本都是跟随者副本，它唯一的任务就是从首领那里复制消息，保持和首领一直的状态。并随时准备在首领崩溃时，替换成新首领。持续请求得到最新消息的副本称为同步的副本，在首领失效时，只有同步的副本才能有可能被选为新首领。
首选首领，创建主题时选定的首领就是分区的首选首领，之所以叫做首选首领，是因为在创建分区时，需要在borker之间均衡首领。
#### 3.ISR
partition首领会维护一个与其基本同步的副本列表，每个partition都会有一个ISR，而且是首领动态维护。如果一个跟随者比一个首领落后太多或者超过一定时间未发起数据复制请求，则首领将其从ISR中移除。当选择ack=all时，只有ISR中的所有分区副本都向首领发送ack时，首领才会提交对请求消息的确认。

#### 4.kafka consumer rebalance
##### 方案一  
Kafka最开始通过Zookeeper的Watcher实现，每个Consumer Group在zookeeper下都维护了一个"/consumers/[group_id]/ids"路径，在此路径下使用临时节点记录属于此Consumer Group的消费者的id，由Consumer启动时创建。还有两个与ids节点同级的节点，分别是：owners,记录了分区与消费者的对应关系；offsets节点，记录了此Consumer Group在某个分区上的消费位置。
每个Broker、Topic以及分区在zookeeper中也都对应一个路径，如下所示：
> /brokers/ids/broker_id:记录了host、port以及分配在此Broker上的Topic的分区列表。</br>
> /brokers/topics/[topic_name]:记录了每个partition的leader、ISR等信息。</br>
> /brokers/topics/[topic_name]/partitions/[partition_num]/state:记录了当前leader、选举epoch等信息。</br>

路径图如下：  

![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/article/7/pic/1.jpg)
每个Consumer都分别在"/consumers/[group_id]/ids"和"/brokers/ids"路径上注册一个Watcher。当"/consumers/[group_id]/ids"路径的子节点发生变化时，便是Consumer group中的消费者出现了变化；当"/brokers/ids"路径的子节点发生变化时，表示Broker出现了增减。这样，通过Watcher，每个消费者就可以监控Consumer Group和Kafka集群的状态了。
这个严重依赖Zookeeper集群的方案，有两个比较严重的问题：羊群效应(一个被watch的zookeeper节点变化，导致大量的watcher通知需要被发送给客户端，导致通知期间其他操作延迟)和脑裂(每个Consumer都是通过zookeeper中保存的元数据判断consumer group状态、broker状态、rebalance结果的，由于zookeeper只保证最终一致性，不同consumer在同一时刻可能连接到zookeeper集群中不同的服务器，看到的元数据就可能不一样，就会造成不正确的rebalance尝试)。
##### 方案二
将全部的Consumer group分成多个子集，每个Consumer group子集在服务端对应一个GroupCoordinator对其进行管理，GroupCoordinator是KafkaServer中用于管理Consumer group的组件，消费者不再依赖zookeeper，而只有GroupCoordinator在zookeeper上添加watcher。消费者在加入或退出Consumer group时会修改zookeeper中保存的元数据，此时会触发GroupCoordinator设置的watcher，通知GroupCoordinator开始rebalance操作。这种方案虽然避免了羊群效应、脑裂问题，但是会显得很麻烦
##### 方案三
当消费者查找到管理当前Consumer group的GroupCoordinator后，就会进入join group阶段，Consumer首先向GroupCoordinator发送JoinGroupRequest请求，其中包含消费者的相关信息；服务端的GroupCoordinator收到JoinGroupRequest后会暂存消息，收集到全部消费者之后，根据JoinGroupRequest中的信息来确定Consumer group中可用的消费者，从中选择一个消费者成为Group leader，还会选择使用的分区分配策略，最后将这些信息封装成JoinGroupResponse返回给消费者。
虽然每个消费者都会收到JoinGroupResponse，但是只有group leader收到的JoinGroupResponse中封装了所有的消费者信息。当消费者确定自己是group leader后，会根据消费者的信息以及选定的分区分配策略进行分区分配。在Synchronizing Group State阶段，每个消费者都会发送SyncGroupRequest到GroupCoordinator，但是只有Group Leader的SyncGroupRequest请求包含了分区的分配结果，GroupCoordinator根据Group Leader的分区分配结果，行程SyncGroupResponse返回给所有的Consumer。消费者收到SyncGroupResponse后进行解析，即可获取分配给自身的分区。

Kafka的启动过程