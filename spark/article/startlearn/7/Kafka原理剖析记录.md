#### 1.控制器的产生
控制器本身也是一个broker，负责分区首领的选举。集群中第一个启动的broker通过zookeeper创建一个临时节点/controller让自己成为控制器。其他broker启动时也会去尝试创建这个节点，在收到“节点已存在”的异常后，创建Zookeeper Watch对象接收zookeeper节点的变更通知，确保集群只有一个控制器存在。
如果控制器被关闭或者与zookeeper断开连接，那么zookeeper上的临时节点就会消失。集群里的其他broker会收到控制器节点消失的通知，它们会主动尝试让自己成为控制器，第一个在zookeeper成功创建控制器节点的broker就会成为新的控制器。
#### 2.复制
首领副本，每个分区都有一个首领副本，所有的生产者请求和消费请求都会经过这里。跟随者副本，除开首领副本意外的副本都是跟随者副本，它唯一的任务就是从首领那里复制消息，保持和首领一直的状态。并随时准备在首领崩溃时，替换成新首领。持续请求得到最新消息的副本称为同步的副本，在首领失效时，只有同步的副本才能有可能被选为新首领。
首选首领，创建主题时选定的首领就是分区的首选首领，之所以叫做首选首领，是因为在创建分区时，需要在borker之间均衡首领。
#### 3.ISR
partition首领会维护一个与其基本同步的副本列表，每个partition都会有一个ISR，而且是首领动态维护。如果一个跟随者比一个首领落后太多或者超过一定时间未发起数据复制请求，则首领将其从ISR中移除。当选择ack=all时，只有ISR中的所有分区副本都向首领发送ack时，首领才会提交对请求消息的确认。
#### 4.Leader Election算法
Kafka所使用的Leader Election算法更像微软的PacificA算法，如何选举Leader
最简单最直观的方案是，所有Follower都在Zookeeper上设置一个Watch，一旦Leader宕机，其对应的ephemeral znode(暂时zookeeper节点)会自动删除，此时所有Follower都尝试创建该节点，而创建成功者（Zookeeper保证只有一个能创建成功）即是新的Leader，其它Replica即为Follower。
Leader宕机--->删除ephemeral znode--->Follower竞争创建此节点
但是该方法会有3个问题：
split-brain 这是由Zookeeper的特性引起的，虽然Zookeeper能保证所有Watch按顺序触发，但并不能保证同一时刻所有Replica“看”到的状态是一样的，这就可能造成不同Replica的响应不一致
herd effect 如果宕机的那个Broker上的Partition比较多，会造成多个Watch被触发，造成集群内大量的调整
Zookeeper负载过重 每个Replica都要为此在Zookeeper上注册一个Watch，当集群规模增加到几千个Partition时Zookeeper负载会过重。
Kafka 0.8.*的Leader Election方案解决了上述问题，它在所有broker中选出一个controller，所有Partition的Leader选举都由controller决定。controller会将Leader的改变直接通过RPC的方式（比Zookeeper Queue的方式更高效）通知需为此作出响应的Broker。同时controller也负责增删Topic以及Replica的重新分配。

#### ５.kafka consumer rebalance
##### 方案一  
Kafka最开始通过Zookeeper的Watcher实现，每个Consumer Group在zookeeper下都维护了一个"/consumers/[group_id]/ids"路径，在此路径下使用临时节点记录属于此Consumer Group的消费者的id，由Consumer启动时创建。还有两个与ids节点同级的节点，分别是：owners,记录了分区与消费者的对应关系；offsets节点，记录了此Consumer Group在某个分区上的消费位置。
每个Broker、Topic以及分区在zookeeper中也都对应一个路径，如下所示：
> /brokers/ids/broker_id:记录了host、port以及分配在此Broker上的Topic的分区列表。</br>
> /brokers/topics/[topic_name]:记录了每个partition的leader、ISR等信息。</br>
> /brokers/topics/[topic_name]/partitions/[partition_num]/state:记录了当前leader、选举epoch等信息。</br>

路径图如下：  

![1.jpg](https://github.com/V-I-C-T-O-R/spark-source-code/blob/master/spark/article/startlearn/7/pic/1.jpg)
每个Consumer都分别在"/consumers/[group_id]/ids"和"/brokers/ids"路径上注册一个Watcher。当"/consumers/[group_id]/ids"路径的子节点发生变化时，便是Consumer group中的消费者出现了变化；当"/brokers/ids"路径的子节点发生变化时，表示Broker出现了增减。这样，通过Watcher，每个消费者就可以监控Consumer Group和Kafka集群的状态了。
这个严重依赖Zookeeper集群的方案，有两个比较严重的问题：羊群效应(一个被watch的zookeeper节点变化，导致大量的watcher通知需要被发送给客户端，导致通知期间其他操作延迟)和脑裂(每个Consumer都是通过zookeeper中保存的元数据判断consumer group状态、broker状态、rebalance结果的，由于zookeeper只保证最终一致性，不同consumer在同一时刻可能连接到zookeeper集群中不同的服务器，看到的元数据就可能不一样，就会造成不正确的rebalance尝试)。
##### 方案二
将全部的Consumer group分成多个子集，每个Consumer group子集在服务端对应一个GroupCoordinator对其进行管理，GroupCoordinator是KafkaServer中用于管理Consumer group的组件，消费者不再依赖zookeeper，而只有GroupCoordinator在zookeeper上添加watcher。消费者在加入或退出Consumer group时会修改zookeeper中保存的元数据，此时会触发GroupCoordinator设置的watcher，通知GroupCoordinator开始rebalance操作。这种方案虽然避免了羊群效应、脑裂问题，但是会显得很麻烦
##### 方案三
当消费者查找到管理当前Consumer group的GroupCoordinator后，就会进入join group阶段，Consumer首先向GroupCoordinator发送JoinGroupRequest请求，其中包含消费者的相关信息；服务端的GroupCoordinator收到JoinGroupRequest后会暂存消息，收集到全部消费者之后，根据JoinGroupRequest中的信息来确定Consumer group中可用的消费者，从中选择一个消费者成为Group leader，还会选择使用的分区分配策略，最后将这些信息封装成JoinGroupResponse返回给消费者。
虽然每个消费者都会收到JoinGroupResponse，但是只有group leader收到的JoinGroupResponse中封装了所有的消费者信息。当消费者确定自己是group leader后，会根据消费者的信息以及选定的分区分配策略进行分区分配。在Synchronizing Group State阶段，每个消费者都会发送SyncGroupRequest到GroupCoordinator，但是只有Group Leader的SyncGroupRequest请求包含了分区的分配结果，GroupCoordinator根据Group Leader的分区分配结果，行程SyncGroupResponse返回给所有的Consumer。消费者收到SyncGroupResponse后进行解析，即可获取分配给自身的分区。

Consumer Rebalance的算法如下：
* 将目标Topic下的所有Partirtion排序，存于PTPT
* 对某Consumer Group下所有Consumer排序，存于CG于CG，第ii个Consumer记为CiCi
* N=size(PT)/size(CG)N=size(PT)/size(CG)，向上取整
* 解除CiCi对原来分配的Partition的消费权（i从0开始）
* 将第i∗Ni∗N到（i+1）∗N−1（i+1）∗N−1个Partition分配给CiCi

目前，最新版（0.8.2.1）Kafka的Consumer Rebalance的控制策略是由每一个Consumer通过在Zookeeper上注册Watch完成的。每个Consumer被创建时会触发Consumer Group的Rebalance，具体启动流程如下：
* High Level Consumer启动时将其ID注册到其Consumer Group下，在Zookeeper上的路径为/consumers/[consumer group]/ids/[consumer id]
* 在/consumers/[consumer group]/ids上注册Watch
* 在/brokers/ids上注册Watch
* 如果Consumer通过Topic Filter创建消息流，则它会同时在/brokers/topics上也创建Watch
* 强制自己在其Consumer Group内启动Rebalance流程
在这种策略下，每一个Consumer或者Broker的增加或者减少都会触发Consumer Rebalance。因为每个Consumer只负责调整自己所消费的Partition，为了保证整个Consumer Group的一致性，当一个Consumer触发了Rebalance时，该Consumer Group内的其它所有其它Consumer也应该同时触发Rebalance。

该方式有如下缺陷：

* Herd effect
   任何Broker或者Consumer的增减都会触发所有的Consumer的Rebalance
   
* Split Brain
每个Consumer分别单独通过Zookeeper判断哪些Broker和Consumer 宕机了，那么不同Consumer在同一时刻从Zookeeper“看”到的View就可能不一样，这是由Zookeeper的特性决定的，这就会造成不正确的Reblance尝试。

* 调整结果不可控
所有的Consumer都并不知道其它Consumer的Rebalance是否成功，这可能会导致Kafka工作在一个不正确的状态。

#### Low Level Consumer
使用Low Level Consumer (Simple Consumer)的主要原因是，用户希望比Consumer Group更好的控制数据的消费。比如：

* 同一条消息读多次
* 只读取某个Topic的部分Partition
* 管理事务，从而确保每条消息被处理一次，且仅被处理一次
与Consumer Group相比，Low Level Consumer要求用户做大量的额外工作。

* 必须在应用程序中跟踪offset，从而确定下一条应该消费哪条消息
* 应用程序需要通过程序获知每个Partition的Leader是谁
* 必须处理Leader的变化

使用Low Level Consumer的一般流程如下

* 查找到一个“活着”的Broker，并且找出每个Partition的Leader
* 找出每个Partition的Follower
* 定义好请求，该请求应该能描述应用程序需要哪些数据
* Fetch数据
* 识别Leader的变化，并对之作出必要的响应

Consumer重新设计
设计方向
1 简化消费者客户端
2 中心Coordinator
3 允许手工管理offset
4 Rebalance后触发用户指定的回调
5 非阻塞式Consumer API :Consumer  Consumer状态机
6 故障检测机制  Coordinator  Coordinator状态机  Coordinator Failover

