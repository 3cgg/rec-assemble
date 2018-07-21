# rec-assemble
简单推荐系统应用，做了用户行为跟踪，master-slave架构，基于hbase数据存储,依赖下面多个模块：<br>
- [基础分布式集群环境__cluster__.git](https://github.com/3cgg/__cluster__.git)
- [HBase应用hb.git](https://github.com/3cgg/hb.git)
- [Zookeeper应用zk.git](https://github.com/3cgg/zk.git)
- [Netty应用_ny_.git](https://github.com/3cgg/_ny_.git)
- [Kafka应用kf.git](https://github.com/3cgg/kf.git)

请求经过spring mvc接收之后，经过简单过滤，直接发送到Kafka；另外一个分布式进程从Kafka中读取数据，发送到leader处理整型数值映射，
产生一个用户矩阵，发送到HBase存储。 Spark进程从HBase中拿到矩阵数据，通过简单的协同过滤算法算出相似度结果，数据推到Hbase或者Redis中供外部服务调用。

# config
- hbase:<br/>
    create 'u2itable','info'<br/>
    create 'userItem','item'<br/>

- kafka：<br/>
    topic -> rec-system<br/>
    ./kafka-console-consumer.sh --topic rec-system --zookeeper one.3cgg.rec:2181 --from-beginning
