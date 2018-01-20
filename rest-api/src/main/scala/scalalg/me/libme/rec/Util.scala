package scalalg.me.libme.rec

import me.libme.kernel._c.util.CliParams
import me.libme.module.kafka.{KafkaProducerConfig, ProducerConnector}
import me.libme.module.zookeeper.{ZooKeeperConfig, ZooKeeperConnector}

import scala.collection.{JavaConversions, mutable}
import scalalg.me.libme.module.hbase.{HBaseCliParam, HBaseConfig, HBaseConnector}
import scalalg.me.libme.module.zookeeper.ZooKeeperCliParam

/**
  * Created by J on 2018/1/17.
  */
object Util {

  private[this] var zkExecutor:ZooKeeperConnector#ZookeeperExecutor=null


  def zookeeper(cliParams: CliParams*): ZooKeeperConnector#ZookeeperExecutor =synchronized{

    if(zkExecutor==null){
      val zooKeeperConfig = new ZooKeeperConfig

      zooKeeperConfig.setConnectString(ZooKeeperCliParam.connectString(cliParams(0)))
      zooKeeperConfig.setNamespace(ZooKeeperCliParam.namespace(cliParams(0)))

      zkExecutor=new ZooKeeperConnector(zooKeeperConfig).connect
    }
    return zkExecutor
  }

  private[this] var hbaseExecutor:HBaseConnector#HBaseExecutor=null


  def hbase(cliParams: CliParams*): HBaseConnector#HBaseExecutor =synchronized{

    if(hbaseExecutor==null){
      val hBaseConfig=new HBaseConfig
      hBaseConfig.connectString=HBaseCliParam.connectString(cliParams(0))

      hbaseExecutor=new HBaseConnector(hBaseConfig).connect()
    }
    return hbaseExecutor
  }


  private[this] var producerExecutor:ProducerConnector#ProducerExecutor[String,String]=null

  def producerExecutor(_cliParams: CliParams*): ProducerConnector#ProducerExecutor[String,String] =synchronized{

    if(producerExecutor==null){

      var cliParams:CliParams=_cliParams(0)
      val conf=KafkaProducerConfig.`def`();
      JavaConversions.mapAsScalaMap(conf)
        .foreach(entry=>{
          if(!cliParams.contains(entry._1)){
            cliParams=cliParams.append(entry._1,entry._2.toString)
          }
        })

      val kafkaProducerConfig=KafkaProducerConfig.build(cliParams.toMap)
      val producerConnecter = new ProducerConnector(kafkaProducerConfig)
      producerExecutor = producerConnecter.connect[String,String]()

    }
    return producerExecutor
  }

































}
