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
object RecUtil {


  private[this] var hbaseExecutor:HBaseConnector#HBaseExecutor=null


  def hbase(cliParams: CliParams*): HBaseConnector#HBaseExecutor =synchronized{

    if(hbaseExecutor==null){
      val hBaseConfig=new HBaseConfig
      hBaseConfig.connectString=HBaseCliParam.connectString(cliParams(0))

      hbaseExecutor=new HBaseConnector(hBaseConfig).connect()
    }
    return hbaseExecutor
  }


}
