package scalalg.me.libme.rec

import java.util
import java.util.concurrent.atomic.AtomicReference

import me.libme.kernel._c.util.CliParams
import me.libme.module.kafka.ProducerConnector
import me.libme.module.zookeeper.ZooKeeperConnector

import scalalg.me.libme.module.hbase.HBaseConnector

/**
  * Created by J on 2018/1/28.
  */
 class RecRuntime private{


  val zookeeperExecutor=new AtomicReference[ZooKeeperConnector#ZookeeperExecutor]

  val hbaseExecutor=new AtomicReference[HBaseConnector#HBaseExecutor]

  val producerExecutor=new AtomicReference[ProducerConnector#ProducerExecutor[String,String]]

  private val cliParams=new AtomicReference[CliParams]

  def isCluster():Boolean={
    cliParams.get().getBoolean("--rec.cluster")
  }

  def serverPort():Int={
    cliParams.get().getInt("--rec.netty.port")
  }

  def u2iTable():String={
    cliParams.get().getString("--rec.u2i.hbase.table")
  }

  def u2iFamily():String={
    cliParams.get().getString("--rec.u2i.hbase.table.family")
  }

  def u2iColumn():String={
    cliParams.get().getString("--rec.u2i.hbase.table.column")
  }


}
object RecRuntime{


  private val defaultSession = new AtomicReference[RecRuntime]



  def builder():Builder=new Builder

  class Builder{

    private[this] var args:Seq[String]=_

    def args(args:Seq[String]):Builder={
      this.args=args;
      this
    }

    def args(args:Array[String]):Builder={
      this.args=args;
      this
    }

    def getOrCreate():RecRuntime= synchronized {

      if(defaultSession.get()!=null){
        return defaultSession.get();
      }


      val config: util.Map[String, AnyRef] = RecConfig.backend

      var cliParams: CliParams = new CliParams(args.toArray)
      import scala.collection.JavaConversions._
      for (entry <- config.entrySet) {
        if (!cliParams.contains(entry.getKey)) cliParams = cliParams.append(entry.getKey, entry.getValue)
      }

      //kafka
      val producerExecutor = Util.producerExecutor(cliParams)

      //zookeeper
      val zookeeperExecutor = Util.zookeeper(cliParams)

      //hbase
      val hbaseExecutor = Util.hbase(cliParams)

      val recRuntime=new RecRuntime

      recRuntime.zookeeperExecutor.set(zookeeperExecutor)
      recRuntime.hbaseExecutor.set(hbaseExecutor)
      recRuntime.producerExecutor.set(producerExecutor)

      recRuntime.cliParams.set(cliParams)
      defaultSession.set(recRuntime)

      return recRuntime
    }



  }

}