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

  def isHBase():Boolean={
    cliParams.get().getBoolean("--rec.hbase")
  }

  def isKafka():Boolean={
    cliParams.get().getBoolean("--rec.kafka")
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

  def allParam():util.Map[String,Object]={
    util.Collections.unmodifiableMap(cliParams.get().toMap)
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

      val recRuntime=new RecRuntime
      recRuntime.cliParams.set(cliParams)

      //zookeeper
      val zookeeperExecutor = Util.zookeeper(cliParams)
      recRuntime.zookeeperExecutor.set(zookeeperExecutor)

      //kafka
      if(recRuntime.isKafka()){
        val producerExecutor = Util.producerExecutor(cliParams)
        recRuntime.producerExecutor.set(producerExecutor)
      }

      //hbase
      if(recRuntime.isHBase()){
        val hbaseExecutor = Util.hbase(cliParams)
        recRuntime.hbaseExecutor.set(hbaseExecutor)
      }

      defaultSession.set(recRuntime)

      return recRuntime
    }



  }

}