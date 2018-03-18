package scalalg.me.libme.rec

import java.util
import java.util.concurrent.atomic.AtomicReference

import me.libme.kernel._c.util.CliParams
import me.libme.module.kafka.ProducerConnector
import me.libme.module.zookeeper.ZooKeeperConnector

import scalalg.me.libme.cls.BasicClsRuntime
import scalalg.me.libme.module.hbase.HBaseConnector

/**
  * Created by J on 2018/1/28.
  */
 class RecRuntime private{

  val basicClsRuntime=new AtomicReference[BasicClsRuntime]

  val zookeeperExecutor=new AtomicReference[ZooKeeperConnector#ZookeeperExecutor]

  val hbaseExecutor=new AtomicReference[HBaseConnector#HBaseExecutor]

  val producerExecutor=new AtomicReference[ProducerConnector#ProducerExecutor[String,String]]

  private val cliParams=new AtomicReference[CliParams]


  def isCluster():Boolean={
    basicClsRuntime.get().isCluster()
  }

  def isHBase():Boolean={
    cliParams.get().getBoolean("--rec.hbase")
  }

  def isKafka():Boolean={
    basicClsRuntime.get().isKafka()
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

      //first we initialize basic cluster runtime environment...
      val basicClsRuntime=BasicClsRuntime.builder().args(args)
        .getOrCreate()


      val config: util.Map[String, AnyRef] = RecConfig.backend

      var cliParams: CliParams = new CliParams(args.toArray)
      import scala.collection.JavaConversions._
      for (entry <- config.entrySet) {
        if (!cliParams.contains(entry.getKey)) cliParams = cliParams.append(entry.getKey, entry.getValue)
      }

      val recRuntime=new RecRuntime
      recRuntime.cliParams.set(cliParams)
      recRuntime.basicClsRuntime.set(basicClsRuntime)

      //zookeeper
      recRuntime.zookeeperExecutor.set(basicClsRuntime.zookeeperExecutor.get())

      //kafka
      if(basicClsRuntime.isKafka()){
        recRuntime.producerExecutor.set(basicClsRuntime.producerExecutor.get())
      }

      //hbase
      if(recRuntime.isHBase()){
        val hbaseExecutor = RecUtil.hbase(cliParams)
        recRuntime.hbaseExecutor.set(hbaseExecutor)
      }

      defaultSession.set(recRuntime)

      return recRuntime
    }



  }

}