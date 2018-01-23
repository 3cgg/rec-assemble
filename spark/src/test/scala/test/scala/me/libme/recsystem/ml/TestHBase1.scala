package test.scala.me.libme.recsystem.ml

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by J on 2018/1/22.
  */
object TestHBase1 {


  def main(args: Array[String]): Unit = {

    val tablename = "account"

    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
      .set("spark.hadoop.validateOutputSpecs","false")
    val sc = new SparkContext(sparkConf)

    //hbase
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum","one.3cgg.rec")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val jobConf = new JobConf(hbaseConf,this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    jobConf.set(TableInputFormat.INPUT_TABLE, tablename)

    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))
    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0).toInt))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("cf"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toInt))
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)




    val usersRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    val count = usersRDD.count()
    println("Users RDD Count:" + count)
    usersRDD.cache()
    //遍历输出
    usersRDD.foreach{ case (_,result) =>
      val key = Bytes.toInt(result.getRow)
      val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
      val age = Bytes.toInt(result.getValue("cf".getBytes,"age".getBytes))
      println("Row key:"+key+" Name:"+name+" Age:"+age)
    }














  }

}
