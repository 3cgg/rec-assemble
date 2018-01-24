package scala.me.libme.recsystem.ml
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions

/**
  * Created by J on 2018/1/24.
  */
class HBaseDataset(tableName:String,sparkConf: SparkConf,hbaseConf: Configuration) extends RatingDataset{


  override def ratingDataset(): DataFrame = {

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    val jobConf = new JobConf(hbaseConf,this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val job = Job.getInstance(jobConf)

    val broadcastRows:Broadcast[java.util.ArrayList[Row]]=spark.sparkContext.broadcast(new java.util.ArrayList[Row])


    val userItemRDD = spark.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = userItemRDD.count()
    println("userItemRDD RDD Count:" + count)
    userItemRDD.cache()

    userItemRDD.foreach{ case (_,result) =>
      val key = Bytes.toString(result.getRow).toInt

      JavaConversions.mapAsScalaMap(result.getFamilyMap(Bytes.toBytes("item")))
            .foreach{case (cf,value)=>{
//              Row(key,Bytes.toInt(entry._1),Bytes.toFloat(entry._2),System.currentTimeMillis())
                val rating=Row(key,Bytes.toString(cf).toInt,
                Bytes.toString(value).toFloat, System.currentTimeMillis().toInt);
                broadcastRows.value.add(rating)
            }}
    }

    val schema = StructType(Array(StructField("userId", DataTypes.IntegerType),
      StructField("itemId", DataTypes.IntegerType),
      StructField("rating", DataTypes.FloatType),
      StructField("timestamp", DataTypes.IntegerType)
    ))

    return sqlContext.createDataFrame(broadcastRows.value,schema)



  }


}
