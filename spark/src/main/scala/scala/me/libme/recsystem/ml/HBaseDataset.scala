package scala.me.libme.recsystem.ml
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val rows=new java.util.ArrayList[Rating]

    val userItemRDD = spark.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    userItemRDD.map{ case (_,result) =>
      val key = Bytes.toInt(result.getRow)

      JavaConversions.mapAsScalaMap(result.getFamilyMap(Bytes.toBytes("userItem")))
            .map(entry=>{
//              Row(key,Bytes.toInt(entry._1),Bytes.toFloat(entry._2),System.currentTimeMillis())
              val rating=Rating(key,Bytes.toInt(entry._1),Bytes.toFloat(entry._2),System.currentTimeMillis());
              rows.add(rating)
            })
    }

//    val schema = StructType(Array(StructField("userId", DataTypes.IntegerType),
//      StructField("itemId", DataTypes.IntegerType),
//      StructField("rating", DataTypes.FloatType),
//      StructField("itemId", DataTypes.IntegerType),
//      StructField("itemId", DataTypes.LongType)
//    ))

    return sqlContext.createDataFrame(rows,classOf[Rating])



  }


}
