package scala.me.libme.recsystem.ml

import java.util

import me.libme.recsystem.ml.RatingPersist
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions

/**
  * Created by J on 2018/1/24.
  */
class HBasePersist(tableName:String, sparkConf: SparkConf, hbaseConf: Configuration) extends RatingPersist{


  override def persist(ratings: util.List[UserItemStruct]): Unit = {


    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val jobConf = new JobConf(hbaseConf,this.getClass)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(jobConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val puts=JavaConversions.collectionAsScalaIterable(ratings)
      .map(userItemStruct=>{
        val userId=userItemStruct.userId
                val put = new Put(Bytes.toBytes(String.valueOf(userId)))
                userItemStruct.itemRatings.foreach(userItemRating=>{
                  put.addColumn(Bytes.toBytes("item"),Bytes.toBytes(String.valueOf(userItemRating.itemId)),
                    Bytes.toBytes(String.valueOf(userItemRating.rating)))

                })
                (new ImmutableBytesWritable, put)
      })

    val seq=puts.toSeq
    spark.sparkContext.makeRDD(seq).saveAsNewAPIHadoopDataset(job.getConfiguration)

    println("==================================Persist HBase OK!===============================")

  }


}
