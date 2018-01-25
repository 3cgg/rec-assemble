package test.scala.me.libme.recsystem.ml

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.me.libme.recsystem.ml.{HBasePersist, UserItemRating, UserItemStruct}

/**
  * Created by J on 2018/1/25.
  */
object TestHBasePersist {


  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
      .set("spark.hadoop.validateOutputSpecs","false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val sc = new SparkContext(sparkConf)

    //hbase
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set("hbase.zookeeper.quorum","one.3cgg.rec")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    // the Spark Session may be created before anything
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()


    val ratings: util.List[UserItemStruct]=new util.ArrayList[UserItemStruct]()

    val userItemStruct:UserItemStruct=new UserItemStruct(1,ArrayBuffer(UserItemRating(1,2,2.5f,9099),UserItemRating(1,3,0.8f,9099)))
    ratings.add(userItemStruct)

    new HBasePersist("aaa",sparkConf,hbaseConf).persist(ratings)




  }


}
