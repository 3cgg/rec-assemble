/**
  * Created by J on 2018/1/8.
  */
package scala.me.libme.recsystem.ml

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions


case class Rating(userId: Int, itemId: Int, rating: Float, timestamp: Long)


object Als {


  def main(args: Array[String]): Unit = {

    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()


    import spark.implicits._

    val ratings = spark.read.textFile("D:\\java_\\spark-2.2.1-bin-hadoop2.7/data/mllib/als/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

    println("End")
  }

}



object AlsScript {


  def main(args: Array[String]): Unit = {

//    def parseRating(str: String): Rating = {
//      val fields = str.split("::")
//      assert(fields.size == 4)
//      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
//    }

    // the Spark Session may be created before anything
//    val spark = SparkSession
//      .builder()
//      .master("local")
//      .appName("Spark SQL basic example")
//      .getOrCreate()

    // first import data from file as testing , all data should be loaded from HBase later
//    val ratings = new FileRatingDataset()
//      .filePath("D:\\java_\\spark-2.2.1-bin-hadoop2.7/data/mllib/als/sample_movielens_ratings.txt")
//      .ratingDataset()

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




    val ratings=new HBaseDataset("userItem",sparkConf,hbaseConf) ratingDataset

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
//    val movieRecs = model.recommendForAllItems(10)

//    new FileWriter().write(userRecs)
    // output the data as case class
    val uis=new CaseClassOutput().write(userRecs)

    // persist case class for front use , replace by redis implementation later
//    new FilePersist().persist(JavaConversions.bufferAsJavaList(uis))

    new HBasePersist("userItemRatingTop",sparkConf,hbaseConf).persist(JavaConversions.bufferAsJavaList(uis))

//    new FileWriter().write(movieRecs)

    println("============================ ALL End!=================================")
  }

}
