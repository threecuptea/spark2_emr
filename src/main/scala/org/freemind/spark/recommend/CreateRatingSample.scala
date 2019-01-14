package org.freemind.spark.recommend

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Try to  create Rating sample either using stratified sampling using 'userId' column or randomsplit so that I can work on small sample and
  * start writing FunSpec unit tests piece by piece before I deploy it to AWS to run integration test
  */
object CreateRatingSample {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("CreateRatingSample [rating-file-path]")
      System.exit(-1)
    }
    val ratingFilePath = args(0)
    val ratingSamplePath = ratingFilePath.substring(0,ratingFilePath.lastIndexOf('/'))
    val ratio: Double = if (args.length == 2) args(1).toDouble else 0.05

    println(s"path= ${ratingSamplePath}")

    val ratingsSchema = StructType(
      StructField("userId", IntegerType, true)::
      StructField("movieId", IntegerType, true)::
      StructField("rating", FloatType, true)::
      StructField("ts", LongType, true)::Nil
    )

    val spark = SparkSession.builder().config("spark.sql.shuffle.partitions", 8)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    import spark.implicits._

    val mrDF = spark.read.option("header", true).schema(ratingsSchema).csv(ratingFilePath).persist(StorageLevel.MEMORY_ONLY_SER)

    //Create a stratified sample
    val fractions  = mrDF.select('userId).distinct().rdd.map{
      case Row(key: Int) => key -> ratio
    }.collectAsMap().toMap

    val stratified = mrDF.stat.sampleBy("userId", fractions, 42l)

    stratified.write.option("header", true).option("compression", "gzip").mode("overwrite").csv(ratingSamplePath)
  }



}


