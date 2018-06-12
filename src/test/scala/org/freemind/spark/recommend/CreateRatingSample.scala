package org.freemind.spark.recommend

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.freemind.spark.SparkSessionTestWrapper

/**
  * Try to  create Rating sample either using stratified sampling using 'userId' column or randomsplit so that I can work on small sample and
  * start writing FunSpec unit tests piece by piece before I use AWS to run integration test
  */
object CreateRatingSample extends SparkSessionTestWrapper {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("CreatStratifiedRatingSample [rating-file-path]")
      System.exit(-1)
    }
    val ratingFilePath = args(0)
    val ratingSamplePath = ratingFilePath.substring(0,ratingFilePath.lastIndexOf('/'))

    println(s"path= ${ratingSamplePath}")

    val ratingsSchema = StructType(
      StructField("userId", IntegerType, nullable = true) ::
        StructField("movieId", IntegerType, nullable = true) ::
        StructField("rating", FloatType, nullable = true) ::
        StructField("timestamp", LongType, nullable = true) :: Nil
    )

    import spark.implicits._

    val mrDS = spark.read.option("header", true).schema(ratingsSchema).csv(ratingFilePath)
    println(s"count= ${mrDS.count}")

    val fraction = mrDS.select('userId).distinct().rdd.map {
      case Row(key: Int) => key -> 0.05
    }.collectAsMap().toMap

    val stratifiedMrDS = mrDS.stat.sampleBy("userId", fraction, 42L)
    val Array(otherDS, randomMrDS) = mrDS.randomSplit(Array(0.95, 0.05))

    stratifiedMrDS.write.option("header", true).option("compression", "gzip").csv(ratingSamplePath)
  }

  def getRatingFilePrefix(ratingPath: String): String = {
    val dotIndex = ratingPath.lastIndexOf('.')
    var ratingFilePrefix: Option[String] = None
    if (dotIndex > -1) {
      //It can be csv file or can be csv.gz file
      if (ratingPath.substring(dotIndex+1) == "gz" ) {
        val newIndex = ratingPath.substring(0, dotIndex).lastIndexOf('.')
        if (newIndex > -1) {
          ratingFilePrefix = Some(ratingPath.substring(0, newIndex))
        }
      }
      else {
        ratingFilePrefix = Some(ratingPath.substring(0, dotIndex))
      }
    }
    ratingFilePrefix match {
      case None =>
        throw new IllegalArgumentException("Invalid rating file path")
      case Some(value) =>
        value
    }
  }

}


