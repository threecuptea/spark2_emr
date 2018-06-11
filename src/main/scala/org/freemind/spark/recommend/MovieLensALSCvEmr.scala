package org.freemind.spark.recommend

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, explode}
import org.apache.spark.storage.StorageLevel

/**
  * CrossValidator is for ML tuning.  However, it needs some work to get the best parameter map used by the
  * CrossValidatorModel
  *
  * @author sling/ threecuptea rewrite, consolidate common methods into MovieLensCommon and clean-up 05/27/2018
  */
object MovieLensALSCvEmr {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: MovieLensALSCvEmr [s3-putput-path] ")
      System.exit(-1)
    }
    val outPath = args(0)

    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    import spark.implicits._

    val mlCommon = new MovieLensCommon(spark)

    val (mrDS, movieDS) = mlCommon.getMovieLensDataFrames()

    println(s"Rating Counts: ${mrDS.count}")
    mrDS.show(10, false)
    println(s"Movie Counts: ${movieDS.count}")
    movieDS.show(10, false)

    //Need to match field names of rating, KEY POINT is coldStartStrategy = "drop": drop lines with 'prediction' = 'NaN'
    val als = new ALS().setMaxIter(20).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setColdStartStrategy("drop")

    val bestModelFromCv = mlCommon.getBestCrossValidatorModel(als, mrDS)

    //Array[ParamMap] zip Array[Double], get bestParamMap so that I can refit with allDS Dataset, sorted requires no augument, sortBy requires one argument)
    //sortWith requires two arguments comparison, sortby reverse + (Ordering[Double].reverse), you can use _ in sortBy, sortWith(_._2 > _._2)
    val bestParamsFromCv = (bestModelFromCv.getEstimatorParamMaps zip bestModelFromCv.avgMetrics).minBy(_._2)._1
    println(s"The best model from CossValidator was trained with param = ${bestParamsFromCv}")

    val augModelFromCv = als.fit(mrDS, bestParamsFromCv)
    ///recommendation: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, recommendations: array<struct<movieId:int,rating:float>>]
    //We explode to flat array then retrieve field from a struct
    val recommendDS = augModelFromCv.recommendForAllUsers(25).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating")).persist(StorageLevel.MEMORY_ONLY_SER)

    val sUserId = 6001
    val sUserRecommendDS = recommendDS.filter($"userId" === sUserId)

    println(s"The top recommendation on AllUsers filter with  user=${sUserId} from ALS model and exclude rated movies")
    val sUserRatedRecommendDS = sUserRecommendDS.join(mrDS.select('userId, 'movieId),
      Seq("userId", "movieId"), "inner").select('userId, 'movieId, 'rating)
    sUserRecommendDS.except(sUserRatedRecommendDS).join(movieDS, 'movieId === 'id, "inner").
      select($"movieId", $"title", $"genres", $"userId", $"rating").sort(desc("rating")).show(false)

    recommendDS.write.option("header", true).parquet(s"${outPath}/recommendAll")  //Used as shared resource

    bestModelFromCv.save(s"${outPath}/cv-model") // It is using MLWrite

    val loadedCvModel = CrossValidatorModel.load(s"${outPath}/cv-model")
    assert(loadedCvModel != null)

    spark.stop()
  }

}
