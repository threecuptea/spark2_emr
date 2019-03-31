package org.freemind.spark.recommend

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  *
  * There are a couple of new finding:
  *
  * 1. spark 2.3.0 optimize join and turn off 'spark.sql.crossJoin'.  I will get
  * "org.apache.spark.sql.AnalysisException: Detected cartesian product for LEFT outer join between logical plans...
  *  Join condition is missing or trivial.Use the CROSS JOIN syntax to allow cartesian products between these relations"
  * if I continue to use movieDS.filter(mv => !pMovieIds.contains(mv.id))
  * I can add config("spark.sql.crossJoin.enabled", "true") to get rid of the error.  However, CROSS JOIN is inefficient.
  * I improved to use inner join to get ratedDS then 'except' to get unratedDS. I don't need to enable spark.sql.crossJoin.enabled
  *
  * 2. I finally figured out why there are difference between my 'unratedDS' way to get recommendation and
  * the recommendation provided by recommendForAllUsers.  recommendForAllUsers is a generic way and it couldn't and did not
  * take what individuals have already rated into consideration.
  *
  * I exclude those rated to make results from those two approaches the same same.
  *
  * ALS recommendation is Collaborative Filtering recommendation system.
  *
  * Run it ALS locally
  * $SPARK_HOME/bin/spark-submit --master local[*] --conf spark.sql.shuffle.partitions=10 --conf spark.default.parallelism=10 \
  * --num-executors 1 --executor-cores 5 --executor-memory 2000m --conf spark.executor.extraJavaOptions=-'XX:ThreadStackSize=2048' \
  * --class org.freemind.spark.recommend.MovieLensALS target/scala-2.11/spark2_emr_2.11-1.0.jar \
  * ALS ~/Downloads/ml-latest/ratings-sample.csv.gz ~/Downloads/ml-latest/movies.csv recommend-$(date +%s)
  *
  * @author sling/ threecuptea consolidated common methods into MovieLensCommon and refactored
  *         I successfully promoted it to EMR on 8/12/2018
  *
  *         The idea is always that it should be generic.  I should be able to verify it locally before promoted it to EMR.
  *         I refactor it to get it work with full set of ratings in EMR as well as sample set of ratings (5%) locally on 2018/12/31.
  *         Also use MovieLensALS and MovieLenCommon for both ALS and ALSCv. So 1 application for 4 combinations.
  */
object MovieLensALS {

  //mrFile and movieFile paths in EMR were predefined and cannot be customized.
  def main(args: Array[String]): Unit = {
    if (args.length != 2 && args.length != 4) {
      println("Usage: MovieLensALS [ALS or ALSCv] [s3-output-path] or Usage: MovieLensALS [ALS or ALSCv] [local-rating-path] [ocal-movie-path] [local-output-path]")
      System.exit(-1)
    }

    val appName = s"MovieLens${args(0)}"
    //spark.serializer        org.apache.spark.serializer.KryoSerializer
    val spark = SparkSession.builder().appName(appName).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    import spark.implicits._
    val mlCommon = new MovieLensCommon(spark)

    val (mrDF, movieDF) = if (args.length == 4) mlCommon.getMovieLensDataFrames(args(1), args(2)) else mlCommon.getMovieLensDataFrames()
    val outputBase = if (args.length == 4) args(3) else args(1)

    //the default ratingCol is 'rating'
    val als = new ALS().setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setMaxIter(20).setColdStartStrategy("drop")

    val (bestParam, cvModel) = if (args(0).equalsIgnoreCase("ALS")) mlCommon.getBestParmMapFromALS(als, mrDF) else mlCommon.getBestFromCrossValidator(als, mrDF)
    println(s"The best model was trained with param = ${bestParam}")
    //refit the whole mrDF population
    val augAlsModel = als.fit(mrDF, bestParam)

    val recommendDF = mlCommon.getRecommendForAllUsers(augAlsModel)

    val sUserId = 60015
    val sUserMrDF = mrDF.filter('userId === sUserId)

    val prediction = mlCommon.getUnratedPrediction(augAlsModel, sUserId, sUserMrDF, movieDF)
    println(s"The prediction on unratedMovie for user=${sUserId} from ALS model")
    prediction.show(false)

    val recommendation = mlCommon.getUnratedRecommendation(recommendDF, sUserId, sUserMrDF, movieDF)
    println(s"The recommendation on unratedMovie for user=${sUserId} from ALS model")
    recommendation.show(false)

    recommendDF.write.option("header", true).parquet(s"${outputBase}/recommendAll")
    if (cvModel.isDefined)
      cvModel.get.save(s"${outputBase}/cv-model")

    spark.stop()
  }



}
