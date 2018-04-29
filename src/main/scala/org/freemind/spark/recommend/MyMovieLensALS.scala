package org.freemind.spark.recommend

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  *
  * This is emr version of MovieLensALS.  See spark_tutorial_2 project
  *
  * @author sling(threecuptea) wrote on 7/10/17.
  */

object MyMovieLensALS {

  val S3RecommendBase = "s3://threecuptea-us-west-2/recommend/ml-1m"

  val mrFile = s"${S3RecommendBase}/ratings.dat.gz"
  val prFile = s"${S3RecommendBase}/personalRatings.txt"
  val movieFile = s"${S3RecommendBase}/movies.dat"


  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      /**
        * s3://threecuptea-us-west-2/recommend/ml-1m/ratings.dat.gz
        * s3://threecuptea-us-west-2/recommend/ml-1m/personalRatings.txt
        * s3://threecuptea-us-west-2/recommend/ml-1m/movies.dat
        *
        * All data are '::' delimited.
        * rating sample is like the following:
        *              0::1::5::1474830626
        * movie sample is like the followings:
        *              1::Toy Story (1995)::Animation|Children's|Comedy
        * *
        */
      println("Usage: MyMovieLensALS [s3-putput-path] ")
      System.exit(-1)
    }

    val outPath = args(0)

    val spark = SparkSession
      .builder()
        .appName("MovieRecommend")
        .config("spark.sql.shuffle.partitions", 8)
        .getOrCreate()

    import spark.implicits._

    val mlParser = new MovieLensParser()
    val mrDS = spark.read.textFile(mrFile).map(mlParser.parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(mlParser.parseRating).cache()
    //I did not use movie to look up in this case, therefore, I don't use broadcast
    val movieDS = spark.read.textFile(movieFile).map(mlParser.parseMovie).cache()

    println(s"Rating counts and movie rating snapshot= ${mrDS.count}, ${prDS.count}")
    mrDS.show(10, false)
    println(s"Movies count and its snapshot= ${movieDS.count}")
    movieDS.show(10, false)

    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(.8,.1,.1)) //validation DS is used to find the best model using ParamGrid
    trainDS.cache()
    valDS.cache()
    testDS.cache()

    /** Calculate baseline RMSE **/
    val avgRating = trainDS.select(mean($"rating")).first().getDouble(0)
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val baselineRmse = spark.sparkContext.broadcast(evaluator.evaluate(testDS.withColumn("prediction", lit(avgRating))))
    printf("The baseline rmse =%3.2f", baselineRmse.value)

    val trainPlusDS = trainDS.union(prDS).cache()
    val allDS = mrDS.union(prDS).cache()

    /**
      * ALS model and Grid search
      */
    val als = new ALS().setColdStartStrategy("drop").setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    var bestRmse = Double.MaxValue
    var bestModel: Option[ALSModel] = None
    var bestParam: Option[ParamMap] = None

    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val iters = Array(20) //the default is 10
    //It is a lambda multipler on the cost to prevent overfitting.  Increasing lambda will penalize the cost which are coefficients of linear regression
    // It penalize Linear Regression Model with large coefficients.
    // Linear Regression Model with large coefficients tends to be more complicated.
    //The default is 1.0
    val regParams = Array(0.1, 0.01)

    val paramGRid = new ParamGridBuilder()
      .addGrid(als.rank, ranks)
      .addGrid(als.maxIter, iters)
      .addGrid(als.regParam, regParams)
      .build()

    for (paramMap <- paramGRid) {
      val model = als.fit(trainPlusDS, paramMap)
      //transform returns a DF with additional field 'prediction'.Filter has different flavors.
      //def filter(func: (T) ⇒ Boolean): Dataset[T] for the followings
      //val prediction = model.transform(valDS).filter(r => !r.getAs[Float]("prediction").isNaN)
      //def filter(condition: Column): Dataset[T], for the following
      val prediction = model.transform(valDS)// Need to exclude NaN from prediction, otherwise rmse will be NaN too
      //NaN is the least
      val rmse = evaluator.evaluate(prediction)

      if (rmse < bestRmse ) {
        bestRmse = rmse
        bestModel = Some(model)
        bestParam = Some(paramMap)
      }
    }

    bestModel match {
      case None =>
        println("Unable to find a good ALSModel.  STOP here")
        System.exit(-1)
      case Some(goodModel) =>
        val testPrediction = goodModel.transform(testDS).filter(!$"prediction".isNaN)
        val testRmse = evaluator.evaluate(testPrediction)
        val improvement = (baselineRmse.value - testRmse) / baselineRmse.value * 100
        println(s"The best model was trained with param = ${bestParam.get}")
        printf("The RMSE of the bestModel on test set is %3.2f, which is %3.2f%% over baseline.\n", testRmse, improvement) //use %% to print %
    }

    //Recall bestModel was fit with trainPlusDS, which cover userId=0 but might miss some movie
    //If I use allDS to fit then distinct movieId  of mr - moveId of PR to transform, allDS will cover the scope. I don't need
    //to filter out isNaN.  However, that would require distinct then join.  Both requires shuffling.
    val augmentModel = als.fit(allDS, bestParam.get)

    val ratedDS = movieDS.join(prDS, movieDS("id") === prDS("movieId")).as[Movie] //Dataset of Movie
    val unratedDS = movieDS.except(ratedDS).withColumnRenamed("id", "movieId").withColumn("userId", lit(0)) //Dataset of Movie

    val recommendation = augmentModel.transform(unratedDS).sort(desc("prediction")).limit(25)
    recommendation.show(25, false)

    //java.lang.UnsupportedOperationException: CSV data source does not support array<string> data type.  I take genres as it it.
    val stringify = udf((vs: Seq[String]) => s"""[${vs.mkString(",")}]""")

    recommendation.withColumn("genres", stringify($"genres")).write.option("header","true").csv(outPath + "top_25")
    //recommend is a Row
    val sUserId = 6001
    val recommendDS = augmentModel.recommendForAllUsers(10).select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating"))

    recommendDS.join(movieDS, recommendDS("movieId") === movieDS("id")).
      select($"movieId", $"title", $"genres", $"userId", $"rating").show(false)

    spark.stop()
  }

}
