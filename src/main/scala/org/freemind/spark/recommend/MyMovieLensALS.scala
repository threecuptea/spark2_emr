package org.freemind.spark.recommend

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
case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

object MyMovieLensALS {

  def parseRating(line: String): Rating = {
    val splits = line.split("::")
    assert(splits.length == 4)
    Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
  }

  def parseMovie(line: String): Movie = {
    val splits = line.split("::")
    assert(splits.length == 3)
    Movie(splits(0).toInt, splits(1), splits(2).split('|'))
  }

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

    val mrFile = "s3://threecuptea-us-west-2/recommend/ml-1m/ratings.dat.gz"
    val prFile = "s3://threecuptea-us-west-2/recommend/ml-1m/personalRatings.txt"
    val movieFile = "s3://threecuptea-us-west-2/recommend/ml-1m/movies.dat"

    val spark = SparkSession
      .builder().
      appName("FlightSample").
      getOrCreate()

    import spark.implicits._

    val mrDS = spark.read.textFile(mrFile).map(parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(parseRating).cache()
    //We did not use movie to look up in this case, therefore, I don't use broadcast
    val movieDS = spark.read.textFile(movieFile).map(parseMovie).cache()

    println(s"Rating counts and movie rating snapshot= ${mrDS.count}, ${prDS.count}")
    mrDS.show(10, false)
    println(s"Movies count and its snapshot= ${movieDS.count}")
    movieDS.show(10, false)

    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(.8,.1,.1)) //validation DS is used to find the best model using ParamGrid
    trainDS.cache()
    valDS.cache()
    testDS.cache()

    val total = trainDS.count() + valDS.count() + testDS.count()
    println(s"TOTAL SUM of splits= $total")  //It's a validation for one of previous bug
    println()

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
    val als = new ALS().setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
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
      //val prediction = model.transform(valDS).filter(r => !r.getAs[Float]("prediction").isNaN) //Need to exclude NaN from prediction, otherwise rmse will be NaN too
      //def filter(condition: Column): Dataset[T], for the following
      val prediction = model.transform(valDS).filter(!$"prediction".isNaN)
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
      case Some(testModel) =>
        val testPrediction = testModel.transform(testDS).filter(!$"prediction".isNaN)
        val testRmse = evaluator.evaluate(testPrediction)
        val improvement = (baselineRmse - testRmse) / baselineRmse.value * 100
        println(s"The best model was trained with param = ${bestParam.get}")
        printf("The RMSE of the bestModel on test set is %3.2f, which is %3.2f%% over baseline.\n", testRmse, improvement) //use %% to print %
        println()
    }

    //Recall bestModel was fit with trainPlusDS, which cover userId=0 but might miss some movie
    val augmentModel = als.fit(allDS, bestParam.get)
    //option 1
    //val pMovieIds = prDS.select($"movieId").map(r => r.getInt(0)).collect()
    val pMovieIds = prDS.map(_.movieId).collect()

    //We might have movieId in movies but not in allDS
    val unratingDS =  movieDS.filter(movie => !pMovieIds.contains(movie.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(0))
    val recommendation = augmentModel.transform(unratingDS).filter(!$"prediction".isNaN).sort(desc("prediction")).limit(100)
    recommendation.write.csv(outPath + "top_100")
    recommendation.show()

  }



}
