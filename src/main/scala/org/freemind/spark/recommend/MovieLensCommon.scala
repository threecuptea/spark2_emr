package org.freemind.spark.recommend

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel


case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

class MovieLensCommon(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val S3RecommendBase = "s3://threecuptea-us-west-2/ml-latest"

  val mrFile = s"${S3RecommendBase}/ratings.csv.gz"
  val prFile = s"${S3RecommendBase}/personalRatings.csv"
  val movieFile = s"${S3RecommendBase}/movies.csv"

  val ratingsSchema = StructType(
    StructField("userId", IntegerType, nullable = true) ::
      StructField("movieId", IntegerType, nullable = true) ::
      StructField("rating", FloatType, nullable = true) ::
      StructField("ts", LongType, nullable = true) :: Nil
  )

  val movieSchema = StructType(
    StructField("id", IntegerType, nullable = true) ::
      StructField("title", StringType, nullable = true) ::
      StructField("genres", StringType, nullable = true) :: Nil
  )

  def getMovieLensDataFrames() = {
    val mrDS = spark.read.option("header", true).schema(ratingsSchema).csv(mrFile).select('userId, 'movieId, 'rating).persist(StorageLevel.MEMORY_ONLY_SER)
    val prDS = spark.read.schema(ratingsSchema).csv(prFile).select('userId, 'movieId, 'rating).cache()
    val movieDS = spark.read.option("header", true).schema(movieSchema).option("quote","\"").option("escape","\"").csv(movieFile).persist(StorageLevel.MEMORY_ONLY_SER)

    (mrDS, prDS, movieDS)
  }

  val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

  def getParamGrid(als: ALS): Array[ParamMap] = {
    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val regParams = Array(0.8, 0.5, 0.1) //the default is 1.0

    new ParamGridBuilder().
      addGrid(als.regParam, regParams).
      addGrid(als.rank, ranks).
      build()
  }

  def getBaselineRmse(trainDS: DataFrame, testDS: DataFrame): Double = {
    val avgRating = trainDS.select(mean("rating")).first().getDouble(0)
    val rmse = evaluator.evaluate(testDS.withColumn("prediction", lit(avgRating)))
    printf("The baseline rmse= %3.2f.\n", rmse)
    rmse
  }

  def getBestParmMapFromALS(als: ALS, mrDS: DataFrame): ParamMap = {
    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8, 0.1, 0.1))
    trainDS.persist(StorageLevel.MEMORY_ONLY_SER)
    valDS.persist(StorageLevel.MEMORY_ONLY_SER)
    testDS.persist(StorageLevel.MEMORY_ONLY_SER)

    var bestModel: Option[ALSModel] = None
    var bestParam: Option[ParamMap] = None
    var bestRmse = Double.MaxValue

    for (paramMap <- getParamGrid(als)) {
      val model = als.fit(trainDS, paramMap)
      val prediction = model.transform(valDS) //This will add additional field 'prediction'
      val rmse = evaluator.evaluate(prediction) //evaluation evaluate base upon 'rating' (label field) and 'prediction' (prediction field)

      if (rmse < bestRmse) {
        bestRmse = rmse
        bestModel = Some(model)
        bestParam = Some(paramMap)
      }
    }

    bestModel match {
      case None =>
        println("Unable to find a valid ALSModel.  STOP here")
        throw new IllegalArgumentException("Unable to find a valid ALSModel")
      case Some(model) =>
        val prediction = model.transform(testDS)
        val testRmse = evaluator.evaluate(prediction)
        val baselineRmse = getBaselineRmse(trainDS, testDS)
        printf("The RMSE of the bestModel from ALS on validation set is %3.4f\n", bestRmse)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        printf("The RMSE of the bestModel on test set is %3.4f, which is %3.2f%% over baseline.\n", testRmse, improvement) //use %% to print %

        bestParam.get
    }
  }

  def getBestCrossValidatorModel(als: ALS, mrDS: DataFrame): CrossValidatorModel = {
    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8, 0.1, 0.1))
    trainDS.cache()
    valDS.cache()
    testDS.cache()

    val cv = new CrossValidator().setEstimator(als).setEstimatorParamMaps(getParamGrid(als)).setEvaluator(evaluator).setNumFolds(10)
    val cvModel = cv.fit(trainDS)
    val prediction = cvModel.transform(valDS)
    val bestRmse = cv.getEvaluator.evaluate(prediction)

    val cvPrediction = cvModel.transform(testDS)
    val cvRmse = cv.getEvaluator.evaluate(cvPrediction)

    printf("The RMSE of the bestModel from CrossValidator on validation set is %3.4f\n", bestRmse)
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.4f\n", cvRmse)
    println()

    cvModel
  }

}
