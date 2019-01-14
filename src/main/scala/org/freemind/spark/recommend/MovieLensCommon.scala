package org.freemind.spark.recommend

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


@SerialVersionUID(5687311l)
class MovieLensCommon(spark: SparkSession)  extends Serializable {

  import spark.implicits._

  val s3RecommendBase = "s3://threecuptea-us-west-2/ml-latest"
  val s3MrFile = s"${s3RecommendBase}/ratings.csv.gz"
  val s3MovieFile = s"${s3RecommendBase}/movies.csv"

  val ratingSchema = StructType(
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

  /**
    *
    * @param mrFile
    * @param movieFile
    * @return mrDF and movieDF: mrDF will not contains 'ts so that it can be consistent with recommend refined DF
    */
  def getMovieLensDataFrames(mrFile: String = s3MrFile, movieFile: String = s3MovieFile): (DataFrame, DataFrame) = {
    val mrDF = spark.read.schema(ratingSchema).option("header", true).csv(mrFile).select('userId, 'movieId, 'rating).
      persist(StorageLevel.MEMORY_ONLY_SER)
    val movieDF = spark.read.schema(movieSchema).option("header", true).option("quote","\"").option("escape","\"").csv(movieFile).
      persist(StorageLevel.MEMORY_ONLY_SER)
      //println(s"Rating Counts: ${mrDF.count}")
      //println(s"Movie Counts: ${movieDF.count}")

    (mrDF, movieDF)
  }

  val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

  def getParamGrid(als: ALS): Array[ParamMap] = {
    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val regParams = Array(0.5, 0.1, 0.01) //the de

    new ParamGridBuilder().
      addGrid(als.rank, ranks).
      addGrid(als.regParam, regParams).
      build()
  }

  /**
    * Establish baseline RMSE so that we can compare rmse obtained from best ALS model and CrossValidatorModel using ALS as the estimator
    *
    * It is using the average rating of trainDF as the prediction value for the testDF
    * @param trainDF
    * @param testDF
    * @return
    */
  def getBaselineRmse(trainDF: DataFrame, testDF: DataFrame): Double = {
    val avgRating = trainDF.select(mean('rating)).first().getDouble(0)
    val rmse =evaluator.evaluate(testDF.withColumn("prediction", lit(avgRating)))
    printf("The baseline rmse= %3.4f.\n", rmse)

    rmse
  }

  /**
    * Loop through param grids and manually find the best ParamMap
    * @param als: ALS
    * @param mrDF: movie rating DF
    * @return ParamMap: the ParamMap which generate lowest RMSE
    */
  def getBestParmMapFromALS(als: ALS, mrDF: DataFrame): (ParamMap, Option[CrossValidatorModel]) = {
    val Array(trainDF, valDF, testDF) = mrDF.randomSplit(Array(0.8,0.1,0.1), 42L)
    trainDF.persist(StorageLevel.MEMORY_ONLY_SER)
    valDF.persist(StorageLevel.MEMORY_ONLY_SER)
    testDF.persist(StorageLevel.MEMORY_ONLY_SER)

    var bestModel: Option[ALSModel] = None
    var bestParam: Option[ParamMap] = None
    var bestRmse = Double.MaxValue

    for (param <- getParamGrid(als)) {
      val model = als.fit(trainDF, param)
      val prediction = model.transform(valDF)
      val rmse = evaluator.evaluate(prediction)
      if (rmse < bestRmse) {
        bestRmse = rmse
        bestModel = Some(model)
        bestParam = Some(param)
      }
    }

    bestModel match {
      case None =>
        println("Unable to find a valid ALSModel.  STOP here")
        throw new IllegalArgumentException("Unable to find a valid ALSModel")
      case Some(model) =>
        val prediction = model.transform(testDF)
        val testRmse = evaluator.evaluate(prediction)
        val baselineRmse = getBaselineRmse(trainDF, testDF)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        printf("The RMSE of the bestModel from ALS on validation set is %3.4f\n", bestRmse)
        printf("The RMSE of the bestModel from ALS on test set is %3.4f, which is %3.2f%% over baseline.\n", testRmse, improvement)

        (bestParam.get, None)
    }

  }

  /**
    * Ask CrossValidator to find the best ParamMap
    * @param als: ALS
    * @param mrDF: movie rating DF
    * @return ParamMap: the ParamMap which correspond to the bestModel
    */
  def getBestFromCrossValidator(als: ALS, mrDF: DataFrame): (ParamMap, Option[CrossValidatorModel]) = {
    val Array(trainDF, testDF) = mrDF.randomSplit(Array(0.9, 0.1), 42L)
    //There should not be any validator set involved
    trainDF.persist(StorageLevel.MEMORY_ONLY_SER)
    testDF.persist(StorageLevel.MEMORY_ONLY_SER)

    val cv = new CrossValidator().setEstimator(als).setEstimatorParamMaps(getParamGrid(als)).setEvaluator(evaluator).setSeed(42L).
      setNumFolds(5).setParallelism(5)

    val cvModel = cv.fit(trainDF)
    val trainRmse = evaluator.evaluate(cvModel.transform(trainDF))
    val prediction = cvModel.transform(testDF)
    val testRmse = evaluator.evaluate(prediction)
    printf("The RMSE of the bestModel from CrossValidator on training set is %3.4f\n", trainRmse)
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.4f\n", testRmse)

    val bestParam = (cvModel.getEstimatorParamMaps zip cvModel.avgMetrics).minBy(_._2)._1
    (bestParam, Some(cvModel))
  }

  /**
    * The idea is to save/ cache this to be shared and queried by multiple users.  recommendDF should be refreshed periodically
    *
    * @param model: ALDModel
    * @return recommendDF: recommendForAllUsers DataFrame refined to have userId, movieId, rating fields and be compatible with mrDF
    */
  def getRecommendForAllUsers(model: ALSModel): DataFrame = {
    // DataFrame of (userCol: Int, recommendations), where recommendations are stored as an array of (itemCol: Int, rating: Float) Rows.
    model.recommendForAllUsers(25).select('userId, explode('recommendations).as("recommend")).
      select('userId, 'recommend.getField("movieId").as("movieId"), 'recommend.getField("rating").as("rating")).persist(StorageLevel.MEMORY_ONLY_SER)

  }

  /**
    * The first approach uses ALSModel.transform to predict the rating for unrated movies for a selected user.
    *
    * It joins sUserMrDF with moveDF first to get rated movies that we select only fields of movieDF so that moveDF can except it
    * to get unratedDF.  Then we change field name to be compliance with ALS field names to prepare for transform/ predict
    *
    * @param model: A given ALDModel
    * @param sUserId: selected userId
    * @param sUserMrDF: mrDF filtered with sUserId only
    * @param movieDF: used to join
    *
    */
  def getUnratedPrediction(model: ALSModel, sUserId: Int, sUserMrDF: DataFrame, movieDF: DataFrame): DataFrame = {
    val ratedDF = sUserMrDF.join(movieDF, 'movieId === 'id, "inner").select('id, 'title, 'genres)
    val unratedDF = movieDF.except(ratedDF).withColumnRenamed("id", "movieId").withColumn("userId", lit(sUserId))

    model.transform(unratedDF).select('movieId, 'title, 'genres, 'userId, 'prediction.as("rating")).orderBy('rating.desc)

  }

  /**
    * Use master recommendDF filtered with sUserId only then subtract rated sUserMrDF to get unratedRecommend then join with movieDF
    * @param recommendDF
    * @param sUserId
    * @param sUserMrDF
    * @param movieDF
    */
  def getUnratedRecommendation(recommendDF: DataFrame, sUserId: Int, sUserMrDF: DataFrame, movieDF: DataFrame): DataFrame = {
    val sRecommendDF = recommendDF.filter('userId === sUserId)

    sRecommendDF.except(sUserMrDF).join(movieDF, 'movieId === 'id, "inner")
      .select('movieId, 'title, 'genres, 'userId, 'rating).orderBy('rating.desc)
  }


}
