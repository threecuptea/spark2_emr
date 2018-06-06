package org.freemind.spark.recommend

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

class MovieLensCommon extends Serializable {

  def parseRating(line: String): Rating = {
    val splits  = line.split("::")
    assert(splits.length == 4)

    Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
  }

  def parseMovie(line: String): Movie = {
    val splits  = line.split("::")
    assert(splits.length == 3)

    Movie(splits(0).toInt, splits(1), splits(2).split('|'))
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

  def getBaselineRmse(trainDS: Dataset[Rating], testDS: Dataset[Rating]): Double = {
    val avgRating = trainDS.select(mean("rating")).first().getDouble(0)
    val rmse = evaluator.evaluate(testDS.withColumn("prediction", lit(avgRating)))
    printf("The baseline rmse= %3.2f.\n", rmse)
    rmse
  }

  def getBestParmMapFromALS(als: ALS, mrDS: Dataset[Rating]): ParamMap = {
    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8, 0.1, 0.1))
    trainDS.cache()
    valDS.cache()
    testDS.cache()

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

  def getBestCrossValidatorModel(als: ALS, mrDS: Dataset[Rating]): CrossValidatorModel = {
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
