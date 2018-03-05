package org.freemind.spark.recommend

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object MovieLensALSColdStartCvEmr {

  val S3RecommendBase = "s3://threecuptea-us-west-2/recommend/ml-1m"

  val mrFile = s"${S3RecommendBase}/ratings.dat.gz"
  val prFile = s"${S3RecommendBase}/personalRatings.txt"
  val movieFile = s"${S3RecommendBase}/movies.dat"

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Usage: MovieLensALSColdStartCvEmr [s3-putput-path] ")
      System.exit(-1)
    }
    val outPath = args(0)

    val spark = SparkSession.builder().appName("MovieLensALSColdStartCv").getOrCreate()
    import spark.implicits._

    val mlParser = new MovieLensParser()
    val mrDS = spark.read.textFile(mrFile).map(mlParser.parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(mlParser.parseRating).cache()
    val movieDS = spark.read.textFile(movieFile).map(mlParser.parseMovie).cache()

    //Initialize data for augmented model
    val allDS = mrDS.union(prDS).cache()

    //Initialize evaluator
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

    /**
      * ALS codes starts here
      * ALS (alternating least square is a popular model that spark-ml use for 'Collaborative filtering'
      * KEY POINT is coldStartStrategy = "drop"
      */
    val als = new ALS()
      .setMaxIter(20).setColdStartStrategy("drop").setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val regParams = Array(0.8, 0.5, 0.1) //the default is 1.0
    //Array[ParamMap]
    val paramGrids = new ParamGridBuilder()
      .addGrid(als.regParam, regParams)
      .addGrid(als.rank, ranks)
      .build()

    val cv = new CrossValidator()
      .setEstimator(als).setEvaluator(evaluator).setEstimatorParamMaps(paramGrids).setNumFolds(10)  // percentage close to 0.8, 0.1, 0.1

    val bestModelFromCR = getBestCrossValidatorModel(cv, mrDS, prDS)

    val descArr = (bestModelFromCR.getEstimatorParamMaps zip bestModelFromCR.avgMetrics).sortBy(_._2)
    val bestParamMap = descArr(0)._1

    val augModelFromCv = als.fit(allDS, bestParamMap)

    val recommendDS = augModelFromCv.recommendForAllUsers(10).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating"))

    recommendDS.write.parquet(outPath + "recommendAll")  //Used as shared resource

    bestModelFromCR.save(outPath + "cv-model") // It is using MLWriter

  }

  def getBestCrossValidatorModel(cv: CrossValidator,
                                 mrDS: Dataset[Rating], prDS: Dataset[Rating]): CrossValidatorModel = {
    val Array(cvTrainValDS, cvTestDS) = mrDS.randomSplit(Array(0.9, 0.1))
    val cvTrainValPlusDS = cvTrainValDS.union(prDS).cache()

    val cvModel = cv.fit(cvTrainValPlusDS)
    val bestRmse = cv.getEvaluator.evaluate(cvModel.transform(cvTrainValPlusDS))
    val cvPrediction = cvModel.transform(cvTestDS)
    val cvRmse = cv.getEvaluator.evaluate(cvPrediction)
    //It only return base and does not give us extra params like regParam or rank
    printf("The RMSE of the bestModel from CrossValidator on validation set is %3.4f\n", bestRmse)
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.4f\n", cvRmse)
    println()
    cvModel
  }


}
