package org.freemind.spark.recommend

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, explode, lit}


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
  * take what individuals have already rated into consideration.  Therefore, it will recommend 527: 'Schindler's List' to
  * pUserId = 0 even though pUserId = 0  rated that movie already
  * It would recommend 1193: "One Flew Over the Cuckoo's Nest", 904: "Rear Window" to sUser = 6001 even though
  * sUser = 6001 has rated that.
  *
  * I finally exclude those rated to make results from those two approaches the same same.
  *
  *
  * @author sling/ threecuptea consolidated common methods into MovieLensCommon and refactored on 05/27/2018
  */
object MovieLensALSEmr {

  val S3RecommendBase = "s3://threecuptea-us-west-2/recommend/ml-1m"

  val mrFile = s"${S3RecommendBase}/ratings.dat.gz"
  val prFile = s"${S3RecommendBase}/personalRatings.txt"
  val movieFile = s"${S3RecommendBase}/movies.dat"

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: MovieLensALSEmr [s3-putput-path] ")
      System.exit(-1)
    }
    val outPath = args(0)

    val spark = SparkSession.builder().appName("MovieLensALSEmr").getOrCreate()
    import spark.implicits._

    val mlCommon = new MovieLensCommon

    val mrDS = spark.read.textFile(mrFile).map(mlCommon.parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(mlCommon.parseRating).cache()
    val movieDS = spark.read.textFile(movieFile).map(mlCommon.parseMovie).cache()

    mrDS.show(10, false)
    println(s"Rating Counts: movie - ${mrDS.count}, personal - ${prDS.count}")
    movieDS.show(10, false)
    println(s"Movie Counts: ${movieDS.count}")
    println()

    val allDS = mrDS.union(prDS)

    //Need to match field names of rating, KEY POINT is coldStartStrategy = "drop": drop lines with 'prediction' = 'NaN'
    val als = new ALS().setMaxIter(20).setUserCol("userId").setItemCol("movieId").setRatingCol("rating").setColdStartStrategy("drop")

    val bestParmsFromALS = (mlCommon.getBestParmMapFromALS(als, mrDS))
    println(s"The best model from ALS was trained with param = ${bestParmsFromALS}")
    val augModelFromALS = als.fit(allDS, bestParmsFromALS)

    val pUserId = 0
    val pRatedDS = prDS.join(movieDS, prDS("movieId") === movieDS("id"), "inner").select($"id", $"title", $"genres").as[Movie]
    val pUnratedDS = movieDS.except(pRatedDS).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId)) //matches with ALS required fields

    println(s"The recommendation on unratedMovie for user=${pUserId} from ALS model")
    augModelFromALS.transform(pUnratedDS).sort(desc("prediction")).show(false)

    ///recommendation: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, recommendations: array<struct<movieId:int,rating:float>>]
    //We explode to flat array then retrieve field from a struct
    val recommendDS = augModelFromALS.recommendForAllUsers(25).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating")).cache()

    val pUserRecommendDS = recommendDS.filter($"userId" === pUserId)

    println(s"The top recommendation on AllUsers filter with user=${pUserId} from ALS model and exclude rated movies")
    //Rename so that I can avoid the error that reference 'userId' is ambiguous, ' shorthand for column.
    //"movie_b" is not part of prDS.  Therefore, you cannot use psDS("movieId_b") to reference it.  However,
    //you can directly reference "movieId_b"
    val pUserRatedRecommendDS = pUserRecommendDS.join(prDS.select('movieId as "movieId_b"),
      'movieId === 'movieId_b, "inner").select('userId, 'movieId, 'rating)
    pUserRecommendDS.except(pUserRatedRecommendDS).join(movieDS, 'movieId ==='id, "inner").
      select($"movieId", $"title", $"genres", $"userId", $"rating").sort(desc("rating")).show(false)

    println()
    val sUserId = 6001
    val sRatedDS = mrDS.filter($"userId" === sUserId).join(movieDS, mrDS("movieId") === movieDS("id"), "inner").select($"id", $"title", $"genres").as[Movie]
    val sUnratedDS = movieDS.except(sRatedDS).withColumnRenamed("id", "movieId").withColumn("userId", lit(sUserId))

    println(s"The recommendation on unratedMovie for user=${sUserId} from ALS model")
    augModelFromALS.transform(sUnratedDS).sort(desc("prediction")).show(false)

    val sUserRecommendDS = recommendDS.filter($"userId" === sUserId)

    println(s"The top recommendation on AllUsers filter with  user=${sUserId} from ALS model and exclude rated movies")
    val sUserRatedRecommendDS = sUserRecommendDS.join(mrDS.select('userId as "userId_b", 'movieId as "movieId_b"),
      'userId === 'userId_b  && 'movieId === 'movieId_b, "inner").select('userId, 'movieId, 'rating)
    sUserRecommendDS.except(sUserRatedRecommendDS).join(movieDS, 'movieId === 'id, "inner").
      select($"movieId", $"title", $"genres", $"userId", $"rating").sort(desc("rating")).show(false)

    recommendDS.write.option("header", true).parquet(s"${outPath}/recommendAll")  //Used as shared resource

    spark.stop()
  }

}
