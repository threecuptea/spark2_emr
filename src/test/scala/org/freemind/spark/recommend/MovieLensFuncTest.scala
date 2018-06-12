package org.freemind.spark.recommend

import org.freemind.spark.SparkSessionTestWrapper
import org.scalatest._


class MovieLensFuncTest extends FunSpec with SparkSessionTestWrapper {
  import spark.implicits._

  describe("#getMovieLensDataFrames") {
    it("Should return DataFrameWithoutExcption") {
      val mlCommon = new MovieLensCommon(spark)

      val mlLatestBase = "/home/fandev/Downloads/ml-latest"
      val mrPath  = s"${mlLatestBase}/ratings-sample.csv.gz"
      val moviePath = s"${mlLatestBase}/movies.csv"

      val (mrDS, movieDS) = mlCommon.getMovieLensDataFrames(mrPath, moviePath)

      assert(mrDS.count == 1300956)
      assert(movieDS.count == 45843)

      mrDS.show(5, false)
      movieDS.show(5, false)

    }

  }

}
