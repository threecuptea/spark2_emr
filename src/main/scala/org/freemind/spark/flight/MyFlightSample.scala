package org.freemind.spark.flight

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

/**
  * Created by fandev on 7/8/17.
  */
object MyFlightSample {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: MyFlightSample [s3-putput-path] ")
      System.exit(-1)
    }

    val outPath = args(0)

    val spark = SparkSession.builder().appName("FlightSample").getOrCreate()
    import spark.implicits._

    val rawDF = spark.read.parquet("s3://us-east-1.elasticmapreduce.samples/flightdata/input/")
    val partitions = rawDF.rdd.getNumPartitions
    var adjPartition = partitions - partitions / 3 - 1 //expect at least 1/3 < 2000

    //cache repeated Dataset
    val flightDS = rawDF.filter($"year" >= 2000).select($"quarter", $"origin", $"dest", $"depdelay", $"cancelled")
      .coalesce(adjPartition).cache()

    /** top departure by origin */
    val topDepartures = flightDS.groupBy($"origin").count().withColumnRenamed("count", "total_departures")
      .sort(desc("total_departures")).limit(10)
    topDepartures.write.csv(s"${outPath}/top_departures")

    /** top short delay by origin */
    val shortDepDelay = flightDS.filter($"depdelay" >= 15).groupBy($"origin").count().withColumnRenamed("count", "total_delays")
      .sort(desc("total_delays")).limit(10)
    shortDepDelay.write.json(s"${outPath}/top_short_delays")

    /** top long delay by origin */
    val longDepDelay = flightDS.filter($"depdelay" >= 60).groupBy($"origin").count().withColumnRenamed("count", "total_delays")
      .sort(desc("total_delays")).limit(10)
    longDepDelay.write.parquet(s"${outPath}/top_long_delays")

    /** top cancellation by origin */
    val topCancel = flightDS.filter($"cancelled" === 1).groupBy($"origin").count().withColumnRenamed("count", "total_cancellations")
      .sort(desc("total_cancellations")).limit(10)
    topCancel.write.csv(s"${outPath}/top_cancellations")

    /** top cancellation by quarter */
    val quarterCancel = flightDS.filter($"cancelled" === 1).groupBy($"quarter").count().withColumnRenamed("count", "total_cancellations")
      .sort(desc("total_cancellations")).limit(10)
    quarterCancel.write.csv(s"${outPath}/rank_quarter_cancellations")

    val popularFlights = flightDS.groupBy($"origin", $"dest").count().withColumnRenamed("count", "total_flights")
      .sort(desc("total_flights")).limit(10)
    popularFlights.write.csv(s"${outPath}/popular_flights")

    spark.stop()
  }

}
