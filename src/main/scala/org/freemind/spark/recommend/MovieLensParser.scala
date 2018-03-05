package org.freemind.spark.recommend

case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

class MovieLensParser extends Serializable {

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

}

