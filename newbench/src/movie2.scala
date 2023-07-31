import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object movie2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val movie = sc.parallelize(Array(",,,,03A00"))
    execute(movie).foreach(println)
  }
  // count the number of movies with duration > 150min for each genre
  def execute(input1: RDD[String]): RDD[String] = {
    input1
    .filter(movie => {
        val duration = movie.split(",")(4)
        val hour = duration.substring(0,2).toInt
        val min = duration.substring(3,5).toInt
        val total_min = hour*60+min
        total_min > 150
    })
    .map(movie => {
        val genre = movie.split(",")(2)
        (genre, 1)
    })
    .reduceByKey(_ + _)
    .map(agg => agg._1 + ":" + agg._2)
  }
}
