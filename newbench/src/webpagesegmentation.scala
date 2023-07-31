import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object webpagesegmentation {
  def main(args: Array[String]): Unit = {
  }

  // find webpage UI components that overlap (considered a UI defect)
  def execute(input1: RDD[String], input2: RDD[String]): RDD[String] = {
    val a = input1
    .map { m =>
      val row = m.split(",")
      val url = row(0)
      val componentID = row(5)
      val componentType = row(6)
      val params = row(1) + ", " + row(2) + ", " + row(3) + ", " + row(4)
      (url+componentID+componentType, params)

    }

    input2.map{ m =>
      val row = m.split(",")
      val url = row(0)
      val componentID = row(5)
      val componentType = row(6)
      val params = row(1) + ", " + row(2) + ", " + row(3) + ", " + row(4)
      (url+componentID+componentType, params)
    }
    .join(a)
    .filter ( arg => {
      val key = arg._1
      val rectangleA = arg._2._1
      val rectangleB = arg._2._2
      val rectA = rectangleA.split(",")
      val rectB = rectangleB.split(",")

      val aTopLeftX = rectA(0).toInt
      val aBottomRightY = rectA(1).toInt
      val aTopLeftY = aBottomRightY + rectA(2).toInt
      val aBottomRightX = aTopLeftX + rectA(3).toInt

      val bTopLeftX = rectB(0).toInt
      val bBottomRightY = rectB(1).toInt
      val bTopLeftY = bBottomRightY + rectB(2).toInt
      val bBottomRightX = bTopLeftX + rectB(3).toInt

      !(aTopLeftX > bBottomRightX || aBottomRightX < bTopLeftX || aTopLeftY < bBottomRightY || aBottomRightY > bTopLeftY)
  })
      .map(arg => arg._1)
  }
}
