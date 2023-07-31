package utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import scala.Array
import utils.SparkProgramTemplate
import scala.collection.mutable

import movie1._
import usedcars._
import airport._
import transit._
import credit._

object TestSuite { 
  def read(filepath: String, sc: SparkContext): RDD[String] = {
    if ((new File(filepath)).exists()) {
      sc.textFile(filepath).zipWithIndex()
        .filter(r => r._2 > 0).map(r => {
          if (r._1.endsWith(",")) r._1 + " "
          else r._1
        })
    }
    else
      sc.emptyRDD[String]
  }
  def execute(prog: SparkProgramTemplate, input1: RDD[String], input2: RDD[String], input3: RDD[String]): String = {
    try {
      val output: RDD[String] = {
        if (input2 == null) prog.execute(input1)
        else if (input3 == null) prog.execute(input1, input2)
        else prog.execute(input1, input2, input3)
      }

      return output.map(r => r + "]").collect().mkString("\n")
      // zipWithIndex().map(r => "Row " + r._2 + ":" + r._1)
    }
    catch {
      case e: Exception =>
        return "crashed";
    }
  }

  val ResultDir = "/mnt/ssd/thaddywu/rinput/BigTest/Result"
  def testBench[T <: SparkProgramTemplate](benchmark: String, category: String, std: SparkProgramTemplate, faultys: Array[T], sc: SparkContext, numberOfArgs: Int): (Int, Int, String) = {
    val file = new File(ResultDir + "/" + benchmark + "/")
    val paths = file.listFiles.map(path => path.getAbsolutePath)

    var detected = mutable.Set.empty[String]
    var summary = "";

    for (path <- paths) {
      val inputdir = path + "/" + category + "/"
      val input1 = { if (numberOfArgs >= 1) read(inputdir + "/input1", sc) else null }
      val input2 = { if (numberOfArgs >= 2) read(inputdir + "/input2", sc) else null }
      val input3 = { if (numberOfArgs >= 3) read(inputdir + "/input3", sc) else null }

      println("\u001B[32m")
      if (input1 != null) {println("input1:"); input1.foreach(println)}
      if (input2 != null) {println("input2:"); input2.foreach(println)}
      if (input3 != null) {println("input3:"); input3.foreach(println)}
      println("\u001B[0m")


      val stdout = execute(std, input1, input2, input3)

      val pathId: String = path.split("/").lastOption.getOrElse("unknown").stripSuffix(".smt2")

      summary = summary + "\n"
      summary = summary + benchmark + ", " + category + ", " + pathId + ": "
      summary = summary + "std: " + {if (stdout == "crashed") stdout else "ok"}

      println(benchmark + ", " + category + ", " + pathId)
      println("stdout: " + stdout)
      for (tst <- faultys) {
        val tstout = execute(tst, input1, input2, input3)
        val name = tst.getClass.getSimpleName.toString.stripSuffix("$")

        val status = {
          if (tstout.equals("crashed") && !stdout.equals("crashed")) "crashed"
          else if (tstout.equals(stdout)) "passed"
          else "failed"
        }
        if (status != "passed")
          summary = summary + ", " + name + ": " + status

        println(tstout)
        println(benchmark + ", " + name + ", " + category + ", " + pathId + ", " + status)
        if (status != "passed" && !detected.contains(name))
          detected.add(name)
      }
    }
    (detected.size, faultys.length, "") //summary)
  }
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("CommuteTime")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val movie1_faulty = Array(movie1WrongOperator, movie1WrongDelim, movie1WrongPredicate, movie1SwapKV, movie1WrongColumn)
    val movie1_primitive_stats = testBench("movie1", "primitive", movie1, movie1_faulty, sc, 1)
    val movie1_refined_stats = testBench("movie1", "refined", movie1, movie1_faulty, sc, 1)

    val usedcars_faulty = Array(usedcarsWrongDelim, usedcarsWrongJoin, usedcarsWrongColumn, usedcarsWrongPredicate, usedcarsSwapKV)
    val usedcars_primitive_stats = testBench("usedcars", "primitive", usedcars, usedcars_faulty, sc, 2)
    val usedcars_refined_stats = testBench("usedcars", "refined", usedcars, usedcars_faulty, sc, 2)
    
    val airport_faulty = Array(airportWrongJoin, airportWrongPredicate, airportWrongOffset, airportSwapKV, airportWrongPredicate2)
    val airport_primitive_stats = testBench("airport", "primitive", airport, airport_faulty, sc, 2)
    val airport_refined_stats = testBench("airport", "refined", airport, airport_faulty, sc, 2)

    val transit_faulty = Array(transitWrongPredicate, transitSwapKV, transitWrongOffsets, transitWrongOperator, transitWrongColumn, transitWrongDelim) // difference
    val transit_primitive_stats = testBench("transit", "primitive", transit, transit_faulty, sc, 1)
    val transit_refined_stats = testBench("transit", "refined", transit, transit_faulty, sc, 1)

    val credit_faulty = Array(creditWrongColumn, creditWrongDelim, creditWrongPredicate, creditWrongPredicate2, creditWrongPredicate3)
    val credit_primitive_stats = testBench("credit", "primitive", credit, credit_faulty, sc, 1)
    val credit_refined_stats = testBench("credit", "refined", credit, credit_faulty, sc, 1)

    println("\u001B[31m")
    println("movie1(primitive): " + movie1_primitive_stats)
    println("movie1(refined)  : " + movie1_refined_stats)
    println("usedcars(primitive): " + usedcars_primitive_stats)
    println("usedcars(refined)  : " + usedcars_refined_stats)
    println("airport(primitive): " + airport_primitive_stats)
    println("airport(refined)  : " + airport_refined_stats)
    println("transit(primitive): " + transit_primitive_stats)
    println("transit(refined)  : " + transit_refined_stats)
    println("credit(primitive): " + credit_primitive_stats)
    println("credit(refined)  : " + credit_refined_stats)
    println("\u001B[0m")
    println("Finished")
  }
}