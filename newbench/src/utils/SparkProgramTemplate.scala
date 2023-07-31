
package utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

trait SparkProgramTemplate { 
    def execute(input1: RDD[String]): RDD[String] = {null}
    def execute(input1: RDD[String], input2: RDD[String]): RDD[String] = {null}
    def execute(input1: RDD[String], input2: RDD[String], input3: RDD[String]): RDD[String] = {null}
}