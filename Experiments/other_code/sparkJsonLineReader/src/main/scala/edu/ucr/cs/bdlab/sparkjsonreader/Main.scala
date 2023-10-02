package edu.ucr.cs.bdlab.sparkjsonreader

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object Main {


  def main(args: Array[String]): Unit = {
    val (count, server, input, sqlFilter) = (args(0), args(1), args(2), args(3))
    val spark =
      SparkSession.builder().appName("spark-json-reader").getOrCreate()
    val df = spark.read
      .format("json")
      .schema(schema)
      .load(input)
    df.printSchema()
    df.createOrReplaceTempView("table")
    val sqlDF = spark.sql("SELECT claims.*  FROM table WHERE " + sqlFilter)
    if(count == "count") {
        println(
        "###########################\n\n\nFOUND RECORDS: " +
        sqlDF.count().toString() + "\n\n\n###########################"
      )
    } else {
        sqlDF.foreach(row => {})
    }
  }
}


