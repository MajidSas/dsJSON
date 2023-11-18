package edu.ucr.cs.bdlab

import org.apache.spark.sql.SparkSession

object Main {


  def main(args: Array[String]): Unit = {
    val (count, server, input, sqlFilter) = (args(0), args(1), args(2), args(3))
    val spark =
      SparkSession.builder().appName("spark-json-reader").getOrCreate()
    val df = spark.read
      .format("json")
      .load(input)
    df.printSchema()
    df.createOrReplaceTempView("table")
    val sqlDF = spark.sql("SELECT *  FROM table WHERE " + sqlFilter)
    if (count == "count") {
      println(
        "###########################\n\n\nFOUND RECORDS: " +
          sqlDF.count().toString() + "\n\n\n###########################"
      )
    } else {
      sqlDF.foreach(row => {})
    }
  }
}
