package edu.ucr.cs.bdlab

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.UDTRegistration
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.DataFrame

object Main {
  def main(args: Array[String]): Unit = {
    val (input, jsonPath, partitioningStrategy) = (args(0), args(1), args(2))
    val conf = new SparkConf()
//      .setMaster(server)
      .set("spark.sql.files.maxPartitionBytes", "134217728") // 128MB
      .set("spark.hadoop.dfs.block.size", "134217728")
      .set(
        "spark.hadoop.mapreduce.input.fileinputformat.split.minsize",
        "134217728"
      )
      .set(
        "spark.hadoop.mapreduce.input.fileinputformat.split.maxsize",
        "134217728"
      )
      .set(
        "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive",
        "true"
      )
    //  .set("spark.driver.extraJavaOptions","-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
      .setAppName("json-stream")
    val spark =
      SparkSession.builder().config(conf).appName("json-stream").getOrCreate()

    val pathGlobFilter = "*.geojson"
    val recursive = false
    val schemaBuilder = if(partitioningStrategy.equals("speculation")) {"start"} else { "fullPass" }
    val encoding = "UTF-8"
    val df = JsonStream.load(
      input,
      pathGlobFilter,
      recursive,
      jsonPath,
      partitioningStrategy,
      schemaBuilder,
      encoding
    )

    df.printSchema()
    // println(
    //   "###########################\n\n\nFOUND RECORDS: " +
    //   df.count().toString() + "\n\n\n###########################"
    // )
    // df.summary().show()

    df.createOrReplaceTempView("features")
    val sqlDF = spark.sql("SELECT *  FROM features")
    sqlDF.show()
    // sqlDF.write.format("csv").save("productIds.csv")
  }
}

object JsonStream {
  def load(
      input: String,
      pathGlobFilter: String,
      recursive: Boolean,
      jsonPath: String,
      partitioningStrategy: String,
      schemaBuilder: String,
      encoding: String
  ): DataFrame = {

    // Register the Geometry user-defined data type
    val spark = SparkSession.builder().getOrCreate()
    SparkSQLRegistration.registerUDT

    return spark.read
      .format("edu.ucr.cs.bdlab.JsonSource")
      .option("jsonPath", jsonPath)
      .option("pathGlobFilter", pathGlobFilter)
      .option("recursiveFileLookup", recursive.toString())
      .option("partitioningStrategy", partitioningStrategy)
      .option("schemaBuilder", schemaBuilder)
      .option("encoding", encoding)
      .load(input)
  }
}
