package edu.ucr.cs.bdlab

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.UDTRegistration
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.DataFrame

/** Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select JPSparkLocalApp when prompted)
  */
object Main {
  def main(args: Array[String]): Unit = {
    val (input, jsonPath, partitioningStrategy) = (args(0), args(1), args(2))
    val conf = new SparkConf()
//      .setMaster(server)
      // .set("spark.sql.files.maxPartitionBytes", "33554432") // 32MB
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
     .set("spark.driver.extraJavaOptions","-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
      .setAppName("json-stream")
    val spark =
      SparkSession.builder().config(conf).appName("json-stream").getOrCreate()

    // val input = "./input/"
    val pathGlobFilter = "*.geojson"
    val recursive = false
    // val jsonPath = "$.products[*]"
    // val partitioningStrategy = "speculation" // "fullPass" 
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
    println(
      "###########################\n\n\nFOUND RECORDS: " +
      df.count().toString() + "\n\n\n###########################"
    )
    // df.summary().show()

    // df.createOrReplaceTempView("products")
    // val sqlDF = spark.sql("SELECT productId  FROM products")
    // sqlDF.show()
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

    // while(files.hasNext()) {
    //   println(files.next.getPath())
    // }
    return spark.read
      .format("edu.ucr.cs.bdlab.JsonSource")
      .option("jsonPath", jsonPath)
      .option("pathGlobFilter", pathGlobFilter)
      .option("recursiveFileLookup", recursive.toString())
      .option("partitioningStrategy", partitioningStrategy)
      .option("schemaBuilder", schemaBuilder)
      .option("encoding", encoding)
      .load(input)
    // .load("./input/*")
    // .option("pathGlobFilter", "*.geojson")
    // .load(inputFile);

    // df.printSchema()
    // df.show();

    // df.filter(row => !row.anyNull).show()

    // println("\n\n\n\n######VALUES COUNT#######\n"+input.values.count())
    // println("#########################\n\n\n\n")

    // val counts = input.mapValues(x => "here"+x.length+x.substring(0,10))
    // counts.collect().foreach(println)
    // for(e <- input.collect()) {
    //   println(e._1.length + ",,,," + e._1.substring(0,10))
    // }
    // println(input.take(5))

    // .saveAsTextFile(outputFile)
    // var path1_tokens = PathProcessor.build("$.features[*].geometry.coordinates");
    // println("$.features[*].geometry.coordinates")
    // var dfa1: DFA = new DFA(path1_tokens);
    // println(dfa1);

    // println("$.products[*].categoryPath[*]")
    // var path2_tokens = PathProcessor.build("$.products[*].categoryPath[*]");
    // var dfa2: DFA = new DFA(path2_tokens);
    // println(dfa2);

    // println("$.pd.shl[1:5].sel[1:2]")
    // var path3_tokens = PathProcessor.build("$.pd.shl[1:5].sel[1:2]");
    // var dfa3: DFA = new DFA(path3_tokens);
    // println(dfa3);
  }
}
