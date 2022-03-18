/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ucr.cs.bdlab
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val (count, hdfsPath, input, jsonPath, partitioningStrategy, sqlFilter) = (args(0), args(1), args(2), args(3), args(4), args(5))
    val extraFields = if (args.length > 6 && args(6) == "extraFields" || args.length > 7 && args(7) == "extraFields") { true } else { false }
    val keepIndex = if(args.length > 6 && args(6) == "keepIndex" || args.length > 7 && args(7) == "keepIndex" ) { true } else { false }
    val spark =
      SparkSession.builder().appName("jsondsp").getOrCreate()
      
    val pathGlobFilter = ""
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
      extraFields,
      keepIndex,
      encoding,
      hdfsPath,
    )

    df.printSchema()

    df.createOrReplaceTempView("table")
    val sqlDF = spark.sql("SELECT *  FROM table WHERE " + sqlFilter)
    // sqlDF.describe().show()

    
    if(count == "count") {
      println(
        "###########################\n\n\nFOUND RECORDS: " +
        sqlDF.count().toString() + "\n\n\n###########################"
      )
//      println(sqlDF.take(2).toString)
    } else {
      sqlDF.take(2).foreach(row => {println(row.toString())})
    }
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
      extraFields: Boolean,
      keepIndex: Boolean,
      encoding: String,
      hdfsPath: String = "local",
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
      .option("extraFields", extraFields.toString())
      .option("keepIndex", keepIndex.toString())
      .option("encoding", encoding)
      .option("hdfsPath", hdfsPath)
      .load(input)
  }
}
