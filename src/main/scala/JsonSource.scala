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

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.{util => ju}

class JsonSource extends TableProvider with DataSourceRegister {
  var jsonOptions: JsonOptions = null

  override def shortName(): String = "jsondsp"
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (options != null) {
      jsonOptions = new JsonOptions()
      jsonOptions.init(options)
    }

    jsonOptions.filePaths = Partitioning.getFilePaths(jsonOptions)

    // TODO: update if statements to handle all possible four choice
    // we only considered two scenarios, but it should work for any choices 
    println("schemaBuilder: " + jsonOptions.schemaBuilder)
    var schema : StructType = null
    if (jsonOptions.schemaBuilder.equals("fullPass")) {
      println("Creating partitions with a full pass ...")
      jsonOptions.partitions = Partitioning.fullPass(jsonOptions)
      println("Inferring schema using whole data...")
      schema = SchemaInference.fullInference(jsonOptions)
    } else {
      println("Inferring schema using start of file...")
      val t0 = System.nanoTime()
      jsonOptions.partitions =
        Partitioning.getFilePartitions(jsonOptions.filePaths, jsonOptions).toArray
      schema = SchemaInference.inferUsingStart(jsonOptions)
      val t1 = System.nanoTime()
      System.err.println("Job -1 finished: collect at SchemaInference.scala:352, took " + (t1-t0)*scala.math.pow(10,-9) + " s")
    }
    if(jsonOptions.extraFields) {
      schema = schema.add(StructField("extra_fields", StringType, true))
    }
    if(jsonOptions.keepIndex) {
      schema = schema.add(StructField("partition_id", LongType, true))
      schema = schema.add(StructField("partition_row_index", LongType, true))
    }
    schema
  }


  override def supportsExternalMetadata() : Boolean = {
    return true // makes it possible to accept user provided schema
  }
  
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    return new JsonTable(schema, jsonOptions);
  }

}
