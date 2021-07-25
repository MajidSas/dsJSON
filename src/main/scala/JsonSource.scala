package edu.ucr.cs.bdlab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.{util => ju}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.beast.sql.GeometryUDT
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.hadoop.fs.FSDataInputStream
import scala.annotation.meta.field
import java.lang.reflect.Field

class JsonSource extends TableProvider with DataSourceRegister {
  var jsonOptions: JsonOptions = null

  override def shortName(): String = "json-parallel-stream"
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if (options != null) {
      val keys = options.keySet();
      println("Options keys: " + keys)
      jsonOptions = new JsonOptions()
      jsonOptions.init(options)
    }

    jsonOptions.filePaths = Partitioning.getFilePaths(jsonOptions)

    println("Inferring scheme...")

    if (jsonOptions.schemaBuilder.equals("fullPass")) {
      println("Creating partitions with a full pass ...")
      jsonOptions.partitions = Partitioning.fullPass(jsonOptions)
      return SchemaInference.fullInference(jsonOptions)
    } else {
      jsonOptions.partitions =
        Partitioning.getFilePartitions(jsonOptions.filePaths).toArray
      return SchemaInference.inferUsingStart(jsonOptions)
    }
  }


  
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: ju.Map[String, String]
  ): Table = {
    return new JsonTable(schema, jsonOptions);
  }

}
