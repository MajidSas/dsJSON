package edu.ucr.cs.bdlab

import org.apache.spark.sql.connector.read.{
  Batch,
  PartitionReaderFactory,
  InputPartition
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.beast.sql.GeometryUDT
import scala.util.Try
import org.apache.spark.sql.sources.Filter


class JsonBatch(
    val schema: StructType,
    val options: JsonOptions,
    val filters: Array[Filter]
) extends Batch {

  override def createReaderFactory(): PartitionReaderFactory = {
    return new JsonPartitionReaderFactory(schema, options, filters);
  }

  override def planInputPartitions(): Array[InputPartition] = {
    createPartitions();
  }

  def createPartitions() : Array[InputPartition] = {
    return options.partitions
  }


}
