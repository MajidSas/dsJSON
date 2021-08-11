package edu.ucr.cs.bdlab

import org.apache.spark.sql.connector.read.{PartitionReaderFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.sources.Filter


class JsonPartitionReaderFactory(val schema : StructType, val options :  JsonOptions) extends PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
        return new JsonPartitionReader(partition.asInstanceOf[JsonInputPartition], schema, options)
    }
}