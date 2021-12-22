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

import org.apache.spark.sql.connector.read.{
  Batch,
  PartitionReaderFactory,
  InputPartition
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import scala.util.Try
import org.apache.spark.sql.sources.Filter


class JsonBatch(
    val schema: StructType,
    val options: JsonOptions,
) extends Batch {

  override def createReaderFactory(): PartitionReaderFactory = {
    return new JsonPartitionReaderFactory(schema, options);
  }

  override def planInputPartitions(): Array[InputPartition] = {
    createPartitions();
  }

  def createPartitions() : Array[InputPartition] = {
    return options.partitions
  }


}
