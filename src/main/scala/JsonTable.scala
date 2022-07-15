/*
 * Copyright ...
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

import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.read.ScanBuilder
import java.{util => ju}
import org.apache.spark.sql.connector.catalog.TableCapability
import java.util.HashSet

class JsonTable(val _schema: StructType, val jsonOptions: JsonOptions)
    extends SupportsRead {

  
  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    
    var _jsonOptions = jsonOptions
    if (jsonOptions == null) {
      _jsonOptions = new JsonOptions()
      _jsonOptions.init(options)
    }

    if (_jsonOptions.filePaths == null) {
      _jsonOptions.filePaths = Partitioning.getFilePaths(_jsonOptions)
    }

    // do partitioning here and serialize for later use
    // createPartitions in JsonBatch is called twice by Spark for some reason
    if (_jsonOptions.partitions == null) {
      if(_jsonOptions.partitioningStrategy.equals("fullPass")) {
        println("Creating partitions with a full pass...")
        _jsonOptions.partitions = Partitioning.fullPass(_jsonOptions)
      } else {
        println("Creating partitions with speculation...")
        _jsonOptions.partitions =
          Partitioning.getFilePartitions(_jsonOptions.filePaths, _jsonOptions).toArray
        println(_jsonOptions.partitions.size)
        val _ = SchemaInference.inferUsingStart(_jsonOptions)
        _jsonOptions.partitions = Partitioning.speculation(_jsonOptions)
        _jsonOptions.partitions = Partitioning.speculation(_jsonOptions)
      }
    } else if (_jsonOptions.partitioningStrategy.equals("speculation")) {
      println("Creating partitions with speculation...")
      _jsonOptions.partitions = Partitioning.speculation(_jsonOptions)
    }
    
    return new JsonScanBuilder(_schema, _jsonOptions)
  }

  override def name(): String = {
    "JSONDSP"
  }

  override def schema(): StructType = {
    _schema
  }

  override def capabilities(): ju.Set[TableCapability] = {
    // if(this.capabilities == null) {
    // this.capabilities = new HashSet<>();
    var cap =
      new HashSet[org.apache.spark.sql.connector.catalog.TableCapability]();
    cap.add(TableCapability.BATCH_READ);
    // capabilities.add(TableCapability.BATCH_WRITE);
    // }
    return cap;
  }
}
