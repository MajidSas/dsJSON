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

    if(_jsonOptions.filePaths == null) {
      _jsonOptions.filePaths = Partitioning.getFilePaths(_jsonOptions)
    }
    // do partitioning here and serialize for later use
    // createPartitions in JsonBatch is called twice by Spark for some reason
   
    if (_jsonOptions.partitions == null) {
        println("Creating partitions with a full pass...")
        _jsonOptions.partitions = Partitioning.fullPass(_jsonOptions)
    }  else if(_jsonOptions.partitioningStrategy.equals("speculation")) {
        println("Creating partitions with speculation...")
        _jsonOptions.partitions =
          Partitioning.speculation(_jsonOptions)
      }

    return new JsonScanBuilder(_schema, _jsonOptions)
  }

  override def name(): String = {
    "JSON_..." // TODO fix name
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
