package edu.ucr.cs.bdlab

import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.StructFilters
import org.apache.spark.sql.types._
import scala.collection.immutable.HashMap

class JsonScanBuilder(val schema : StructType, val options :  JsonOptions) extends ScanBuilder  with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
    private var _requiredSchema: StructType = null
    private var _pushedFilters: Array[Filter] = Array.empty

    def schemaToRowMap(dataType: DataType): HashMap[String, (Int, DataType, Any)] = {
    var rowMap: HashMap[String, (Int, DataType, Any)] = new HashMap[String, (Int, DataType, Any)]()
    var index = 0
    if (dataType.isInstanceOf[StructType]) {
      val structType = dataType.asInstanceOf[StructType]
      for (field <- structType.iterator) {
        // if(!field.dataType.isInstanceOf[NullType]) {
        rowMap += (field.name -> (index, field.dataType, schemaToRowMap(field.dataType)))
        index += 1
        // }
      }
    } else if (dataType.isInstanceOf[ArrayType]) {
      return schemaToRowMap(dataType.asInstanceOf[ArrayType].elementType)
    } else {
      return null
    }

    return rowMap
  }
    override def build(): Scan = {
        // println(SparkSession.builder.getOrCreate().sessionState.conf.getAllConfs)
        // println(SparkSession.builder.getOrCreate().sessionState.conf.jsonFilterPushDown)
        options.rowMap = schemaToRowMap(_requiredSchema)
        println(options.rowMap)
        return new JsonScan(_requiredSchema, options, _pushedFilters)
    }


  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    
    if (SparkSession.builder.getOrCreate().sessionState.conf.jsonFilterPushDown) {
      _pushedFilters = StructFilters.pushedFilters(filters, schema)
      println("FILTERS")
      println(_pushedFilters)
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  def pruneColumns(requiredSchema: StructType): Unit = {
    println("REQUIRED SCHEMA")
    println(requiredSchema)
    _requiredSchema = requiredSchema
    // requiredSchema
  }

}

