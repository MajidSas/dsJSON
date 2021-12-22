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
import org.apache.spark.beast.sql.GeometryUDT

class JsonScanBuilder(val schema : StructType, val options :  JsonOptions) extends ScanBuilder  with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
    private var _requiredSchema: StructType = null
    private var _pushedFilters: Array[Filter] = Array.empty

    def schemaToRowMap(dataType: DataType): HashMap[String, (Int, DataType, Any)] = {
    var rowMap: HashMap[String, (Int, DataType, Any)] = new HashMap[String, (Int, DataType, Any)]()
    var index = 0
    if (dataType.isInstanceOf[StructType]) {
      val structType = dataType.asInstanceOf[StructType]
      for (field <- structType.iterator) {
        if(field.dataType.isInstanceOf[GeometryUDT]) {
          rowMap += (field.name -> (index, field.dataType.asInstanceOf[GeometryUDT].sqlType, schemaToRowMap(field.dataType.asInstanceOf[GeometryUDT].sqlType)))
        } else {
          rowMap += (field.name -> (index, field.dataType, schemaToRowMap(field.dataType)))
        }
        index += 1
      }
    } else if (dataType.isInstanceOf[ArrayType]) {
      return schemaToRowMap(dataType.asInstanceOf[ArrayType].elementType)
    } else {
      return null
    }
    println(rowMap)
    return rowMap
  }
    override def build(): Scan = {
        options.rowMap = schemaToRowMap(_requiredSchema)
        var filterString = ""
        var i = 0
        for(f <- _pushedFilters) {
          filterString += "(" + FilterProcessor.sparkFilterToJsonFilter(f.toString(), options.rowMap) + ")"
          i += 1
          if(i < _pushedFilters.size) {
            filterString += " && "
          }
        }
        options.setFilter(filterString)

        val filterKeys = """(@.\w+)""".r.findAllMatchIn(options.filterString).toList.map(_.toString.substring(2))
        val existingNames = _requiredSchema.fieldNames
        for(key <- filterKeys) {
          if(!(existingNames contains key))
          _requiredSchema = _requiredSchema.add(schema.apply(key))
        }
        options.rowMap = schemaToRowMap(_requiredSchema)

        
        println(_requiredSchema)
        println(options.rowMap)
        println(filterString)
        return new JsonScan(_requiredSchema, options)
    }


  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    
    if (SparkSession.builder.getOrCreate().sessionState.conf.jsonFilterPushDown) {
      _pushedFilters = StructFilters.pushedFilters(filters, schema)
      _pushedFilters.map(f => println(f.toString))
    }
    filters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  def pruneColumns(requiredSchema: StructType): Unit = {
    _requiredSchema = requiredSchema
  }

}

