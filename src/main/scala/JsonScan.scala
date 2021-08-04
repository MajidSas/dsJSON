package edu.ucr.cs.bdlab


import org.apache.spark.sql.connector.read.{Scan, Batch}
import org.apache.spark.sql.types.StructType
 import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.sources.Filter


class JsonScan(val schema : StructType, val options :  JsonOptions, val filters : Array[Filter]) extends Scan {

    override def readSchema(): StructType = {
        schema
    }

    override def description(): String = {
        "Some description... query path ??" // TODO fix description
    }

    

    override def toBatch(): Batch = {
        
        return new JsonBatch(schema, options, filters)
    }
}