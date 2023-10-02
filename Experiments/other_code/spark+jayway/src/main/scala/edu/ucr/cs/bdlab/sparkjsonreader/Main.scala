package edu.ucr.cs.bdlab.sparkjsonreader

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import com.jayway.jsonpath.JsonPath.parse;
import java.util.LinkedHashMap
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import java.util.LinkedList
object Main {

  def main(args: Array[String]): Unit = {
    val (count, server, input, jsonPath) = (args(0), args(1), args(2), args(3))

    val spark =
      SparkSession.builder().appName("spark-jayway").getOrCreate()

    val rdd=spark.sparkContext.textFile(input)
    .flatMap(r => {
      if(r.size > 10) { // avoids empty lines
        val s = r.toString;
        val output : Any = parse(s).read(jsonPath);
        if (output != null && output.isInstanceOf[net.minidev.json.JSONArray]) {
          var arr = new ArrayBuffer[LinkedHashMap[String, Any]]
          val j_arr = output.asInstanceOf[net.minidev.json.JSONArray].toArray()
          for(item <- j_arr) {
            val m = item.asInstanceOf[LinkedHashMap[String, Any]]
            arr.append(m)
          }
          arr.toList
        } else if(output != null) {
          val m = output.asInstanceOf[LinkedHashMap[String, Any]]
          List(m)
        } else {
          List()
        }
      } else {
        List()
      }
    })
    println(
        "###########################\n\n\nFOUND RECORDS: " +
        rdd.count().toString() + "\n\n\n###########################"
    )

    
  }
}


