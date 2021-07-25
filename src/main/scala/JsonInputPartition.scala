package edu.ucr.cs.bdlab

import org.apache.spark.sql.connector.read.{InputPartition}


class JsonInputPartition(val path : String, val start : Long, val end : Long, val startLevel : Int, val dfaState : Int) extends InputPartition {
    // override def preferredLocations() : Array[String] = {
    //     Array[String]("localhost")
    // }
}