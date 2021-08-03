package edu.ucr.cs.bdlab

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.read.InputPartition
import scala.collection.immutable.HashMap
class JsonOptions() extends Serializable {
  var filepath = "" // could be a file, a directory, or a GlobPattern
  var jsonpath = "" // JSONPath query string
  var pathGlobFilter = ""
  var recursive = "" 
  var partitioningStrategy = "speculation" // or "fullPass"
  var schemaBuilder = "start" // or "fullPass"
  var encoding = "UTF-8"
  var encounteredTokens: HashMap[String, Set[(Int,Int)]] = _
  var filePaths : Seq[String] = _
  var partitions : Array[InputPartition] = _
  def init(options : CaseInsensitiveStringMap) {
    this.filepath = options.get("path")
    this.jsonpath = options.get("jsonpath")
    if(options.get("pathGlobFilter") != null)
        this.pathGlobFilter = options.get("pathGlobFilter")
    if(options.get("recursiveFileLookup") != null)
        this.recursive = options.get("recursiveFileLookup")
    if(options.get("partitioningStrategy") != null)
        this.partitioningStrategy = options.get("partitioningStrategy")
    if(options.get("schemaBuilder") != null)
        this.schemaBuilder = options.get("schemaBuilder")
    if(options.get("encoding") != null)
      this.encoding = options.get("encoding")
  }
  def getDFA(): DFA = {
    val pathTokens = PathProcessor.build(this.jsonpath);
    return new DFA(pathTokens);;
  }

}
