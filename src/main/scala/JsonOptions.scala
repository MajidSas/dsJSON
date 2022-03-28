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

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.read.InputPartition
import scala.collection.immutable.HashMap
import org.apache.spark.sql.types.DataType

class JsonOptions() extends Serializable {
  // TODO: more options to expose include -> 
  //      - schema inference limits (e.g. max records to infer on,
  //                                percentage of data to infer on,
  //                                limits of nested objects,
  //                                 use filters for inference),
  //      - speculation limits (e.g. max number of keys to speculate on,
  //                                 when to cancel speculation [i.e. parsed 25% of partition and didn't find a key] )
  //      - other options may include how to handle keys that exist in a filter but don't exist in a record
  //          this could be a boolean to keep those records or discard them
  
  var filepath = "" // could be a file, a directory, or a GlobPattern
  var jsonpath = "" // JSONPath query string
  var filterString = "" // filters extracted from jsonPath and SQL query
  var pathGlobFilter = ""
  var recursive = "" 
  var partitioningStrategy = "speculation" // or "fullPass"
  var schemaBuilder = "start" // or "fullPass"
  var extraFields = false // adds additional string column to store attributes not in schema
  var keepIndex = false
  var encoding = "UTF-8"
  var hdfsPath = "local"
  var encounteredTokens: HashMap[String, Set[(Int,Int,Int, List[Int], List[Char])]] = _
  var speculationKeys: HashMap[String, (Int,Int,Int, List[Int], List[Char])] = _
  var filePaths : Seq[String] = _
  var partitions : Array[InputPartition] = _
  var rowMap: HashMap[String, (Int, DataType, Any)] = _
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
    if(options.get("extraFields") != null)
      this.extraFields = if(options.get("extraFields") == "true") { true } else { false }
    if(options.get("keepIndex") != null) {
      this.keepIndex =  if(options.get("keepIndex") == "true") { true } else { false }
    }
    if(options.get("encoding") != null)
      this.encoding = options.get("encoding")
    if(options.get("hdfsPath") != null)
      this.hdfsPath  = options.get("hdfsPath")
  }

  def getProjectionTree(): HashMap[String,ProjectionNode] = {
    PathProcessor.build(this.jsonpath)
  }
  def getPDA(): PDA = {
    new PDA(PathProcessor.getPDAPath(this.jsonpath))
  }

  def setFilter(sqlFilter : String) = {
    // use an array of strings, but the sql fitler would only be added to the last one
    // or kept seperatly and added later as a subtree of its own
//    val (pathTokens, pathFilters) = PathProcessor.build(this.jsonpath);
//    var dfa = new DFA(pathTokens)
//    filterString = if(dfa.states.last.stateType == "array" && pathFilters.size > 0) {
//      "(" + pathFilters.last + ")"
//    } else { "" }
//
//    if(sqlFilter.trim() != "" && filterString != "") {
//      filterString += " && "
//    }
//    if(sqlFilter.trim() != "")
//    filterString += "(" + sqlFilter + ")"
  }

}
