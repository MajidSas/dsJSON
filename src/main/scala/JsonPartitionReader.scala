/*
 * Copyright ....
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable


class JsonPartitionReader extends PartitionReader[InternalRow] {
  var inputPartition: JsonInputPartition = _
  var schema: StructType = _
  var options: JsonOptions = _
  var parser : Parser = _
  var partitionId : Int = 0
  var keepExtras = false
  var keepIndex = false
  var key: String = ""
  var value: Any = _
  var projection : ProjectionNode = _
  var parserMaxMemSize = 0L

  def this(
      inputPartition: JsonInputPartition,
      schema: StructType,
      options: JsonOptions) {
    this()
    this.inputPartition = inputPartition
    this.schema = schema
    this.options = options
    this.parser = new Parser(
      inputPartition.path,
      options.hdfsPath,
      options.encoding,
      options.getPDA(),
      inputPartition.start,
      inputPartition.end
    )
    projection = options.getProjectionTree()("*")
    val (variables, nPredicates) = if(options.filterString != "") {
       FilterProcessor.parseExpr(options.filterString, options.rowMap).asInstanceOf[(HashMap[String, Variable], Int)]
    } else { (new HashMap[String, Variable](), 0) }
    // assign to outputNode
    var currentNode = projection
    val nodeQueue = new mutable.Queue[ProjectionNode]()
    while(!currentNode.isOutputNode) {
      for((k,v) <- currentNode.childrenTree) {
        nodeQueue.enqueue(v)
      }
      for((k,v) <- currentNode.descendantsTree) {
        nodeQueue.enqueue(v)
      }
      currentNode = nodeQueue.dequeue()
    }
    nodeQueue.clear()

    currentNode.sqlFilterVariables = variables
    currentNode.nSQLPredicates = nPredicates
    currentNode.outputsRowMap = options.rowMap

    rowMapToProjection(currentNode)

    val filePath = inputPartition.path
    // Initialize partition
    partitionId = inputPartition.id
    keepExtras = options.extraFields
    keepIndex = options.keepIndex
    val startLevel = inputPartition.startLevel
    // ^ these values have already been set in previous stages
//    val (_stream, _fileSize) = Parser.getInputStream(filePath, options.hdfsPath)
//    stream = _stream
//    fileSize = _fileSize
//    println(
//      inputPartition.start,
//      inputPartition.end,
//      inputPartition.startLevel,
//      inputPartition.dfaState
//    )
//    println("Filters: "+options.filterString)
//    val (_, _filterVariables, _filterSize) : (Any, HashMap[String, Variable], Int) =  FilterProcessor.parseExpr(options.filterString, options.rowMap)
//    filterVariables = _filterVariables
//    rowMap = _rowMap
//    childTree = _childTree
//    descendantTree = _descendantTree
//    filterSize = _filterSize
    // println(options.encounteredTokens)
    val initialState = inputPartition.initialState
    parser.initSyntaxStack(initialState)
    parser.pda.setState(inputPartition.dfaState)
    parser.pda.setLevels(inputPartition.stateLevels)
//    println(parser.syntaxStackArray)
//    println(parser.pda)
  }

  def rowMapToProjection(projectionNode : ProjectionNode): Unit = {
    for((k,v) <- projectionNode.childrenTree ++ projectionNode.descendantsTree) {
      if(projectionNode.outputsRowMap contains k) {
        val (_, _, subType) = projectionNode.outputsRowMap(k)
        if(subType != null) {
          v.outputsRowMap = subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]]
          rowMapToProjection(v)
        }
      }
    }
  }
  override def next(): Boolean = {

    val (hasNext, _value, _) = parser.getNextMatch(
      projection,
      getTokens = false,
      getTypes = false,
      keepExtras = keepExtras,
      partitionId
    )
    // if(hasNext == false || value == null) {
    //     println(hasNext + " start: " + start + " pos: " + pos + " end: " + end + " count: " + count)
    // }
    value = _value
    if(!hasNext && options.partitioningStrategy.equals("speculation")) {
      val conf = if (options.hdfsPath == "local") {
        new Configuration()
      } else {
        val _conf = new Configuration()
        _conf.set("fs.defaultFS", options.hdfsPath)
        _conf
      }
      val fs = FileSystem.get(conf)
      var path = new Path("./dsJSON_tmp/"+inputPartition.id+"_partition_boundaries.txt")
      val outputStream: FSDataOutputStream = fs.create(path, true)
      var s = inputPartition.dfaState.toString
      for(c <- inputPartition.initialState) {
        s += c.toString
      }
      s+= "\n" + parser.pda.currentState.toString
      for(c <- parser.syntaxStackArray.take(parser.stackPos+1)) {
        s += c.toString
      }
      s+="\n" + inputPartition.speculationAttribute
      outputStream.writeBytes(s)
      outputStream.close()

//      var totalMem : Long = parserMaxMemSize;
//      totalMem += SizeEstimator.estimate(inputPartition)
//      totalMem += SizeEstimator.estimate(schema)
//      totalMem += SizeEstimator.estimate(options)
//      totalMem += SizeEstimator.estimate(projection)
//
//      path = new Path("./dsJSON_tmp/"+inputPartition.id+"_memory.txt")
//      val outputStream2: FSDataOutputStream = fs.create(path, true)
//      println("TOTAL MEM: " + totalMem.toString)
//      outputStream2.writeBytes(totalMem.toString)
//      outputStream2.close()
      return hasNext
    }
//    this.parserMaxMemSize =  Math.max(parserMaxMemSize, SizeEstimator.estimate(cmake -DCMAKE_BUILD_TYPE=Release ..))

    hasNext
  }


  override def get(): GenericInternalRow = {
    value.asInstanceOf[GenericInternalRow]
  }

  override def close() {
    parser.close()
  }
}
