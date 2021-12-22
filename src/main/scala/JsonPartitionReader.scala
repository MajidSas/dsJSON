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

import org.apache.spark.sql.connector.read.{PartitionReader}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import com.fasterxml.jackson.module.scala.deser.overrides
import org.apache.hadoop.fs.FSDataInputStream
import java.io.BufferedReader
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.Filter
import scala.collection.immutable.HashMap


class JsonPartitionReader extends PartitionReader[InternalRow] {
  var inputPartition: JsonInputPartition = null
  var schema: StructType = null
  var options: JsonOptions = null

  var dfa: DFA = _
  var start, end = 0L
  var pos = 0L
  var stream: FSDataInputStream = _
  var reader: BufferedReader = _
  var count = 0L
  var key: String = ""
  var value: Any = null
  var fileSize: Long = 0L;
  var syntaxStackArray: ArrayBuffer[Char] = ArrayBuffer[Char]()
  var stackPos : Int = -1
  var maxStackPos : Int = -1
  var stateLevel = 0;
  var splitPath: String = "";
  var filterVariables : HashMap[String, Variable] = null
  var filterSize : Int = 0
  def this(
      inputPartition: JsonInputPartition,
      schema: StructType,
      options: JsonOptions) {
    this()
    this.inputPartition = inputPartition
    this.schema = schema
    this.options = options


    dfa = options.getDFA()
    val filePath = inputPartition.path
    // Initialize partition
    start = inputPartition.start
    end = inputPartition.end
    val startLevel = inputPartition.startLevel
    // ^ these values have already been set in previous stages
    val (_stream, _fileSize) = Parser.getInputStream(filePath, options.hdfsPath)
    stream = _stream
    fileSize = _fileSize
    println(
      inputPartition.start,
      inputPartition.end,
      inputPartition.startLevel,
      inputPartition.dfaState
    )
    println("Filters: "+options.filterString)
    val (_, _filterVariables, _filterSize) : (Any, HashMap[String, Variable], Int) =  FilterProcessor.parseExpr(options.filterString, options.rowMap)
    filterVariables = _filterVariables
    filterSize = _filterSize
    // println(options.encounteredTokens)
    val initialState = inputPartition.initialState
    syntaxStackArray = Parser.initSyntaxStack(dfa, startLevel, initialState)
    stackPos = syntaxStackArray.size-1
    maxStackPos = stackPos
    dfa.setState(inputPartition.dfaState)

    println(syntaxStackArray)
    println(dfa)

    reader = Parser.getBufferedReader(stream, options.encoding, start)
    pos = start;
  }

  override def next() = {

    val (hasNext, _value, _, newPos, _stackPos, _maxStackPos) = Parser.getNextMatch(
      reader,
      options.encoding,
      start,
      end,
      pos,
      syntaxStackArray,
      stackPos,
      maxStackPos,
      dfa,
      rowMap = options.rowMap,
      filterVariables = filterVariables,
      filterSize = filterSize,
    )
    pos = newPos
    stackPos = _stackPos
    maxStackPos = _maxStackPos
    // count += 1
    // if(hasNext == false || value == null) {
    //     println(hasNext + " start: " + start + " pos: " + pos + " end: " + end + " count: " + count)
    // }
    value = _value
    
    hasNext
  }


  override def get(): GenericInternalRow = {
    value.asInstanceOf[GenericInternalRow]
  }

  override def close() {
    reader.close()
  }
}
