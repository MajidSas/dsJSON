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

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.SparkContext
import org.apache.spark.beast.sql.GeometryUDT
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

object SchemaInference {


  def getEncounteredTokens(
      options: JsonOptions,
      schema: StructType
  ): HashMap[String, Set[(Int, Int, Int)]] = {
    // extract token levels from query, encountered tokens, and schema
    var dfa = options.getDFA()
    var tokenLevels = HashMap[String, Set[(Int, Int, Int)]]()
    var maxQueryLevel = dfa.states.length
    for (token <- dfa.getTokens()) {
      for (state <- dfa.getTokenStates(token)) {
        tokenLevels = Parser.addToken(tokenLevels, token, state, state)
      }
    }
    return Parser.mergeMapSet(tokenLevels, options.encounteredTokens)

  }

  def getArrayType(
      t: Any,
      nullToString: Boolean = false,
      detectGeometry: Boolean = false
  ): DataType = {
    if (t.isInstanceOf[HashMap[_, _]]) {
      return mapToStruct(
        t.asInstanceOf[HashMap[String, Any]],
        nullToString,
        detectGeometry
      )
    } else {
      val (subT, _subT2) = t.asInstanceOf[(DataType, Any)]
      if (subT.isInstanceOf[ArrayType]) {
        return ArrayType(getArrayType(_subT2))
      } else {
        return if (nullToString && subT.isInstanceOf[NullType]) { StringType }
        else { subT }
      }
    }
  }
  def mapToStruct(
      m: HashMap[String, Any],
      nullToString: Boolean = false,
      detectGeometry: Boolean = false,
      noComplexNesting: Boolean = true, // converts big nested fields to string
      maxSubFields: Int = 100
  ): StructType = {
    var schema = new StructType()
    for ((k, v) <- m) {
      if(k == "geometry" && v.isInstanceOf[HashMap[_, _]] &&
        v.asInstanceOf[HashMap[String, Any]].contains("coordinates")) {
        schema = schema.add(
          StructField(k, new GeometryUDT(), true)
        )
      }
      else if (v.isInstanceOf[HashMap[_, _]]) {
        val t = v.asInstanceOf[HashMap[String, Any]]
        if (noComplexNesting && t.size > maxSubFields) {
          schema = schema.add(
            StructField(k, StringType, true)
          )
        } else {
            schema = schema.add(
            StructField(k, mapToStruct(t, nullToString, detectGeometry), true)
          )
        }
        
      } else {
        val (t, _subT) = v.asInstanceOf[(DataType, Any)]
        if (t.isInstanceOf[ArrayType]) {
          schema = schema.add(
            StructField(
              k,
              ArrayType(getArrayType(_subT, nullToString, detectGeometry)),
              true
            )
          )
        } else {
          val _t = if (nullToString && t.isInstanceOf[NullType]) { StringType }
          else { t }
          schema = schema.add(StructField(k, _t, true))
        }
      }

    }
    return schema
  }

  def selectType(t1: DataType, t2: DataType): DataType = {
    // resolves type conflicts when merging
    if (t1 == t2 || t2 == NullType) {
      return t1
    } else if (t1 == NullType) {
      return t2
    } else if ((t1 == DoubleType && t2 == LongType) || (t1 == LongType && t2 == DoubleType)) {
      return DoubleType
    } else {
      return t1
    }
  }

  def reduceKey(from1: (String, Any), from2: (String, Any)): (String, Any) = {
    val (k, v1) = from1
    val (_, v2) = from2

    if (v1.isInstanceOf[HashMap[_, _]]) {
      val t1 = v1.asInstanceOf[HashMap[String, Any]]
      if (v2.isInstanceOf[HashMap[_, _]]) {
        val t2 = v2.asInstanceOf[HashMap[String, Any]]
        return (k, t1.merged(t2)(reduceKey))
      } else {
        return (k, v1)
      }
    } else if (v2.isInstanceOf[HashMap[_, _]]) {
      return (k, v2)
    }

    val (_t1, _subT1) = v1.asInstanceOf[(Any, Any)]
    val (_t2, _subT2) = v2.asInstanceOf[(Any, Any)]
    if (_t1.isInstanceOf[ArrayType]) {
      if (_t2.isInstanceOf[ArrayType]) {
        val (_, newT) = reduceKey((k, _subT1), (k, _subT2))
        return (k, (ArrayType(NullType), newT))
      } else {
        return (k, v1)
      }
    } else if (_t2.isInstanceOf[ArrayType]) {
      return (k, v2)
    } else {
      val t1 = _t1.asInstanceOf[DataType]
      val t2 = _t2.asInstanceOf[DataType]
      return (k, (selectType(t1, t2), null))
    }
  }
  def inferOnPartition(
      partition: JsonInputPartition,
      limit: Int,
      useWhole: Boolean,
      getTokens: Boolean,
      jsonOptions: JsonOptions
  ): (HashMap[String, Any], HashMap[String, Set[(Int, Int, Int)]], Int) = {
    var parsedRecords = new ArrayBuffer[Any];
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int)]]()
    var dfa = jsonOptions.getDFA()
    val (inputStream: FSDataInputStream, fileSize: Long) =
      Parser.getInputStream(partition.path, jsonOptions.hdfsPath)
    val end = if (partition.end == -1) { fileSize }
    else { partition.end }
    val reader = Parser.getBufferedReader(
      inputStream,
      jsonOptions.encoding,
      partition.start
    )
    dfa = jsonOptions.getDFA()
    var syntaxStackArray = Parser.initSyntaxStack(dfa, partition.startLevel, partition.initialState)
    var stackPos = syntaxStackArray.size-1
    var maxStackPos = stackPos
    dfa.setState(partition.dfaState)
    val lastQueryToken = dfa.states.last.value
    var pos: Long = partition.start
    var found: Boolean = true
    var mergedMaps = new HashMap[String, Any]()
    var count = 0
    while ((count < limit || useWhole) && found) {
      val (_found, value, recordEncounteredTokens, newPos, _stackPos, _maxStackPos, _) =
        Parser.getNextMatch(
          reader,
          jsonOptions.encoding,
          partition.start,
          end,
          pos,
          syntaxStackArray,
          stackPos,
          maxStackPos,
          dfa,
          getTokens,
          true // parse type
        )
      found = _found
      stackPos = _stackPos
      maxStackPos = _maxStackPos
      count += 1
      if (value != null) {
        val _value = if (value.isInstanceOf[HashMap[_, _]]) {
          value.asInstanceOf[HashMap[String, Any]]
        } else {
          new HashMap[String, Any] + (lastQueryToken -> value)
        }
        mergedMaps = mergedMaps.merged(_value)(reduceKey)

        encounteredTokens =
          Parser.mergeMapSet(encounteredTokens, recordEncounteredTokens)
        encounteredTokens = Parser.mergeMapSet(
                encounteredTokens,
                Parser.getEncounteredTokens(
                  value,
                  dfa.currentState,
                  dfa.currentState
                )
              )
      }
      pos = newPos
    }

    reader.close()
    inputStream.close()

    return (mergedMaps, encounteredTokens, count)
  }
  def inferUsingStart(jsonOptions: JsonOptions): StructType = {
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int)]]()
    var mergedMaps = new HashMap[String, Any]()
    var dfa = jsonOptions.getDFA()
    val filePaths = jsonOptions.filePaths
    val limit = 1000
    var nParsedRecords = 0
    var i = 0
    while (nParsedRecords < limit && i < filePaths.size) {
      val partition = new JsonInputPartition(filePaths(i), 0, -1, 0, 0)
      val (schemaMap, _encounteredTokens, _nParsedRecords) =
        inferOnPartition(
          partition,
          limit - nParsedRecords,
          false,
          true,
          jsonOptions
        )
      mergedMaps = mergedMaps.merged(schemaMap)(reduceKey)
      nParsedRecords += _nParsedRecords
      encounteredTokens =
        Parser.mergeMapSet(encounteredTokens, _encounteredTokens)
      i += 1
    }

    val schema =
      mapToStruct(mergedMaps, nullToString = true, detectGeometry = true)

    jsonOptions.encounteredTokens = encounteredTokens

    return schema
  }

  def fullInference(jsonOptions: JsonOptions): StructType = {
    var dfa = jsonOptions.getDFA()
    val partitions = jsonOptions.partitions
    val sc = SparkContext.getOrCreate()
    val stageOutput = sc
      .makeRDD(partitions.map(p => (p, p.preferredLocations())))
      .map(partition =>
        inferOnPartition(
          partition._1.asInstanceOf[JsonInputPartition],
          -1,
          true,
          false,
          jsonOptions
        )
      )
      .collect()

    var mergedMaps = new HashMap[String, Any]()
    for (elem <- stageOutput) {
      val (schemaMap, _encounteredTokens, _nParsedRecords) =
        elem.asInstanceOf[
          (HashMap[String, Any], HashMap[String, Set[(Int, Int, Int)]], Int)
        ]
      mergedMaps = mergedMaps.merged(schemaMap)(reduceKey)
    }

    var schema =
      mapToStruct(mergedMaps, nullToString = true, detectGeometry = true)
    return schema
  }

}
