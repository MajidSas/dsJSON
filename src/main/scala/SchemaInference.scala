/*
 * Copyright ...
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

import org.apache.spark.SparkContext
import org.apache.spark.beast.sql.GeometryUDT
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

object SchemaInference {


//  def getEncounteredTokens(
//      options: JsonOptions,
//      schema: StructType
//  ): HashMap[String, Set[(Int, Int, Int, List[Char])]] = {
//    // extract token levels from query, encountered tokens, and schema
//    val parser = new Parser()
//    var pda = options.getPDA()
//    var tokenLevels = HashMap[String, Set[(Int, Int, Int)]]()
//    var maxQueryLevel = pda.states.length
//    for (token <- pda.getTokens()) {
//      for (state <- pda.getTokenStates(token)) {
//        tokenLevels = parser.addToken(tokenLevels, token, state, state)
//      }
//    }
//    return parser.mergeMapSet(tokenLevels, options.encounteredTokens)
//
//  }

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
      if(t.isInstanceOf[(DataType, Any)]) {
        val (subT, _subT2) = t.asInstanceOf[(DataType, Any)]
        if (subT.isInstanceOf[ArrayType]) {
          return ArrayType(getArrayType(_subT2, nullToString, detectGeometry))
        } else {
          return if (nullToString && subT.isInstanceOf[NullType]) { StringType }
          else { subT }
        }
      } else {
        if (nullToString && t.isInstanceOf[NullType]) { StringType }
        else { t.asInstanceOf[DataType] }
      }
    }
  }
  def mapToStruct(
      m: HashMap[String, Any],
      nullToString: Boolean = false,
      detectGeometry: Boolean = false,
      noComplexNesting: Boolean = false, // converts big nested fields to string
      maxSubFields: Int = 1000
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
//      println("WARNING: conflicting data types will be parsed as strings (" + t1.toString + "," + t2.toString + ")")
      return StringType
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

    val (_t1, __subT1) = v1.asInstanceOf[(Any, Any)]
    val (_t2, __subT2) = v2.asInstanceOf[(Any, Any)]
    val _subT1 = if(__subT1.isInstanceOf[DataType]) {
      (__subT1, null)
    } else {  __subT1 }

    val _subT2 = if(__subT2.isInstanceOf[DataType]) {
      (__subT2, null)
    } else {  __subT2 }

    if (_t1.isInstanceOf[ArrayType]) {
      if (_t2.isInstanceOf[ArrayType]) {
        val newT = reduceKey((k, _subT1), (k, _subT2))._2
        if(newT.isInstanceOf[HashMap[_,_]]) {
          return (k, (ArrayType(NullType), newT))
        }
        else {
          val _newT = newT.asInstanceOf[(Any, Any)]
          if(_newT._2 == null) {
            return (k, (ArrayType(NullType), _newT._1))
          } else {
            return (k, (ArrayType(NullType), _newT))
          }
        }
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

  def getOutputNodeName(projection : ProjectionNode, name : String = "*") : String = {
    if(projection.isOutputNode) {
      return name
    }
    for((k,v) <- projection.childrenTree) {
        return getOutputNodeName(v, k)
    }
    for((k,v) <- projection.descendantsTree) {
      return getOutputNodeName(v, k)
    }
    return name // correct projection tree wouldn't reach this statement
  }
  def inferOnPartition(
      partition: JsonInputPartition,
      limit: Int,
      useWhole: Boolean,
      getTokens: Boolean,
      getTokensOnly: Boolean,
      jsonOptions: JsonOptions
  ): (HashMap[String, Any], HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]], Int) = {
    var parsedRecords = new ArrayBuffer[Any];
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]]()

    val projection = jsonOptions.getProjectionTree()("*")

    val parser : Parser = new Parser(
      partition.path,
      jsonOptions.hdfsPath,
      jsonOptions.encoding,
      jsonOptions.getPDA(),
      partition.start,
      partition.end
    )
    parser.initSyntaxStack(partition.initialState)
    parser.pda.setState(partition.dfaState)
    parser.pda.setLevels(partition.stateLevels)
    var lastQueryToken = getOutputNodeName(projection)
      if (lastQueryToken == "*")
        lastQueryToken = parser.pda.states.last.value
    var found: Boolean = true
    var mergedMaps = new HashMap[String, Any]()
    var count = 0
    while ((count < limit || useWhole) && found) {
      val (_found, value, recordEncounteredTokens) =
        parser.getNextMatch(
          projection,
          getTokens,
          true, // parse type
          getTokensOnly=getTokensOnly
        )
//      println(value)
      found = _found
      count += 1
      if(getTokens) {
        encounteredTokens = parser.mergeMapSet(encounteredTokens, recordEncounteredTokens)
      }
      if (value != null) {
        val _value = if (value.isInstanceOf[HashMap[_, _]]) {
          value.asInstanceOf[HashMap[String, Any]]
        } else {
          new HashMap[String, Any] + (lastQueryToken -> value)
        }
        mergedMaps = mergedMaps.merged(_value)(reduceKey)

//        encounteredTokens = parser.mergeMapSet(
//                encounteredTokens,
//                parser.getEncounteredTokens(
//                  value,
//                  parser.pda.currentState,
//                  parser.pda.currentState,
//                )
//              )
      }
    }

    parser.close()

    return (mergedMaps, encounteredTokens, count)
  }
  def inferUsingStart(jsonOptions: JsonOptions): StructType = {
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]]()
    var mergedMaps = new HashMap[String, Any]()
    val filePaths = jsonOptions.filePaths
    val limit = 1000
    val getTokens = if(jsonOptions.partitioningStrategy.equals("speculation"))  { true } else { false }
    val getTokensOnly = if(jsonOptions.partitioningStrategy.equals("speculation") && jsonOptions.schemaBuilder.equals("fullPass")) { true } else { false }
    var nParsedRecords = 0
    var i = 0
    while (nParsedRecords < limit && i < filePaths.size) {
      val partition = new JsonInputPartition(filePaths(i), jsonOptions.hdfsPath, 0, -1, 0, 0, List[Int](), List[Char]())
      val (schemaMap, _encounteredTokens, _nParsedRecords) =
        inferOnPartition(
          partition,
          limit - nParsedRecords,
          false,
          getTokens,
          getTokensOnly,
          jsonOptions
        )
      if(!getTokensOnly) {
        mergedMaps = mergedMaps.merged(schemaMap)(reduceKey)
      }
      nParsedRecords += _nParsedRecords
      encounteredTokens = _encounteredTokens
//      encounteredTokens =
//        parser.mergeMapSet(encounteredTokens, _encounteredTokens)
      i += 1
    }

    val schema =
      mapToStruct(mergedMaps, nullToString = true, detectGeometry = true)

    jsonOptions.encounteredTokens = encounteredTokens

    return schema
  }

  def fullInference(jsonOptions: JsonOptions): StructType = {
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
