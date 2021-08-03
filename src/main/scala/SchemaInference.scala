package edu.ucr.cs.bdlab

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.beast.sql.GeometryUDT
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.hadoop.fs.FSDataInputStream
import scala.annotation.meta.field
import java.lang.reflect.Field
import scala.collection.immutable.HashMap
import org.apache.hadoop.util.hash.Hash
import javax.xml.crypto.Data
object SchemaInference {

  def getTokenLevels(
      schema: StructType,
      level: Int,
      dfaState: Int
  ): HashMap[String, Set[(Int, Int)]] = {
    var m: HashMap[String, Set[(Int, Int)]] = HashMap[String, Set[(Int, Int)]]()
    var m2: HashMap[String, Set[(Int, Int)]] = null
    for (field <- schema.iterator) {
      if (field.dataType.isInstanceOf[StructType]) {
        m2 = getTokenLevels(
          field.dataType.asInstanceOf[StructType],
          level + 1,
          dfaState
        )
      } else if (
        field.dataType.isInstanceOf[ArrayType] &&
        field.dataType
          .asInstanceOf[ArrayType]
          .elementType
          .isInstanceOf[StructType]
      ) {
        m2 = getTokenLevels(
          field.dataType
            .asInstanceOf[ArrayType]
            .elementType
            .asInstanceOf[StructType],
          level + 2,
          dfaState
        )
      } else if (field.dataType.isInstanceOf[GeometryUDT]) {
        m2 = getTokenLevels(
          field.dataType
            .asInstanceOf[GeometryUDT]
            .sqlType,
          level + 1,
          dfaState
        )
      }

      if (m2 != null) {
        m = Parser.mergeMapSet(m, m2)
      }
      m = Parser.addToken(m, field.name, level, dfaState)
    }
    m
  }

  def getEncounteredTokens(
      options: JsonOptions,
      schema: StructType
  ): HashMap[String, Set[(Int, Int)]] = {
    // extract token levels from query, encountered tokens, and schema
    var dfa = options.getDFA()
    var tokenLevels = HashMap[String, Set[(Int, Int)]]()
    var maxQueryLevel = dfa.states.length
    for (token <- dfa.getTokens()) {
      for (state <- dfa.getTokenStates(token)) {
        tokenLevels = Parser.addToken(tokenLevels, token, state, state)
      }
    }
    val lastQueryToken = dfa.states.last.value;
    val schemaHasLastToken = Try(schema.apply(lastQueryToken)).isSuccess
    val isArrayLastToken = if (schemaHasLastToken && schema.length == 1) {
      schema.apply(lastQueryToken).dataType.isInstanceOf[ArrayType]
    } else {
      false
    }
    if (
      !(schemaHasLastToken && schema.length == 1
        && !Parser.hasInnerStruct(schema.apply(lastQueryToken).dataType))
    ) {
      val _schema = if (isArrayLastToken) {
        schema
          .apply(lastQueryToken)
          .dataType
          .asInstanceOf[ArrayType]
          .elementType
          .asInstanceOf[StructType]
      } else {
        schema
      }
      val tokenLevelsSchema =
        getTokenLevels(_schema, maxQueryLevel + 1, maxQueryLevel)
      tokenLevels = Parser.mergeMapSet(tokenLevels, tokenLevelsSchema)
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
      detectGeometry: Boolean = false
  ): StructType = {
    var schema = new StructType()
    for ((k, v) <- m) {
      if (v.isInstanceOf[HashMap[_, _]]) {
        val t = v.asInstanceOf[HashMap[String, Any]]
        schema = schema.add(
          StructField(k, mapToStruct(t, nullToString, detectGeometry), true)
        )
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
    // only replaces NullType or selects the first type if conflict (e.g. one double and one string)
    // when support for IntegerType is added conflict with DoubleType can be resolved (along with other type conflicts)
    if (t1 == t2 || t2 == NullType) {
      return t1
    } else if (t1 == NullType) {
      return t2
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
  ): (HashMap[String, Any], HashMap[String, Set[(Int, Int)]], Int) = {
    var parsedRecords = new ArrayBuffer[Any];
    var encounteredTokens = HashMap[String, Set[(Int, Int)]]()
    var dfa = jsonOptions.getDFA()
    val (inputStream: FSDataInputStream, fileSize: Long) =
      Parser.getInputStream(partition.path)
    val end = if (partition.end == -1) { fileSize }
    else { partition.end }
    val reader = Parser.getBufferedReader(
      inputStream,
      jsonOptions.encoding,
      partition.start
    )
    dfa = jsonOptions.getDFA()
    var syntaxStackArray = Parser.initSyntaxStack(dfa, partition.startLevel)
    dfa.setState(partition.dfaState)
    val lastQueryToken = dfa.states.last.value
    var pos: Long = partition.start
    var found: Boolean = true
    var mergedMaps = new HashMap[String, Any]()
    var count = 0
    while ((count < limit || useWhole) && found) {
      val (_found, value, recordEncounteredTokens, newPos) =
        Parser.getNextMatch(
          reader,
          jsonOptions.encoding,
          0,
          end,
          pos,
          syntaxStackArray,
          dfa,
          getTokens,
          true // parse type
        )
      found = _found
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
      }
      pos = newPos
    }

    println("RECORDS FOUND: " + count)
    reader.close()
    inputStream.close()

    return (mergedMaps, encounteredTokens, count)
  }
  def inferUsingStart(jsonOptions: JsonOptions): StructType = {
    var encounteredTokens = HashMap[String, Set[(Int, Int)]]()
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
    jsonOptions.encounteredTokens = encounteredTokens

    val schema =
      mapToStruct(mergedMaps, nullToString = true, detectGeometry = false)

    encounteredTokens = getEncounteredTokens(jsonOptions, schema)
    jsonOptions.encounteredTokens = encounteredTokens

    return schema
  }

  def fullInference(jsonOptions: JsonOptions): StructType = {
    var dfa = jsonOptions.getDFA()
    val partitions = jsonOptions.partitions
    val sc = SparkContext.getOrCreate()
    val stageOutput = sc
      .parallelize(partitions)
      .map(partition =>
        inferOnPartition(
          partition.asInstanceOf[JsonInputPartition],
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
          (HashMap[String, Any], HashMap[String, Set[(Int, Int)]], Int)
        ]
      mergedMaps = mergedMaps.merged(schemaMap)(reduceKey)
    }

    val schema =
      mapToStruct(mergedMaps, nullToString = true, detectGeometry = false)
    return schema
  }

}
