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

import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import org.apache.hadoop.fs.{Path, FSDataInputStream, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.hadoop.conf.Configuration
import java.io.File
import java.io.BufferedReader
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import scala.collection.immutable.HashMap
import java.lang.StringBuilder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.beast.sql.GeometryUDT

object Parser {
  def getInputStream(filename: String, hdfsPath: String = "local"): (FSDataInputStream, Long) = {
    val conf = if(hdfsPath == "local") {
      SparkContext.getOrCreate().hadoopConfiguration
    } else {
      val _conf = new Configuration()
      _conf.set("fs.defaultFS", hdfsPath)
      _conf
    }
    val fs = FileSystem.get(conf)
    val path = new Path(filename)
    val fileSize: Long = fs.getContentSummary(path).getLength
    val inputStream: FSDataInputStream = fs.open(path)
    return (inputStream, fileSize)
  }

  def getBufferedReader(
      inputStream: FSDataInputStream,
      encoding: String,
      startPos: Long
  ): BufferedReader = {
    // TODO: add support for compressed files
    inputStream.seek(startPos)
    val decoder = Charset.forName(encoding).newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val source = scala.io.Source.fromInputStream(inputStream)(decoder)
    return source.bufferedReader()
  }

  def charSize(i: Int): Int = {
    if (i < 128) return { 1 }
    else if (i < 2048) return { 2 }
    else if (i < 65534) return { 3 }
    else { return 4 }
  }

  def stringSize(s: String, encoding: String = "UTF-8"): Int = {
    var size: Int = 0
    for (c <- s) {
      size += charSize(c.toInt)
    }
    return size
  }

  def initSyntaxStack(
      dfa: DFA,
      level: Int,
      initialState: Array[Char] = null
  ): ArrayBuffer[Char] = {
    // TODO: should be initialized using provided initial path instead
    //       may cause issues for query with multiple descendent types
    var syntaxStackArray: ArrayBuffer[Char] = ArrayBuffer[Char]()
    var i = 0
    if (level > 0) {
      for (i <- 0 to level - 1) {
        if(initialState != null) {
          syntaxStackArray.append(initialState(i))
        }
        else if (dfa.getStates()(i).stateType.equals("array")) {
          syntaxStackArray.append('[')
        } else {
          syntaxStackArray.append('{')
        }
      }
    }
    syntaxStackArray
  }

  def appendToStack(stack : ArrayBuffer[Char], value : Char, stackPos : Int, maxStackPos : Int) : (Int, Int) = {
    // appending is done like this
    // mainly to avoid having to access the last element (linear time)
    // and also to avoid memory re-allocation if possible
    // NOT thoroughly evaluated compared to just using append
    val newPos = stackPos+1
    if (newPos <= maxStackPos) {
      stack(newPos) = value
      return (newPos, maxStackPos)
    } else {
      stack.append(value)
      return (newPos, newPos)
    }
  }

  def isValidString(s: String): Boolean = {
    // TODO: improve this regular expression for readability
    //       and verify it covers all possible cases
    return !s
      .replaceAll(raw"(?i)(false|alse|lse|s|e|true|rue|ue|null|ull|ll|l|NaN|aN|N|Infinity|nfinity|finity|inity|nity|ity|ty|y|[0-9]+|-|\.)", "")
      .matches(raw"[\s+{}\[\],:]+")
  }

  def isWhiteSpace(c: Char): Boolean = {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 13
  }

  def getNextToken(
      reader: BufferedReader,
      encoding: String,
      start: Long,
      end: Long
  ): (String, Int) = {

    // TODO: this function can be improved
    var i: Int = 0
    var accept: Boolean = false
    var index = 0
    var pos = start
    i = reader.read()
    pos += charSize(i)
    var c: Char = i.toChar

    while (i != -1 && pos <= end) {
      while (i != -1 && (c != '"' || (c == '"' && !accept))) {
        if (c == ',' || c == '{') {
          accept = true
        } else if (accept && !isWhiteSpace(c)) {
          accept = false
        }
        i = reader.read()
        pos += charSize(i)
        c = i.toChar
        index += charSize(i)
      }
      if (accept && i != -1) {
        val (token, newPos) = consume(reader, encoding, pos, end, c)
        i = reader.read()

        var t = i.toChar
        var tmpIndex = charSize(i)
        while (isWhiteSpace(t)) {
          i = reader.read()
          t = i.toChar;
          tmpIndex += charSize(i)
        }
        pos = newPos + tmpIndex
        if (t == ':') {
          return (token.substring(1, token.size - 1), index)
        } else {
          index += tmpIndex
        }
      }

    }
    return ("", -1)
  }

  def skipLevels(
      reader: BufferedReader,
      encoding: String,
      levels: Int,
      end: Long
  ): (Int) = {
    var index: Int = 0
    var i: Int = 0
    var c: Char = '0'
    var remainingLevels = levels;
    i = reader.read()
    c = i.toChar
    index += charSize(i)
    while (i != -1) {
      if (c == '{' || c == '[' || c == '"') {
        val pos : Long = skip(reader, encoding, 0, end, c)
        index += pos.toInt
      } else if (c == '}' || c == ']') {
        remainingLevels -= 1
        if (remainingLevels == 0) {
          return index
        }
      }
      i = reader.read()
      c = i.toChar
      index += charSize(i)
    }
    index
  }

  def hasInnerStruct(dataType: DataType): Boolean = {
    var hasStruct = false
    if (dataType.isInstanceOf[ArrayType]) {
      hasStruct = hasInnerStruct(dataType.asInstanceOf[ArrayType].elementType)
    } else if (dataType.isInstanceOf[StructType]) {
      hasStruct = true
    }
    hasStruct
  }

  def addToken(
      tokens: HashMap[String, Set[(Int, Int, Int)]],
      token: String,
      level: Int,
      dfaState: Int,
      count: Int = 1,
  ): HashMap[String, Set[(Int, Int, Int)]] = {
    var _tokens = tokens
    if (tokens contains token) {
      var isAdded = false
      for((a,b,c) <- _tokens(token)) {
        if(a == level && b == dfaState) {
          isAdded = true
          _tokens = _tokens.updated(token, _tokens(token) - ((a,b,c)) + ((a,b,c+count)))
        }
      }
      if(!isAdded) {
        _tokens = _tokens.updated(token, _tokens(token) + ((level,dfaState,count)))
      }
    } else {
      _tokens = _tokens + (token -> Set((level, dfaState, count)))
    }
    _tokens
  }

  def mergeMapSet(
      m1: HashMap[String, Set[(Int, Int, Int)]],
      m2: HashMap[String, Set[(Int, Int, Int)]]
  ): HashMap[String, Set[(Int, Int, Int)]] = {
    var m = m1
    for ((k, v) <- m2) {
      for((a,b,c) <- v) {
        m = addToken(m, k, a, b, c)
      }
    }
    m
  }
  def getEncounteredTokens(
      parsedValue: Any,
      level: Int,
      dfaState: Int
  ): HashMap[String, Set[(Int, Int, Int)]] = {
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int)]]()
    // TODO: also store path with these tokens
    parsedValue match {
      case _: HashMap[_, _] => {
        for (
          (k, v) <- parsedValue.asInstanceOf[HashMap[String, Any]].iterator
        ) {
          encounteredTokens =
            addToken(encounteredTokens, k, level + 1, dfaState)
          if(v.isInstanceOf[HashMap[_, _]]) {
            encounteredTokens = mergeMapSet(
                encounteredTokens,
                getEncounteredTokens(v, level + 1, dfaState)
            )
          }
          else {
            val (d, subType) = v
            if(subType != null) { // its array type (get keys in its subType)
              encounteredTokens = mergeMapSet(
                encounteredTokens,
                getEncounteredTokens(subType, level + 2, dfaState)
              )
            }
          }
        }
        return encounteredTokens
      }
      case _: List[_] => {
        for (v <- parsedValue.asInstanceOf[List[_]]) {
          encounteredTokens = mergeMapSet(
            encounteredTokens,
            getEncounteredTokens(v, level + 1, dfaState)
          )
        }
        return encounteredTokens
      }
      case _ => return encounteredTokens
    }

  }

  def finalizeValue(value : Any, keepIndex: Boolean, partitionId:Long, count:Long, rowMap: HashMap[String, (Int, DataType, Any)]): InternalRow = {
    val (partitionIdIndex, _, _) = if(rowMap contains "partition_id") {rowMap("partition_id")} else {(-1, null, null)}
    val (rowIndex, _, _) = if(rowMap contains "partition_row_index") {rowMap("partition_row_index")} else {(-1, null, null)}

    value match {
      case arr: Array[Any] =>
        if(partitionIdIndex > -1) {
          arr(partitionIdIndex) = partitionId
          arr(rowIndex) = count
        }
        InternalRow.fromSeq(arr.toSeq)
      case _ =>
        if (partitionIdIndex > -1) {
          InternalRow.fromSeq(Seq(value, partitionId, count))
        } else {
          InternalRow.fromSeq(Seq(value))
        }
    }
  }
  def getNextMatch(
      reader: BufferedReader,
      encoding: String,
      start: Long,
      end: Long,
      _pos: Long,
      syntaxStackArray: ArrayBuffer[Char],
      __stackPos : Int,
      __maxStackPos : Int,
      dfa: DFA,
      getTokens: Boolean = false,
      getTypes: Boolean = false,
      rowMap: HashMap[String, (Int, DataType, Any)] = null,
      filterVariables: HashMap[String, Variable] =
        new HashMap[String, Variable],
      filterSize: Int = -1,
      keepExtras : Boolean = false,
      keepIndex : Boolean = false,
      partitionId : Long = 0L,
      _count: Long = 0L
  ): (Boolean, Any, HashMap[String, Set[(Int, Int, Int)]], Long, Int, Int, Long) = {

    var stackPos = __stackPos
    var maxStackPos = __maxStackPos
    var count = _count
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int)]]()
    var pos = _pos
    while (true) {
      if (pos+1 >= end) {
        return (false, null, encounteredTokens, pos, stackPos, maxStackPos, count)
      }
      val i = reader.read()
      val c = i.toChar;
      pos += charSize(i)
      if (isWhiteSpace(c)) {
        // SKIP
      } else if (
        (c == ',' || c == '{') && dfa.checkArray()
      ) {
        if (getTypes) {
          val (value, _p) = parseType(reader, encoding, pos, end, c)
          pos = _p
          return (true, value, encounteredTokens, pos, stackPos, maxStackPos, count)
        } else {
          val (_value, _p, skipRow) = _parse(
            reader,
            encoding,
            pos,
            end,
            "",
            rowMap,
            filterVariables,
            filterSize,
            c
          )

          pos = _p
          count+=1
          if (!skipRow) {
            val value = finalizeValue(_value, keepIndex, partitionId, count, rowMap)
            return (true, value, encounteredTokens, pos, stackPos, maxStackPos, count)
          }
        }
      } else if (c == '{') {
        val (_stackPos, _maxStackPos) : (Int, Int) = appendToStack(syntaxStackArray, c, stackPos, maxStackPos)
        stackPos = _stackPos
        maxStackPos = _maxStackPos
        if(dfa.currentState == 0 && dfa.states(0).value == "$") {
          dfa.toNextState()
        }
      } else if (c == '[') {
        
        if (!dfa.toNextStateIfArray(stackPos+1) &&
            !dfa.states(dfa.currentState).stateType.equals("descendant")) {
          pos = skip(reader, encoding, pos, end, c)
        } else {
          val (_stackPos, _maxStackPos) : (Int, Int) = appendToStack(syntaxStackArray, c, stackPos, maxStackPos)
        stackPos = _stackPos
        maxStackPos = _maxStackPos
        }

      } else if ((c == '}' || c == ']')) {
        // TODO handle error if pop is not equal to c
        // or if stack is empty (i.e. invalid initialization, or malformed input)
        stackPos-=1
        if (stackPos+1 == dfa.getPrevStateLevel()) {
          dfa.toPrevState(stackPos+1);
        }

      } else if (c == '"') {
        // get token
        val (value, _p) = consume(reader, encoding, pos, end, c)
        var token = value
        pos = _p
        var _i = reader.read()
        var t = _i.toChar;
        pos += charSize(_i)
        // skip white space if any
        while (isWhiteSpace(t)) {
          _i = reader.read()
          t = _i.toChar;
          pos += charSize(_i)
        }
        if (t == ':') {
          token = token.substring(1, token.size - 1)
          if (getTokens) {
            encounteredTokens = addToken(
                encounteredTokens,
                token,
                stackPos+1,
                dfa.currentState
              )
          }
          // TODO: the filter at this level can be evaluated here
          // 1. check if token in filterVariables
          // 2. check if filter is evaluated to false
          // 3. call skip function and discard of stored values appropriately
          val dfaResponse = dfa.checkToken(token, stackPos+1)

          if (dfaResponse.equals("accept")) {
            if (getTypes) {
              val (value, _p) = parseType(reader, encoding, pos, end)
              pos = _p
              return (true, value, encounteredTokens, pos, stackPos, maxStackPos, count)
            } else {
              val (_value, _p, skipRow) = _parse(
                reader,
                encoding,
                pos,
                end,
                "",
                rowMap,
                filterVariables,
                filterSize
              )
              pos = _p
              count += 1
              if (!skipRow) {
                val value = finalizeValue(_value, keepIndex, partitionId, count, rowMap)
                return (true, value, encounteredTokens, pos, stackPos, maxStackPos, count)
              }
            }
          } else if (dfaResponse.equals("reject")) {

            if (getTokens) {
              val (parsedValue, _p) = parseType(reader, encoding, pos, end)
              pos = _p
              encounteredTokens = mergeMapSet(
                encounteredTokens,
                getEncounteredTokens(
                  parsedValue,
                  stackPos+1,
                  dfa.currentState
                )
              )
            } else {
              pos = skip(reader, encoding, pos, end)
            }

          }
        } else if(t == ']' || t == '}') {
          stackPos -= 1
        }
      }
    }
    return (false, null, encounteredTokens, pos, stackPos, maxStackPos, count)
  }

  def consume(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      currentChar: Char = '\u0000'
  ): (String, Long) = {
    var pos = _pos
    var output = new StringBuilder()
    var localStack = scala.collection.mutable.ArrayBuffer[Char]()
    var stackPos = -1
    var maxStackPos = -1
    var isEscaped = false
    var isString = false
    var prevC = '"'
    var countEscape = 0
    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return (output.toString(), pos)
      }
      c = i.toChar
      pos += charSize(i)
    } else {
      c = currentChar;
    }
    while (true) {
      if (stackPos+1 == 0 && (c == ',' || c == ']' || c == '}')) {
        reader.reset()
        pos -= charSize(c.toInt)
        return (output.toString(), pos);
      } else if (
        !isString &&
        (c == '{' || c == '[' ||
          (!isEscaped && c == '"'))
      ) {
        output.append(c)
        val (_stackPos, _maxStackPos) : (Int, Int) = appendToStack(localStack, c, stackPos, maxStackPos)
        stackPos = _stackPos
        maxStackPos = _maxStackPos
        if (c == '"')
          isString = true
      } else if (
        (!isString && (c == '}' || c == ']')) ||
        (isString && !isEscaped && c == '"')
      ) {
        output.append(c)
        stackPos-=1
        if (c == '"')
          isString = false;
        if (stackPos+1 == 0) {
          return (output.toString(), pos)
        }
      } else {
        output.append(c)
        if (isString && c == '\\') {
          if (prevC == '\\')
            countEscape += 1;
          else
            countEscape = 1
          if (countEscape % 2 != 0)
            isEscaped = true
          else
            isEscaped = false
        }
      }

      if (c != '\\') {
        isEscaped = false
        countEscape = 0
      }
      prevC = c
      if (pos >= end && stackPos+1 == 0) {
        return (output.toString(), pos)
      }
      reader.mark(1);
      val i = reader.read()

      if (i == -1) {
        return (output.toString(), pos)
      }
      c = i.toChar
      pos += charSize(i)
    }
    return (output.toString(), pos)
  }

  // like consume but doesn't store the value
  def skip(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      currentChar: Char = '\u0000'
  ): Long = {
    var pos = _pos
    var localStack = scala.collection.mutable.ArrayBuffer[Char]()
    var stackPos = -1
    var maxStackPos = -1
    var isEscaped = false
    var isString = false
    var prevC = '"'
    var countEscape = 0
    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return pos
      }
      c = i.toChar
      pos += charSize(i)
    } else {
      c = currentChar;
    }
    while (true) {
      if (stackPos+1 == 0 && (c == ',' || c == ']' || c == '}')) {
        reader.reset()
        pos -= charSize(c.toChar)
        return pos;
      } else if (
        !isString &&
        (c == '{' || c == '[' ||
          (!isEscaped && c == '"'))
      ) {
        // localStack.append(c)
        val (_stackPos, _maxStackPos) : (Int, Int) = appendToStack(localStack, c, stackPos, maxStackPos)
        stackPos = _stackPos
        maxStackPos = _maxStackPos
        if (c == '"')
          isString = true
      } else if (
        (!isString && (c == '}' || c == ']')) ||
        (isString && !isEscaped && c == '"')
      ) {
        // localStack.trimEnd(1)
        stackPos -= 1
        if (c == '"')
          isString = false;
        if (stackPos+1 == 0) {
          return pos
        }
      } else {
        if (isString && c == '\\') {
          if (prevC == '\\')
            countEscape += 1;
          else
            countEscape = 1
          if (countEscape % 2 != 0)
            isEscaped = true
          else
            isEscaped = false
        }
      }

      if (c != '\\') {
        isEscaped = false
        countEscape = 0
      }
      prevC = c
      if (pos >= end && stackPos+1 == 0) {
        return pos
      }
      reader.mark(1);
      val i = reader.read()

      if (i == -1) {
        return pos
      }
      c = i.toChar
      pos += charSize(i)
      // if (pos >= end) {
      //   return pos
      // }
    }
    return pos
  }

  val numRegExp =
    """[0-9\-NI]""".r // a number must start with a digit, -, or N for NaN or I for Infinity

  def _parse(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      key: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      filterVariables: HashMap[String, Variable] =
        new HashMap[String, Variable],
      filterSize: Int = -1,
      currentChar: Char = '\u0000'
  ): (Any, Long, Boolean) = {
    var pos: Long = _pos

    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return (null, pos, false)
      }
      c = i.toChar
      pos += charSize(i)
    } else {
      c = currentChar;
    }

    val (_, dataType, subType): (_, DataType, Any) = if (rowMap.contains(key)) {
      rowMap(key)
    } else { (-1, NullType, null) }

    while (true) {
      c match {
        case '{' => {
          val (obj, newPos, skipRow) =
            _parseObject(
              reader,
              encoding,
              pos,
              end,
              "",
              rowMap,
              filterVariables,
              filterSize
            )
          return (obj, newPos, skipRow)
        }
        case '[' => {
          val (arr, newPos) = _parseArray(
            reader,
            encoding,
            pos,
            end,
            "",
            subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]],
            dataType
          )
          return (arr, newPos, false)
        }
        case '"' => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          if (dataType.isInstanceOf[StringType]) {
            return (
                UTF8String.fromString(str.substring(1, str.length - 1)),
              newPos,
              false
            )
          } else {
            return (null, newPos, false)
          }
        }
        case 'n' => {
          reader.skip(3)
          return (
            null,
            pos + stringSize("ull", encoding),
            false
          )
        }
        case 'f' => {
          reader.skip(4)
          return (
            false,
            pos + stringSize("alse", encoding),
            false
          )
        }
        case 't' => {
          reader.skip(3)
          return (
            true,
            pos + stringSize("rue", encoding),
            false
          )
        }
        case numRegExp() => {
          val (str, newPos) = _getNum(reader, encoding, pos, end, c)
          if (dataType.isInstanceOf[DoubleType]) {
            return (str.toDouble, newPos, false)
          } else if (dataType.isInstanceOf[LongType]){
            return (str.toLong, newPos, false)
          } else if (dataType.isInstanceOf[StringType]) {
            // may happen if schema inferred to null and we replaced nulls
            // with strings (in case of speculation)
            return (UTF8String.fromString(str), newPos, false)
          } else {
            return (null, newPos, false)
          }
        }
        case _ => {} // these are skipped (e.g. whitespace)
      }

      val i = reader.read()
      if (i == -1) {
        return (null, pos, false)
      }
      c = i.toChar
      pos += charSize(i)
    }
    return (null, pos, false)
  }

  def _parseObject(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      parentKey: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      filterVariables: HashMap[String, Variable] =
        new HashMap[String, Variable],
      filterSize: Int = -1
  ): (Any, Long, Boolean) = {
    // TODO: - re-implemented to avoid recursion for nested objects (would make code even less readable)
    //       - update record initalization to something more efficient
    //          here first it allocates a memory block, and then adds references
    //          to the values, this probably also requires copying when converted
    //          to a row
    val rowSequence = new Array[Any](rowMap.size)
    val predicateValues = if (filterSize > 0) { new Array[Any](filterSize) }
    else { new Array[Any](1) }
    var rowCounter = 0
    if(rowMap contains "partition_id") {
      rowCounter += 2
    }
    var isKey = true
    var key = ""

    var pos: Long = _pos
    while (true) { // parse until full object or end of file
      if (rowCounter == rowMap.size) {
        pos = skip(reader, encoding, pos, end, '{')
        if(parentKey == "")
          return (rowSequence, pos, false)
        else
          return (InternalRow.fromSeq(rowSequence.toSeq), pos, false)
      }
      val i = reader.read()
      if (i == -1) {
        return (
          null,
          pos,
          true
        ) // maybe raise an exception since object is not fully parsed
      }
      val c = i.toChar
      pos += charSize(i)

      val (index, dataType, subType): (Int, DataType, Any) = if (!isKey) {
        rowMap(key)
      } else { (-1, NullType, null) }
      //  println((parentKey, key, index, dataType))
      c match {
        case '{' => {

          if(dataType.isInstanceOf[StringType]) {
            val (obj, newPos) =
              consume(reader, encoding, pos, end, c)
            rowSequence(index) = UTF8String.fromString(obj)
            pos = newPos

          } else {
            val (obj, newPos, _) = _parseObject(
              reader,
              encoding,
              pos,
              end,
              key,
              subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]]
            )
            rowSequence(index) = obj
            pos = newPos
          }
          rowCounter += 1
          isKey = true
        }
        case '[' => {
          if(dataType.isInstanceOf[StringType]) {
            val (str, newPos) =
              consume(reader, encoding, pos, end, c)
            rowSequence(index) = UTF8String.fromString(str.trim)
            pos = newPos
          } else {
            val (arr, newPos) = _parseArray(
              reader,
              encoding,
              pos,
              end,
              key,
              subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]],
              dataType
            )
            // map = map + ((key, arr))
            rowSequence(index) = arr
            pos = newPos
          }
          rowCounter += 1
          isKey = true

        }
        case '"' => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          pos = newPos

          if (isKey) {
            key = str.substring(1, str.length - 1)
            if (rowMap contains key) {
              isKey = false
            } else {
              pos = skip(reader, encoding, pos, end)
            }
          } else {
            if (dataType.isInstanceOf[StringType]) {
              rowSequence(index) = UTF8String.fromString(str.substring(1, str.length - 1))
            }
            rowCounter += 1
            isKey = true
          }
        }
        case numRegExp() => {
          val (str, newPos) = _getNum(reader, encoding, pos, end, c)
          if (dataType.isInstanceOf[DoubleType]) {
            rowSequence(index) = str.toDouble
          } else if (dataType.isInstanceOf[LongType]){
            rowSequence(index) = str.toLong
          } else if (dataType.isInstanceOf[StringType]) {
            // may happen if schema inferred to null and we replaced nulls
            // with strings (in case of speculation)
            rowSequence(index) = UTF8String.fromString(str)
          }
          rowCounter += 1
          isKey = true
          pos = newPos
        }
        case 'n' => {
          // map = map + ((key, null))
          // rowSequence(index)
          // already null by default

          rowCounter += 1
          isKey = true
          pos = pos + stringSize("ull", encoding)
          reader.skip(3)
        }
        case 'f' => {
          // map = map + ((key, false))
          rowCounter += 1
          if (dataType.isInstanceOf[StringType]) {
            rowSequence(index) = UTF8String.fromString("false")

          } else {
            rowSequence(index) = false
          }
          isKey = true
          pos = pos + stringSize("alse", encoding)
          reader.skip(4)
        }
        case 't' => {
          // map = map + ((key, true))
          if (dataType.isInstanceOf[StringType]) {
            rowSequence(index) = UTF8String.fromString("true")

          } else {
            rowSequence(index) = true
          }
          rowCounter += 1
          isKey = true
          pos = pos + stringSize("rue", encoding)
          reader.skip(3)
        }
        case '}' => {
          if(parentKey == "")
            return (rowSequence, pos, false)
          else
            return (InternalRow.fromSeq(rowSequence.toSeq), pos, false)
          // return (row, pos, false)
        }
        case _ => {} // skip character
      }

      if (
        isKey && predicateValues(0) == null && filterVariables.contains(key)
      ) {
        filterVariables(key).propagate(predicateValues, rowSequence)
      }

      if (predicateValues(0) == false) {
        pos = skip(reader, encoding, pos, end, '{')
        return (null, pos, true)
      }
    }

    throw new Exception(
      "Couldn't parse object at " + pos
    )
  }

  def _parseArray(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      parentKey: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      dataType: DataType
  ): (ArrayData, Long) = {
    // TODO: re-implement to avoid recursion if possible ()
    var arr = new ArrayBuffer[Any]()
    var pos: Long = _pos

    while (true) {
      val i = reader.read()
      if (i == -1) {
        return (null, pos)
      }
      val c = i.toChar
      pos += charSize(i)

      c match {
        case '{' => {
          if(rowMap != null) { // if null probably speculation only encountered empty lists
            val (obj, newPos, _) =
              _parseObject(reader, encoding, pos, end, parentKey, rowMap)
            arr.append(obj)
            pos = newPos
            } else if (dataType.isInstanceOf[StringType]) {
              val (str, newPos) = consume(reader, encoding, pos, end, '{')
              pos = newPos
              arr.append(str)
            } else { // skip it
              pos = skip(reader, encoding, pos, end, '{')
            }
        }
        case '[' => {
          if (dataType.isInstanceOf[DoubleType]) {
            val (str, newPos) = consume(reader, encoding, pos, end, '[')
            pos = newPos
            val _doubleArr =
              str.trim.substring(1, str.size - 1).split(",").map(_.toDouble)
            return (ArrayData.toArrayData(_doubleArr), pos)
          } else if (dataType.isInstanceOf[LongType]) {
            val (str, newPos) = consume(reader, encoding, pos, end, '[')
            pos = newPos
            val _longArr =
              str.trim.substring(1, str.size - 1).split(",").map(_.toLong)
            return (ArrayData.toArrayData(_longArr), pos)
          } else {
            val _dataType = if (dataType.isInstanceOf[ArrayType]) {
              dataType.asInstanceOf[ArrayType].elementType
            } else { dataType }
            val (_arr, newPos) =
              _parseArray(
                reader,
                encoding,
                pos,
                end,
                parentKey,
                rowMap,
                _dataType
              )
            arr.append(_arr)
            pos = newPos
          }
        }
        case '"' => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          arr.append(UTF8String.fromString(str.substring(1, str.length - 1)))
          pos = newPos
        }
        case numRegExp() => {
          val (str, newPos) = _getNum(reader, encoding, pos, end, c)
          val _dataType = if (dataType.isInstanceOf[ArrayType]) {
              dataType.asInstanceOf[ArrayType].elementType
            } else { dataType }
          if (_dataType.isInstanceOf[DoubleType]) {
            arr.append(str.toDouble)
          } else if (dataType.isInstanceOf[LongType]){
            arr.append(str.toLong)
          } else if (dataType.isInstanceOf[StringType]) {
            // may happen if schema inferred to null and we replaced nulls
            // with strings (in case of speculation)
            arr.append(UTF8String.fromString(str))
          }
          // val (num, newPos) = _parseDouble(reader, encoding, pos, end, c)
          // arr.append(num)
          pos = newPos
        }
        case 'n' => {
          arr.append(null)
          pos = pos + stringSize("ull", encoding)
          reader.skip(3)
        }
        case 'f' => {
          arr.append(false)
          pos = pos + stringSize("alse", encoding)
          reader.skip(4)
        }
        case 't' => {
          arr.append(true)
          pos = pos + stringSize("rue", encoding)
          reader.skip(3)
        }
        case ']' => {
          return (ArrayData.toArrayData(arr), pos)
        }
        case _ => {} // skip character
      }
    }
    throw new Exception(
      "Couldn't parse array at " + pos
    )
  }

  def _getNum(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      currentChar: Char = '\u0000'
  ): (String, Long) = {
    var pos: Long = _pos
    var str = new StringBuilder()

    var c = currentChar
    while (true) {
      if(isWhiteSpace(c)) {
        return (str.toString(), pos)
      }
      else if (c == ']' || c == '}' || c == ',') {
        reader.reset()
        pos -= charSize(c.toInt)
        return (str.toString(), pos)
      } else {
        str.append(c)
      }

      reader.mark(1);
      val i = reader.read()
      if (i == -1) {
        throw new Exception(
          "Couldn't parse double at " + pos
        )
      }
      c = i.toChar
      pos += charSize(i)
    }

    throw new Exception(
      "Couldn't parse double at " + pos
    )
  }

  def parseType(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      currentChar: Char = '\u0000'
  ): (Any, Long) = {
    // TODO: update to also evaluate JSONPath filters at the accept level
    //       and re-implement to avoid recursion
    var pos: Long = _pos

    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return (null, pos)
      }
      c = i.toChar
      pos += charSize(i)
    } else {
      c = currentChar;
    }
    while (true) {
      c match {
        case '{' => {
          val (objType, newPos) =
            parseObjectType(reader, encoding, pos, end, "")
          // if(!(objType contains("GO")))
          // println(c, objType)
          return (objType, newPos)
        }
        case '[' => {
          val (arrType, newPos) = parseArrayType(reader, encoding, pos, end, "")
          return (arrType, newPos)
        }
        case '"' => {
          val newPos = skip(reader, encoding, pos, end, c)
          return ((StringType, null), newPos)
        }
        case 'n' => {
          reader.skip(3)
          return ((NullType, null), pos + stringSize("ull", encoding))
        }
        case 'f' => {
          reader.skip(4)
          return ((BooleanType, null), pos + stringSize("alse", encoding))
        }
        case 't' => {
          reader.skip(3)
          return ((BooleanType, null), pos + stringSize("rue", encoding))
        }
        case numRegExp() => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          pos = newPos
          if(str contains ".")
            return ((DoubleType, null), newPos)
          else
            return ((LongType, null), newPos)
          // val newPos = skip(reader, encoding, pos, end, c)
        }
        case _ => {} // these are skipped (e.g. whitespace)
      }

      val i = reader.read()
      if (i == -1) {
        return ((NullType, null), pos)
      }
      c = i.toChar
      pos += charSize(i)
    }
    return ((NullType, null), pos)
  }

  def parseObjectType(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      parentKey: String
  ): (HashMap[String, Any], Long) = {
    var map = HashMap[String, Any]()
    var isKey = true
    var key = ""

    var pos: Long = _pos
    while (true) { // parse until full object or end of file
      val i = reader.read()
      if (i == -1) {
        return (null, pos)
      }
      val c = i.toChar
      pos += charSize(i)
      c match {
        case '{' => {
          val (objType, newPos) =
            parseObjectType(reader, encoding, pos, end, key)
          map = map + ((key, objType))
          isKey = true
          pos = newPos
        }
        case '[' => {
          if (
            (parentKey.equals("geometry") && (key.equals("coordinates")
            || parentKey.equals("geometries")))
          ) {
            val newPos =
              skip(reader, encoding, pos, end, c)
            // encoded as string because a geometry can have a different dimension
            // based on type, e.g. points are 1d, polygons are 3d, etc.
            map = map + ((key, (StringType, null)))
            pos = newPos
          } else {
            val (arrType, newPos) =
              parseArrayType(reader, encoding, pos, end, key)
            map = map + ((key, arrType))
            pos = newPos
          }
          isKey = true

        }
        case '"' => {
          if (isKey) {
            val (str, newPos) = consume(reader, encoding, pos, end, c)
            key = str.substring(1, str.length - 1)
            isKey = false
            pos = newPos
          } else {
            val newPos =
              skip(reader, encoding, pos, end, c)
            map = map + ((key, (StringType, null)))
            isKey = true
            pos = newPos

          }
        }
        case numRegExp() => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          // pos = skip(reader, encoding, newPos, end, c)
          if(str contains ".") 
            map = map + ((key, (DoubleType, null)))
          else
            map = map + ((key, (LongType, null)))
          isKey = true
          pos = newPos
        }
        case 'n' => {
          map = map + ((key, (NullType, null)))
          isKey = true
          pos = pos + stringSize("ull", encoding)
          reader.skip(3)
        }
        case 'f' => {
          map = map + ((key, (BooleanType, null)))
          isKey = true
          pos = pos + stringSize("alse", encoding)
          reader.skip(4)
        }
        case 't' => {
          map = map + ((key, (BooleanType, null)))
          isKey = true
          pos = pos + stringSize("rue", encoding)
          reader.skip(3)
        }
        case '}' => {
          return (map, pos)
        }
        case _ => {} // skip character
      }

    }

    throw new Exception(
      "Couldn't parse object at " + pos
    )
  }

  def parseArrayType(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      parentKey: String
  ): ((Any, Any), Long) = {
    var pos: Long = _pos
    while (true) {
      val i = reader.read()
      if (i == -1) {
        return (null, pos)
      }
      val c = i.toChar
      pos += charSize(i)
      c match {
        case '{' => {
          val (objType, newPos) =
            parseObjectType(reader, encoding, pos, end, parentKey)
          val newPos2 = skip(reader, encoding, newPos, end, '[')
          pos = newPos2
          return ((ArrayType(NullType), objType), pos)
        }
        case '[' => {
          val (arrType, newPos) =
            parseArrayType(reader, encoding, pos, end, parentKey)
          val newPos2 = skip(reader, encoding, newPos, end, '[')
          pos = newPos2
          return ((ArrayType(NullType), arrType), pos)
        }
        case '"' => {
          val newPos = skip(reader, encoding, pos, end, c)
          val newPos2 = skip(reader, encoding, newPos, end, '[')
          pos = newPos2
          return ((ArrayType(NullType), (StringType, null)), pos)
        }
        case numRegExp() => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          pos = skip(reader, encoding, newPos, end, '[')
          if(str contains ".")
            return ((ArrayType(NullType), (DoubleType, null)), pos)
          else
            return ((ArrayType(NullType), (LongType, null)), pos)
        }
        case 'n' => {
          pos = pos + stringSize("ull", encoding)
          reader.skip(3)
          // don't return here in case not all are null
        }
        case 'f' => {
          pos = pos + stringSize("alse", encoding)
          reader.skip(4)
          return ((ArrayType(NullType), BooleanType), pos)
        }
        case 't' => {
          pos = pos + stringSize("rue", encoding)
          reader.skip(3)
          return ((ArrayType(NullType), (BooleanType, null)), pos)
        }
        case ']' => {
          return (
            (ArrayType(NullType), (NullType, null)),
            pos
          ) // no type was identified
        }
        case _ => {} // skip character
      }
    }
    throw new Exception(
      "Couldn't parse type of array at " + pos
    )
  }

}
