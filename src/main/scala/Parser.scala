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

import SchemaInference.reduceKey

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.BufferedReader
import java.nio.charset.{Charset, CodingErrorAction}
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue


class Parser(val filePath : String = "",
             val hdfsPath : String = "local",
             val encoding : String = "UTF8",
             var pda : PDA = null,
             var pos : Long = 0,
             var end : Long = -1
            ) {

  var (inputStream, fileSize) : (FSDataInputStream, Long) = getInputStream
  end = if(end == -1L) {fileSize} else { end }
  var reader : BufferedReader = getBufferedReader(pos)
  var syntaxStackArray: ArrayBuffer[Char] = new ArrayBuffer[Char]()
  var stackPos : Int = -1
  var maxStackPos : Int = -1
  var count : Long = 0
  val parsedRecords : Queue[Any] = Queue[Any]()
  var nParsedRecords : Int = 0
  def getInputStream: (FSDataInputStream, Long) = {
    if(filePath == "") return (null, -1)
    val conf = if(hdfsPath == "local") {
      SparkContext.getOrCreate().hadoopConfiguration
    } else {
      val _conf = SparkContext.getOrCreate().hadoopConfiguration
      _conf.set("fs.defaultFS", hdfsPath)
      _conf
    }
    val fs = FileSystem.get(conf)
    val path = new Path(filePath)
    val fileSize: Long = fs.getContentSummary(path).getLength
    val inputStream: FSDataInputStream = fs.open(path)
    return (inputStream, fileSize)
  }

  def repositionReader(newPos : Long): Unit = {
    pos = newPos
    reader = getBufferedReader(pos)
  }

  def getBufferedReader(
      startPos: Long
  ): BufferedReader = {
    if(inputStream == null) return (null)
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

  def stringSize(s: String): Int = {
    var size: Int = 0
    for (c <- s) {
      size += charSize(c.toInt)
    }
    return size
  }

  def initSyntaxStack(
                       initialState: List[Char]
  ) : Unit = {
    // TODO: should be initialized using provided initial path instead
    //       may cause issues for query with multiple descendent types
    val syntaxStackArray: ArrayBuffer[Char] = ArrayBuffer[Char]()
      for (i <- initialState.indices) {
        syntaxStackArray.append(initialState(i))
      }
    this.syntaxStackArray = syntaxStackArray
    stackPos = syntaxStackArray.size-1
    maxStackPos = stackPos
  }

  def appendToStack(stack : ArrayBuffer[Char], value : Char): Unit = {
    stackPos = stackPos + 1
    if (stackPos <= maxStackPos) {
      stack(stackPos) = value
    } else {
      stack.append(value)
      maxStackPos = stackPos
    }
  }

  def appendToStack(stack : ArrayBuffer[Char], value : Char, stackPos : Int, maxStackPos : Int): (Int, Int) = {
    // appending is done like this
    // mainly to avoid having to access the last element (linear time)
    // and also to avoid memory re-allocation if possible
    // NOT thoroughly evaluated compared to just using append
    val newPos = stackPos + 1
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
      .replaceAll(raw"(?i)(false|alse|lse|s|e|true|rue|ue|null|ull|ll|l|NaN|aN|N|Infinity|nfinity|finity|inity|nity|ity|ty|y|x|[0-9]+|-|\.)", "")
      .matches(raw"[\s+{}\[\],:]+")
  }

  def isWhiteSpace(c: Char): Boolean = {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 13
  }

  def getNextToken: (String, Int) = {
    // TODO: this function can be improved
    var i: Int = 0
    var accept: Boolean = false
    var index = 0
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
        val token = consume(c)
        i = reader.read()

        var t = i.toChar
        var tmpIndex = charSize(i)
        while (isWhiteSpace(t)) {
          i = reader.read()
          t = i.toChar;
          tmpIndex += charSize(i)
        }
        pos += tmpIndex
        if (t == ':') {
          return (token.substring(1, token.length - 1), index)
        } else {
          index += tmpIndex
        }
      }

    }
    return ("", -1)
  }
  def skipLevels(
      levels: Int,
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
        val prevPos = pos
        skip(c)
        index += (pos-prevPos).toInt
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
                tokens: HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]],
                token: String,
                level: Int,
                pdaState: Int,
                count: Int,
                stateLevels : List[Int],
                syntaxStack : List[Char]
  ): HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]] = {
    var _tokens = tokens
    if (tokens contains token) {
      var isAdded = false
      for((a,b,c,d,e) <- _tokens(token)) {
        if(a == level && b == pdaState) {
          isAdded = true
          _tokens = _tokens.updated(token, _tokens(token) - ((a,b,c, d, e)) + ((a,b,c+count, d, e)))
        }
      }
      if(!isAdded) {
        _tokens = _tokens.updated(token, _tokens(token) + ((level,pdaState,count, stateLevels, syntaxStack)))
      }
    } else {
      _tokens = _tokens + (token -> Set((level, pdaState, count, stateLevels, syntaxStack)))
    }
    _tokens
  }

  def mergeMapSet(
      m1: HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]],
      m2: HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]]
  ): HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]] = {
    var m = m1
    for ((k, v) <- m2) {
      for((a,b,c,d, e) <- v) {
        m = addToken(m, k, a, b, c, d, e)
      }
    }
    m
  }
  def getEncounteredTokens(
      parsedValue: Any,
      level: Int,
      dfaState: Int,
      stateLevels : List[Int],
      syntaxStack : List[Char]
  ): HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]] = {
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]]()
    parsedValue match {
      case _: HashMap[_, _] => {
        for (
          (k, v) <- parsedValue.asInstanceOf[HashMap[String, Any]].iterator
        ) {
          encounteredTokens =
            addToken(encounteredTokens, k, level + 1, dfaState, 1, stateLevels, syntaxStack)
          if(v.isInstanceOf[HashMap[_, _]]) {
            encounteredTokens = mergeMapSet(
                encounteredTokens,
                getEncounteredTokens(v, level + 1, dfaState, stateLevels, syntaxStack)
            )
          }
          else {
            val (_, subType) = v
            if(subType != null) { // its array type (get keys in its subType)
              encounteredTokens = mergeMapSet(
                encounteredTokens,
                getEncounteredTokens(subType, level + 2, dfaState, stateLevels, syntaxStack)
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
            getEncounteredTokens(v, level + 1, dfaState, stateLevels, syntaxStack)
          )
        }
        return encounteredTokens
      }
      case _ => return encounteredTokens
    }

  }

  def finalizeValue(value : Any, partitionId:Long, rowMap: HashMap[String, (Int, DataType, Any)]): InternalRow = {
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
                    projection : ProjectionNode,
                    getTokens: Boolean = false,
                    getTypes: Boolean = false,
                    keepExtras : Boolean = false,
                    partitionId : Long = 0L,
  ): (Boolean, Any, HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]]) = {
    var encounteredTokens = HashMap[String, Set[(Int, Int, Int, List[Int], List[Char])]]()
    while (true) {
      if(parsedRecords.nonEmpty) {
        val record =  parsedRecords.dequeue()
//        parsedRecords.remove(0)
        if(getTypes) {
          return (true, record, encounteredTokens)
        } else {
          count+=1
          return (true, finalizeValue(record, partitionId, projection.rowMap), encounteredTokens)
        }
      }
      if (pos+1 >= end) {
        return (false, null, encounteredTokens)
      }

      val i = reader.read()
      val c = i.toChar;
      pos += charSize(i)
      if (isWhiteSpace(c) || c == ':') {
        // SKIP
      } else if (
        (c == ',' || c == '{') && pda.checkArray()
      ) {
        if (getTypes) {
          val prevPos = pos
          val value = parseType(c, projection)
          val tokensValue =  if(getTokens && (!projection.acceptAll || value.isEmpty)) {
            repositionReader(prevPos)
            parseType(c, new ProjectionNode(acceptAll=true))
          } else {  value }
          if(getTokens && tokensValue.nonEmpty) {
            encounteredTokens = mergeMapSet(
              encounteredTokens,
              getEncounteredTokens(
                tokensValue.get,
                stackPos+1,
                pda.currentState,
                pda.getStateLevels,
                syntaxStackArray.take(stackPos+1).toList
              )
            )
          }
        } else {
          val value = _parse(
            "",
            projection.rowMap,
            projection.filterVariables,
            c
          )
        }
      } else if (c != ',' && c != ']' && (c == '[' || pda.states(pda.currentState).stateType.equals("array"))) {
        if(c != '[') { // happens only if user provided a query that doesn't match the data
          println("Query expects array but found other type. Skipping this item.")
          println("Found character: " + c)
          println(pda)
          skip(c)
          pda.toPrevState(stackPos)
        } else if (!pda.toNextStateIfArray(stackPos+1) &&
            !pda.states(pda.currentState).stateType.equals("descendant")) {
          skip(c)
        } else {
          appendToStack(syntaxStackArray, c)
        }

      } else if (c == '{') {
        appendToStack(syntaxStackArray, c)
        if(pda.currentState == 0 && pda.states(0).value == "$") {
          pda.toNextState()
        }
      } else if ((c == '}' || c == ']')) {
        if(stackPos < 0 || (c == '}' && syntaxStackArray(stackPos) != '{') || (c == ']' && syntaxStackArray(stackPos) != '[')) {
          // NOTE THIS EXCEPTION WAS NEVER RAISED IN ALL EXPERIMENTS AND TESTS (with valid JSON input)
          new Exception(
            """Encountered mismatched character to content of stack! Possible invalid initialization or invalid input syntax.
              |In case of speculation, check the printed tokens used for speculation, to identify invalid attributes.
              |Use full-pass partitioning to verify the syntactical correctness of the input.""".stripMargin)
        }
        // or if stack is empty (i.e. invalid initialization, or malformed input)
        if (stackPos <= pda.getPrevStateLevel()) {
          pda.toPrevState(stackPos);
        }
        stackPos -= 1
      } else if (c == '"') {
        // get token
        val value = consume(c)
        var token = value
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
                pda.currentState,
              1,
              pda.getStateLevels,
                syntaxStackArray.take(stackPos+1).toList
              )
          }
          if(token.equals("shippingLevelsOfService") || token.equals("products")) {
            Unit
          }
          val pdaResponse = pda.checkToken(token, stackPos+1)

          if (pdaResponse.equals("accept")) {
            if (getTypes) {
              val prevPos = pos
              val value = parseType(projection=projection)
              val tokensValue =  if(getTokens && (!projection.acceptAll || value.isEmpty)) {
                repositionReader(prevPos)
                parseType(projection=new ProjectionNode(acceptAll=true))
              } else {  value }
              if(getTokens && tokensValue.nonEmpty) {
                encounteredTokens = mergeMapSet(
                  encounteredTokens,
                  getEncounteredTokens(
                    tokensValue.get,
                    stackPos+1,
                    pda.currentState,
                    pda.getStateLevels,
                    syntaxStackArray.take(stackPos+1).toList
                  )
                )
              }

            } else {
              val value = _parse(
                token,
                projection.rowMap,
                projection.filterVariables
              )
            }
          } else if (pdaResponse.equals("reject")) {

            if (getTokens) {
              val _parsedValue = parseType(projection=new ProjectionNode(acceptAll = true))
              parsedRecords.clear()
              _parsedValue match {
                case Some(v) => encounteredTokens = mergeMapSet(
                  encounteredTokens,
                  getEncounteredTokens(
                    v,
                    stackPos+1,
                    pda.currentState,
                    pda.getStateLevels,
                    syntaxStackArray.take(stackPos+1).toList
                  )
                )
                case None => {}
              }

            } else {
              skip()
            }

          }
        } else if(t == ']' || t == '}') {
          stackPos -= 1
        }
      }
    }
    return (false, null, encounteredTokens)
  }

  def consume(
      currentChar: Char = '\u0000'
  ): String = {
    val output = new StringBuilder()
    val localStack = scala.collection.mutable.ArrayBuffer[Char]()
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
        return output.toString()
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
        return output.toString()
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
          return output.toString()
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
        return output.toString()
      }
      reader.mark(1);
      val i = reader.read()

      if (i == -1) {
        return output.toString()
      }
      c = i.toChar
      pos += charSize(i)
    }
    return output.toString()
  }

  // like consume but doesn't store the value
  def skip(
      currentChar: Char = '\u0000'
  ): Unit = {
    val localStack = scala.collection.mutable.ArrayBuffer[Char]()
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
        return
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
        return;
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
          return
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
        return
      }
      reader.mark(1);
      val i = reader.read()

      if (i == -1) {
        return
      }
      c = i.toChar
      pos += charSize(i)
      // if (pos >= end) {
      //   return pos
      // }
    }
  }

  val numRegExp =
    """[0-9\-NI]""".r // a number must start with a digit, -, or N for NaN or I for Infinity

  def _parse(
      key: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      filterVariables: HashMap[String, Variable] =
        new HashMap[String, Variable],
      currentChar: Char = '\u0000'
  ): Option[Any] = {
    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return None
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
        case '{' =>  return _parseObject(
              "",
              rowMap,
              filterVariables
            )
        case '[' => return _parseArray(
            "",
            subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]],
            dataType
          )
        case '"' => {
          val str = consume(c)
          if (dataType.isInstanceOf[StringType]) {
            return Some(UTF8String.fromString(str.substring(1, str.length - 1)))
          } else {
            return Some(null)
          }
        }
        case 'n' => {
          reader.skip(3)
          pos += stringSize("ull")
          return Some(null)
        }
        case 'f' => {
          reader.skip(4)
          pos += stringSize("alse")
          return  Some(false)
        }
        case 't' => {
          reader.skip(3)
          pos += stringSize("rue")
          return Some(true)
        }
        case numRegExp() => {
          val str = _getNum(c)
          if (dataType.isInstanceOf[DoubleType]) {
            return Some(str.toDouble)
          } else if (dataType.isInstanceOf[LongType]){
            return Some(str.toLong)
          } else if (dataType.isInstanceOf[StringType]) {
            // may happen if schema inferred to null and we replaced nulls
            // with strings (in case of speculation)
            return Some(UTF8String.fromString(str))
          } else {
            return None
          }
        }
        case _ => {} // these are skipped (e.g. whitespace)
      }

      val i = reader.read()
      if (i == -1) {
        return None
      }
      c = i.toChar
      pos += charSize(i)
    }
    return None
  }

  def _parseObject(
      parentKey: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      filterVariables: HashMap[String, Variable] =
        new HashMap[String, Variable],
  ): Option[Any] = {
    val rowSequence = new Array[Any](rowMap.size)
    val predicateValues = if(filterVariables != null) {new Array[Any](filterVariables.size)} else {new Array[Any](0)}
    var rowCounter = 0
    if(rowMap contains "partition_id") {
      rowCounter += 2
    }
    var isKey = true
    var key = ""

    while (true) { // parse until full object or end of file
      if (rowCounter == rowMap.size) {
        skip('{')
        if(parentKey == "*")
          return Some(rowSequence)
        else
          return Some(InternalRow.fromSeq(rowSequence.toSeq))
      }
      val i = reader.read()
      if (i == -1) {
        return None
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
            val obj =
              consume(c)
            rowSequence(index) = UTF8String.fromString(obj)
          } else {
            val obj = _parseObject(
              key,
              subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]]
            )
            // Some
            obj match {
              case Some(o) => rowSequence(index) = o
              case None => {}
            }
          }
          rowCounter += 1
          isKey = true
        }
        case '[' => {
          if(dataType.isInstanceOf[StringType]) {
            val str =
              consume(c)
            rowSequence(index) = UTF8String.fromString(str.trim)
          } else {
            val arr = _parseArray(
              key,
              subType.asInstanceOf[HashMap[String, (Int, DataType, Any)]],
              dataType
            )
            // map = map + ((key, arr))
            arr match {
              case Some(a) => rowSequence(index) = arr
              case None => {}}

          }
          rowCounter += 1
          isKey = true
        }
        case '"' => {
          val str = consume(c)

          if (isKey) {
            key = str.substring(1, str.length - 1)
            if (rowMap contains key) {
              isKey = false
            } else {
              skip()
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
          val str = _getNum(c)
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
        }
        case 'n' => {
          // map = map + ((key, null))
          // rowSequence(index)
          // already null by default

          rowCounter += 1
          isKey = true
          pos = pos + stringSize("ull")
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
          pos = pos + stringSize("alse")
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
          pos = pos + stringSize("rue")
          reader.skip(3)
        }
        case '}' => {
          if(parentKey == "")
            return Some(rowSequence)
          else
            return Some(InternalRow.fromSeq(rowSequence.toSeq))
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
        skip('{')
        return None
      }
    }

    throw new Exception(
      "Couldn't parse object at " + pos
    )
  }

  def _parseArray(
      parentKey: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      dataType: DataType
  ): Option[ArrayData] = {
    val arr = new ArrayBuffer[Any]()

    while (true) {
      val i = reader.read()
      if (i == -1) {
        return None
      }
      val c = i.toChar
      pos += charSize(i)

      c match {
        case '{' => {
          if(rowMap != null) { // if null probably speculation only encountered empty lists
            val obj =
              _parseObject(parentKey, rowMap)
            obj match {
              case Some(obj) => arr.append(obj)
              case None => {}
            }
            } else if (dataType.isInstanceOf[StringType]) {
              val str = consume('{')
              arr.append(str)
            } else { // skip it
              skip('{')
            }
        }
        case '[' => {
          if (dataType.isInstanceOf[DoubleType]) {
            val str = consume('[')
            val _doubleArr =
              str.trim.substring(1, str.size - 1).split(",").map(_.toDouble)
            return Some(ArrayData.toArrayData(_doubleArr))
          } else if (dataType.isInstanceOf[LongType]) {
            val str = consume('[')
            val _longArr =
              str.trim.substring(1, str.size - 1).split(",").map(_.toLong)
            return Some(ArrayData.toArrayData(_longArr))
          } else {
            val _dataType = if (dataType.isInstanceOf[ArrayType]) {
              dataType.asInstanceOf[ArrayType].elementType
            } else { dataType }
            val _arr =
              _parseArray(
                parentKey,
                rowMap,
                _dataType
              )
            arr.append(_arr)
          }
        }
        case '"' => {
          val str= consume(c)
          arr.append(UTF8String.fromString(str.substring(1, str.length - 1)))
        }
        case numRegExp() => {
          val str= _getNum(c)
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
        }
        case 'n' => {
          arr.append(null)
          pos = pos + stringSize("ull")
          reader.skip(3)
        }
        case 'f' => {
          arr.append(false)
          pos = pos + stringSize("alse")
          reader.skip(4)
        }
        case 't' => {
          arr.append(true)
          pos = pos + stringSize("rue")
          reader.skip(3)
        }
        case ']' => {
          return Some(ArrayData.toArrayData(arr))
        }
        case _ => {} // skip character
      }
    }
    throw new Exception(
      "Couldn't parse array at " + pos
    )
  }

  def _getNum(
      currentChar: Char = '\u0000'
  ): String = {
    var str = new StringBuilder()

    var c = currentChar
    while (true) {
      if(isWhiteSpace(c)) {
        return str.toString()
      }
      else if (c == ']' || c == '}' || c == ',') {
        reader.reset()
        pos -= charSize(c.toInt)
        return str.toString()
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



  /*********************************************
   ROW PARSER FUNCTIONS
   *********************************************/

  def parse(
                 currentChar: Char = '\u0000',
                 projection: ProjectionNode
               ): Option[Any] = {
    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return None
      }
      c = i.toChar
      pos += charSize(i)
    } else {
      c = currentChar;
    }
//    if(!projection.hasFilter && projection.outputsRowMap.)
    while (true) {
      c match {
        case '{' => {
          val objType =
            parseObjectType("*", projection)
          // if(!(objType contains("GO")))
          // println(c, objType)
          return objType
        }
        case '[' => {
          val arrType =
            parseArrayType("*", projection)
          return arrType
        }
        case '"' => {
          val prevPos = pos
          skip(c)
          val t = if(pos-prevPos <= 1) {  (NullType, null) } else { (StringType, null) } // empty strings are treated as NullType (this solves an issue in one dataset, where they provide an empty string for missing values
          parsedRecords.enqueue(t)
          return Some(t)
        }
        case 'n' => {
          reader.skip(3)
          pos += stringSize("ull")
          parsedRecords.enqueue((NullType, null))
          return Some((NullType, null))
        }
        case 'f' => {
          reader.skip(4)
          pos += stringSize("alse")
          parsedRecords.enqueue((BooleanType, null))
          return Some((BooleanType, null))
        }
        case 't' => {
          reader.skip(3)
          pos += stringSize("rue")
          parsedRecords.enqueue((BooleanType, null))
          return Some((BooleanType, null))
        }
        case numRegExp() => {
          val str= consume(c)
          if(!(str matches "\\d+")) {
            parsedRecords.enqueue((DoubleType, null))
            return Some((DoubleType, null))
          }
          else {
            parsedRecords.enqueue((LongType, null))
            return Some((LongType, null))
          }
          // val newPos = skip(reader, encoding, pos, end, c)
        }
        case _ => {} // these are skipped (e.g. whitespace)
      }

      val i = reader.read()
      if (i == -1) {
        return None
      }
      c = i.toChar
      pos += charSize(i)
    }
    return None
  }

  def parseObject(
                       parentKey: String,
                       projection: ProjectionNode,
                     ): Option[HashMap[String, Any]]= {
    var map = HashMap[String, Any]()
    var isKey = true
    var key = ""
    var inFilter = false
    var inChildren = false
    var inDescendants = false
    var keyIndex = -1
    var keyProjection : ProjectionNode = new ProjectionNode()
    val rowSequence = new Array[Any](projection.rowMap.size)
    val predicateValues = new Array[Any](projection.nPredicates)
    while (true) { // parse until full object or end of file
      val i = reader.read()
      if (i == -1) {
        return None
      }
      val c = i.toChar
      pos += charSize(i)

      var propagateFilter = false
      c match {
        case '{' => {

          val objType =
            parseObjectType(key, keyProjection)
          objType match {
            case Some(o) => {
              var newSubMap = new HashMap[String, Any]()
              for((k,v) <- o) {
                if(projection.descendantsTree.contains(k) && projection.descendantsTree(k).parentKey.equals(parentKey)){
                  //                  map = map + ((".." + k,v))
                  map = map + ((k,v))
                } else if((keyProjection.notDescending && keyProjection.acceptAll) || keyProjection.childrenTree.contains(k)) {
                  newSubMap = newSubMap + ((k, v))
                } else {
                  map = map + ((k,v))
                }
              }
              map = map + ((key, newSubMap))
            }
            case None => {}
          }
          if(inFilter) {
            // TODO fix it (may require passing the inputStream to reset the position
            // Only filter for Geometry is supported for this type
            // has to be a nested filter otherwise
            // ignore it for now
            propagateFilter = true
          }
          isKey = true
        }
        case '[' => {
          if (
            (parentKey.equals("geometry") && (key.equals("coordinates")
              || parentKey.equals("geometries")))
          ) {
            skip(c)
            // encoded as string because a geometry can have a different dimension
            // based on type, e.g. points are 1d, polygons are 3d, etc.
            if(projection.acceptAll || inChildren || inDescendants) {
              map = map + ((key, (StringType, null)))
            }
          } else {
            val arrType =
              parseArrayType(key, keyProjection)
            arrType match {
              case Some(at) => {
                if(!inChildren && !inDescendants && !projection.acceptAll) {
                  val subType = at._2.asInstanceOf[HashMap[String, Any]]
                  for((k,v) <- subType) {
                    //                      for((_k,v) <- subType) {
                    //                      val k  = if(projection.descendantsTree(_k).parentKey.equals(parentKey)) { ".." + _k } else {_k}
                    map = map + ((k, (ArrayType(NullType), v)))
                  }
                } else {
                  map = map + ((key, at))
                }
              }
              case None => {}
            }

            if(inFilter) {
              // TODO fix it (may require passing the inputStream to reset the position
              // Only filter for arrays with basic types is supported
              // has to be a nested filter otherwise
              // ignore it for now
              if(arrType.nonEmpty)
                rowSequence(keyIndex) = arrType.get
              propagateFilter = true
            }

          }
          isKey = true

        }
        case '"' => {
          if (isKey) {
            val str = consume(c)
            key = str.substring(1, str.length - 1)
            isKey = false
            inFilter = projection.rowMap.contains(key)
            inChildren = projection.childrenTree.contains(key)
            inDescendants = projection.descendantsTree.contains(key)
            if(!projection.acceptAll && !projection.hasDescendants && !inChildren && !inFilter) {
              skip()
              isKey = true
            } else {
              if(inFilter) {
                val (_keyIndex, _, _) = projection.rowMap(key)
                keyIndex = _keyIndex
              }
              keyProjection = if(inChildren) {projection.childrenTree(key) } else if(inDescendants) { projection.descendantsTree(key) } else {
                new ProjectionNode()
              }
              keyProjection.acceptAll = projection.acceptAll || keyProjection.acceptAll
              keyProjection.descendantsTree = keyProjection.descendantsTree ++ projection.descendantsTree
              if(projection.hasDescendants && !inChildren && !inDescendants) {
                keyProjection.notDescending = false
              }

            }
          } else {

            val prevPos = pos
            val str = if(inFilter) {
              consume(c)
            } else { skip(c); ""}
            if(inFilter) {
              rowSequence(keyIndex) = str
              propagateFilter = true
            }
            if(projection.acceptAll || inChildren || inDescendants) {
              if(pos-prevPos <= 1) {
                map = map + ((key, (NullType, null)))
              } else {
                map = map + ((key, (StringType, null)))
              }
            }
            if(keyProjection.isOutputNode) {
              parsedRecords.enqueue(map(key))
            }
            isKey = true

          }
        }
        case numRegExp() => {
          val str = consume(c)
          // pos = skip(reader, encoding, newPos, end, c)
          if(!(str matches "\\d+")) {
            if(inFilter) {
              rowSequence(keyIndex) = str.toDouble
              propagateFilter = true
            }
            if(projection.acceptAll || inChildren || inDescendants) {
              map = map + ((key, (DoubleType, null)))
            }
          } else {
            if(inFilter) {
              rowSequence(keyIndex) = str.toLong
              propagateFilter = true
            }
            if(projection.acceptAll || inChildren || inDescendants) {
              map = map + ((key, (LongType, null)))
            }
          }
          isKey = true
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
        }
        case 'n' => {
          // ignore null fields if not already in projection
          if(inFilter || inChildren || inDescendants) { map = map + ((key, (NullType, null))) }
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
          isKey = true
          pos = pos + stringSize("ull")
          reader.skip(3)
        }
        case 'f' => {
          if(inFilter) {
            rowSequence(keyIndex) = false
            propagateFilter = true
          }
          if(projection.acceptAll || inChildren || inDescendants) {
            map = map + ((key, (BooleanType, null)))
          }
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
          isKey = true
          pos = pos + stringSize("alse")
          reader.skip(4)
        }
        case 't' => {
          if(inFilter) {
            rowSequence(keyIndex) = true
            propagateFilter = true
          }
          if(projection.acceptAll || inChildren || inDescendants) {
            map = map + ((key, (BooleanType, null)))
          }
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
          isKey = true
          pos = pos + stringSize("rue")
          reader.skip(3)
        }
        case '}' => {
          if(map.isEmpty) {
            return None
          }
          if(projection.hasFilter && predicateValues(0) == null) {
            for((_, predicate) <- projection.filterVariables) {
              predicate.propagate(predicateValues, rowSequence)
            }
          }
          if(projection.hasFilter && predicateValues(0) == false) {
            parsedRecords.clear()
            return None
          } else {
            if(projection.isOutputNode) {
              parsedRecords.enqueue(map)
            }
            return Some(map)
          }
        }
        case _ => {} // skip character
      }

      if (propagateFilter && predicateValues(0) == null) {
        projection.filterVariables(key).propagate(predicateValues, rowSequence)
      }
      if (propagateFilter && predicateValues(0) == false) {
        skip('{')
        parsedRecords.clear()
        return None
      }
    }

    throw new Exception(
      "Couldn't parse object at " + pos
    )
  }

  def parseArray(
                      parentKey: String,
                      _projection: ProjectionNode
                    ): Option[(Any, Any)] = {
    var mergedMaps = new HashMap[String, Any]()
    var mergedArrType : (Any, Any) = (ArrayType(NullType), NullType)
    var selectedType : DataType = NullType

    val projection = if(_projection.childrenTree.contains("[*]")) {_projection.childrenTree("[*]")} else {_projection}
    if(_projection.childrenTree.contains("[*]")) {
      projection.acceptAll = projection.acceptAll || _projection.acceptAll
      projection.descendantsTree = _projection.descendantsTree ++ projection.descendantsTree
    }
    if(!_projection.childrenTree.contains("[*]") && !projection.acceptAll && projection.descendantsTree.isEmpty) {
      skip('[')
      return None
    }
    while (true) {
      val i = reader.read()
      if (i == -1) {
        return None
      }
      val c = i.toChar
      pos += charSize(i)
      c match {
        case '{' => {
          val objType =
            parseObjectType("[*]", projection)
          objType match {
            case Some(ot) => mergedMaps = mergedMaps.merged(ot)(reduceKey)
            case None => {}
          }
        }
        case '[' => {
          val arrType =
            parseArrayType(parentKey, projection)
          arrType match {
            case Some(at) => mergedArrType = SchemaInference.reduceKey(("_", mergedArrType), ("_", at))._2.asInstanceOf[(Any, Any)]
            case None => {}
          }
        }
        case '"' => {
          skip(c)
          if(projection.notDescending) {
            selectedType = SchemaInference.selectType(selectedType, StringType)
          }
        }
        case numRegExp() => {
          val str = consume(c)
          if(projection.notDescending) {
            if(str matches "\\d+") {
              selectedType = SchemaInference.selectType(selectedType, LongType)
            } else {
              selectedType = SchemaInference.selectType(selectedType, DoubleType)
            }
          }
        }
        case 'n' => {
          pos = pos + stringSize("ull")
          reader.skip(3)
        }
        case 'f' => {
          pos = pos + stringSize("alse")
          reader.skip(4)
          if(projection.notDescending) {
            selectedType = SchemaInference.selectType(selectedType, BooleanType)
          }
        }
        case 't' => {
          pos = pos + stringSize("rue")
          reader.skip(3)
          if(projection.notDescending) {
            selectedType = SchemaInference.selectType(selectedType, BooleanType)
          }
        }
        case ']' => {
          if(mergedMaps.nonEmpty) {
            if(projection.isOutputNode) {
              parsedRecords.enqueue((ArrayType(NullType), mergedMaps))
            }
            return Some((ArrayType(NullType), mergedMaps))
          }
          if(mergedArrType._2 != NullType) {
            if(projection.isOutputNode) {
              parsedRecords.enqueue((ArrayType(NullType), mergedArrType))
            }
            return Some((ArrayType(NullType), mergedArrType))
          }
          if(projection.notDescending && selectedType != NullType) {
            if(projection.isOutputNode) {
              parsedRecords.enqueue((ArrayType(NullType), selectedType))
            }
            return Some((ArrayType(NullType), selectedType))
          }
          return None
        }
        case _ => {} // skip character
      }
    }
    throw new Exception(
      "Couldn't parse type of array at " + pos
    )
  }

  /*************************************
   SCHEMA INFERENCE FUNCTIONS (same as row parsing but returns types)
   *******************************/
  def parseType(
                 currentChar: Char = '\u0000',
                 projection: ProjectionNode
               ): Option[Any] = {
    var c = '"'
    if (currentChar == '\u0000' || currentChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return None
      }
      c = i.toChar
      pos += charSize(i)
    } else {
      c = currentChar;
    }
    while (true) {
      c match {
        case '{' => {
          val objType =
            parseObjectType("*", projection)
          // if(!(objType contains("GO")))
          // println(c, objType)
          return objType
        }
        case '[' => {
          val arrType =
            parseArrayType("*", projection)
          return arrType
        }
        case '"' => {
          val prevPos = pos
          skip(c)
          val t = if(pos-prevPos <= 1) {  (NullType, null) } else { (StringType, null) } // empty strings are treated as NullType (this solves an issue in one dataset, where they provide an empty string for missing values
          parsedRecords.enqueue(t)
          return Some(t)
        }
        case 'n' => {
          reader.skip(3)
          pos += stringSize("ull")
          parsedRecords.enqueue((NullType, null))
          return Some((NullType, null))
        }
        case 'f' => {
          reader.skip(4)
          pos += stringSize("alse")
          parsedRecords.enqueue((BooleanType, null))
          return Some((BooleanType, null))
        }
        case 't' => {
          reader.skip(3)
          pos += stringSize("rue")
          parsedRecords.enqueue((BooleanType, null))
          return Some((BooleanType, null))
        }
        case numRegExp() => {
          val str= consume(c)
          if(!(str matches "\\d+")) {
            parsedRecords.enqueue((DoubleType, null))
            return Some((DoubleType, null))
          }
          else {
            parsedRecords.enqueue((LongType, null))
            return Some((LongType, null))
          }
          // val newPos = skip(reader, encoding, pos, end, c)
        }
        case _ => {} // these are skipped (e.g. whitespace)
      }

      val i = reader.read()
      if (i == -1) {
        return None
      }
      c = i.toChar
      pos += charSize(i)
    }
    return None
  }

  def parseObjectType(
                       parentKey: String,
                       projection: ProjectionNode,
                     ): Option[HashMap[String, Any]]= {
    var map = HashMap[String, Any]()
    var isKey = true
    var key = ""
    var inFilter = false
    var inChildren = false
    var inDescendants = false
    var keyIndex = -1
    var keyProjection : ProjectionNode = new ProjectionNode()
    val rowSequence = new Array[Any](projection.rowMap.size)
    val predicateValues = new Array[Any](projection.nPredicates)
    while (true) { // parse until full object or end of file
      val i = reader.read()
      if (i == -1) {
        return None
      }
      val c = i.toChar
      pos += charSize(i)

      var propagateFilter = false
      c match {
        case '{' => {

          val objType =
            parseObjectType(key, keyProjection)
          objType match {
            case Some(o) => {
              var newSubMap = new HashMap[String, Any]()
              for((k,v) <- o) {
                if(projection.descendantsTree.contains(k) && projection.descendantsTree(k).parentKey.equals(parentKey)){
                  //                  map = map + ((".." + k,v))
                  map = map + ((k,v))
                } else if((keyProjection.notDescending && keyProjection.acceptAll) || keyProjection.childrenTree.contains(k)) {
                  newSubMap = newSubMap + ((k, v))
                } else {
                  map = map + ((k,v))
                }
              }
              map = map + ((key, newSubMap))
            }
            case None => {}
          }
          if(inFilter) {
            // TODO fix it (may require passing the inputStream to reset the position
            // Only filter for Geometry is supported for this type
            // has to be a nested filter otherwise
            // ignore it for now
            propagateFilter = true
          }
          isKey = true
        }
        case '[' => {
          if (
            (parentKey.equals("geometry") && (key.equals("coordinates")
              || parentKey.equals("geometries")))
          ) {
            skip(c)
            // encoded as string because a geometry can have a different dimension
            // based on type, e.g. points are 1d, polygons are 3d, etc.
            if(projection.acceptAll || inChildren || inDescendants) {
              map = map + ((key, (StringType, null)))
            }
          } else {
            val arrType =
              parseArrayType(key, keyProjection)
            arrType match {
              case Some(at) => {
                if(!inChildren && !inDescendants && !projection.acceptAll) {
                  val subType = at._2.asInstanceOf[HashMap[String, Any]]
                  for((k,v) <- subType) {
                    //                      for((_k,v) <- subType) {
                    //                      val k  = if(projection.descendantsTree(_k).parentKey.equals(parentKey)) { ".." + _k } else {_k}
                    map = map + ((k, (ArrayType(NullType), v)))
                  }
                } else {
                  map = map + ((key, at))
                }
              }
              case None => {}
            }

            if(inFilter) {
              // TODO fix it (may require passing the inputStream to reset the position
              // Only filter for arrays with basic types is supported
              // has to be a nested filter otherwise
              // ignore it for now
              if(arrType.nonEmpty)
                rowSequence(keyIndex) = arrType.get
              propagateFilter = true
            }

          }
          isKey = true

        }
        case '"' => {
          if (isKey) {
            val str = consume(c)
            key = str.substring(1, str.length - 1)
            isKey = false
            inFilter = projection.rowMap.contains(key)
            inChildren = projection.childrenTree.contains(key)
            inDescendants = projection.descendantsTree.contains(key)
            if(!projection.acceptAll && !projection.hasDescendants && !inChildren && !inFilter) {
              skip()
              isKey = true
            } else {
              if(inFilter) {
                val (_keyIndex, _, _) = projection.rowMap(key)
                keyIndex = _keyIndex
              }
              keyProjection = if(inChildren) {projection.childrenTree(key) } else if(inDescendants) { projection.descendantsTree(key) } else {
                new ProjectionNode()
              }
              keyProjection.acceptAll = projection.acceptAll || keyProjection.acceptAll
              keyProjection.descendantsTree = keyProjection.descendantsTree ++ projection.descendantsTree
              if(projection.hasDescendants && !inChildren && !inDescendants) {
                keyProjection.notDescending = false
              }

            }
          } else {

            val prevPos = pos
            val str = if(inFilter) {
              consume(c)
            } else { skip(c); ""}
            if(inFilter) {
              rowSequence(keyIndex) = str
              propagateFilter = true
            }
            if(projection.acceptAll || inChildren || inDescendants) {
              if(pos-prevPos <= 1) {
                map = map + ((key, (NullType, null)))
              } else {
                map = map + ((key, (StringType, null)))
              }
            }
            if(keyProjection.isOutputNode) {
              parsedRecords.enqueue(map(key))
            }
            isKey = true

          }
        }
        case numRegExp() => {
          val str = consume(c)
          // pos = skip(reader, encoding, newPos, end, c)
          if(!(str matches "\\d+")) {
            if(inFilter) {
              rowSequence(keyIndex) = str.toDouble
              propagateFilter = true
            }
            if(projection.acceptAll || inChildren || inDescendants) {
              map = map + ((key, (DoubleType, null)))
            }
          } else {
            if(inFilter) {
              rowSequence(keyIndex) = str.toLong
              propagateFilter = true
            }
            if(projection.acceptAll || inChildren || inDescendants) {
              map = map + ((key, (LongType, null)))
            }
          }
          isKey = true
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
        }
        case 'n' => {
          // ignore null fields if not already in projection
          if(inFilter || inChildren || inDescendants) { map = map + ((key, (NullType, null))) }
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
          isKey = true
          pos = pos + stringSize("ull")
          reader.skip(3)
        }
        case 'f' => {
          if(inFilter) {
            rowSequence(keyIndex) = false
            propagateFilter = true
          }
          if(projection.acceptAll || inChildren || inDescendants) {
            map = map + ((key, (BooleanType, null)))
          }
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
          isKey = true
          pos = pos + stringSize("alse")
          reader.skip(4)
        }
        case 't' => {
          if(inFilter) {
            rowSequence(keyIndex) = true
            propagateFilter = true
          }
          if(projection.acceptAll || inChildren || inDescendants) {
            map = map + ((key, (BooleanType, null)))
          }
          if(keyProjection.isOutputNode) {
            parsedRecords.enqueue(map(key))
          }
          isKey = true
          pos = pos + stringSize("rue")
          reader.skip(3)
        }
        case '}' => {
          if(map.isEmpty) {
            return None
          }
          if(projection.hasFilter && predicateValues(0) == null) {
            for((_, predicate) <- projection.filterVariables) {
              predicate.propagate(predicateValues, rowSequence)
            }
          }
          if(projection.hasFilter && predicateValues(0) == false) {
            parsedRecords.clear()
            return None
          } else {
            if(projection.isOutputNode) {
              parsedRecords.enqueue(map)
            }
            return Some(map)
          }
        }
        case _ => {} // skip character
      }

      if (propagateFilter && predicateValues(0) == null) {
        projection.filterVariables(key).propagate(predicateValues, rowSequence)
      }
      if (propagateFilter && predicateValues(0) == false) {
        skip('{')
        parsedRecords.clear()
        return None
      }
    }

    throw new Exception(
      "Couldn't parse object at " + pos
    )
  }

  def parseArrayType(
                      parentKey: String,
                      _projection: ProjectionNode
                    ): Option[(Any, Any)] = {
    var mergedMaps = new HashMap[String, Any]()
    var mergedArrType : (Any, Any) = (ArrayType(NullType), NullType)
    var selectedType : DataType = NullType

    val projection = if(_projection.childrenTree.contains("[*]")) {_projection.childrenTree("[*]")} else {_projection}
    if(_projection.childrenTree.contains("[*]")) {
      projection.acceptAll = projection.acceptAll || _projection.acceptAll
      projection.descendantsTree = _projection.descendantsTree ++ projection.descendantsTree
    }
    if(!_projection.childrenTree.contains("[*]") && !projection.acceptAll && projection.descendantsTree.isEmpty) {
      skip('[')
      return None
    }
    while (true) {
      val i = reader.read()
      if (i == -1) {
        return None
      }
      val c = i.toChar
      pos += charSize(i)
      c match {
        case '{' => {
          val objType =
            parseObjectType("[*]", projection)
          objType match {
            case Some(ot) => mergedMaps = mergedMaps.merged(ot)(reduceKey)
            case None => {}
          }
        }
        case '[' => {
          val arrType =
            parseArrayType(parentKey, projection)
          arrType match {
            case Some(at) => mergedArrType = SchemaInference.reduceKey(("_", mergedArrType), ("_", at))._2.asInstanceOf[(Any, Any)]
            case None => {}
          }
        }
        case '"' => {
          skip(c)
          if(projection.notDescending) {
            selectedType = SchemaInference.selectType(selectedType, StringType)
          }
        }
        case numRegExp() => {
          val str = consume(c)
          if(projection.notDescending) {
            if(str matches "\\d+") {
              selectedType = SchemaInference.selectType(selectedType, LongType)
            } else {
              selectedType = SchemaInference.selectType(selectedType, DoubleType)
            }
          }
        }
        case 'n' => {
          pos = pos + stringSize("ull")
          reader.skip(3)
        }
        case 'f' => {
          pos = pos + stringSize("alse")
          reader.skip(4)
          if(projection.notDescending) {
            selectedType = SchemaInference.selectType(selectedType, BooleanType)
          }
        }
        case 't' => {
          pos = pos + stringSize("rue")
          reader.skip(3)
          if(projection.notDescending) {
            selectedType = SchemaInference.selectType(selectedType, BooleanType)
          }
        }
        case ']' => {
          if(mergedMaps.nonEmpty) {
            if(projection.isOutputNode) {
              parsedRecords.enqueue((ArrayType(NullType), mergedMaps))
            }
            return Some((ArrayType(NullType), mergedMaps))
          }
          if(mergedArrType._2 != NullType) {
            if(projection.isOutputNode) {
              parsedRecords.enqueue((ArrayType(NullType), mergedArrType))
            }
            return Some((ArrayType(NullType), mergedArrType))
          }
          if(projection.notDescending && selectedType != NullType) {
            if(projection.isOutputNode) {
              parsedRecords.enqueue((ArrayType(NullType), selectedType))
            }
            return Some((ArrayType(NullType), selectedType))
          }
          return None
        }
        case _ => {} // skip character
      }
    }
    throw new Exception(
      "Couldn't parse type of array at " + pos
    )
  }

  def close() : Unit = {
    reader.close()
  }

}
