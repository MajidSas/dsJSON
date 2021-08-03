package edu.ucr.cs.bdlab

import java.nio.charset.Charset
import java.nio.charset.CodingErrorAction
import org.apache.hadoop.fs.{Path, FSDataInputStream, FileSystem}
import org.apache.spark.SparkContext
import java.io.File
import java.io.BufferedReader
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import scala.collection.immutable.HashMap
import java.lang.StringBuilder 


object Parser {
  def getInputStream(filename: String): (FSDataInputStream, Long) = {
    val conf = SparkContext.getOrCreate().hadoopConfiguration
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
    inputStream.seek(startPos)
    val decoder = Charset.forName(encoding).newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val source = scala.io.Source.fromInputStream(inputStream)(decoder)
    return source.bufferedReader()
  }


  def charSize(i: Int): Int = {
    if(i < 128) return { 1 }
    else if(i < 2048) return { 2 }
    else if(i < 65534) return { 3 }
    else {return 4}
  }

  def stringSize(s: String, encoding: String = "UTF-8"): Int = {
    var size : Int = 0
    for(c <- s) {
      size += charSize(c.toInt)
    }
    return size
  }

  def initSyntaxStack(
      dfa: DFA,
      level: Int
  ): scala.collection.mutable.ArrayBuffer[Char] = {
    var syntaxStackArray: ArrayBuffer[Char] = ArrayBuffer[Char]()
    var i = 0
    if (level > 0) {
      for (i <- 0 to level - 1) {
        if (dfa.getStates()(i).stateType.equals("array")) {
          syntaxStackArray.append(('['))
        } else {
          syntaxStackArray.append(('{'))
        }
      }
    }
    syntaxStackArray
  }

  def isValidString(s: String): Boolean = {
    return !s
      .replaceAll("(?i)(false|true|null|NaN|Infinity|Inf)", "")
      .matches(raw"[\s+{}\[\],:0-9.\-]+")
  }

  def isWhiteSpace(c: Char): Boolean = {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 13
  }

  def getNextToken(
      reader: BufferedReader,
      encoding: String,
      end: Long
  ): (String, Int) = {
    var i: Int = 0
    var accept: Boolean = false
    var index = 0
    i = reader.read()
    var c: Char = i.toChar
    while (i != -1) {
      while (i != -1 && (c != '"' || (c == '"' && !accept))) {
        if (c == ',' || c == '{') {
          accept = true
        } else if (accept && !isWhiteSpace(c)) {
          accept = false
        }
        i = reader.read()
        c = i.toChar
        index += charSize(i)
      }
      if (accept && i != -1) {
        val (token, _) = consume(reader, encoding, 0, end, c)
        i = reader.read()
        var t = i.toChar
        var tmpIndex = charSize(i)
        while (isWhiteSpace(t)) {
          i = reader.read()
          t = i.toChar;
          tmpIndex += charSize(i)
        }
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
        val (s, pos) = consume(reader, encoding, 0, end, c)
        index += stringSize(s.substring(1))
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
      tokens: HashMap[String, Set[(Int, Int)]],
      token: String,
      level: Int,
      dfaState: Int
  ): HashMap[String, Set[(Int, Int)]] = {
    var _tokens = tokens

    if (tokens contains token) {
      _tokens = _tokens.updated(token, _tokens(token) + ((level, dfaState)))
    } else {
      _tokens = _tokens + (token -> Set((level, dfaState)))
    }
    _tokens
  }
  def getEncounteredTokens(
      parsedValue: Any,
      level: Int,
      dfaState: Int
  ): HashMap[String, Set[(Int, Int)]] = {
    var encounteredTokens = HashMap[String, Set[(Int, Int)]]()
    parsedValue match {
      case _: HashMap[_, _] => {
        for (
          (k, v) <- parsedValue.asInstanceOf[HashMap[String, Any]].iterator
        ) {
          encounteredTokens =
            addToken(encounteredTokens, k, level + 1, dfaState)
          encounteredTokens = mergeMapSet(
            encounteredTokens,
            getEncounteredTokens(v, level + 1, dfaState)
          )
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

  def getNextMatch(
      reader: BufferedReader,
      encoding: String,
      start: Long,
      end: Long,
      _pos: Long,
      syntaxStackArray: ArrayBuffer[Char],
      dfa: DFA,
      getTokens: Boolean = false,
      getTypes: Boolean = false
  ): (Boolean, Any, HashMap[String, Set[(Int, Int)]], Long) = {

    var encounteredTokens = HashMap[String, Set[(Int, Int)]]()
    var pos = _pos
    while (true) {
      if (pos >= end) {
        return (false, null, encounteredTokens, pos)
      }
      val i = reader.read()
      val c = i.toChar;
      pos += charSize(i)

      if (isWhiteSpace(c)) {
        // SKIP
      } else if (
        syntaxStackArray.size == dfa.states.size && dfa
          .checkArray()
          .equals("accept") && c != ']' && c != '}'
      ) {
        val (value, _p) = if (getTypes) {
          parseType(reader, encoding, pos, end, c)
        } else {
          _parse(reader, encoding, pos, end, c)
        }
        pos = _p
        return (true, value, encounteredTokens, pos)
      } else if (c == '{') {
        syntaxStackArray.append((c))
      } else if (c == '[') {
        if (!dfa.toNextStateIfArray()) {
          pos = skip(reader, encoding, pos, end, c)
        } else {
          syntaxStackArray.append((c))

        }
      } else if ((c == '}' || c == ']')) {
        // TODO handle error if pop is not equal to c
        // or if stack is empty (i.e. invalid initialization)
        if (syntaxStackArray.size == dfa.getCurrentState()) {
          dfa.toPrevState();
        }
        syntaxStackArray.trimEnd(1)
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
          val dfaResponse = dfa.checkToken(token, syntaxStackArray.size)
          // println(token, dfaResponse, dfa.currentState, syntaxStackArray)
          if (dfaResponse.equals("accept")) {

            val (value, _p) = if (getTypes) {
              parseType(reader, encoding, pos, end)
            } else {
              _parse(reader, encoding, pos, end)
            }
            pos = _p

            return (true, value, encounteredTokens, pos)
          } else if (dfaResponse.equals("reject")) {

            if (getTokens) {
              val (parsedValue, _p) = _parse(reader, encoding, pos, end)
              pos = _p
              encounteredTokens = addToken(
                encounteredTokens,
                token,
                syntaxStackArray.size,
                dfa.currentState
              )
              encounteredTokens = mergeMapSet(
                encounteredTokens,
                getEncounteredTokens(
                  parsedValue,
                  syntaxStackArray.size,
                  dfa.currentState
                )
              )
            } else {
              pos = skip(reader, encoding, pos, end)
            }

          }
        }
      }
    }
    return (false, null, encounteredTokens, pos)
  }

  def mergeMapSet(
      m1: HashMap[String, Set[(Int, Int)]],
      m2: HashMap[String, Set[(Int, Int)]]
  ): HashMap[String, Set[(Int, Int)]] = {
    var m = m1
    for ((k, v) <- m2) {
      if (m contains k) {
        m = m.updated(k, m(k) ++ m2(k))
      } else {
        m = m + (k -> v)
      }
    }
    m
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
      if (localStack.size == 0 && (c == ',' || c == ']' || c == '}')) {
        reader.reset()
        pos -= charSize(c.toInt)
        return (output.toString(), pos);
      } else if (
        !isString &&
        (c == '{' || c == '[' ||
          (!isEscaped && c == '"'))
      ) {
        output.append(c)
        localStack.append(c)
        if (c == '"')
          isString = true
      } else if (
        (!isString && (c == '}' || c == ']')) ||
        (isString && !isEscaped && c == '"')
      ) {
        output.append(c)
        localStack.trimEnd(1)
        if (c == '"')
          isString = false;
        if (localStack.size == 0) {
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
      if (pos >= end && localStack.size == 0) {
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
      if (localStack.size == 0 && (c == ',' || c == ']' || c == '}')) {
        reader.reset()
        pos -= charSize(c.toChar)
        return pos;
      } else if (
        !isString &&
        (c == '{' || c == '[' ||
          (!isEscaped && c == '"'))
      ) {
        localStack.append(c)
        if (c == '"')
          isString = true
      } else if (
        (!isString && (c == '}' || c == ']')) ||
        (isString && !isEscaped && c == '"')
      ) {
        localStack.trimEnd(1)
        if (c == '"')
          isString = false;
        if (localStack.size == 0) {
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
      if (pos >= end && localStack.size == 0) {
        return pos
      }
      reader.mark(1);
      val i = reader.read()

      if (i == -1) {
        return pos
      }
      c = i.toChar
      pos += charSize(i)
      if (pos >= end) {
        return pos
      }
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
      currentChar: Char = '\u0000'
  ): (Any, Long) = {
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
          val (obj, newPos) = _parseObject(reader, encoding, pos, end, "")
          return (obj, newPos)
        }
        case '[' => {
          val (arr, newPos) = _parseArray(reader, encoding, pos, end, "")
          return (arr, newPos)
        }
        case '"' => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          return (str.substring(1, str.length - 1), newPos)
        }
        case 'n' => {
          reader.skip(3)
          return (null, pos + stringSize("ull", encoding))
        }
        case 'f' => {
          reader.skip(4)
          return (false, pos + stringSize("alse", encoding))
        }
        case 't' => {
          reader.skip(3)
          return (true, pos + stringSize("rue", encoding))
        }
        case numRegExp() => {
          val (num, newPos) = _parseDouble(reader, encoding, pos, end, c)
          return (num, newPos)
        }
        case _ => {} // these are skipped (e.g. whitespace)
      }

      val i = reader.read()
      if (i == -1) {
        return (null, pos)
      }
      c = i.toChar
      pos += charSize(i)
    }
    return (null, pos)
  }

  def _parseObject(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      parentKey: String
  ): (HashMap[String, Any], Long) = {
    // TODO add filtering and projection
    var map = HashMap[String, Any]()
    var isKey = true
    var key = ""

    var pos: Long = _pos
    while (true) { // parse until full object or end of file
      val i = reader.read()
      if (i == -1) {
        return (null, pos) // maybe raise an exception since object is not fully parsed
      }
      val c = i.toChar
      pos += charSize(i)
      c match {
        case '{' => {
          val (obj, newPos) = _parseObject(reader, encoding, pos, end, key)
          map = map + ((key, obj))
          isKey = true
          pos = newPos
        }
        case '[' => {
          if (
            (parentKey.equals("geometry") || parentKey.equals("geometries"))
            && key.equals("coordinates")
          ) {
            val (coordinates, newPos) =
              consume(reader, encoding, pos, end, c)
            map =
              map + ((key, coordinates.substring(1, coordinates.length - 1)))
            pos = newPos
          } else {
            val (arr, newPos) = _parseArray(reader, encoding, pos, end, key)
            map = map + ((key, arr))
            pos = newPos
          }
          isKey = true

        }
        case '"' => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          if (isKey) {
            key = str.substring(1, str.length - 1)
            isKey = false
          } else {
            map = map + ((key, str.substring(1, str.length - 1)))
            isKey = true
          }
          pos = newPos
        }
        case numRegExp() => {
          val (num, newPos) = _parseDouble(reader, encoding, pos, end, c)
          map = map + ((key, num))
          isKey = true
          pos = newPos
        }
        case 'n' => {
          map = map + ((key, null))
          isKey = true
          pos = pos + stringSize("ull", encoding)
          reader.skip(3)
        }
        case 'f' => {
          map = map + ((key, false))
          isKey = true
          pos = pos + stringSize("alse", encoding)
          reader.skip(4)
        }
        case 't' => {
          map = map + ((key, true))
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

  def _parseArray(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      parentKey: String
  ): (List[Any], Long) = {
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
          val (obj, newPos) =
            _parseObject(reader, encoding, pos, end, parentKey)
          arr.append(obj)
          pos = newPos
        }
        case '[' => {

          val (_arr, newPos) =
            _parseArray(reader, encoding, pos, end, parentKey)
          arr.append(_arr)
          pos = newPos
        }
        case '"' => {
          val (str, newPos) = consume(reader, encoding, pos, end, c)
          arr.append(str.substring(1, str.length - 1))
          pos = newPos
        }
        case numRegExp() => {
          val (num, newPos) = _parseDouble(reader, encoding, pos, end, c)
          arr.append(num)
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
          return (arr.toList, pos)
        }
        case _ => {} // skip character
      }
    }
    throw new Exception(
      "Couldn't parse array at " + pos
    )
  }

  def _parseDouble(
      reader: BufferedReader,
      encoding: String,
      _pos: Long,
      end: Long,
      currentChar: Char = '\u0000'
  ): (Double, Long) = {
    var pos: Long = _pos
    var hasDecimal: Boolean = false
    var str = new StringBuilder()
    var strSize = 0
    if (currentChar == 'N') {
      pos += stringSize("aN", encoding)
      reader.skip(2)
      return (Double.NaN, pos)
    } else if (currentChar == 'I') {
      pos += stringSize("nfinity", encoding)
      reader.skip(7)
      return (Double.PositiveInfinity, pos)
    }

    var c = currentChar
    while (true) {
      if (
        c.isDigit || (strSize > 0 && c == '.' && !hasDecimal) || (strSize == 0 && c == '-')
        || (hasDecimal && (c == 'E' || c == 'e'))
      ) {
        if (c == '.') {
          hasDecimal = true
        }
        // str = str + c
        str.append(c)
        strSize += 1
      } else if (c == 'I' && str.equals("-")) {
        pos += stringSize("nfinity", encoding)
        reader.skip(7)
        return (Double.NegativeInfinity, pos)
      } else {
        reader.reset()
        pos -= charSize(c.toInt)
        return (str.toString.toDouble, pos)
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
          val newPos = skip(reader, encoding, pos, end, c)
          return ((DoubleType, null), newPos)
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
            (parentKey.equals("geometry") && key.equals("coordinates"))
            || parentKey.equals("geometries") && key.equals("coordinates")
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
          val newPos = skip(reader, encoding, pos, end, c)
          map = map + ((key, (DoubleType, null)))
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
          val newPos = skip(reader, encoding, pos, end, '[')
          pos = newPos
          return ((ArrayType(NullType), (DoubleType, null)), pos)
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
