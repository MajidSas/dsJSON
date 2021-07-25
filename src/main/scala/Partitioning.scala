package edu.ucr.cs.bdlab

import org.apache.hadoop.fs.GlobFilter
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.fs.{Path, FSDataInputStream, FileSystem}
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.PathFilter
import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types._
import org.apache.spark.beast.sql.GeometryUDT
import scala.util.Try

object Partitioning {

  def getFilePaths(options: JsonOptions): Seq[String] = {
    val hadoopConf = SparkContext.getOrCreate().hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    var filter: PathFilter = null
    if (!options.pathGlobFilter.equals("")) {
      filter = new GlobFilter(options.pathGlobFilter).asInstanceOf[PathFilter]
    }
    var filePaths = new ArrayBuffer[String]()
    val hasWildcard = options.filepath contains '*'
    val isDirectory = if (hasWildcard) { false }
    else { fs.getFileStatus(new Path(options.filepath)).isDirectory }

    println("Searching for matching files in path:")
    if (hasWildcard) {
      val statues = fs.globStatus(new Path(options.filepath), filter)
      for (fileStatus <- statues) {
        val path = fileStatus.getPath().toString()
        filePaths.append(path)
        println(path)
      }
    } else if (isDirectory) {
      val iterator =
        fs.listFiles(new Path(options.filepath), options.recursive.toBoolean)
      while (iterator.hasNext()) {
        val status = iterator.next()
        val path = status.getPath()
        if (status.isFile && (filter == null || filter.accept(path))) {
          filePaths.append(path.toString)
          println(path)
        }
      }
    } else {
      filePaths.append(options.filepath)
      println(options.filepath)
    }

    if (filePaths.length == 0)
      println("No files were found!")

    return filePaths.toSeq
  }

  def getFilePartitions(filePaths: Seq[String]): ArrayBuffer[InputPartition] = {
    var partitions: ArrayBuffer[InputPartition] =
      new ArrayBuffer[InputPartition]()
    val sparkBucketSize = SparkContext
      .getOrCreate()
      .getConf
      .getOption("spark.sql.files.maxPartitionBytes")
      .get
      .toLong

    for (path <- filePaths) {
      println("Partitioning " + path)
      val (inputStream, fileSize) = Parser.getInputStream(path)
      val bucketSize = sparkBucketSize.min(fileSize)
      val nPartitions = (1.0 * fileSize / bucketSize).ceil.toInt

      var startIndex = 0L;
      var endIndex = bucketSize;
      for (i <- 0 to nPartitions - 1) {
        partitions.append(
          new JsonInputPartition(
            path,
            startIndex,
            endIndex,
            0,
            0
          )
        )
        println(i + "-> start: " + startIndex + " endIndex: " + endIndex)

        startIndex = endIndex
        endIndex = (startIndex + bucketSize).min(fileSize)
      }
    }

    return partitions
  }

  def speculate(
      partition: JsonInputPartition,
      options: JsonOptions
  ): JsonInputPartition = {
    // println(partition.start)
    // println(options.encounteredTokens)

    var start = partition.start
    var end = partition.end
    var partitionLevel = 0
    var startLevel = 0
    var startState = 0
    var startToken = ""
    var tokenLevels = options.encounteredTokens
    val (inputStream, fileSize) = Parser.getInputStream(partition.path)
    var dfa = options.getDFA()
    val maxQueryLevel = dfa.states.length

    // println(tokenLevels)
    // shift the start index and determine label and level
    var shiftedEndIndex = start
    if (start > 0) {
      var token = ""
      var partitionLevel = maxQueryLevel
      var partitionState = 0
      var partitionLabel = ""
      var skippedLevels = false
      var foundToken = false
      while (!foundToken && shiftedEndIndex < fileSize) {
        val bufferedReader =
          Parser.getBufferedReader(
            inputStream,
            options.encoding,
            shiftedEndIndex
          )
        val (tmpToken, index) =
          Parser.getNextToken(bufferedReader, options.encoding, fileSize)
        // println(tmpToken)
        if (
          (tokenLevels contains tmpToken) && tokenLevels(tmpToken).size == 1
        ) {
          foundToken = true
          token = tmpToken
          //   val ddd =
          val (__level, __state) = tokenLevels(token).toSeq(0)
          partitionLevel = __level
          partitionState = __state
          partitionLabel = token
          if (partitionLevel > partitionState) {
            shiftedEndIndex = shiftedEndIndex + Parser.skipLevels(
              bufferedReader,
              options.encoding,
              partitionLevel - partitionState,
              fileSize
            ) + 1
            partitionLevel = partitionState
            skippedLevels = true
          }
        }
        if (index == -1) {
          shiftedEndIndex = fileSize
        } else {
          shiftedEndIndex += index + Parser.stringSize(
            token,
            options.encoding
          ) + 2 //+ tmpToken.length + 2
        }
      }
      if (!skippedLevels) {
        shiftedEndIndex -= Parser.stringSize(
          partitionLabel,
          options.encoding
        ) + 2
      }

      startLevel = partitionLevel
      startState = partitionState
      start = shiftedEndIndex

      if (startState > 0 && startState == startLevel && !skippedLevels) {
        startState -= 1
        //   _dfa = options.getDFA()
        //   _dfa.setState(startState-1)
        //   val response = _dfa.checkToken(partitionLabel)
        //   if(response.equals("reject")) {
        //     startState -= 1
        //   }
        println(
          "label: " + partitionLabel + " startLevel: " + startLevel + " startState: " + startState
        )
      }

    }

    // shift the end index
    shiftedEndIndex = end
    if (end < fileSize) {
      var token = ""
      var partitionLevel = maxQueryLevel
      var partitionState = 0
      var partitionLabel = ""
      var skippedLevels = false
      var foundToken = false
      while (!foundToken && shiftedEndIndex < fileSize) {
        val bufferedReader =
          Parser.getBufferedReader(
            inputStream,
            options.encoding,
            shiftedEndIndex
          )
        val (tmpToken, index) =
          Parser.getNextToken(bufferedReader, options.encoding, fileSize)
        if (
          (tokenLevels contains tmpToken) && tokenLevels(tmpToken).size == 1
        ) {
          foundToken = true
          token = tmpToken
          val (__level, __state) = tokenLevels(token).toSeq(0)
          partitionLevel = __level
          partitionState = __state
          partitionLabel = token
          if (partitionLevel > partitionState) {
            shiftedEndIndex = shiftedEndIndex + Parser.skipLevels(
              bufferedReader,
              options.encoding,
              partitionLevel - partitionState,
              fileSize
            )
            partitionLevel = partitionState
            skippedLevels = true
          }
        }
        if (index == -1) {
          shiftedEndIndex = fileSize
        } else {
          shiftedEndIndex += index + Parser.stringSize(
            token,
            options.encoding
          ) + 2 //+ tmpToken.length + 2
        }
      }
      if (!skippedLevels) {
        shiftedEndIndex -= Parser.stringSize(
          partitionLabel,
          options.encoding
        ) + 3
      }

      end = shiftedEndIndex
    }

    println(start, end, startLevel, startState)

    return new JsonInputPartition(
      partition.path,
      start,
      end,
      startLevel,
      startState
    )
  }

//   def speculation(
//       options: JsonOptions,
//       schema: StructType
//   ): Array[InputPartition] = {
//     val filePaths: Seq[String] = getFilePaths(options)

//     val tokenLevels = getEncounteredTokens(options, schema)
//     // println("TOKEN LEVELS")
//     // println(tokenLevels)

//     options.encounteredTokens = tokenLevels

//     val partitions = getFilePartitions(filePaths)

//     val sc = SparkContext.getOrCreate()
//     val x = sc.parallelize(partitions)
//     val y = x.map(partition =>
//       speculate(partition.asInstanceOf[JsonInputPartition], options)
//     )
//     val z = y.collect()
//     var q: Array[InputPartition] = new Array[InputPartition](z.length)
//     var i = 0
//     for (p <- z) {
//       val partition = p.asInstanceOf[JsonInputPartition]
//       q(i) = partition
//       i += 1
//     }
//     // println(z(0).asInstanceOf[JsonInputPartition].startLabel)

//     // for (path <- filePaths) {
//     //   println("Partitioning " + path)
//     //   val (inputStream, fileSize) = Parser.getInputStream(path)
//     //   val bucketSize = sparkBucketSize.min(fileSize)
//     //   val nPartitions = (1.0 * fileSize / bucketSize).ceil.toInt
//     //   dfa = options.getDFA()

//     //   // var countPartitions = 0
//     //   var startIndex = 0L;
//     //   var endIndex = bucketSize;
//     //   var partitionLevel = 0
//     //   var skippedLevels = false
//     //   var partitionLabel = ""
//     //   var i = 0
//     //   for (i <- 0 to nPartitions - 1) {
//     //     var shiftedEndIndex = endIndex
//     //     var token = ""
//     //     var nextPartitionLevel = maxQueryLevel
//     //     var nextPartitionLabel = ""
//     //     if (i < nPartitions - 1) {
//     //       // println(i, "Partition token index", token, index)
//     //       var foundToken = false
//     //       while (!foundToken && shiftedEndIndex < fileSize) {
//     //         val bufferedReader =
//     //           Parser.getBufferedReader(inputStream, shiftedEndIndex)
//     //         val (tmpToken, index) =
//     //           Parser.getNextToken(bufferedReader, fileSize)
//     //         if (
//     //           (tokenLevels contains tmpToken) && tokenLevels(tmpToken).size == 1
//     //         ) {
//     //           foundToken = true
//     //           token = tmpToken
//     //           nextPartitionLevel = tokenLevels(token).toSeq(0)
//     //           nextPartitionLabel = token
//     //           // println(token, tokenLevel, shiftedEndIndex, index)
//     //           if (nextPartitionLevel > maxQueryLevel) {
//     //             shiftedEndIndex = shiftedEndIndex + Parser.skipLevels(
//     //               bufferedReader,
//     //               nextPartitionLevel - maxQueryLevel,
//     //               fileSize
//     //             )
//     //             nextPartitionLevel = maxQueryLevel
//     //             skippedLevels = true
//     //           }
//     //         }
//     //         if (index == -1) {
//     //           shiftedEndIndex = fileSize
//     //         } else {
//     //           shiftedEndIndex += index + token.length + 2 //+ tmpToken.length + 2
//     //         }

//     //       }

//     //       // if(nextPartitionLevel == maxQueryLevel &&
//     //       // dfa.states.last.value.equals(nextPartitionLabel)) {
//     //       //     shiftedEndIndex = shiftedEndIndex - (nextPartitionLabel.length + 2)
//     //       // }

//     //       // println(
//     //       //   (foundToken, token, endIndex, shiftedEndIndex, tokenLevels(token)),
//     //       //   tokenLevel
//     //       // )
//     //     }
//     //     //   println(i, startIndex, shiftedEndIndex, token, tokenLevel, nextPartitionLabel, nextPartitionLevel)
//     //     if (!skippedLevels) {
//     //       shiftedEndIndex -= (nextPartitionLabel.length() + 2)
//     //     }
//     //     if (startIndex < fileSize) {
//     //       partitions.append(
//     //         new JsonInputPartition(
//     //           path,
//     //           startIndex,
//     //           shiftedEndIndex,
//     //           partitionLevel,
//     //           partitionLabel
//     //         )
//     //       )

//     //       println(
//     //         "Partition: " + (i + 1) + ", start: " + startIndex + ", end: " + shiftedEndIndex + " Level: " + partitionLevel + " LabeL: " + partitionLabel
//     //       )
//     //       // countPartitions = i + 1
//     //       partitionLevel = nextPartitionLevel
//     //       partitionLabel = nextPartitionLabel
//     //     }
//     //     startIndex = shiftedEndIndex
//     //     endIndex = (startIndex + bucketSize).min(fileSize)
//     //   }
//     // }

//     q
//   }

  def mergeSyntaxStack(
      _s1: ArrayBuffer[String],
      _s2: ArrayBuffer[String],
      _s2Positions: ArrayBuffer[Long],
      prevEnd: Long
  ): (ArrayBuffer[String], ArrayBuffer[String], ArrayBuffer[Long]) = {
    var s2 = new ArrayBuffer[String]()
    var s2Positions = new ArrayBuffer[Long]()
    var s3 = new ArrayBuffer[String]()
    var skippedString = false
    var i = 0
    while (i < _s2.size) {
      val pos = _s2Positions(i)
      val elem = _s2(i)
      if (pos > prevEnd) {
        s2.append(elem)
        s2Positions.append(pos)
      }
      //   else {
      //     s2.trimStart(1)
      //     s2Positions.trimEnd(1)
      //   }
      i += 1
    }

    for (elem <- _s1) {
      s3.append(elem)
    }
    for (elem <- s2) {
      if (s3.isEmpty) {
        s3.append(elem)
      } else if (elem.equals("}")) {
        if (s3.last.equals("{")) {
          s3.trimEnd(1)
        } else {
          s3.trimEnd(2)
        }
      } else if (elem.equals("]")) {
        s3.trimEnd(1)
      } else if (elem.equals("{")) {
        s3.append(elem)
      } else if (elem.equals("[")) {
        s3.append(elem)
      } else { // key
        s3.append(elem)
      }
    }
    return (s3, s2, s2Positions)
  }
  def getEndState(
      partition: JsonInputPartition,
      options: JsonOptions
  ): (String, Long, Long, ArrayBuffer[String], ArrayBuffer[Long], Boolean) = {
    val now = System.nanoTime;
    var syntaxStack: ArrayBuffer[String] = new ArrayBuffer[String]()
    var syntaxPositions: ArrayBuffer[Long] = new ArrayBuffer[Long]()

    val (inputStream, fileSize) = Parser.getInputStream(partition.path)
    var bufferedReader =
      Parser.getBufferedReader(inputStream, options.encoding, partition.start)
    var pos = partition.start
    var start = partition.start
    var token = ""
    var acceptToken = false
    var isValue = false
    var countSymbols = 0 // counts the open occurrences of {
    var prevC: Char = '0'
    var ignore = false
    var isString = false
    var isEscaped = false
    var escapeCounter = 0
    val controlChars = List("{", "}", "[", "]", "\"")
    while (pos < partition.end) {
      val c = bufferedReader.read().toChar;
      pos += Parser.charSize(c)

      //   if (isString && (c != '"' || isEscaped)) {
      //     if (!isValue) {
      //       token += c
      //     }
      //     if (c == '\\') {
      //       escapeCounter += 1
      //       if (escapeCounter % 2 == 1) {
      //         isEscaped = true
      //       } else {
      //         isEscaped = false
      //       }
      //     } else {
      //       isEscaped = false
      //       escapeCounter = 0
      //     }
      //   } else
      if (c == '{') {
        syntaxStack.append("{")
        syntaxPositions.append(pos)
        isValue = false
      } else if (c == '[') {
        syntaxStack.append("[")
        syntaxPositions.append(pos)
        // isValue = false
      } else if (c == '}') {
        if (syntaxStack.nonEmpty) {
          if (
            !(controlChars contains syntaxStack.last) && syntaxStack.length >= 2
          ) { // isToken
            syntaxStack.trimEnd(1) // remove token
            syntaxPositions.trimEnd(1)
          }
          if (syntaxStack.last == "{") {
            syntaxStack.trimEnd(1)
            syntaxPositions.trimEnd(1)
          } else {
            syntaxStack.append("}")
            syntaxPositions.append(pos)
          }
        } else { // empty
          syntaxStack.append("}")
          syntaxPositions.append(pos)
        }
      } else if (c == ']') {
        if (syntaxStack.nonEmpty && syntaxStack.last == "[") {
          syntaxStack.trimEnd(1)
          syntaxPositions.trimEnd(1)
        } else {
          syntaxStack.append("]")
          syntaxPositions.append(pos)
        }
      } else if (c == '"') {
        if (syntaxStack.nonEmpty) {
          if (syntaxStack.last == "{") {
            val (_token, _pos) = Parser.consume(
              bufferedReader,
              options.encoding,
              pos,
              partition.end,
              c
            )
            token = _token.substring(1, _token.size - 1)
            pos = _pos
            syntaxStack.append(token)
            syntaxPositions.append(pos)
          } else if (!(controlChars contains syntaxStack.last)) {
            syntaxStack.trimEnd(1)
            syntaxPositions.trimEnd(1)
            val (_token, _pos) = Parser.consume(
              bufferedReader,
              options.encoding,
              pos,
              partition.end,
              c
            )
            token = _token.substring(1, _token.size - 1)
            pos = _pos
            syntaxStack.append(token)
            syntaxPositions.append(pos)
          } else if(isValue) {
            pos =
            Parser.skip(bufferedReader, options.encoding, pos, partition.end, c)
          }
        } else if (isValue) {
          pos =
            Parser.skip(bufferedReader, options.encoding, pos, partition.end, c)
          // token = _token.substring(1, _token.size-1)
          // if (syntaxStack.last == "\"") {
          //   isString = false
          //   isValue = false
          //   isEscaped = false
          //   escapeCounter = 0
          //   syntaxStack.trimEnd(1)
          //   syntaxPositions.trimEnd(1)
          // } else {
          //   isString = true
          //   isEscaped = false
          //   escapeCounter = 0
          // }
          //   }
        } else {
          val (_token, _pos) = Parser.consume(
            bufferedReader,
            options.encoding,
            pos,
            partition.end,
            c
          )
          // println("START OF PARTITION")
          val isValidString = Parser.isValidString(
            _token
              .substring(1, _token.size - 1)
          )
          // println(_token, isValidString)

          if (isValidString) { // skip it
            pos = _pos
          } else { // only skip the quote character
            syntaxStack.append("\"")
            syntaxPositions.append(pos)
            // reset reader to consider json characters
            bufferedReader =
              Parser.getBufferedReader(inputStream, options.encoding, pos)
          }
          // start = pos
        }
      } else if (c == ':') {
        isValue = true
      } else if (
        c == ',' && (syntaxStack.isEmpty || 
        (syntaxStack.nonEmpty && !syntaxStack.last.equals("[")))
      ) {
        isValue = false
      }
    }
    val pastEnd = pos > partition.end

    val micros = (System.nanoTime - now) / 1000;
    // println("%d microseconds".format(micros));
    // println(partition.start, pos, syntaxStack, syntaxPositions)
    return (
      partition.path,
      partition.start,
      pos,
      syntaxStack,
      syntaxPositions,
      pastEnd
    )

  }

  def partitionLevelSkipping(
      state: Array[String],
      options: JsonOptions
  ): (Int, Int, Int) = {
    var dfa = options.getDFA()

    var level = 0
    var skipLevels = 0
    var dfaState = 0

    // get level before the first rejected or accepted state
    var i = 0;
    var isComplete = false // or accepted
    while (i < state.length && !isComplete) {
      var response = ""
      val elem = state(i)
      if (elem.equals("[")) {
        if (dfa.toNextStateIfArray()) {
          response = dfa.checkArray()
          println(response, elem, level)
          if (response.equals("accept") || response.equals("continue")) {
            level += 1
          } else {
            response = "reject"
            i -= 1
          }
        }
      } else if (elem.equals("{")) {
        level += 1
      } else { // key
        response = dfa.checkToken(elem, level)
        // println(response, elem, level)
        // if(response.equals("reject")) {
        //     level -= 1
        //     skipLevels += 1
        // }
      }

      if (response.equals("accept") || response.equals("reject")) {
        isComplete = true
      }
      i += 1

    }

    while (i < state.length) {
      val elem = state(i)
      if (elem.equals("[") || elem.equals("{")) {
        skipLevels += 1
      }
      i += 1
    }

    return (level, skipLevels, dfa.getCurrentState())
  }

  def fullPass(
      options: JsonOptions
  ): Array[InputPartition] = {
    val filePaths: Seq[String] = if (options.filePaths == null) {
      getFilePaths(options)
    } else {
      options.filePaths
    }

    val partitions = getFilePartitions(filePaths)

    println("######################")
    println(partitions.length)

    val sc = SparkContext.getOrCreate()
    val endStates = sc
      .parallelize(partitions)
      .map(partition =>
        getEndState(partition.asInstanceOf[JsonInputPartition], options)
      )
      .collect()
    // var q: Array[InputPartition] = new Array[InputPartition](z.length)
    // var i = 0
    var prevStack = new ArrayBuffer[String]()
    var prevIsString = false
    var prevEnd = 0L
    var i: Integer = 0

    var partitionInitialStates = ArrayBuffer[
      (
          String, // path
          Long, // start
          Long, // end
          ArrayBuffer[String], // initial state (end state of previous)
          ArrayBuffer[String], // in-state
          ArrayBuffer[Long], // positions of in-state
          Boolean, // isString
          Int, // Level
          Int, // skipLevels
          Int // dfaState
      )
    ]()

    for (p <- endStates) {
      val (path, start, end, syntaxStack, syntaxPositions, isString) =
        p.asInstanceOf[
          (String, Long, Long, ArrayBuffer[String], ArrayBuffer[Long], Boolean)
        ]

      val (level, skipLevels, dfaState) =
        partitionLevelSkipping(prevStack.toArray, options)

      // println("####### " + i)
      println("Start state: " + prevStack)
      println("End state: " + syntaxStack)
      println(level + " " + skipLevels + " " + dfaState)

      val (stack, _syntaxStack, _syntaxPositions) =
        mergeSyntaxStack(prevStack, syntaxStack, syntaxPositions, prevEnd)

      //   if(stringEndPos > 0) {
      //       prevEnd = stringEndPos
      //   }
      //   println(
      //       path,
      //       prevEnd,
      //       end,
      //   prevStack.toString,
      //   _syntaxStack.toString,
      //   _syntaxPositions.toString,
      //       isString,
      //       level,
      //       skipLevels
      // )
      // println(i + "-> start: " + prevEnd + " endIndex: " + end)

      partitionInitialStates.append(
        (
          path,
          prevEnd,
          end,
          prevStack,
          _syntaxStack,
          _syntaxPositions,
          isString,
          level,
          skipLevels,
          dfaState
        )
      )
      prevStack = stack
      prevIsString = isString
      prevEnd = end
      i += 1
      //   val partition = p.asInstanceOf[JsonInputPartition]
      //   q(i) = partition
      //   i += 1
    }

    i = partitionInitialStates.length-1
    var prevStart = 0L
    var prevPath = ""
    var finalPartitions: ArrayBuffer[InputPartition] =
      new ArrayBuffer[InputPartition]
    while (i >= 0) {
      val (
        path,
        start,
        end,
        initialState,
        syntaxStack,
        syntaxPositions,
        isString,
        level,
        skipLevels,
        dfaState
      ) =
        partitionInitialStates(i)

      //     println(i + "-> start: " + start + " endIndex: " + end)

      //     println(
      //     initialState,
      //     syntaxStack,
      //     syntaxPositions,
      //     isString,
      //     level,
      //     skipLevels
      //   )

      var _skipLevels = skipLevels
      var shiftedStart = start
      var j = i
      while (_skipLevels > 0 && j < partitionInitialStates.length) {
        val (
          path2,
          start2,
          end2,
          _,
          syntaxStack2,
          syntaxPositions2,
          _,
          _,
          _,
          _
        ) =
          partitionInitialStates(j)

        if (path.equals(path2)) {
          var k = 0;
          while (k < syntaxStack2.length && _skipLevels > 0) {
            val c = syntaxStack2(k)
            val pos = syntaxPositions2(k)
            if (c.equals("}") || c.equals("]")) {
              _skipLevels -= 1
              // println(_skipLevels, c, pos)
              if (_skipLevels == 0) {
                shiftedStart = pos
              }
            }
            k += 1
          }
        } else {
          j = partitionInitialStates.length
        }
        j += 1
      }

      if (shiftedStart < end) {
      //   println(shiftedStart, end, level, dfaState)
      // val (inputStream, fileSize) = Parser.getInputStream(path)

      //   var bufferedReader = Parser.getBufferedReader(inputStream, options.encoding,
      //       shiftedStart
      //     )
      //     var s = ""
      //     for (i <- 0 to 100) {
      //       s += bufferedReader.read().toChar
      //     }
      //   println("Start: " + s)
      //   bufferedReader = Parser.getBufferedReader(inputStream, options.encoding,
      //       end-100
      //     )
      //     s = ""
      //     for (i <- 0 to 100) {
      //       s += bufferedReader.read().toChar
      //     }
      //   println("End: " + s)
        val _end = if(prevPath.equals(path)) { prevStart } else { end }
        println(shiftedStart, _end, level, dfaState)

        finalPartitions.append(
          (new JsonInputPartition(path, shiftedStart, _end, level, dfaState))
            .asInstanceOf[InputPartition]
        )

        prevPath = path
        prevStart = shiftedStart
      } else {
        prevPath = ""
      }

      i -= 1
    }

    finalPartitions.toArray.reverse

  }

}
