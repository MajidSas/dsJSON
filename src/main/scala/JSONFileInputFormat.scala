package edu.ucr.cs.bdlab

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import java.io.BufferedReader
import scala.io.BufferedSource

class JSONFileInputFormat extends FileInputFormat[String, String] {

  override def createRecordReader(
                                   split: InputSplit,
                                   context: TaskAttemptContext
                                 ): RecordReader[String, String] = new JSONRecordReader()
}

class JSONRecordReader() extends RecordReader[String, String] {
  var dfa: DFA = _
  var start, end = 0L
  var pos = 0
  var stream: FSDataInputStream = _
  var reader: BufferedReader = _
  var count = 0L
  var key: String = ""
  var value: String = _
  var fileSize: Long = 0L;
  var initialTokenStates : scala.collection.mutable.ArrayBuffer[Int] = scala.collection.mutable.ArrayBuffer[Int]()
  var syntaxStackArray : scala.collection.mutable.ArrayBuffer[String]= scala.collection.mutable.ArrayBuffer[String]()
  var syntaxStackIndex = 0
  var stateLevel = 0;
  var splitPath : String = "";
  override def initialize(
                           inputSplit: InputSplit,
                           context: TaskAttemptContext
                         ): Unit = {

//    var now = Calendar.getInstance().getTimeInMillis
    val split = inputSplit.asInstanceOf[FileSplit]

    fileSize = split.getPath
      .getFileSystem(context.getConfiguration)
      .getContentSummary(split.getPath)
      .getLength;

    start = split.getStart
    end = start + split.getLength

    stream = split.getPath
      .getFileSystem(context.getConfiguration)
      .open(split.getPath)

    val path = context.getConfiguration.get("path");
    val pathTokens = PathProcessor.build(path);
    dfa = new DFA(pathTokens);
    if (start > 0) {
      stream.seek(start)
      reader = new BufferedSource(stream).bufferedReader()
      var startIndex = nextTokenIndex();
      if (startIndex == -1) {
        start = fileSize
        end = fileSize
      } else {
        start = start + startIndex;
      }
    }

    // get appropriate end position
    if (end < fileSize && start < end) {
      stream.seek(end)
      reader = new BufferedSource(stream).bufferedReader()
      val endIndex = nextTokenIndex();
      if(endIndex == -1) {
        end = fileSize
      } else {
        end = end + endIndex
      }
    }

    stream.seek(start)
    reader = new BufferedSource(stream).bufferedReader()
    pos = 0;

    val tokens = dfa.getTokens()
    var toeknStart = 0
    if (start > 0) {
      var i = 0
      var accept = false
      while(!accept && i != -1) {
        i = reader.read()
        pos += 1
        val c = i.toChar
        if(c == '"') {
          toeknStart = pos
          var token = consume('"');
          var t = reader.read().toChar;
          pos += 1
          // skip white space if any
          while (t == ' ' || t == '\t' || t == '\n' || t == 13) {
            t = reader.read().toChar;
            pos += 1
          }
          if (t == ':') {
            token = token.substring(1, token.size - 1)
            if (tokens.contains(token)) {
              accept = true
              initialTokenStates = dfa.getTokenStates(token);
              val stateIndex = initialTokenStates(0);
              var i = 0;
              for (i <- 0 to stateIndex - 1) {
                if (dfa.getStates()(i).stateType.equals("array")) {
                  syntaxStackArray.append(("["))
                } else {
                  syntaxStackArray.append(("{"))
                }
                syntaxStackIndex += 1
              }
              dfa.setState(syntaxStackArray.size)
            }
          }
        }
      }
    }

    stream.seek(start)
    reader = new BufferedSource(stream).bufferedReader()
    pos = 0;

//    now = Calendar.getInstance().getTimeInMillis-now
//    println("Initialization time: " + now.toString)
  }

  override def getCurrentKey(): String = {
    key
  }

  override def getCurrentValue(): String = {
    value
  }

  override def getProgress(): Float = {
    if (pos == 0) {
      0.0f
    } else {
      Math.min(1.0f, (pos).toLong / (end - start).toFloat)
    }
  }

  override def nextKeyValue(): Boolean = {
    while (true) {
      if (pos + start > end) {
        return false
      }
      val c =  reader.read().toChar;
      if (syntaxStackArray.size == dfa.states.size && dfa.checkArray().equals("accept") && c != ']' && c != '}') {
        value = consume(c)
        key = start + "," + end + "," + count
        count += 1
        return true
      }
      else if (c == '[' || c == '{') {
        // TODO make sure the user specified an array in the query
        // the query must contain an array if an accepted token
        // contains an array

        syntaxStackArray.append((c.toString))
        if(c == '[') {
          if (!dfa.toNextStateIfArray()){
            consume(c);
          }
        }
        pos += 1
      } else if ((c == '}' || c == ']')) {
        // TODO handle error if pop is not equal to c
        // or if stack is empty
        if(syntaxStackArray.size == dfa.getCurrentState()) {
          dfa.toPrevState();
        }
        syntaxStackArray.trimEnd(1)
        pos += 1
      } else if (c == '"') {
        // get token
        pos += 1;
        var token = consume('"');
        var t = reader.read().toChar;
        pos += 1
        // skip white space if any
        while (t == ' ' || t == '\t' || t == '\n' || t == 13) {
          t = reader.read().toChar;
          pos += 1
        }
        if (t == ':') {
          token = token.substring(1, token.size - 1)
          val dfaResponse = dfa.checkToken(token, syntaxStackArray.size)
          if (dfaResponse.equals("accept")) {
            value = consume()
            key = start + "," + end + "," + count
            count += 1
            return true
          } else if (dfaResponse.equals("reject")) {
            consume();
          }
        }
      } else {
        pos += 1
      }
    }
    false
  }

  override def close(): Unit = {
    reader.close();
  }

  def consume(firstChar: Char = '\u0000'): String = {
    var output = ""
    var localStack = scala.collection.mutable.ArrayBuffer[Char]()
    var isEscaped = false
    var isString = false
    var prevC = '"'
    var countEscape = 0
    var c = '"'
    if (firstChar == '\u0000' || firstChar == ',') {
      val i = reader.read()
      if (i == -1) {
        return output
      }
      c = i.toChar
      pos += 1
    } else {
      c = firstChar;
    }
    while (true) {
      if (localStack.size == 0 && (c == ',' || c == ']' || c == '}')) {
        reader.reset()
        pos -= 1
        return output;
      } else if (
        !isString &&
          (c == '{' || c == '[' ||
            (!isEscaped && c == '"'))
      ) {
        output += c;
        localStack.append(c)
        if (c == '"')
          isString = true
      } else if (
        (!isString && (c == '}' || c == ']')) ||
          (isString && !isEscaped && c == '"')
      ) {
        output += c;
        localStack.trimEnd(1)
        if(c == '"')
          isString = false;
        if (localStack.size == 0) {
          return output
        }
      }
      else {
        output += c
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
      if (pos + start >= end) {
        return output
      }
      reader.mark(1);
      val i = reader.read()

      if (i == -1) {
        return output
      }
      c = i.toChar
      pos += 1
    }
    output // FIXME handle exception for not fully parsed object
  }
  def nextTokenIndex(): Int = {
    var i: Int = 0
    var accept: Boolean = false
    var index = 0
      i = reader.read()
      var c : Char = i.toChar
      while(i != -1 && (c != '"' || (c == '"' && !accept))) {
        if (c == ',' || c == '{' || c == '[') {
          accept = true
        }
        else if (accept && !(c == ' ' || c == '\t' || c == '\n' || c == 13)){
          accept = false
        }
        i = reader.read()
        index += 1
        c = i.toChar
      }

      if (i == -1) {
        return -1
      }
      index
    }
}
