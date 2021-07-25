package edu.ucr.cs.bdlab

import org.apache.spark.sql.connector.read.{PartitionReader}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import com.fasterxml.jackson.module.scala.deser.overrides
import org.apache.hadoop.fs.FSDataInputStream
import java.io.BufferedReader
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.Filter

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
  var stateLevel = 0;
  var splitPath: String = "";
  
  def this(
      _inputPartition: JsonInputPartition,
      schema: StructType,
      options: JsonOptions,
      filters: Array[Filter]) {
    this()
    this.inputPartition = if(options.partitioningStrategy.equals("speculation")) {
      println("Speculating.....")
      Partitioning.speculate(_inputPartition, options)
    } else {_inputPartition}
    this.schema = schema
    this.options = options


    dfa = options.getDFA()
    val filePath = inputPartition.path
    // Initialize partition
    start = inputPartition.start
    end = inputPartition.end
    val startLevel = inputPartition.startLevel
    val (_stream, _fileSize) = Parser.getInputStream(filePath)
    stream = _stream
    fileSize = _fileSize
    println(
      inputPartition.start,
      inputPartition.end,
      inputPartition.startLevel,
      inputPartition.dfaState
      // inputPartition.startLabel
    )

    // println(options.encounteredTokens)

    syntaxStackArray = Parser.initSyntaxStack(dfa, startLevel)
    dfa.setState(inputPartition.dfaState)

    println(syntaxStackArray)
    println(dfa)
    // println(schema)
    // println(filters.length)
    // println(filters(0).references.toList)
    reader = Parser.getBufferedReader(stream, options.encoding, start)
    var i = 0
    var s = ""
    for (i <- 0 to 100) {
      s += reader.read().toChar
    }
    println("START: " + s)

    s = ""
    reader = Parser.getBufferedReader(stream, options.encoding, end-100)
    for (i <- 0 to 100) {
      s += reader.read().toChar
    }
    println("END: " + s)
    reader = Parser.getBufferedReader(stream, options.encoding, start)
    pos = start;
  }

  override def next() = {

    val (hasNext, _value, _, newPos) = Parser.getNextMatch(
      reader,
      options.encoding,
      start,
      end,
      pos,
      syntaxStackArray,
      dfa
    )
    pos = newPos
    // val cc = if (value.length == 0) {
    //   true
    // } else {
    //   false
    // }

    // false
    // println(hasNext, value)
    count += 1
    if(hasNext == false || value == null) {
        println(hasNext + " start: " + start + " pos: " + pos + " end: " + end + " count: " + count)
        // println(value)
    }
    value = _value
    // println(value)
    // if(!value.equals("null")) {
    //     println(value)
    // }
    // println("NEW " +  cc + value)
    // cc
    // println(hasNext)
    
    hasNext
  }

  def toInternalType(parsedRecord: Any, dataType: DataType, isRoot : Boolean = true): List[Any] = {
    // println(parsedRecord, dataType)
    if (parsedRecord == null) {
      return List(null)
    }

    
        if(dataType.isInstanceOf[StructType]) {
            if(!parsedRecord.isInstanceOf[Map[_,_]]){
                return List(null)
            }
            val structType = dataType.asInstanceOf[StructType]
            val r = parsedRecord.asInstanceOf[Map[String, Any]]
            var l = List[Any]()
            for(field <- structType.iterator) {
                if(r contains field.name) {
                    l = l ++ toInternalType(r(field.name), field.dataType, false)
                } else {
                    l = l ++ List(null)
                }
            }
            if(isRoot) {
                return l
            } else {
                return List(InternalRow.fromSeq(l))
            }
        }
        else if(dataType.isInstanceOf[ArrayType]) {
            if(!parsedRecord.isInstanceOf[List[Any]]) {
                return List(null)
            }
            val r = parsedRecord.asInstanceOf[List[Any]]
            val elementType = dataType.asInstanceOf[ArrayType].elementType
            var l = List[Any]()
            for(e <- r.iterator) {
                l = l ++ toInternalType(e, elementType, false)
            }
            return List(l)
        }
        else if(dataType.isInstanceOf[StringType]) {
            if(!parsedRecord.isInstanceOf[String]) {
                return List(null)
            }
            return List(UTF8String.fromString(parsedRecord.asInstanceOf[String]))
        }
        else if(dataType.isInstanceOf[DoubleType]) {
            if(!parsedRecord.isInstanceOf[Double]) {
                return List(null)
            }
            return List(parsedRecord)
        }
        else if(dataType.isInstanceOf[BooleanType]) {
            if(!parsedRecord.isInstanceOf[Boolean]) {
                return List(null)
            }
            return List(parsedRecord)
        }
        else {
            return List(parsedRecord)
        }

    // } else if (dataType.isInstanceOf[StructType]) {
    //     val _dataType = dataType.asInstanceOf[StructType]
    //     val r = parsedRecord.asInstanceOf[Map[String, Any]]
    //     var l = List[Any]()
    //     for(field <- _dataType.iterator) {
    //         if(r contains field.name) {
    //             l = l ++ toInternalType(r(field.name), field.dataType, false)
    //         } else {
    //             l = l ++ List(null)
    //         }
    //     }
    //     if(isRoot) {
    //         return l
    //     } else {
    //         return List(InternalRow.fromSeq(l))
    //     }
    // } else if (dataType.isInstanceOf[ArrayType]) {
    //     val r = parsedRecord.asInstanceOf[List[Any]]
    //     val elementType = dataType.asInstanceOf[ArrayType].elementType
    //     var l = List[Any]()
    //     for(e <- r.iterator) {
    //         l = l ++ toInternalType(e, elementType, false)
    //     }
    //     return List(l)
    // } else if (dataType.isInstanceOf[StringType]) {
    //   return List(UTF8String.fromString(parsedRecord.asInstanceOf[String]))
    // } else if(dataType.isInstanceOf[DoubleType] && parsedRecord.isInstanceOf[String]) {
    //     println("################################")
    //     println(parsedRecord, dataType)
    //     return List(null)
    //     // return List(parsedRecord)
    // } else {
    //     // if(parsedRecord.isInstanceOf[String]) {
    //     //     parsedRecord
    //     // }
    //   return List(parsedRecord)
    // }
  }
  def recordToRow(parsedRecord: Any, schema: StructType): InternalRow = {
//     // var row = InternalRow()
    // val _schema = if (schema.length == 1){
    //     schema.apply(0).dataType
    // } else {
    //     schema
    // }
//     // println("recordToRow:..... " + InternalRow(toInternalType(parsedRecord)))
    val row = InternalRow.fromSeq(toInternalType(parsedRecord, schema))
    // println(row)
//     // }

//     // if(parsedRecord != null) {
//     //     println(pos + " " + row)
//     // }

    row
  }

  override def get(): InternalRow = {
    // val (parsedRecord, _) = Parser.parse(value)
    // println(parsedRecord)
    // println(parsedRecord)
    val row = recordToRow(value, schema)
    // println("GOT THE ROW......." + row)
    // println(row)
    // println(parsedRecord)
    // InternalRow(ArrayData.toArrayData(Array[Int](5)))
    // InternalRow.empty
    row
  }

  override def close() {
    reader.close()
  }
}
