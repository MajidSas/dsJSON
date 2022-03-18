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

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import org.apache.spark.sql.types._
import scala.util.Try
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.beast.sql.GeometryUDT
import org.locationtech.jts.geom.Geometry;

abstract class Predicate(
    val index: Int, //
    val parent: Predicate
) {
  def evaluate(
      predicateValues: Array[Any],
      rowSequence: Array[Any],
      predicateIndex: Int
  ): Unit = {}
  def propagate(
      predicateValues: Array[Any],
      rowSequence: Array[Any]
  ): Unit = {
    if (parent != null) {
      parent.evaluate(predicateValues, rowSequence, index)
    }

  }
}

class Variable(
    val index: Int // index of value in rowSequence
) {
  var parents: ArrayBuffer[Predicate] = new ArrayBuffer[Predicate]()
  def propagate(predicateValues: Array[Any], rowSequence: Array[Any]) {
    for (p <- parents) {
      if (predicateValues(p.index) == null) {
        p.evaluate(predicateValues, rowSequence, index)
      }
    }
  }
}

class Constant(
    val value: Any
) {}

// this class is used for Boolean variables
// so that their value is also in the predicateValues list
class BooleanPredicate(
    override val index: Int,
    override val parent: Predicate,
) extends Predicate(index, parent) {
    override def evaluate(
      predicateValues: Array[Any],
      rowSequence: Array[Any],
      predicateIndex: Int
    ): Unit = {
        predicateValues(index) = rowSequence(predicateIndex)
        if(predicateValues(index) != null)
            propagate(predicateValues, rowSequence)
    }
}

class And(
    override val index: Int,
    override val parent: Predicate
) extends Predicate(index, parent) {
  var operands: Array[Predicate] = null
  override def evaluate(
      predicateValues: Array[Any],
      rowSequence: Array[Any],
      predicateIndex: Int
  ): Unit = {
    if (predicateValues(predicateIndex) == false) {
      predicateValues(index) = false
      propagate(predicateValues, rowSequence)
    } else {
      var counter = 0
      for (operand <- operands) {
        if (predicateValues(operand.index) == true) {
          counter += 1
        }
      }
      if (counter == operands.size) {
        predicateValues(index) = true
        propagate(predicateValues, rowSequence)
      }
    }
  }
}

class Or(
    override val index: Int,
    override val parent: Predicate
) extends Predicate(index, parent) {
  var operands: Array[Predicate] = null
  override def evaluate(
      predicateValues: Array[Any],
      rowSequence: Array[Any],
      predicateIndex: Int
  ): Unit = {
    if (predicateValues(predicateIndex) == false) {
      var counter = 0
      for (operand <- operands) {
        if (predicateValues(operand.index) == false) {
          counter += 1
        }
      }
      if (counter == operands.size) {
        predicateValues(index) = false
        propagate(predicateValues, rowSequence)
      }
    } else {
      predicateValues(index) = true
      propagate(predicateValues, rowSequence)
    }
  }
}

class Not(
    override val index: Int,
    override val parent: Predicate,
) extends Predicate(index, parent) {
  override def evaluate(
      predicateValues: Array[Any],
      rowSequence: Array[Any],
      predicateIndex: Int
  ): Unit = {
    predicateValues(index) = !predicateValues(predicateIndex).asInstanceOf[Boolean]
    propagate(predicateValues, rowSequence)
  }
}

class BinaryPredicate(
    override val index: Int,
    override val parent: Predicate
) extends Predicate(index, parent) {
  var left: Any = null
  var right: Any = null
  def getLeft(rowSequence: Array[Any]): Any = {
    if (left.isInstanceOf[Variable]) {
      return rowSequence(left.asInstanceOf[Variable].index)
    } else {
      return left.asInstanceOf[Constant].value
    }
  }

  def getRight(rowSequence: Array[Any]): Any = {
    if (right.isInstanceOf[Variable]) {
      return rowSequence(right.asInstanceOf[Variable].index)
    } else {
      return right.asInstanceOf[Constant].value
    }
  }
  def operator(v1: Any, v2: Any): Boolean = { false }

  override def evaluate(
      predicateValues: Array[Any],
      rowSequence: Array[Any],
      predicateIndex: Int
  ): Unit = {
    val v1 = getLeft(rowSequence)
    val v2 = getRight(rowSequence)

    if (
      !(
        v1.isInstanceOf[Variable] && v2.isInstanceOf[Variable] && (v1 == null || v2 == null)
      )
      // this means that operations on two variables that are both set to null are not supported
      // e.g. variable1 == variable2 (will not work if both are null)
      // this can be solved by making the expression:
      // ((varialbe1 == variable2) || (variable1 == null && variable2 == null))
    ) {
      predicateValues(index) = operator(v1, v2)
      propagate(predicateValues, rowSequence)
    }
  }
}

class Equal(
    override val index: Int,
    override val parent: Predicate,
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = { v1 == v2 }
}

class NotEqual(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = { v1 != v2 }
}

class LessThan(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[Double]) {
      v1.asInstanceOf[Double]
    } else {
      v1.asInstanceOf[Long]
    }
    val _v2 = if(v2.isInstanceOf[Double]) {
      v2.asInstanceOf[Double]
    } else {
      v2.asInstanceOf[Long]
    }
    _v1 < _v2
  }
}

class LessThanOrEquals(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[Double]) {
      v1.asInstanceOf[Double]
    } else {
      v1.asInstanceOf[Long]
    }
    val _v2 = if(v2.isInstanceOf[Double]) {
      v2.asInstanceOf[Double]
    } else {
      v2.asInstanceOf[Long]
    }
    _v1 <= _v2
  }
}

class GreaterThan(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[Double]) {
      v1.asInstanceOf[Double]
    } else {
      v1.asInstanceOf[Long]
    }
    val _v2 = if(v2.isInstanceOf[Double]) {
      v2.asInstanceOf[Double]
    } else {
      v2.asInstanceOf[Long]
    }
    _v1 > _v2
  }
}

class GreaterThanOrEquals(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[Double]) {
      v1.asInstanceOf[Double]
    } else {
      v1.asInstanceOf[Long]
    }
    val _v2 = if(v2.isInstanceOf[Double]) {
      v2.asInstanceOf[Double]
    } else {
      v2.asInstanceOf[Long]
    }
    _v1 >= _v2
  }
}

class In(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    v2.asInstanceOf[List[Any]] contains v1
  }
}

class StringContains(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[UTF8String]) { v1.asInstanceOf[UTF8String].toString } else { v1.asInstanceOf[String] }
    val _v2 = if(v2.isInstanceOf[UTF8String]) { v2.asInstanceOf[UTF8String].toString } else { v2.asInstanceOf[String] }
    _v1 contains _v2.asInstanceOf[String]
  }
}

class StringStartsWith(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[UTF8String]) { v1.asInstanceOf[UTF8String].toString } else { v1.asInstanceOf[String] }
    val _v2 = if(v2.isInstanceOf[UTF8String]) { v2.asInstanceOf[UTF8String].toString } else { v2.asInstanceOf[String] }
    _v1 startsWith _v2.asInstanceOf[String]
  }
}

class StringEndsWith(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[UTF8String]) { v1.asInstanceOf[UTF8String].toString } else { v1.asInstanceOf[String] }
    val _v2 = if(v2.isInstanceOf[UTF8String]) { v2.asInstanceOf[UTF8String].toString } else { v2.asInstanceOf[String] }
    _v1 endsWith _v2.asInstanceOf[String]
  }
}

class GeometryType(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  val geom = new GeometryUDT()
  override def operator(v1: Any, v2: Any): Boolean = {
    val _v1 = if(v1.isInstanceOf[UTF8String]) {v1.asInstanceOf[UTF8String].toString} else {v1.asInstanceOf[String]}
    val geomObj = geom.deserialize(_v1)
    
    val _v2 = if(v2.isInstanceOf[UTF8String]) {v2.asInstanceOf[UTF8String].toString} else {v2.asInstanceOf[String]}
    return geomObj.getGeometryType.equalsIgnoreCase(_v2)
  }
}

class GeometryWithin(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  val geom = new GeometryUDT()
  override def operator(v1: Any, v2: Any): Boolean = {
    val geom1 = geom.deserialize(v1)
    val geom2 = v2.asInstanceOf[Geometry]
    return geom1.within(geom2)
  }
}

object FilterProcessor {
  val multinaryOperators = List[String]( // takes two or more operands
    """\|\|""",
    "&&"
  )
  val binaryOperators = List( // takes exactly two operands
    "==",
    "!=",
    "<=",
    "<",
    ">=",
    ">",
    " in ",
    " stringContains ",
    " stringStartsWith ",
    " stringEndsWith ",
    " geometryType ",
    " geometryWithin "
  )
  val unaryOperators = List( // takes one operand
    "~"
  )

  // TODO: add functionality to easily extend those lists instead of manually modifying this file

  def splitTuple(tuple : String) : (String, String) = {
      var countBrackets = 0
      var i = 0
      for(c <- tuple) {
          if(c == '(') {
              countBrackets += 1
          } else if(c == ')') {
              countBrackets -= 1
          } else if(c == ',' && countBrackets == 1) {
              return (tuple.substring(1, i), tuple.substring(i+1, tuple.size-1))
          }
          i+=1
      }
      return ("", "")
  }
  def sparkFilterToJsonFilter(filter : String, rowMap : scala.collection.immutable.HashMap[String, (Int, DataType, Any)]) : String = {
    //"Or(And(GreaterThan(productId,100.0),EqualTo(albumTitle,ABC)),GreaterThan(lengthInMinutes,10.0))"
    
    val sparkFilterOperators = List(
            "AlwaysFalse", //
            "AlwaysTrue", //
            "And", //
            "EqualNullSafe", //
            "EqualTo", //
            "GreaterThan", //
            "GreaterThanOrEqual", //
            "In",
            "IsNotNull", //
            "IsNull", //
            "LessThan", //
            "LessThanOrEqual", //
            "Not", //
            "Or", //
            "StringContains",
            "StringEndsWith",
            "StringStartsWith")

    // TODO: improve code to avoid all of these 'if' statements
    for(op <- sparkFilterOperators) {
        if(filter startsWith  op+"(") {
            if(op == "AlwaysFalse") {
                return "false"
            } else if(op == "AlwaysTrue") {
                return "true"
            } else if(op == "IsNotNull") {
                return sparkFilterToJsonFilter(filter.substring(op.size+1, filter.size-1), rowMap) + " != null"
            } else if(op == "IsNull") {
                return sparkFilterToJsonFilter(filter.substring(op.size+1, filter.size-1), rowMap) + " == null"
            } else if(op == "Not") {
                return "~" + sparkFilterToJsonFilter(filter.substring(op.size+1, filter.size-1), rowMap)
            } else {
                val (v1, v2) = splitTuple(filter.substring(op.size))
                val _op = if(op == "And") { "&&"}
                else if(op == "Or") { "||" }
                else if(op == "EqualNullSafe" || op == "EqualTo") { "==" }
                else if(op == "GreaterThan") { ">" }
                else if(op == "GreaterThanOrEqual") { ">=" }
                else if(op == "LessThan") { "<" }
                else if(op == "LessThanOrEqual") { "<=" }
                else { op }
                return sparkFilterToJsonFilter(v1, rowMap) + " " + _op + " " + sparkFilterToJsonFilter(v2, rowMap)
            }
        } 
    }
    
    val constantList = List("true", "false", "null")
    if(rowMap contains filter) {
        return "@." + filter
    } else if(Try(filter.toDouble).isSuccess ||  constantList.contains(filter)) {
        return filter
    } else {
        return "\"" + filter + "\""
    }
  }

  def splitByBracket(_expr: String): (String, HashMap[String, String]) = {
    // this function replaces expressions within brackets with a variable
    // it makes it easy to parse for the other operands
    
    // first handle strings within quotes
    var expr = _expr
    val foundStrings = """((?<![\\])['"])((?:.(?!(?<![\\])\1))*.?)\1""".r.findAllMatchIn(expr).toList.map(_.toString)
    var foundStringsMap = new HashMap[String, String]()
    var i = 0
    for(s <- foundStrings) {
      expr = expr.replace(s, "##" + i)
      foundStringsMap += ("##" + i -> s)
      i+=1
    }
    var counter = 0
    var map = new HashMap[String, String]()
    var newExpr = ""

    var startIndex = -1
    var endIndex = -1
    var subExprCount = -1

    i = 0
    for (c <- expr) {
      if (c == '(') {
        if (counter == 0) {
          startIndex = i
          if (i > 0) {
            subExprCount += 1
            newExpr += expr.substring(endIndex + 1, i)
          }
        }
        counter += 1
      } else if (c == ')') {
        counter -= 1
        if (counter == 0) {
          endIndex = i
          subExprCount += 1
          newExpr += "$" + subExprCount
          map += ("$" + subExprCount -> expr
            .substring(startIndex + 1, endIndex)
            .trim())
        }
      }

      i += 1
    }

    if (endIndex < expr.size) {
      newExpr += expr.substring(endIndex + 1)
    }
    for((k,v) <- foundStringsMap) {
      newExpr = newExpr.replace(k, v)
      map = map.map({ case (k1,v1) => k1 -> v1.replace(k, v)})
    }
    return (newExpr.trim(), map)
  }

  def parseExpr(
      expr: String,
      rowMap: HashMap[String, (Int, DataType, Any)],
      index: Int = 0,
      parent: Predicate = null,
      _variables: HashMap[String, Variable] = new HashMap[String, Variable]()
  ): (Any, HashMap[String, Variable], Int) = {
    var newIndex = index
    var variables = _variables
    val (_expr, map) = splitByBracket(expr.replaceAll(raw"~\s+", "~")) // handle brackets
    
    if(_expr.trim == "") {
        return (null, _variables, index)
    }
    if (map.contains(_expr) && map.size == 1) {
      // because splitByBracket only removed brackets
      // that enclosed the whole expression
      return parseExpr(map(_expr), rowMap, index, parent, variables)
    }

    for (op <- multinaryOperators) {
      val operands = _expr.split(op).map(_.trim)

      if (operands.size > 1) {
        var predicate = if(op == "&&") {
            new And(index, parent)
        } else {
            new Or(index, parent)
        }
        val operandPredicates = new Array[Predicate](operands.size)
        var i = 0
        for (_operand <- operands) {
            var operand = _operand
            for((v, stmt) <- map) {
                if(operand contains v) {
                    operand = operand.replace(v, stmt)
                }
            }
          val subExpr = if(map contains operand) { map(operand) } else { operand }
          val (subPredicate, _variables2, _index) = parseExpr(subExpr, rowMap, newIndex+1, predicate, variables)
          variables = _variables2
          operandPredicates(i) = subPredicate.asInstanceOf[Predicate]
          i+=1
          newIndex = _index
        }
        if(predicate.isInstanceOf[And]) {
            predicate.asInstanceOf[And].operands = operandPredicates
        } else {
            predicate.asInstanceOf[Or].operands = operandPredicates
        }
        return (predicate, variables, newIndex)
      }
    }
    for (op <- binaryOperators) {
      val operands = _expr.split("(?i)"+op).map(_.trim)
      // TODO: improve code to avoid all of these 'if' statements and to easily
      //       extend with new operators
      if (operands.size == 2) {
        var predicate : BinaryPredicate = if(op == "==") {
            new Equal(newIndex, parent)
        } else if(op == "!=") {
            new NotEqual(newIndex, parent)
        }  else if(op == "<") {
            new LessThan(newIndex, parent)
        } else if(op == "<=") {
            new LessThanOrEquals(newIndex, parent)
        } else if(op == ">") {
            new GreaterThan(newIndex, parent)
        } else if(op == ">=") {
            new GreaterThanOrEquals(newIndex, parent)
        } else if(op == " in ") {
            new In(newIndex, parent)
        } else if(op == " stringContains ") {
            new StringContains(newIndex, parent)
        } else if(op == " stringStartsWith ") {
            new StringStartsWith(newIndex, parent)
        } else if(op == " stringEndsWith ") {
            new StringEndsWith(newIndex, parent)
        } else if(op == " geometryType ") {
           new GeometryType(newIndex, parent)
        } else if(op == " geometryWithin ") {
           new GeometryWithin(newIndex, parent)
        } else {
            new BinaryPredicate(newIndex, parent)
        }
        val leftExpr = if(map contains operands(0)) { map(operands(0)) } else { operands(0) }
        val rightExpr = if(map contains operands(1)) { map(operands(1)) } else { operands(1) }
        
        val (left, _variables2,  _index0) = parseExpr(leftExpr, rowMap, newIndex+1, predicate, variables)
        val (right, _variables3, _index) = parseExpr(rightExpr, rowMap, _index0+1, predicate, _variables2)
        variables = _variables3
        newIndex = _index
        predicate.left = left
        predicate.right = right
        return (predicate, variables, newIndex)
      }
    }
    for(op <- unaryOperators) {
        if(_expr startsWith op) {
            val predicate : Predicate = if(op == "~") {
                new Not(newIndex+1, parent)
            } else {
                new Not(newIndex+1, parent)
            }
            val operand = _expr.substring(op.length())
            val _operand = if(map contains operand) { map(operand) } else { operand }
            val (subPredicate, _variables2, _index) = parseExpr(_operand, rowMap, newIndex+1, predicate, variables)
            return (predicate, _variables2, _index)
        }
    }
    if(_expr startsWith "@.") { // variable
        // get variable row index
        val key = _expr.substring(2)
        val (rowIndex, dataType, _) = rowMap(key)
        // var predicate
        // check if parent requires boolean (And, Or, Not)
        val _parent = if(parent == null || 
        parent.isInstanceOf[And] || 
        parent.isInstanceOf[Or] || parent.isInstanceOf[Not]) {
            newIndex += 1
            new BooleanPredicate(newIndex-1, parent)
        } else { parent}
        if(variables contains key) {
            variables(key).parents.append(_parent)
        } else {
            val variable = new Variable(rowIndex)
            variable.parents.append(_parent)
            variables += (key -> variable)
        }
        return (variables(key), variables, newIndex)
        
    } else { // constant
        val value : Any = if(_expr startsWith "\"") { // string
          val value = if(parent.isInstanceOf[GeometryWithin]) {
            // parse string as geometry for geometry filter
            val geom = new GeometryUDT()
            geom.deserializeWKT(_expr.substring(1, _expr.size-1))
          } else {
            _expr
          }
            return(new Constant(value), variables, newIndex)
        } else if (_expr startsWith "[") { // array
            // TODO parse array
            return(new Constant(null), variables, newIndex)
        } else if(_expr.toLowerCase() == "null") { // null
            return(new Constant(null), variables, newIndex)
        } else if(_expr.toLowerCase() == "false") {
            return(new Constant(false), variables, newIndex)
        } else if(_expr.toLowerCase() == "true") {
            return(new Constant(true), variables, newIndex)
        } else if ("""[0-9\-NI]""".r.pattern.matcher(_expr.substring(0,1)).matches) { // assume it is a number for now
            if(_expr contains ".")
            return(new Constant(_expr.toDouble), variables, newIndex)
            else
            return(new Constant(_expr.toLong), variables, newIndex)
        } else { // default to string even if not within double quotes
          val value = if(parent.isInstanceOf[GeometryWithin]) {
            val geom = new GeometryUDT()
            geom.deserializeWKT(_expr)
          } else {
            _expr
          }
          return(new Constant(value), variables, newIndex)
        }
    }
    return (null, variables, newIndex)
  }

  def extractVariables(s : String) : Array[String] = {
    var l = List[String]()
    var variable = ""
    var count = 0
    for(c <- s) {
      if(c == '@') {
        count = 1
      } else if(c == '.' && count == 1) {
        count = 2
      } else if(count == 2 && "&|<>=!~ \t\n\r".contains(c)){
        count = 0
        l = l ++ List(variable)
        variable = ""
      } else if(count == 2) {
        variable += c
      }

    }
    if(variable != "") {
      l = l ++ List(variable)
    }
    l.toArray
  }
}