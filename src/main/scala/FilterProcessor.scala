package edu.ucr.cs.bdlab

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import org.apache.spark.sql.types._
import scala.util.Try

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
        // println("booleanPredicate: " + index + " " + predicateIndex)
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
    //   println("LEFT: " + left.getClass())
    if (left.isInstanceOf[Variable]) {
      return rowSequence(left.asInstanceOf[Variable].index)
    } else {
      return left.asInstanceOf[Constant].value
    }
  }

  def getRight(rowSequence: Array[Any]): Any = {
    //   println("RIGHT: " + right.getClass())

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
      (v1 != null || v1.isInstanceOf[Constant] || (v1 == null && v2.isInstanceOf[Constant]))
      && (v2 != null || v2.isInstanceOf[Constant] || (v2 == null && v1.isInstanceOf[Constant]))
      // this means that operations on two variables that are both set to null are not supported
      // e.g. variable1 == variable2 (will not work if both are null)
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
    v1.asInstanceOf[Double] < v2.asInstanceOf[Double]
  }
}

class LessThanOrEquals(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    v1.asInstanceOf[Double] <= v2.asInstanceOf[Double]
  }
}

class GreaterThan(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    v1.asInstanceOf[Double] > v2.asInstanceOf[Double]
  }
}

class GreaterThanOrEquals(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    v1.asInstanceOf[Double] >= v2.asInstanceOf[Double]
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
    v1.asInstanceOf[String] contains v2.asInstanceOf[String]
  }
}

class StringStartsWith(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    v1.asInstanceOf[String] startsWith v2.asInstanceOf[String]
  }
}

class StringEndsWith(
    override val index: Int,
    override val parent: Predicate
) extends BinaryPredicate(index, parent) {
  override def operator(v1: Any, v2: Any): Boolean = {
    v1.asInstanceOf[String] endsWith v2.asInstanceOf[String]
  }
}

object FilterProcessor {
  val multinaryOperators = List[String]( // take two or more operands
    """\|\|""",
    "&&"
  )
  val binaryOperators = List( // take exactly two operands
    "==",
    "!=",
    "<=",
    "<",
    ">=",
    ">",
    " in ",
    " stringContains ",
    " stringStartsWith ",
    " stringEndsWith "
  )
  val unaryOperators = List( // take one operand
    "~"
  )

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
                // println(op, v1, v2)
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

  def splitByBracket(expr: String): (String, HashMap[String, String]) = {
    // this function replaces expressions with in quotes with a variable
    // it makes it easy to parse for the other operands
    var counter = 0
    var map = new HashMap[String, String]()
    var newExpr = ""

    var startIndex = -1
    var endIndex = -1
    var subExprCount = -1

    var i = 0
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
            // val variable = variables(key)
            variables(key).parents.append(_parent)
            // variables(key) = variable
        } else {
            val variable = new Variable(rowIndex)
            variable.parents.append(_parent)
            variables += (key -> variable)
        }
        return (variables(key), variables, newIndex)
        
    } else { // constant
        val value : Any = if(_expr startsWith "\"") { // string
            return(new Constant(_expr.substring(1, _expr.size-1)), variables, newIndex)
        } else if (_expr startsWith "[") { // array
            // TODO parse array
            return(new Constant(null), variables, newIndex)
        } else if(_expr.toLowerCase() == "null") { // null
            return(new Constant(null), variables, newIndex)
        } else if(_expr.toLowerCase() == "false") {
            return(new Constant(false), variables, newIndex)
        } else if(_expr.toLowerCase() == "true") {
            return(new Constant(true), variables, newIndex)
        } else { // assume it is a number for now
            return(new Constant(_expr.toDouble), variables, newIndex)
        }
    }

    return (null, variables, newIndex)
  }
}