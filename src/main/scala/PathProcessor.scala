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
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

object PathProcessor {
  def findMatchingBracket(str : String, isSquare : Boolean = false) : Int = {
    var counter = 0
    val openBracket = if(isSquare) { '[' } else { '(' }
    val closeBracket = if(isSquare) { ']' } else { ')' }
    var i = 0
    while(i < str.size) {
      if(str(i) == openBracket) {
        counter += 1
      } else if(str(i) == closeBracket) {
        counter -= 1
        if(counter == 0) {
          return i+1
        }
      }
      i+=1
    }
    return i
  }
  def nextSplitIndex(q : String) : Int = {
    val arrayBracket = q.indexOf("[")
    val hasArray = arrayBracket > -1
    val objectBracket = q.indexOf("(")
    val hasObject = objectBracket > -1
    val hasBoth = hasObject && hasArray
    val _dot = if(q startsWith "..") {
      q.substring(2).indexOf(".")
    } else if(q startsWith ".") {
      q.substring(1).indexOf(".")
    } else {
      q.indexOf(".")
    }
    val dot = if(q startsWith "..") {
      _dot + 2
    } else if(q startsWith ".") {
      _dot + 1
    } else {
      _dot
    }
    val hasDot = _dot > 0

    if (hasDot
      && (!hasArray || dot < arrayBracket)
      && (!hasObject || dot < objectBracket)) {
      return dot
    }
    if((hasArray && !hasObject) || (hasBoth && arrayBracket < objectBracket)) {
      if (arrayBracket == 0) {
        return findMatchingBracket(q, isSquare=true)
      } else {
        return arrayBracket
      }
    } else if(hasObject) {
      if (objectBracket == 0) {
        return findMatchingBracket(q, isSquare=false)
      } else {
        return objectBracket
      }
    } else {
      return q.size
    }
  }
  def tokenize(q : String) : (Array[String], Array[String]) = {
    val tokens = new ArrayBuffer[String]()
    val filters = new ArrayBuffer[String]()
    var _q = q.trim()
    var i = nextSplitIndex(_q)
    while(i != 0) {
      var token = _q.substring(0,i)
      if (token matches """[\(\[]{1}[\s(]*[\*]{0,1}[\s)]*[(\s*)]*[\)\]]{1}""") {
        // no filters
        if(token startsWith "[") {
          tokens.append("[*]")
          filters.append("*")
        }
      } else if(token startsWith "[") {
        tokens.append("[*]")
        filters.append("("+token.substring(1,token.length-1).trim()+")")
      } else if(token startsWith "(") {
        filters.trimEnd(1)
        filters.append(token)
      } else {
        tokens.append(token)
        filters.append("*")
      }
      _q =  _q.substring(i).trim()
      i = nextSplitIndex(_q)
    }
    (tokens.toArray, filters.toArray)
  }

  def commonPath(tokenizedQueries : Array[Array[String]], filters : Array[Array[String]]) : Array[String] = {
    val minQ = tokenizedQueries.map(q => q.length).min
    var i = 0
    var allEqual = true
    val path = new ArrayBuffer[String]()
    while(i < minQ && allEqual) {
      val token = tokenizedQueries(0)(i)
      allEqual = tokenizedQueries.forall(q => q(i) == token)
      if(allEqual) {
        path.append(token)
      }
      allEqual = allEqual && filters.forall(f => f(i) == "*")
      i += 1
    }
    path.toArray
  }

  def finalizeProjectionTree(projectionTree: HashMap[String, ProjectionNode], parentKey : String = "", parentIsOutput : Boolean = false): HashMap[String, ProjectionNode] = {
    var tree = new HashMap [String, ProjectionNode]()
    for((_k,node) <- projectionTree) {
      val k = if(parentKey == "") { "*" } else { _k }
      val acceptAll = node.acceptAll
      val isOutputNode = !parentIsOutput && ((node.childrenTree.size + node.descendantsTree.size) > 1 || node.acceptAll)
      val filter = node.filterString
      var rowMap = new HashMap[String, (Int, DataType, Any)]()
      val (filterVariables, nPredicates) = if(filter == "") { (new HashMap[String, Variable](), 0)} else {
        val filterVariableNames = FilterProcessor.extractVariables(filter).distinct
        for(i <- filterVariableNames.indices) {
          rowMap = rowMap + (filterVariableNames(i) -> (i, null, null))
        }
        (FilterProcessor.parseExpr(filter, rowMap)._2, FilterProcessor.parseExpr(filter, rowMap)._3+1)
      }
      val childrenTree : HashMap[String, ProjectionNode] =finalizeProjectionTree(node.childrenTree, k, isOutputNode || parentIsOutput)
      val descendantsTree : HashMap[String, ProjectionNode] =finalizeProjectionTree(node.descendantsTree, k, isOutputNode || parentIsOutput)

      tree = tree + (k -> new ProjectionNode(acceptAll, isOutputNode, parentKey, filterVariables, nPredicates, rowMap, childrenTree, descendantsTree))
    }
    tree
  }
  def getPDAPath(_queries: String) : Array[String] = {
    val queries = _queries.split(";").filter(s => s.trim().nonEmpty)
    val pair = queries.map(q=>tokenize(q)).unzip
    val tokenizedQueries = pair._1
    val filters = pair._2
    commonPath(tokenizedQueries, filters)
  }
  def build(_queries: String) : HashMap[String, ProjectionNode] = {
    val queries = _queries.split(";").filter(s => s.trim().length() > 0)
    val pair = queries.map(q=>tokenize(q)).unzip
    val tokenizedQueries = pair._1
    val filters = pair._2
    val dfaQueryTokens = commonPath(tokenizedQueries, filters)
//    tokenizedQueries.foreach(tt => {
//      tt.foreach(t => print(t))
//      println()
//    })
//    val queryMap = new scala.collection.mutable.HashMap[String, (Boolean, String, Any, Any)]
    val queryTree = new ProjectionNode()
    var nestedQueryMap = new scala.collection.mutable.HashMap[String, (Boolean, String, Any, Any)]
    for(i <- tokenizedQueries.indices) {
      var curProjection = queryTree
      val ts = tokenizedQueries(i)
      for(j <- dfaQueryTokens.length-1 until ts.length) {
        var isNested = false
        val token = if(ts(j) startsWith "..") {
          isNested = true
          ts(j).substring(2)
        } else if(ts(j) startsWith ".") {
          ts(j).substring(1)
        } else { ts(j) }
        var filter = if(filters(i)(j) == "*") { "" } else {filters(i)(j)}
        var acceptAll = j == ts.length-1
        val selectedSubTree = if(isNested) { curProjection.descendantsTree } else { curProjection.childrenTree }

          if(selectedSubTree contains token) {
            val curFilter = selectedSubTree(token).filterString
            filter = if(curFilter == "" || filter == "") { curFilter } else { curFilter + " && " + filter }
            acceptAll =  acceptAll || selectedSubTree(token).acceptAll
            curProjection = selectedSubTree(token)
            curProjection.acceptAll = acceptAll
            curProjection.filterString = filter
          } else {
            val newProjection = selectedSubTree + ((token, new ProjectionNode(
              acceptAll = acceptAll,
              filterString = filter
            )))
            if(isNested) {
              curProjection.descendantsTree = newProjection
              curProjection = curProjection.descendantsTree(token)
            } else {
              curProjection.childrenTree = newProjection
              curProjection = curProjection.childrenTree(token)
            }
          }
      }
    }
    if(queryTree.childrenTree.nonEmpty)
      finalizeProjectionTree(queryTree.childrenTree)
    else
      finalizeProjectionTree(queryTree.descendantsTree)
  }
}