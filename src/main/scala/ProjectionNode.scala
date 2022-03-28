package edu.ucr.cs.bdlab
import org.apache.spark.sql.types.DataType

import scala.collection.immutable.HashMap

class ProjectionNode(var acceptAll: Boolean = false,
                     val parentKey: String = "",
                     val filterVariables: HashMap[String, Variable] = new HashMap[String, Variable](),
                     val rowMap: HashMap[String, (Int, DataType, Any)] = new HashMap[String, (Int, DataType, Any)](),
                     var childrenTree: HashMap[String, ProjectionNode] = new HashMap[String, ProjectionNode](),
                     var descendantsTree: HashMap[String, ProjectionNode] = new HashMap[String, ProjectionNode](),
                     var filterString: String = ""
                    ) {
  val hasFilter : Boolean = filterVariables.nonEmpty
  var notDescending = true
  def hasDescendants : Boolean = descendantsTree.nonEmpty
}