package org.sparklinedata.hive.lineage

import java.io.Writer

import org.sparklinedata.hive.hook.qinfo.{QueryInfo, OperatorInfo}

case class OperatorNode(info : OperatorInfo,
                        children : Seq[GraphNode]) extends PrintableGraphNode {
  def id : String = info.id
  def printNode(prefix : String, out : Writer) : Unit = info.printNode(prefix, out)
}


case class QueryNode(info : QueryInfo,
                     children : Seq[GraphNode]) extends PrintableGraphNode {
  def id : String = info.id
  def printNode(prefix : String, out : Writer) : Unit = info.printNode(prefix, out)
}
