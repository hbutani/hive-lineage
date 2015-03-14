package org.sparklinedata.hive.lineage.transform.rules

import org.apache.hadoop.hive.ql.plan.GroupByDesc
import org.apache.hadoop.hive.ql.plan.api.OperatorType
import org.sparklinedata.hive.hook.qinfo.{GroupByOperatorInfo, TableScanOperatorInfo}
import org.sparklinedata.hive.lineage.{OperatorNode, GraphNode}

import scala.collection.mutable.ArrayBuffer


abstract class RemoveOperatorsRule {

  def meetsCond(nd : GraphNode) : Boolean

  def apply : PartialFunction[GraphNode,GraphNode] = {
    case op : OperatorNode => {
      var newChildren = ArrayBuffer[OperatorNode]()
      var fnd = false
      op.children.foreach { c =>
        if ( meetsCond(c)) {
          fnd = true
          val newC = if (c.children.size != 0) c.children(0) else null
          if ( newC != null ) {
            newChildren += c.children(0).asInstanceOf[OperatorNode]
          }
        } else {
          newChildren += c.asInstanceOf[OperatorNode]
        }
      }
      if ( fnd ) OperatorNode(op.info, newChildren.toSeq,op.schemaMapping) else op
    }
  }

}

class RemoveSinkOperatorsRule extends RemoveOperatorsRule {

  def meetsCond(nd: GraphNode) = nd match {
    case op: OperatorNode => op.info.op.getType == OperatorType.REDUCESINK ||
      op.info.op.getType == OperatorType.FILESINK
    case _ => false
  }
}

class RemoveIntermediateTableScansRule extends RemoveOperatorsRule {

  def meetsCond(nd: GraphNode) = nd match {
    case op: OperatorNode => op.info match {
      case ts: TableScanOperatorInfo => ts.table == null && ts.partitions.size == 0
      case _ => false
    }
    case _ => false
  }
}

class RemoveMapSideGroupByRule extends RemoveOperatorsRule {

  def meetsCond(nd: GraphNode) = nd match {
    case op: OperatorNode => op.info match {
      case gBy: GroupByOperatorInfo => gBy.conf.getMode == GroupByDesc.Mode.PARTIAL1 ||
        gBy.conf.getMode == GroupByDesc.Mode.PARTIAL2 ||
        gBy.conf.getMode == GroupByDesc.Mode.PARTIALS ||
        gBy.conf.getMode == GroupByDesc.Mode.HASH
      case _ => false
    }
    case _ => false
  }
}