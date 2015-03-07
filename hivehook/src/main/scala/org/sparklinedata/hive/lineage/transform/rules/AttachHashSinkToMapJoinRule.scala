package org.sparklinedata.hive.lineage.transform.rules

import org.apache.hadoop.hive.ql.plan.api.OperatorType
import org.sparklinedata.hive.lineage.{GraphNode, OperatorNode, QueryNode}

import scala.collection.mutable.ArrayBuffer


class AttachHashSinkToMapJoinRule {

  val mapJoinOpStack = scala.collection.mutable.Stack[OperatorNode]()
  val tableScansToPromote = ArrayBuffer[GraphNode]()

  def apply : PartialFunction[GraphNode,GraphNode] = {
    case q : QueryNode => {
      if (!tableScansToPromote.isEmpty) {
        val children = q.children ++ tableScansToPromote
        new QueryNode(q.info, children)
      }
      q
    }
    case op : OperatorNode if op.info.op.getType == OperatorType.MAPJOIN => {
      mapJoinOpStack.push(op)
      op
    }
    case htsOp : OperatorNode if htsOp.info.op.getType == OperatorType.HASHTABLESINK => {
      val mjOp = mapJoinOpStack.pop()
      //val htsNewChildren = htsOp.children.map(_.transformUp(removeMapJoin(mjOp.id)))
      val htsNewChildren = htsOp.children
      tableScansToPromote ++= htsNewChildren
      new OperatorNode(htsOp.info, Seq(mjOp))
    }
  }


  def removeMapJoin(mjOpId : String)  : PartialFunction[GraphNode,GraphNode] = {
    case op : OperatorNode => {
      val nwChildren = op.children.filter(_.id != mjOpId)
      if (nwChildren.size == op.children.size) {
        op
      } else {
        new OperatorNode(op.info, nwChildren)
      }
    }
  }

}
