package org.sparklinedata.hive.lineage

import java.io.Writer

import org.sparklinedata.hive.hook.qinfo.{SchemaMapping, QueryInfo, OperatorInfo}
import org.sparklinedata.hive.lineage.transform.rules.{RemoveMapSideGroupByRule, RemoveIntermediateTableScansRule, RemoveSinkOperatorsRule, AttachHashSinkToMapJoinRule}

import scala.collection.mutable.ArrayBuffer

case class OperatorNode(info : OperatorInfo,
                        children : Seq[GraphNode]) extends PrintableGraphNode {
  private[lineage] var _qNode : QueryNode = _
  private[lineage] var _parents : Seq[OperatorNode] = Seq()
  def qNode = _qNode
  def parents = _parents
  def id : String = info.id
  def op = info.op
  def rowSchema = info.rowSchema
  def printNode(prefix : String, out : Writer) : Unit = {
    info.printNode(prefix, out)
    out.write(s"$prefix schema = ${schemaMapping}\n")
  }

  private[lineage] var _schemaMapping : SchemaMapping = null

  lazy val schemaMapping : SchemaMapping =
    if ( _schemaMapping == null) {
      val s = SchemaMapping(this)
      _schemaMapping = s
      s
    } else {
      _schemaMapping
    }

  override def makeCopy(args: Array[AnyRef]): GraphNode = {
    val c = super.makeCopy(args)
    c.asInstanceOf[OperatorNode]._schemaMapping = _schemaMapping
    c
  }
}

object OperatorNode {

  def apply(info : OperatorInfo,
            children : Seq[GraphNode],
            schemaMapping : SchemaMapping) : OperatorNode = {
    val opNd = new OperatorNode(info, children)
    opNd._schemaMapping = schemaMapping
    opNd
  }
}


case class QueryNode(info : QueryInfo,
                     children : Seq[GraphNode]) extends PrintableGraphNode {
  _throwNPE
  def id : String = info.id
  def printNode(prefix : String, out : Writer) : Unit = info.printNode(prefix, out)

  private[lineage] def _initialize : Unit = {

    val parentMap = scala.collection.mutable.Map[String, ArrayBuffer[OperatorNode]]()

    def pre1(oNode: GraphNode) : Unit = oNode match {
      case opNode : OperatorNode => {
        oNode.children.foreach { c =>
          val parents = parentMap.getOrElse(c.id, ArrayBuffer())
          parents += opNode
          parentMap += (c.id -> parents)
        }
      }
      case _ => ()
    }
    def post(bldrNd : GraphNode) :Unit = ()
    def pre2(oNode: GraphNode) : Unit = oNode match {
      case opNode : OperatorNode => {
        opNode._qNode = this
        opNode._parents = parentMap.getOrElse(opNode.id, Seq()).toSeq
      }
      case _ => ()
    }

    def pre3(oNode: GraphNode) : Unit = oNode match {
      case opNode : OperatorNode => {
        opNode.schemaMapping
      }
      case _ => ()
    }

    this.traverse(pre1, post)(scala.collection.mutable.Set())
    this.traverse(pre2, post)(scala.collection.mutable.Set())
    this.traverse(pre3, post)(scala.collection.mutable.Set())
  }

  override def makeCopy(args: Array[AnyRef]): GraphNode = {
    val c = super.makeCopy(args)
    c.asInstanceOf[QueryNode]._initialize
    c
  }

}

object QueryNode {

  def apply(info : QueryInfo,
            children : Seq[GraphNode],
             applyAttachHasSinkRule : Boolean = false) : QueryNode = {

    var qNode = new QueryNode(info, children)

    if (applyAttachHasSinkRule) {
      val rule1 = new AttachHashSinkToMapJoinRule

      qNode = qNode.transformUp(rule1.apply).asInstanceOf[QueryNode]
    }
    qNode._initialize
    qNode
  }
}
