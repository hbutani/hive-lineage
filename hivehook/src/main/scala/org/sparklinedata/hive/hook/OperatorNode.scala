package org.sparklinedata.hive.hook

import java.io.Writer

import org.apache.hadoop.hive.ql.exec.{HashTableSinkOperator, FileSinkOperator, ReduceSinkOperator}

class QNode[T <: PrintableNode](val info : T) extends PrintableNode {

  def id : String = info.id

  private val childNodes = new collection.mutable.ArrayBuffer[QNode[_]]()
  private val parentNodes = new collection.mutable.ArrayBuffer[QNode[_]]()

  def add(child : QNode[_]) :Unit = {
    childNodes += child
    child.parentNodes += this
  }

  def children = childNodes.toSeq

  def printNode(prefix : String, out : Writer) : Unit = info.printNode(prefix, out)
}

class OperatorNode(info : OperatorInfo)  extends QNode[OperatorInfo](info)
class QueryNode(info : QueryInfo)  extends QNode[QueryInfo](info)


class BuildOperatorGraph private (val qInfo : QueryInfo) {

  var rootNode : QueryNode = _

  val operatorNodeStack = scala.collection.mutable.Stack[QNode[_]]()
  val sinkOperatorNodeStack = scala.collection.mutable.Stack[OperatorNode]()
  val taskStack = scala.collection.mutable.Stack[TaskInfo]()
  val oNodes = scala.collection.mutable.Map[String,OperatorNode]()

  def isFirstOperatorOfTask(opNd : OperatorNode) : Boolean = {
    taskStack.top match {
      case mr : MapRedTaskInfo => {
        mr.mapWork.getAliasToWork.values().contains(opNd.info.op)
      }
      case mrlt : MapRedLocalTaskInfo => {
        mrlt.mapWork.getAliasToWork.values().contains(opNd.info.op)
      }
      case _ => false
    }
  }

  def connectToParent2(opNd : OperatorNode) : Unit = {
    val parentOpExists = !(operatorNodeStack.top eq rootNode)
    val currTask = taskStack.top
    val currTaskIsRoot = qInfo.startingTasks.contains(currTask.id)
    val isOpFirst = isFirstOperatorOfTask(opNd)
    val canConnectToSink = !sinkOperatorNodeStack.isEmpty

    opNd._throwNPE

    (parentOpExists, currTaskIsRoot, isOpFirst, canConnectToSink) match {
      case (false, true, true, _) => { // connect a RootNode Op to the QueryNode
        val p = operatorNodeStack.top
        p.add(opNd)
      }
      case (false, false, true, true) => { // try to connect first Op to the previous Sink
        sinkOperatorNodeStack.reverse.foreach { p =>
          p.add(opNd)
        }
        sinkOperatorNodeStack.clear()
      }
      case (false, _, false, true) => { // connect reduce side first Op to the map-side sink.
        sinkOperatorNodeStack.reverse.foreach { p =>
          p.add(opNd)
        }
        sinkOperatorNodeStack.clear()
      }
      case _ => {
        val p = operatorNodeStack.top
        p.add(opNd)
      }
    }
  }

  def _connectToParent(opNd : OperatorNode) : Unit = {
    if ( !(operatorNodeStack.top eq rootNode) ) {
      val p = operatorNodeStack.top
      p.add(opNd)
    }else if ( !sinkOperatorNodeStack.isEmpty) {
      sinkOperatorNodeStack.reverse.foreach { p =>
        p.add(opNd)
      }
      sinkOperatorNodeStack.clear()
    }  else if ( isFirstOperatorOfTask(opNd) ) {
      val p = operatorNodeStack.top
      p.add(opNd)
    } else {
      //throw new InternalError(s"Node w/o a Parent ${opNd}")
      val p = operatorNodeStack.top
      p.add(opNd)
    }
  }

  def preVisit(nd: Node) : Unit = nd match {
    case qI : QueryInfo => {
      val opNode = new QueryNode(qI)
      operatorNodeStack.push(opNode)
      rootNode = opNode
    }
    case op : OperatorInfo => {
      val opNode = oNodes.getOrElse(op.id, new OperatorNode(op))
      connectToParent2(opNode)
      if (oNodes.contains(op.id)) return
      oNodes += (op.id -> opNode)
      operatorNodeStack.push(opNode)
      op.op match {
        case r : ReduceSinkOperator => sinkOperatorNodeStack.push(opNode)
        case f : FileSinkOperator => sinkOperatorNodeStack.push(opNode)
        case h : HashTableSinkOperator => sinkOperatorNodeStack.push(opNode)
        case _ => ()
      }
    }
    case tI : TaskInfo => taskStack.push(tI)
    case _ => ()
  }

  def postVisit(nd: Node) : Unit = nd match {
    case op : OperatorInfo => operatorNodeStack.pop()
    case qI : QueryInfo => operatorNodeStack.pop()
    case tI : TaskInfo => taskStack.pop()
    case _ => ()
  }

}

object BuildOperatorGraph  {

  def apply(qInfo : QueryInfo) : QueryNode = {
    val b = new BuildOperatorGraph(qInfo)
    val visited : scala.collection.mutable.Set[String] = scala.collection.mutable.Set()
    qInfo.traverse(b.preVisit, b.postVisit)(visited)
    b.rootNode
  }

}