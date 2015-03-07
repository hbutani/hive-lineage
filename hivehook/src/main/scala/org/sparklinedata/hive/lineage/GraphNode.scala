package org.sparklinedata.hive.lineage

import java.io.Writer

import org.apache.commons.io.output.StringBuilderWriter
import org.sparklinedata.hive.lineage.errors._

abstract class GraphNode {

  self : GraphNode with Product =>

  def id : String
  def children : Seq[GraphNode]

  def fastEquals(other: GraphNode): Boolean = {
    this.eq(other) || this == other
  }

  def traverse(preVisit: GraphNode => Unit, postVisit: GraphNode => Unit)
              (implicit visited : scala.collection.mutable.Set[String]): Unit = {
    val alreadyVisited = visited.contains(id)
    if (!alreadyVisited)  visited.add(id)
    preVisit(this)
    if ( alreadyVisited) return
    children.foreach(_.traverse(preVisit, postVisit))
    postVisit(this)
  }

  def makeCopy(args: Array[AnyRef]): GraphNode = attachTree(this, "makeCopy") {
    //val typ = ReflectionUtils.getType(this)
    //ReflectionUtils.construct(typ, args:_*).asInstanceOf[GraphNode]

    val defaultCtor = getClass.getConstructors.find(_.getParameterTypes.size != 0).head
    defaultCtor.newInstance(args: _*).asInstanceOf[this.type]
  }

  def transformUp(rule: PartialFunction[GraphNode, GraphNode]): GraphNode = {
    _transformUp(rule)(scala.collection.mutable.Map[String, GraphNode]())
  }
  private def _transformUp(rule: PartialFunction[GraphNode, GraphNode])
                          (implicit visitedMap : scala.collection.mutable.Map[String, GraphNode]): GraphNode = {
    val afterRuleOnChildren = transformChildrenUp(rule)
    rule.applyOrElse(afterRuleOnChildren, identity[GraphNode])
  }

  private def transformChildrenUp(rule: PartialFunction[GraphNode, GraphNode])
                                 (implicit visitedMap : scala.collection.mutable.Map[String, GraphNode]): GraphNode = {
    if ( visitedMap.contains(id)) {
      return visitedMap(id)
    }
    var changed = false
    val newArgs = productIterator.map {
      case arg: GraphNode if children contains arg =>
        val newChild = arg._transformUp(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: GraphNode) if children contains arg =>
        val newChild = arg._transformUp(rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      case m: Map[_,_] => m
      case args: Traversable[_] => args.map {
        case arg: GraphNode if children contains arg =>
          val newChild = arg._transformUp(rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    val newNode = if (changed) makeCopy(newArgs) else this
    visitedMap += (id -> newNode)
    newNode
  }
}

trait PrintableGraphNode extends GraphNode {

  self : PrintableGraphNode with Product =>

  def printNode(prefix : String, out : Writer) : Unit

  def printGraph(filter : GraphNode => Boolean, prefix : String, out : Writer)
                (implicit visited : scala.collection.mutable.Set[String]) : Unit = {
    val allReadyVisited = visited.contains(id)
    if ( !allReadyVisited)  visited.add(id)
    var p = prefix
    if (filter(this)) {
      printNode(prefix, out)
      p = p + "  "
      if ( allReadyVisited ) {
        out.write(s"$p ...\n")
      }
    }
    if ( allReadyVisited) return
    children.foreach {
      case c : PrintableGraphNode => c.printGraph(filter, p, out)
      case n => out.write(s"$p $n\n")
    }
  }

  def printGraph(prefix : String, out : Writer)
                (implicit visited : scala.collection.mutable.Set[String]) : Unit =
    printGraph({n : GraphNode => true}, prefix, out)

  def toStringTree() : String = {
    val out = new StringBuilderWriter()
    val visited : scala.collection.mutable.Set[String] = scala.collection.mutable.Set()
    printGraph("", out)(visited)
    return out.toString
  }

}