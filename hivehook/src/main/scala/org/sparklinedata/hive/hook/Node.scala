package org.sparklinedata.hive.hook

import java.io.Writer

import org.apache.commons.io.output.StringBuilderWriter

trait Node {
  node =>

  def id : String

  def fastEquals(other: Node): Boolean = {
    this.eq(other) || this == other
  }

  def children : Seq[Node]

  def foreach(f: Node => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  def traverse(preVisit: Node => Unit, postVisit: Node => Unit)
              (implicit visited : scala.collection.mutable.Set[String]): Unit = {
    val alreadyVisited = visited.contains(id)
    if (!alreadyVisited)  visited.add(id)
    preVisit(this)
    if ( alreadyVisited) return
    children.foreach(_.traverse(preVisit, postVisit))
    postVisit(this)
  }

  def map[A](f: Node => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  def flatMap[A](f: Node => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret
  }

  def collect[B](pf: PartialFunction[Node, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret
  }

  // for debugging in java
  def _throwNPE = {
    try {
      throw new NullPointerException
    } catch {
      case _ : NullPointerException => ()
    }
  }

}

trait PrintableNode extends Node {

  def printNode(prefix : String, out : Writer) : Unit

  def printGraph(filter : Node => Boolean, prefix : String, out : Writer)
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
      case c : PrintableNode => c.printGraph(filter, p, out)
      case n => out.write(s"$p $n\n")
    }
  }

  def printGraph(prefix : String, out : Writer)
                (implicit visited : scala.collection.mutable.Set[String]) : Unit =
    printGraph({n : Node => true}, prefix, out)

  def toStringTree() : String = {
    val out = new StringBuilderWriter()
    val visited : scala.collection.mutable.Set[String] = scala.collection.mutable.Set()
    printGraph("", out)(visited)
    return out.toString
  }

}