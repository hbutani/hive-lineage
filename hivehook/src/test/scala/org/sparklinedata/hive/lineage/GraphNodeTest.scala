package org.sparklinedata.hive.lineage

import java.io.Writer

import org.junit.{Before, Test}

case class AStruct(i : Int)

case class A(s : AStruct) extends PrintableGraphNode {
  def id = toString
  def children : Seq[GraphNode] = Seq()
  def printNode(prefix : String, out : Writer) = out.write(s"$prefix$this")
}

case class B(a : A, children : Seq[A]) extends PrintableGraphNode {
  def id = toString
  def printNode(prefix : String, out : Writer) = out.write(s"$prefix$this")
}

class GraphNodeTest {

  @Test
  def testA1() : Unit = {
    val a = A(AStruct(1))
    print(a.makeCopy(Array(AStruct(2))).asInstanceOf[PrintableGraphNode].toStringTree())
  }

  @Test
  def testAMany() : Unit = {
    val a = A(AStruct(1))
    (0 until 100).foreach { i =>
      println(a.makeCopy(Array(AStruct(i))).asInstanceOf[PrintableGraphNode].toStringTree())
    }
  }

  @Test
  def testB1() : Unit = {
    val a1 = A(AStruct(1))
    val a2 = A(AStruct(1))
    val b = B(a1, Seq(a2))
    print(b.makeCopy(Array(a2, Seq(a1))).asInstanceOf[PrintableGraphNode].toStringTree())
  }
}
