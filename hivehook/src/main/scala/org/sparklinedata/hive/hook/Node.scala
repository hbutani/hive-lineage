package org.sparklinedata.hive.hook

import java.io.Writer


trait Node {

  def id : String

  def printGraph(prefix : String, out : Writer)
                (implicit queryInfo : QueryInfo, visited : scala.collection.mutable.Set[String])

}
