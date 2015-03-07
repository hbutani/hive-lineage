package org.sparklinedata.hive.lineage

package object errors {

  class TreeNodeException(
      tree: GraphNode, msg: String, cause: Throwable)
    extends Exception(msg, cause) {

    def this(tree: GraphNode, msg: String) = this(tree, msg, null)

    override def getMessage: String = {
      val treeString = tree.toString
      s"${super.getMessage}, tree:${if (treeString contains "\n") "\n" else " "}$tree"
    }
  }

  def attachTree[A](tree: GraphNode, msg: String = "")(f: => A): A = {
    try f catch {
      case e: Exception => throw new TreeNodeException(tree, msg, e)
    }
  }
}
