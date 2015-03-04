package org.sparklinedata.hive.hook

import java.io.Writer

import org.apache.hadoop.hive.ql.plan.TableScanDesc

import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.exec.{Operator, TableScanOperator}

class OperatorInfo(qInfo : QueryInfo, tInfo: TaskInfo, val op : Operator[_]) extends Node {

  def id = op.getOperatorId

  def printGraph(prefix : String, out : Writer)
                (implicit queryInfo : QueryInfo, visited : scala.collection.mutable.Set[String]) : Unit = {
    if ( visited.contains(id)) return
    visited.add(id)
    out.write(s"$prefix ${op.getClass.getSimpleName}[$id]${operatorDetails(prefix)}\n")
    childOperators.foreach { c =>
      queryInfo(c).printGraph(prefix + "  ", out)
    }
  }

  lazy val parentOperators : Seq[String] = {
    val parents = op.getParentOperators
    if ( parents != null ) {
      parents.map {p => p.getOperatorId }
    } else {
      Seq()
    }
  }

  lazy val childOperators : Seq[String] = {
    val children = op.getChildOperators
    if ( children != null ) {
      children.map {c => c.getOperatorId }
    } else {
      Seq()
    }
  }

  def operatorDetails(prefix : String) : String = ""

}

object OperatorInfo {
  def apply(qInfo : QueryInfo, tInfo: TaskInfo, op : Operator[_]) : OperatorInfo = op match {
    case ts : TableScanOperator => new TableScanOperatorInfo(qInfo, tInfo, ts)
    case _ => new OperatorInfo(qInfo, tInfo, op)
  }
}

class TableScanOperatorInfo(qInfo : QueryInfo, tInfo: TaskInfo, op : TableScanOperator)
  extends OperatorInfo(qInfo, tInfo, op) {

  def conf : TableScanDesc = op.getConf
  def alias = conf.getAlias
  lazy val filterExpr : String = {
    if (conf.getFilterExpr != null) conf.getFilterExpr.getExprString else null
  }

  private lazy val filterExprStr = if (filterExpr != null ) s", filterExpr = $filterExpr" else ""

  val projectedColumns = conf.getNeededColumns
  private lazy val projectColumnsStr = ", columns = " + projectedColumns.mkString("[", ",", "]")

  override def operatorDetails(prefix : String) : String = {
    s"(alias = ${alias}${projectColumnsStr}${filterExprStr})"
  }
}