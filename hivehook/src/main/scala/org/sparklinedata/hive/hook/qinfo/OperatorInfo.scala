package org.sparklinedata.hive.hook.qinfo

import java.io.Writer

import org.apache.hadoop.hive.ql.exec.{Operator, TableScanOperator}
import org.apache.hadoop.hive.ql.plan.TableScanDesc
import org.sparklinedata.hive.hook.PrintableNode
import org.sparklinedata.hive.metadata.{PartitionDef, TableDef}

import scala.collection.JavaConversions._

class OperatorInfo(val qInfo : QueryInfo, tInfo: TaskInfo, val op : Operator[_]) extends PrintableNode {

  def id = op.getOperatorId

  def children = childOperators.map(qInfo(_))

  def printNode(prefix : String, out : Writer) : Unit = {
    out.write(s"$prefix ${op.getClass.getSimpleName}[$id]${operatorDetails(prefix)}\n")
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
  private lazy val aliasStr = " alias=" + alias

  lazy val filterExpr : String = {
    if (conf.getFilterExpr != null) conf.getFilterExpr.getExprString else null
  }

  private lazy val filterExprStr = if (filterExpr != null ) s" filterExpr=$filterExpr" else ""

  val projectedColumns = conf.getNeededColumns
  private lazy val projectColumnsStr = " columns=" + projectedColumns.mkString("[", ",", "]")

  lazy val extractInputs : (TempFileDef, TableDef, Seq[PartitionDef]) = tInfo match {
    case mrt : MapRedTaskInfo => {
      val inpDefs = mrt.opToInputDefs.getOrElse(op, Seq())
      val tables = inpDefs.filter(_.isInstanceOf[TableDef])
      val table : TableDef = if (tables.isEmpty) null else tables.head.asInstanceOf[TableDef]
      val partitionsDef : Seq[PartitionDef] =
        inpDefs.filter(_.isInstanceOf[PartitionDef]).map(_.asInstanceOf[PartitionDef])
      val tempFiles =  inpDefs.filter(_.isInstanceOf[TempFileDef])
      val tempFile : TempFileDef = if (tempFiles.isEmpty) null else tempFiles.head.asInstanceOf[TempFileDef]
      (tempFile, table, partitionsDef)
    }
    case _ => (null, null, Seq())
  }

  // have to first extract into a tuple and then get each position from tuple.
  // o.w. get a matchError for nulls.
  // see http://stackoverflow.com/questions/2024841/match-tuple-with-null
  val tempFile = extractInputs._1
  val  table =  extractInputs._2
  val partitions = extractInputs._3

  private lazy val tempFileStr = if (tempFile != null ) " tempFile=" + tempFile.fqn else ""
  private lazy val tableStr = if (table != null ) " table=" + table.fqn else ""
  private lazy val parttionStr =
    if (partitions.size > 0 ) " partitions=" + partitions.map(_.fqn).mkString("[", ",", "]") else ""

  override def operatorDetails(prefix : String) : String = {
    s"(${tempFileStr}${tableStr}${parttionStr}${aliasStr}${projectColumnsStr}${filterExprStr})"
  }
}