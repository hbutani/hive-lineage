package org.sparklinedata.hive.hook.qinfo

import java.io.Writer

import org.apache.hadoop.hive.ql.exec.{Operator, TableScanOperator,
FilterOperator, CommonJoinOperator, MapJoinOperator, SelectOperator, GroupByOperator}
import org.apache.hadoop.hive.ql.plan.{TableScanDesc, FilterDesc, JoinDesc, JoinCondDesc, SelectDesc, GroupByDesc}
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
    case filter : FilterOperator => new FilterOperatorInfo(qInfo, tInfo, filter)
    case mjoin : MapJoinOperator => new MapJoinOperatorInfo(qInfo, tInfo, mjoin)
    case join : CommonJoinOperator[_] =>
      new JoinOperatorInfo(qInfo, tInfo, join.asInstanceOf[CommonJoinOperator[_ <: JoinDesc]])
    case sel : SelectOperator => new SelectOperatorInfo(qInfo, tInfo, sel)
    case gBy : GroupByOperator => new GroupByOperatorInfo(qInfo, tInfo, gBy)
    case _ => new OperatorInfo(qInfo, tInfo, op)
  }
}

class TableScanOperatorInfo(qInfo : QueryInfo, tInfo: TaskInfo, op : TableScanOperator)
  extends OperatorInfo(qInfo, tInfo, op) {

  def conf : TableScanDesc = op.getConf
  def alias = conf.getAlias
  private def aliasStr(p : String) = if (alias != null )s"\n$p alias=$alias" else ""

  lazy val filterExpr : String = {
    if (conf.getFilterExpr != null) conf.getFilterExpr.getExprString else null
  }

  private def filterExprStr(p : String) = if (filterExpr != null ) s"\n$p filterExpr=$filterExpr" else ""

  val projectedColumns = conf.getNeededColumns
  private def projectColumnsStr(p : String) = s"\n$p columns=${projectedColumns.mkString("[", ",", "]")}"

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

  private def tempFileStr(p : String) = if (tempFile != null ) s"\n$p tempFile=${tempFile.fqn}" else ""
  private def tableStr(p : String) = if (table != null ) s"\n$p table=${table.fqn}" else ""
  private def parttionStr(p : String) =
    if (partitions.size > 0 ) s"\n$p partitions=${partitions.map(_.fqn).mkString("[", ",", "]")}" else ""

  override def operatorDetails(prefix : String) : String = {
    val p = prefix + "    "
    s"(${tempFileStr(p)}${tableStr(p)}${parttionStr(p)}${aliasStr(p)}${projectColumnsStr(p)}${filterExprStr(p)}\n$p)"
  }
}

class FilterOperatorInfo(qInfo : QueryInfo, tInfo : TaskInfo, op : FilterOperator)
  extends OperatorInfo(qInfo, tInfo, op) {

  def conf : FilterDesc = op.getConf
  def predicate = conf.getPredicate

  def predicateStr = predicate.getExprString

  override def operatorDetails(prefix : String) : String = {
    s"(predicate=${predicateStr})"
  }
}

class JoinOperatorInfo(qInfo : QueryInfo, tInfo : TaskInfo, op : CommonJoinOperator[_ <: JoinDesc])
  extends OperatorInfo(qInfo, tInfo, op) {

  def conf = op.getConf
  def joinConds = conf.getKeysString
  def filters = conf.getFilters
  def nullSafes = conf.getNullSafes
  def joinCondTypes = conf.getConds

  def aliases : IndexedSeq[String] = (conf.getLeftAlias :: conf.getRightAliases.toList).toIndexedSeq

  def joinStr(condDesc : JoinCondDesc, nullSafe : Boolean) : String = {

    val leftAlias = condDesc.getLeft.toByte
    val rightAlias = condDesc.getRight.toByte

    val joinType = condDesc.getType match {
      case 0 =>  "Inner Join "
      case 1 => "Left Outer Join"
      case 2 => "Right Outer Join"
      case 3 => "Outer Join "
      case 4 => "Unique Join"
      case 5 => "Left Semi Join "
      case _ => "Unknow Join "
    }

    def eqOp = if (nullSafe) "<=>" else "="

    val lExprs = joinConds(leftAlias)
    val rExprs = joinConds(rightAlias)

    val joinPredicates = s"leftCond = $lExprs, rightCond = $rExprs"

    val lFilters = filters(leftAlias).map{ _.getExprString}
    val rFilters = filters(rightAlias).map{ _.getExprString}

    val joinStr = {
      var jStr = joinPredicates
      if (lFilters.size > 0 ) {
        jStr = jStr + (s", leftFilters = $lFilters")
      }
      if (rFilters.size > 0 ) {
        jStr = jStr + (s", rightFilters = $rFilters")
      }

      s"($jStr)"
    }

    val leftAliasStr = aliases(leftAlias)
    val rightAliasStr = aliases(rightAlias)

    s"$leftAliasStr $joinType $rightAliasStr on $joinStr"
  }

  lazy val joinStr : String = {
    (joinCondTypes zip nullSafes).map{t =>
      val c = t._1
      val n = t._2
      joinStr(c, n)
    }.mkString("\n")
  }

  override def operatorDetails(prefix : String) : String = {
    _throwNPE
    s"($joinStr)"
  }

}

class MapJoinOperatorInfo(qInfo : QueryInfo, tInfo : TaskInfo, op : MapJoinOperator)
  extends JoinOperatorInfo(qInfo, tInfo, op) {

  override  def aliases : IndexedSeq[String] = IndexedSeq("Input_0", "Input_1")
}

class SelectOperatorInfo(qInfo : QueryInfo, tInfo : TaskInfo, op : SelectOperator)
  extends OperatorInfo(qInfo, tInfo, op) {

  def conf : SelectDesc = op.getConf

  override def operatorDetails(prefix : String) : String = {
    val selList = (conf.getColList zip conf.getOutputColumnNames).map { t =>
      val e = t._1.getExprString
      val al = t._2
      s"$e as $al"
    }.mkString(", ")
    val p = prefix + "    "
    s"(\n${p}selectList=${selList}\n$p)"
  }
}

class GroupByOperatorInfo(qInfo : QueryInfo, tInfo : TaskInfo, op : GroupByOperator)
  extends OperatorInfo(qInfo, tInfo, op) {

  def conf : GroupByDesc = op.getConf

  override def operatorDetails(prefix : String) : String = {
    val keysStr = conf.getKeys.map(_.getExprString).mkString(", ")
    val p = prefix + "    "
    val aggStr = conf.getAggregators.map(_.getExprString).mkString("\n" + p + "  ")

    s"(\n${p}keys=${keysStr}\n${p}aggregations=${aggStr}\n$p)"
  }
}