package org.sparklinedata.hive.hook.qinfo

import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.exec._
import org.apache.hadoop.hive.ql.plan.{TableScanDesc, FilterDesc, JoinDesc, JoinCondDesc, SelectDesc, GroupByDesc}
import org.sparklinedata.hive.lineage.OperatorNode

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

case class Column(tabAlias : String, name : String){

  def isSame(e : ExprNodeColumnDesc) : Boolean = {
    tabAlias == e.getTabAlias && name == e.getColumn
  }
}
case class SchemaMapping(columns : Map[String, Column]) {

  def map(el : Seq[ExprNodeDesc]) : (Boolean, Seq[ExprNodeDesc]) = {
    var changed = false
    var newE = ArrayBuffer[ExprNodeDesc]()

    el.foreach { e =>
      val nE = map(e)
      if ( !(e eq nE) ) changed = true
      newE += nE
    }

    (changed, newE)
  }

  def map(e : ExprNodeDesc) : ExprNodeDesc = e match {
    case c : ExprNodeColumnDesc => {
      val mapC = columns(c.getColumn)
      if (mapC isSame c) c
      else
        new ExprNodeColumnDesc(c.getTypeInfo, mapC.name, mapC.tabAlias,
          c.getIsPartitionColOrVirtualCol, c.isSkewedCol)
    }
    case cl : ExprNodeColumnListDesc => {
      val (changed, newE) = map(cl.getChildren)
      if (changed) {
        val newCL = new ExprNodeColumnListDesc
        newE.foreach(newCL.addColumn(_))
        newCL
      } else {
        cl
      }
    }
    case f : ExprNodeFieldDesc => {
      val mapD = map(f.getDesc)
      if ( mapD eq f.getDesc) {
        f
      } else {
        new ExprNodeFieldDesc(f.getTypeInfo, mapD, f.getFieldName, f.getIsList)
      }
    }
    case fn : ExprNodeGenericFuncDesc => {
      val (changed, newE) = map(fn.getChildren)
      if (changed) {
        new ExprNodeGenericFuncDesc(fn.getWritableObjectInspector, fn.getGenericUDF,
          fn.getFuncText,
          newE)

      } else {
        fn
      }
    }
    case _ => e
  }

}

object SchemaMapping {
  def apply(op : OperatorNode) : SchemaMapping = op.op match {
    case ts : TableScanOperator => {
      SchemaMapping(op.rowSchema.getSignature./*filter( ci =>
        !ts.getConf.getNeededColumns.contains(ci.getInternalName)).*/map { ci =>
        (ci.getInternalName, Column(ci.getTabAlias, ci.getAlias))
      }.toMap)
    }
    case rs : ReduceSinkOperator => {
      val inputSchema = op.parents(0).schemaMapping
      val conf = rs.getConf
      val keyColumns = conf.getKeyCols.map { k =>
        inputSchema.columns(k.asInstanceOf[ExprNodeColumnDesc].getColumn)
      }
      val valueColumns = conf.getValueCols.map { v =>
        inputSchema.columns(v.asInstanceOf[ExprNodeColumnDesc].getColumn)
      }
      val colMap = op.rowSchema.getSignature.zipWithIndex.map { t =>
        val cI = t._1
        val i = t._2
        if ( i < keyColumns.size ) {
          (cI.getInternalName, keyColumns(i))
        } else {
          (cI.getInternalName, valueColumns(i - keyColumns.size))
        }
      }
      SchemaMapping(colMap.toMap)
    }
    case jOp : CommonJoinOperator[_] => {
      val desc : JoinDesc = jOp.getConf.asInstanceOf[JoinDesc]
      val colMap = jOp.getColumnExprMap
      val reverseMap = desc.getReversedExprs
      val parentSchemas = op.parents.map(_.schemaMapping)
      var notMappable = false
      val schColMap =  colMap.map { t =>
        val oName = t._1
        val colExpr = t._2
        val iColNm = colExpr.asInstanceOf[ExprNodeColumnDesc].getColumn
        val pIdx : Int = {
          if ( reverseMap != null ) {
            reverseMap(oName).toInt
          } else {
            // @todo fix this: for now if reverseMap not available
            //                 assume that column is unique across inputs and pick the input that has this column

            val possibleParents = parentSchemas.zipWithIndex.filter(_._1.columns.contains(iColNm))
            if (possibleParents.isEmpty) -1 else possibleParents.head._2
          }
        }
        if ( pIdx == -1) {
          notMappable = true
          (oName, Column(null, null))
        } else {
          (oName, parentSchemas(pIdx).columns(iColNm))
        }
      }
      SchemaMapping(schColMap.toMap)
    }
    case _ if op.parents.size == 1 => op.parents(0).schemaMapping
    case _ => null
  }
}
