package org.sparklinedata.hive.hook

import java.io.Writer

import org.apache.hadoop.hive.ql.exec.mr.{MapredLocalTask, MapRedTask}
import org.apache.hadoop.hive.ql.plan.{MapredLocalWork, MapredWork}

import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.exec.{TableScanOperator, ConditionalTask, Task, Operator}

import org.sparklinedata.hive.metadata.Def

import scala.collection.mutable.ArrayBuffer

class TaskInfo(val qInfo : QueryInfo, val task : Task[_]) extends PrintableNode {

  def id = task.getId
  def work = task.getWork

  def children = childOperatorNames.map(qInfo(_)) ++ childTaskNames.map(qInfo(_))

  def printNode(prefix : String, out : Writer) : Unit = {
    out.write(s"$prefix ${task.getClass.getSimpleName}[$id]\n")
  }

  lazy val parentTasks : Seq[String] = {
    val parents = task.getParentTasks
    if ( parents != null ) {
      parents.map {p => p.getId }
    } else {
      Seq()
    }
  }

  def childTasks : Seq[Task[_]] = {
    val children = task.getChildTasks
    if ( children != null ) {
      children.filter(_.done)
    } else {
      Seq()
    }
  }

  lazy val childTaskNames : Seq[String] = childTasks.map(_.getId)

  def childOperators : Seq[Operator[_]] = Seq()

  lazy val childOperatorNames : Seq[String] = childOperators.map(_.getOperatorId)

  def terminalOperators : Option[Set[String] ] = None
}

object TaskInfo {
  def apply(qInfo : QueryInfo, task : Task[_]) : TaskInfo = task match {
    case ct : ConditionalTask => new ConditionalTaskInfo(qInfo, ct)
    case mrt : MapRedTask => new MapRedTaskInfo(qInfo, mrt)
    case mrlt : MapredLocalTask => new MapRedLocalTaskInfo(qInfo, mrlt)
    case _ => new TaskInfo(qInfo, task)
  }
}

class ConditionalTaskInfo(qInfo : QueryInfo, task : ConditionalTask) extends TaskInfo(qInfo, task) {

  override def childTasks : Seq[Task[_]] = task.getListTasks.filter(_.done)
}

class MapRedTaskInfo(qInfo : QueryInfo, task : MapRedTask) extends TaskInfo(qInfo, task) {

  override def childOperators : Seq[Operator[_]] = {
    val w : MapredWork = task.getWork
    val mapWrk = w.getMapWork
    val redWrk = w.getReduceWork
    val ops = mapWrk.getAliasToWork.values().toSeq
    if ( redWrk != null ) ops :+ redWrk.getReducer  else ops
  }

  val mapWork = task.getWork.getMapWork
  val pathToDef : Map[String, Def] = mapWork.getPathToPartitionInfo.entrySet().map{ e =>
    val path = e.getKey
    val pDesc = e.getValue
    val location = pDesc.getProperties.getProperty("location", path)
    val df = qInfo.locationMap.getOrElse(location, new TempFileDef(location))
    (path -> df)
  }.toMap

  lazy val opToInputDefs  = {
    val opInputPairs = for {
      (path, df) <- pathToDef
      if mapWork.getPathToAliases.containsKey(path)
      alias <- mapWork.getPathToAliases.get(path)
      op = mapWork.getAliasToWork.get(alias)
    } yield (df, op)

    val m = scala.collection.mutable.Map[Operator[_], ArrayBuffer[Def]]()

    opInputPairs.foreach { t =>
      val df = t._1
      val op = t._2
      if (!m.containsKey(op)) m += ((op, ArrayBuffer()))
      val b = m(op)
      b += df
    }
    m.toMap
  }

  override def terminalOperators : Option[Set[String] ] = {
    import HivePlanUtils._
    val s = if (task.getWork.getReduceWork != null ) {
      terminalOps(task.getWork.getReduceWork.getReducer)
    } else {
      mapWork.getAliasToWork.values().map(terminalOps(_)).reduce(_ ++ _)
    }
    Some(s)
  }
}

class MapRedLocalTaskInfo(qInfo : QueryInfo, task : MapredLocalTask) extends TaskInfo(qInfo, task) {

  override def childOperators : Seq[Operator[_]] = {
    val w : MapredLocalWork = task.getWork
    w.getAliasToWork.values().toSeq
  }

  val mapWork : MapredLocalWork = task.getWork

  override def terminalOperators : Option[Set[String] ] = {
    import HivePlanUtils._
    val s = {
      mapWork.getAliasToWork.values().map(terminalOps(_)).reduce(_ ++ _)
    }
    Some(s)
  }

}

class TempFileDef(val path : String) extends Def {
  def fqn = path
}
