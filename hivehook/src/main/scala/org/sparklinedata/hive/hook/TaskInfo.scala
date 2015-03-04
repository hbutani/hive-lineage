package org.sparklinedata.hive.hook

import java.io.Writer

import org.apache.hadoop.hive.ql.exec.mr.{MapredLocalTask, MapRedTask}
import org.apache.hadoop.hive.ql.plan.{MapredLocalWork, MapredWork}

import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.exec.{TableScanOperator, ConditionalTask, Task, Operator}

import org.sparklinedata.hive.metadata.Def

class TaskInfo(qInfo : QueryInfo, val task : Task[_]) extends Node {

  def id = task.getId
  def work = task.getWork

  def printGraph(prefix : String, out : Writer)
                (implicit queryInfo : QueryInfo, visited : scala.collection.mutable.Set[String]) : Unit = {
    if (visited.contains(id)) return
    visited.add(id)
    out.write(s"$prefix ${task.getClass.getSimpleName}[$id]\n")
    childOperatorNames.foreach { c =>
      queryInfo(c).printGraph(prefix + "  ", out)
    }
    childTaskNames.foreach { c =>
      queryInfo(c).printGraph(prefix + "  ", out)
    }
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

  val pathToDef : Map[String, Def] = task.getWork.getMapWork.getPathToPartitionInfo.entrySet().map{ e =>
    val path = e.getKey
    val pDesc = e.getValue
    val location = pDesc.getProperties.getProperty("location", "")
    val df = qInfo.locationMap.getOrElse(location, new TempFileDef(location))
    (location -> df)
  }.toMap

  // todo walk pathToDef, build opId -> [Def]
}

class MapRedLocalTaskInfo(qInfo : QueryInfo, task : MapredLocalTask) extends TaskInfo(qInfo, task) {

  override def childOperators : Seq[Operator[_]] = {
    val w : MapredLocalWork = task.getWork
    w.getAliasToWork.values().toSeq
  }

}

class TempFileDef(val path : String) extends Def {
  def fqn = path
}
