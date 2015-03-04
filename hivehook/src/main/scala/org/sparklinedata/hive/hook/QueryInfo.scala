package org.sparklinedata.hive.hook

import java.io.Writer

import org.apache.commons.io.output.StringBuilderWriter
import org.apache.hadoop.hive.ql.exec.mr.{MapredLocalTask, MapRedTask}
import org.apache.hadoop.hive.ql.plan.{MapredLocalWork, MapredWork}
import org.sparklinedata.hive.metadata.Def

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.QueryPlan
import org.apache.hadoop.hive.ql.exec.{ConditionalTask, Task, Operator}
import scala.collection.immutable.Queue


class QueryInfo(val locationMap : Map[String, Def], val queryPlan : QueryPlan) extends Node {

  qInfo =>

  def id = queryPlan.getQueryId
  def queryString = queryPlan.getQueryString

  val (taskMap, operatorMap) = new GraphWalker().walk

  lazy val rootTasks = queryPlan.getRootTasks.map(_.getId)

  def apply(s : String) : Node = {
    if (taskMap.contains(s)) taskMap(s) else operatorMap(s)
  }

  def printGraph(prefix : String, out : Writer)
                (implicit queryInfo : QueryInfo, visited : scala.collection.mutable.Set[String]) : Unit = {
    out.write(s"$prefix QueryPlan[$id]\n")
    rootTasks.foreach { c =>
      queryInfo(c).printGraph(prefix + "  ", out)
    }
  }

  override  def toString() : String = {
    try {
      throw new NullPointerException
    } catch {
      case _ : NullPointerException => ()
    }

    val out = new StringBuilderWriter()
    val visited : scala.collection.mutable.Set[String] = scala.collection.mutable.Set()
    printGraph("", out)(this, visited)
    return out.toString
  }

  class GraphWalker {

    private val taskMap : scala.collection.mutable.Map[String, TaskInfo] =
      scala.collection.mutable.Map()

    private val operatorMap : scala.collection.mutable.Map[String, OperatorInfo] =
      scala.collection.mutable.Map()
    var queue: Queue[Any] = Queue()

    def walk: (Map[String, TaskInfo], Map[String, OperatorInfo]) = {
      queryPlan.getRootTasks.foreach {t => queue = queue :+ t }
      while(!queue.isEmpty) {
        val (n,q) = queue.dequeue
        queue = q
        n match {
          case t : Task[_] => processTask(t)
          case (tInfo: TaskInfo, op : Operator[_]) => processOperator(tInfo, op)
        }
      }
      (taskMap.toMap, operatorMap.toMap)
    }

    def processTask(task : Task[_]): Unit =  {
      val id = task.getId
      if (taskMap.contains(id)) return
      val taskInfo = TaskInfo(qInfo, task)
      taskMap(id) = taskInfo

      /*
       * add childTasks to Queue
       */
      taskInfo.childTasks.foreach{t =>
        if (t.done()) queue = queue :+ t
      }

      /*
       * add operators to Queue
       */
      taskInfo.childOperators.foreach{o =>
        queue = queue :+ (taskInfo, o)
      }

    }

    def processOperator(tInfo: TaskInfo, op : Operator[_]) : Unit = {
      val id = op.getOperatorId
      if (operatorMap.contains(id)) return
      val opInfo = OperatorInfo(qInfo, tInfo, op)
      operatorMap.put(id, opInfo)
      if ( op.getChildOperators != null ) {
        op.getChildOperators.foreach { c => queue = queue :+ (tInfo,c) }
      }
    }
  }

}
