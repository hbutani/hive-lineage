package org.sparklinedata.hive.hook

import java.io.{StringWriter, InputStream}

import org.apache.commons.io.{IOUtils, FileUtils}
import org.apache.hadoop.hive.ql.QueryPlan
import org.apache.hadoop.hive.ql.exec.{Utilities, Operator}
import org.sparklinedata.hive.hook.ConvertHelper._
import org.sparklinedata.hive.metadata._

import scala.collection.JavaConversions._

object HivePlanUtils {

  def terminalOps(op : Operator[_]) : Set[String] = {
    val s = scala.collection.mutable.Set[String]()

    val q = scala.collection.mutable.Queue[Operator[_]]()
    q += op
    while(!q.isEmpty) {
      val o = q.dequeue()
      if (o.getChildOperators != null && o.getChildOperators.size() > 0) {
        o.getChildOperators.foreach(q += _)
      } else {
        s += o.getOperatorId
      }
    }

    s.toSet

  }

  def readStreamIntoString(ins : InputStream) : String = {
    val writer = new StringWriter()
    IOUtils.copy(ins, writer)
    writer.toString()
  }

  def readQueryPlan(ins : InputStream) : QueryPlan = {
    Utilities.deserializeObject(readStreamIntoString(ins), classOf[QueryPlan])
  }

  def querPlanToOperatorGraph(qP : QueryPlan) : QueryNode = {

    implicit val model = new Model
    var locationMap : Map[String, Def] = Map()

    def addLocation(d : Def) = d match {
      case p : PartitionDef => locationMap += (p.location -> p)
      case t : TableDef => locationMap += (t.location -> t)
      case d : DirectoryDef => locationMap += (d.path -> d)
      case _ => ()
    }
    val inputs = qP.getInputs
    val outputs = qP.getOutputs

    import ConvertHelper._
    inputs.foreach { e =>
      val d = convert(e)
      addLocation(d)
    }

    outputs.foreach { e =>
      val d = convert(e)
      addLocation(d)
    }
    val qInfo = new QueryInfo(locationMap, qP)
    BuildOperatorGraph(qInfo)
  }
}
