package org.sparklinedata.hive.hook

import org.apache.hadoop.hive.ql.QueryPlan
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType
import org.apache.hadoop.hive.ql.hooks._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation

import org.apache.hadoop.hive.ql.session.SessionState.LogHelper
import org.sparklinedata.hive.hook.qinfo.QueryInfo
import org.sparklinedata.hive.metadata._

class PostExecHook  extends ExecuteWithHookContext {

  @throws(classOf[Exception])
  def run (hookContext: HookContext) : Unit = {
    import scala.collection.JavaConversions._
    assert(hookContext.getHookType() == HookType.POST_EXEC_HOOK);
    val ss : SessionState = SessionState.get();
    val inputs : Set[ReadEntity]= hookContext.getInputs().toSet
    val outputs : Set[WriteEntity]  = hookContext.getOutputs().toSet
    val linfo : LineageInfo = hookContext.getLinfo();
    val ugi : UserGroupInformation = hookContext.getUgi();

    val qP : QueryPlan = hookContext.getQueryPlan


    val console : LogHelper = SessionState.getConsole();

    if (console == null) {
      return;
    }

    if (ss != null) {
      console.printError("Lineage Hook: query: " + ss.getCmd().trim());
      console.printError("Lineage Hook: type: " + ss.getCommandType());
    }


    implicit val model = new Model
    var locationMap : Map[String, Def] = Map()

    def addLocation(d : Def) = d match {
      case p : PartitionDef => locationMap += (p.location -> p)
      case t : TableDef => locationMap += (t.location -> t)
      case d : DirectoryDef => locationMap += (d.path -> d)
      case _ => ()
    }

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
    val opNode = OperatorGraphBuilder(qInfo)

    if (ss != null) {
      console.printError("Lineage Hook: query: " + ss.getCmd().trim())
      console.printError("Lineage Hook: type: " + ss.getCommandType())
      //console.printError("Lineage Hook: model: " + model)
      console.printError(s"Query Plan: \n${qInfo.toStringTree}")
      console.printError(s"Query Node Graph: \n${opNode.toStringTree}")
    }

  }
}
