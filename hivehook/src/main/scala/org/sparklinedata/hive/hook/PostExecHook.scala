package org.sparklinedata.hive.hook

import org.apache.hadoop.hive.ql.hooks.HookContext.HookType
import org.apache.hadoop.hive.ql.hooks._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation

import org.apache.hadoop.hive.ql.session.SessionState.LogHelper

class PostExecHook  extends ExecuteWithHookContext {

  @throws(classOf[Exception])
  def run (hookContext: HookContext) : Unit = {
    assert(hookContext.getHookType() == HookType.POST_EXEC_HOOK);
    val ss : SessionState = SessionState.get();
    val inputs : java.util.Set[ReadEntity]= hookContext.getInputs();
    val outputs : java.util.Set[WriteEntity]  = hookContext.getOutputs();
    val linfo : LineageInfo = hookContext.getLinfo();
    val ugi : UserGroupInformation = hookContext.getUgi();


    val console : LogHelper = SessionState.getConsole();

    if (console == null) {
      return;
    }

    if (ss != null) {
      console.printError("Lineage Hook: query: " + ss.getCmd().trim());
      console.printError("Lineage Hook: type: " + ss.getCommandType());
    }

  }
}
