package org.sparklinedata.hive.hook

import org.junit.{Before, Test}

class BasicTest {


  /*
   * to get a plan file, in hive debugger break in Driver.execute, and execute:
   * org.apache.commons.io.FileUtils.writeStringToFile(new java.io.File("/tmp/q1.plan"), Utilities.serializeObject(this.plan))
   */

  @Test
  def testQ1() : Unit = {
    val ins = getClass.getClassLoader.getResourceAsStream("sampleplans/q1.plan")
    val qp = HivePlanUtils.readQueryPlan(ins)

    val opNode = HivePlanUtils.querPlanToOperatorGraph(qp)

    println(opNode.toStringTree())

  }

  @Test
  def testQ27() : Unit = {
    val ins = getClass.getClassLoader.getResourceAsStream("sampleplans/q27.plan")
    val qp = HivePlanUtils.readQueryPlan(ins)

    val opNode = HivePlanUtils.querPlanToOperatorGraph(qp)

    println(opNode.toStringTree())

  }

}
