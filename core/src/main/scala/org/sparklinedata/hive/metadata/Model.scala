package org.sparklinedata.hive.metadata

import scala.collection.mutable.{Map => MutableMap}

class Model {

  val defMap : MutableMap[String, Def] = MutableMap()

  def add(d : Def) : Unit = {
    defMap(d.fqn) = d
  }

  def apply[T <: Def](s : String) : Option[T] =  {
    val v = defMap.get(s)
    v match {
      case None => None
      case Some(d) => Some(d.asInstanceOf[T])
    }
  }

  override def toString = defMap.toString()
}
