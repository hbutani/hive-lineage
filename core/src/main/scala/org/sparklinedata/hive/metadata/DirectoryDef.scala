package org.sparklinedata.hive.metadata

case class DirectoryDef (
                     fqn : String,
                     path : String,
                     isLocal : Boolean)(implicit m : Model)  extends Def {
  m.add(this)
}
