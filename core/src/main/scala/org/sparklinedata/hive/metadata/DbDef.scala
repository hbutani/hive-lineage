package org.sparklinedata.hive.metadata


case class DbDef (
              name : String,
              fqn : String,
              description : String,
              params : Map[String, String],
              owner : String
              )(implicit m : Model)  extends Def {
  m.add(this)
}
