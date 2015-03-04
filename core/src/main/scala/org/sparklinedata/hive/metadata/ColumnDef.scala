package org.sparklinedata.hive.metadata

case class ColumnDef(
                    tableName : String,
                    name : String,
                    dataType : String,
                    comment : String
                      )(implicit m : Model)  extends Def {
  val fqn = s"${tableName}.$name"
  m.add(this)
}
