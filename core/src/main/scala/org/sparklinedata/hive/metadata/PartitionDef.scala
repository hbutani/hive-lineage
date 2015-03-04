package org.sparklinedata.hive.metadata


case class PartitionDef(
                    name : String,
                    fqn : String,
                    db : String,
                    table : String,
                    values : Seq[String],
                    createTime : Int,
                    lastAccessTime : Int,
                    params : Map[String, String],
                    location : String,
                    inputFormat : String,
                    outputFormat : String
                    )(implicit m : Model)  extends Def {
  m.add(this)
}
