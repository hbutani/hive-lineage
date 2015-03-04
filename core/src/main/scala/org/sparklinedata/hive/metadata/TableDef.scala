package org.sparklinedata.hive.metadata

case class TableDef(
                   name : String,
                   fqn : String,
                   db : String,
                   owner : String,
                   createTime : Int,
                   lastAccessTime : Int,
                   tableType : String, // table, view
                   params : Map[String, String],
                   viewSql : Option[(String, String)],
                   columns : Seq[ColumnDef],
                   location : String,
                   inputFormat : String,
                   outputFormat : String,
                   compressed : Boolean,
                   temporary : Boolean,
                   partitionColumns : Option[Seq[ColumnDef]],
                   partitions : Option[Seq[PartitionDef]]
                   // todo add bucket and skew information
                     )(implicit m : Model)  extends Def {
  m.add(this)

  def add(p : PartitionDef)(implicit m : Model) : TableDef = {
    val t = partitions match {
      case None => this.copy(partitions = Some(Seq(p)))
      case _ => this.copy(partitions = Some(partitions.get :+ p))
    }
    m.add(t)
    t
  }
}
