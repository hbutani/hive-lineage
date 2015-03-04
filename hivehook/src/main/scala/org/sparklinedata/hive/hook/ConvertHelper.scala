package org.sparklinedata.hive.hook

import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.ql.hooks.Entity
import org.apache.hadoop.hive.ql.metadata.{Table, Partition}
import org.sparklinedata.hive.metadata._

object ConvertHelper {

  def dbFQN(dbName : String) = "database:" + dbName
  def dfsPathFQN(path : String) = "dfs:" + path
  def localPathFQN(path : String) = "local:" + path

  def fqn(e : Any) : String = e match {
    case d : Database => dbFQN(d.getName)
    case t : Table => t.getDbName() + "@" + t.getTableName()
    case p : Partition => fqn(p.getTable) + "@" + p.getName
    case _ => e.toString
  }

  def convertDB(dbName : String)(implicit m : Model) : DbDef = {
    val dbfqn = dbFQN(dbName)
    val d = m[DbDef](dbfqn)
    d match {
      case Some(dDef) => dDef
      case _ => {
        import scala.collection.JavaConversions._
        DbDef(dbName,
          dbfqn,
          null,
          null,
          null)
      }
    }
  }

  def convert(db : Database)(implicit m : Model) : DbDef = {
    val dbfqn = fqn(db)
    val d = m[DbDef](dbfqn)
    d match {
      case Some(dDef) => dDef
      case _ => {
        import scala.collection.JavaConversions._
        DbDef(db.getName,
        dbfqn,
        db.getDescription,
        db.getParameters.toMap,
        db.getOwnerName)
      }
    }
  }

  def convert(tbl : Table)(implicit m : Model) : TableDef = {
    val dbDef : DbDef = m[DbDef](dbFQN(tbl.getDbName)).get
    val tfqn = fqn(tbl)
    val t = m[TableDef](tfqn)
    t match {
      case Some(tD) if tD.lastAccessTime > tbl.getLastAccessTime => tD
      case _ => {
        import scala.collection.JavaConversions._
        val name = tbl.getTableName
        val owner = tbl.getOwner
        val createTime = tbl.getTTable.getCreateTime
        val lastAccessTime = tbl.getTTable.getLastAccessTime
        val tableType = tbl.getTableType.toString
        val params : scala.collection.mutable.Map[String, String] = tbl.getParameters
        val viewSql : Option[(String, String)] = {
          if (tbl.getViewOriginalText != null ) {
            Some(tbl.getViewOriginalText, tbl.getViewExpandedText)
          } else {
            None
          }
        }
        val columns : Seq[ColumnDef] = tbl.getTTable.getSd.getCols map { f =>
          ColumnDef(tfqn, f.getName, f.getType, f.getComment)
        }
        val location = tbl.getTTable.getSd.getLocation
        val inputFormat  = tbl.getTTable.getSd.getInputFormat
        val outputFormat = tbl.getTTable.getSd.getOutputFormat
        val compressed = tbl.getTTable.getSd.isCompressed
        val temporary = tbl.isTemporary
        val partitionColumns : Option[Seq[ColumnDef]] = {
          if ( !tbl.isPartitioned ) {
            None
          } else {
            val cDs = tbl.getPartCols.map { f =>
              ColumnDef(tfqn, f.getName, f.getType, f.getComment)
            }
            Some(cDs)
          }
        }
        val partitions : Option[Seq[PartitionDef]] = None
        TableDef(
          tfqn,
          name,
          dbDef.fqn,
          owner,
          createTime,
          lastAccessTime,
          tableType,
          params.toMap,
          viewSql,
          columns,
          location,
          inputFormat,
          outputFormat,
          compressed,
          temporary,
          partitionColumns,
          partitions
        )
      }
    }
  }

  def convert(part : Partition)(implicit m : Model) : PartitionDef = {
    val tbl = convert(part.getTable)
    val pfqn = fqn(part)
    val eP = m[PartitionDef](pfqn)
    eP match {
      case Some(pDef) if pDef.lastAccessTime > part.getLastAccessTime => pDef
      case _ => {
        import scala.collection.JavaConversions._
        val pDef = PartitionDef(
          part.getName,
          pfqn,
          tbl.db,
          tbl.fqn,
          part.getValues,
          part.getTPartition.getCreateTime,
          part.getTPartition.getLastAccessTime,
          part.getParameters.toMap,
          part.getTPartition.getSd.getLocation,
          part.getTPartition.getSd.getInputFormat,
          part.getTPartition.getSd.getOutputFormat
        )
        tbl.add(pDef)
        pDef
      }
    }
  }

  @throws(classOf[UnsupportedOperationException])
  def convert(e : Entity)(implicit m : Model) : Def = {
    e.getTyp match {
      case Entity.Type.DATABASE => convert(e.getDatabase)
      case Entity.Type.TABLE => {
        convertDB(e.getTable.getDbName)
        convert(e.getTable)
      }
      case Entity.Type.PARTITION => {
        convertDB(e.getTable.getDbName)
        convert(e.getTable)
        convert(e.getPartition)
      }
      case Entity.Type.DFS_DIR => {
        DirectoryDef(dfsPathFQN(e.getD.toString), e.getD.toString, false)
      }
      case Entity.Type.LOCAL_DIR => {
        DirectoryDef(localPathFQN(e.getD.toString), e.getD.toString, false)
      }
      case _ => throw new UnsupportedOperationException("Cannot convert entity " + e)
    }
  }
}
