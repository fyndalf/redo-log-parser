package schema

import scala.collection.mutable

class Column(
    columnName: String,
    columnTable: Table,
    columnIsPrimaryKey: Boolean,
    columnIsSubsetOf: Seq[Column],
    columnValues: mutable.HashMap[String, String] // ROWID -> VALUE
) {
  val name: String = columnName
  val table: Table = columnTable
  var isPrimaryKey: Boolean = columnIsPrimaryKey
  var isSubsetOf: Seq[Column] = columnIsSubsetOf
  val values: mutable.Map[String, String] = columnValues

  override def toString: String = {
    val primaryKey = if (isPrimaryKey) {
      " (PRIMARY KEY)"
    } else {
      ""
    }
    val foreignKeyTargets = isSubsetOf.map(column => {
      column.table.name + "." + column.name
    })
    val foreignKeyString = foreignKeyTargets.size match {
      case 0 => ""
      case _ => "FK CANDIDATE FOR: " + foreignKeyTargets.mkString(" AND ")
    }
    s"$name$primaryKey $foreignKeyString"
  }
}
