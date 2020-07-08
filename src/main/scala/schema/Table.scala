package schema

import scala.collection.mutable

class Table(
    tableName: String,
    tableColumns: mutable.HashMap[String, Column] = mutable.HashMap()
) {

  val columns: mutable.Map[String, Column] = tableColumns
  val name: String = tableName

  def addValue(columnId: String, value: String, rowId: String) {
    if (!columns.contains(columnId)) {
      columns += (columnId -> new Column(
        columnId,
        columnTable = this,
        columnIsPrimaryKey = true,
        columnIsSubsetOf = Seq(),
        columnValues = mutable.HashMap(rowId -> value)
      ))
    } else {
      columns(columnId).values += (rowId -> value)
    }
  }

  override def toString: String = {
    val columnsString =
      columns.map(column => column._2.toString).mkString("\n")
    s"TABLE $name\n$columnsString"
  }
}
