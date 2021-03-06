package schema

import scala.collection.mutable

/**
  * Represents a table from the database, holding several columns.
  * @param tableName The name of the table
  * @param tableColumns The columns belonging to the table
  */
class Table(
    tableName: String,
    tableColumns: TableColumns = mutable.HashMap()
) {

  val columns: TableColumns = tableColumns
  val name: String = tableName

  def addValue(columnId: String, value: String, rowId: String) {
    if (!columns.contains(columnId)) {
      columns += (columnId -> new Column(
        columnId,
        columnTable = this,
        columnCanBePrimaryKey = true,
        areColumnValuesIncreasing = true,
        columnIsSubsetOf = Seq(),
        columnValues = mutable.HashMap(rowId -> value)
      ))
    } else {
      columns(columnId).values += (rowId -> value)
      columns(columnId).verifyIncreasingValuesOnChange()
    }
  }

  override def clone(): Table = {
    new Table(name, columns.clone())
  }

  override def toString: String = {
    val columnsString =
      columns.map(column => column._2.toString).mkString("\n")
    s"TABLE $name\n$columnsString"
  }
}
