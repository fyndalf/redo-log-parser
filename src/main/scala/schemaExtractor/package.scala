import parser.ParsedStatement.{
  DeleteStatement,
  InsertStatement,
  UpdateStatement
}
import parser.{LogEntryWithRedoStatement, ParsedStatement}

import scala.collection.mutable

package object schemaExtractor {

  class Column(
      columnName: String,
      columnTable: Table,
      columnIsPrimaryKey: Boolean,
      columnForeignKeyTargetNames: Seq[Column],
      columnValues: mutable.HashMap[String, String] // ROWID -> VALUE
  ) {
    val name = columnName
    val table = columnTable
    var isPrimaryKey = columnIsPrimaryKey
    val foreignKeyTargetNames = columnForeignKeyTargetNames
    val values = columnValues
  }

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
          this,
          columnIsPrimaryKey = true,
          Seq(),
          mutable.HashMap(rowId -> value)
        ))
      } else {
        columns(columnId).values += (rowId -> value)
      }
    }
  }

  def checkPrimaryKeyDuplicates(column: Column): Unit = {
    val values = column.values.map(_._2).toList
    if (values.size > values.distinct.size) {
      column.isPrimaryKey = false
    }
  }

  def updateSchemaProperties(
      schema: mutable.HashMap[String, Table],
      table: Table,
      affectedColumnIds: Seq[String]
  ): Unit = {
    // Add rules
    affectedColumnIds.foreach(columnId => {
      checkPrimaryKeyDuplicates(table.columns(columnId))
    })
  }

  /**
    * Extract the database schema from the events
    *
    * @param logEntries
    * @return
    */
  def extractDatabaseSchema(
      logEntries: Seq[LogEntryWithRedoStatement]
  ): mutable.HashMap[String, Table] = {
    val schema = mutable.HashMap[String, Table]()

    logEntries.foreach(logEntry => {

      if (!schema.contains(logEntry.tableID)) {
        schema += (logEntry.tableID -> new Table(logEntry.tableID))
      }

      val table = schema(logEntry.tableID)
      val rowId = logEntry.rowID
      val affectedColumnIds = extractColumnIDs(logEntry.statement)

      logEntry.statement match {
        case insert: InsertStatement => extractFromInsert(insert, rowId, table)
        case update: UpdateStatement => extractFromUpdate(update, rowId, table)
        case delete: DeleteStatement => extractFromDelete(delete, rowId, table)
      }
      updateSchemaProperties(schema, table, affectedColumnIds)
    })
    schema
  }

  private def extractColumnIDs(statement: ParsedStatement): Seq[String] = {
    (statement match {
      case insert: InsertStatement => insert.insertedAttributesAndValues.keys
      case update: UpdateStatement => Seq(update.affectedAttribute)
      case delete: DeleteStatement =>
        delete.identifyingAttributesAndValues.keys.filter(key => key != "ROWID")
    }).toSeq
  }

  private def extractFromInsert(
      statement: InsertStatement,
      rowID: String,
      table: Table
  ): Unit = {
    statement.insertedAttributesAndValues.foreach(entry => {
      val (attribute, value) = entry
      if (!table.columns.contains(attribute)) {
        table.columns += (attribute -> new Column(
          attribute,
          table,
          columnIsPrimaryKey = true,
          Seq(),
          mutable.HashMap(rowID -> value)
        ))
      } else {
        table.columns(attribute).values += (rowID -> value)
      }
    })
  }

  private def extractFromUpdate(
      statement: UpdateStatement,
      rowID: String,
      table: Table
  ): Unit = {
    if (!table.columns.contains(statement.affectedAttribute)) {
      table.columns += (statement.affectedAttribute -> new Column(
        statement.affectedAttribute,
        table,
        columnIsPrimaryKey = true,
        Seq(),
        mutable.HashMap(rowID -> statement.newAttributeValue)
      ))
    } else {
      table.columns(statement.affectedAttribute).values(rowID) =
        statement.newAttributeValue
    }
  }

  private def extractFromDelete(
      statement: DeleteStatement,
      rowID: String,
      table: Table
  ): Unit = {
    statement.identifyingAttributesAndValues
      .filter(entry => entry._1 != "ROWID")
      .keys
      .foreach(attribute => {
        if (!table.columns.contains(attribute)) {
          table.columns += (attribute -> new Column(
            attribute,
            table,
            columnIsPrimaryKey = true,
            Seq(),
            mutable.HashMap[String, String]()
          ))
        } else {
          table.columns(attribute).values -= rowID
        }
      })
  }
}
