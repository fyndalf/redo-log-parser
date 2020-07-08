import parser.ParsedStatement.{
  DeleteStatement,
  InsertStatement,
  UpdateStatement
}
import parser.{LogEntryWithRedoStatement, ParsedStatement}

import scala.collection.mutable

package object schemaExtractor {

  class Table(
      name: String,
      columns: mutable.HashMap[String, Column] = mutable.HashMap()
  ) {

    def addValue(columnId: String, value: String, rowId: String) {
      if (!columns.contains(columnId)) {
        columns += (columnId -> Column(
          columnId,
          this,
          true,
          Seq(),
          mutable.HashMap((rowId -> value))
        ))
      } else {
        columns(columnId).values += (rowId -> value)
      }
    }
  }

  case class Column(
      name: String,
      table: Table,
      isPrimaryKey: Boolean,
      foreignKeyTargetNames: Seq[Column],
      values: mutable.HashMap[String, String] // ROWID -> VALUE
  )

  def updateSchemaProperties(
      schema: mutable.HashMap[String, Table],
      affectedTableId: String,
      affectedColumnIds: Seq[String]
  ): Unit = {
    ???
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
        schema += (logEntry.tableID -> Table(name))
      }

      val table = schema(logEntry.tableID)
      val rowId = logEntry.rowID
      val affectedTableId = logEntry.tableID
      val affectedColumnIds = extractColumnIDs(logEntry.statement)

      logEntry.statement match {
        case insert: InsertStatement => extractFromInsert(insert, rowId, table)
        case update: UpdateStatement => extractFromUpdate(update, rowId, table)
        case delete: DeleteStatement => extractFromDelete(delete, rowId, table)
      }
      updateSchemaProperties(schema, affectedTableId, affectedColumnIds)
    })
    schema
  }

  private def extractColumnIDs(statement: ParsedStatement): Seq[String] = {
    (statement match {
      case insert: InsertStatement => insert.insertedAttributesAndValues.keys
      case update: UpdateStatement => Seq(update.affectedAttribute)
      case delete: DeleteStatement => delete.identifyingAttributesAndValues.keys
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
        table.columns += (attribute -> Column(
          attribute,
          table,
          isPrimaryKey = true,
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
      table.columns += (statement.affectedAttribute -> Column(
        statement.affectedAttribute,
        table,
        isPrimaryKey = true,
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
      .filter(entry => entry._1 == "ROWID")
      .keys
      .foreach(attribute => {
        if (!table.columns.contains(attribute)) {
          table.columns += (attribute -> Column(
            attribute,
            table,
            isPrimaryKey = true,
            Seq(),
            mutable.HashMap[String, String]()
          ))
        } else {
          table.columns(attribute).values -= rowID
        }
      })
  }
}
