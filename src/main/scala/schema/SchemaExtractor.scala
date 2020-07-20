package schema

import parser.ParsedStatement.{
  DeleteStatement,
  InsertStatement,
  UpdateStatement
}
import parser.{LogEntryWithRedoStatement, ParsedStatement}
import schema.SchemaDeriver.updateSchemaProperties

import scala.collection.mutable

object SchemaExtractor {

  /**
    * Extract the database schema from the events
    *
    * @param logEntries
    * @return
    */
  def extractDatabaseSchema(
      logEntries: Seq[LogEntryWithRedoStatement]
  ): DatabaseSchema = {

    val schema: DatabaseSchema = mutable.HashMap[String, Table]()

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
