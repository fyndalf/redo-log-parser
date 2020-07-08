import parser.ParsedStatement.{DeleteStatement, InsertStatement, UpdateStatement}
import parser.{LogEntryWithRedoStatement, ParsedStatement}

import scala.collection.mutable

package object schemaExtractor {

  case class Table(
                    name: String,
                    columns: Seq[Column]
                  )

  case class Column(
                     isPrimaryKey: Boolean,
                     foreignKeyTargetNames: Seq[String], // Column names
                     values: mutable.HashMap[String, String] // ROWID -> VALUE
                   )

  def updateSchemaProperties(
                              schema: mutable.HashMap[String, mutable.HashMap[String, Column]],
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
                           ): mutable.HashMap[String, mutable.HashMap[String, Column]] = {
    val schema = mutable.HashMap[String, mutable.HashMap[String, Column]]()

    logEntries.foreach(logEntry => {

      if (!schema.contains(logEntry.tableID)) {
        schema += (logEntry.tableID -> mutable.HashMap[String, Column]())
      }

      val table = schema(logEntry.tableID)
      val rowId = logEntry.rowID
      val affectedTableId = logEntry.tableID
      val affectedColumnIds = generateColumnIDs(logEntry.statement)

      logEntry.statement match {
        case insert: InsertStatement => extractFromInsert(insert, rowId, table)
        case update: UpdateStatement => extractFromUpdate(update, rowId, table)
        case delete: DeleteStatement => extractFromDelete(delete, rowId, table)
      }
      updateSchemaProperties(schema, affectedTableId, affectedColumnIds)
    })
    schema
  }

  private def generateColumnIDs(statement: ParsedStatement): Seq[String] = {
    (statement match {
      case insert: InsertStatement => insert.insertedAttributesAndValues.keys
      case update: UpdateStatement => Seq(update.affectedAttribute)
      case delete: DeleteStatement => delete.identifyingAttributesAndValues.keys
    }).toSeq
  }

  private def extractFromInsert(
                                 statement: InsertStatement,
                                 rowID: String,
                                 table: mutable.HashMap[String, Column]): mutable.HashMap[String, Column] = {
    statement.insertedAttributesAndValues.foreach(entry => {
      val (attribute, value) = entry
      if (!table.contains(attribute)) {
        table += (attribute -> Column(
          isPrimaryKey = true,
          Seq(),
          mutable.HashMap(rowID -> value)
        ))
      } else {
        table(attribute).values += (rowID -> value)
      }
    })
    table
  }

  private def extractFromUpdate(statement: UpdateStatement,
                                rowID: String,
                                table: mutable.HashMap[String, Column]): mutable.HashMap[String, Column] = {
    if (!table.contains(statement.affectedAttribute)) {
      table += (statement.affectedAttribute -> Column(
        isPrimaryKey = true,
        Seq(),
        mutable.HashMap(rowID -> statement.oldAttributeValue)
      ))
    }
    table(statement.affectedAttribute).values(rowID) =
      statement.newAttributeValue
    table
  }

  private def extractFromDelete(statement: DeleteStatement,
                                rowID: String,
                                table: mutable.HashMap[String, Column]): mutable.HashMap[String, Column] = {
    statement.identifyingAttributesAndValues
      .filter(entry => entry._1 == "ROWID").keys
      .foreach(attribute => {
        if (!table.contains(attribute)) {
          table += (attribute -> Column(
            isPrimaryKey = true,
            Seq(),
            mutable.HashMap[String, String]()
          ))
        } else {
          table(attribute).values -= rowID
        }
      })
    table
  }
}

