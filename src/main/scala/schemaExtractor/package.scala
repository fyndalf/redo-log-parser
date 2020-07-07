import scala.collection.mutable.HashMap
import parser.ParsedStatement.{
  DeleteStatement,
  InsertStatement,
  UpdateStatement
}
import parser.{LogEntryWithRedoStatement, ParsedStatement}

package object schemaExtractor {

  case class Table(
      name: String,
      columns: Seq[Column]
  )

  case class Column(
      isPrimaryKey: Boolean,
      foreignKeyTargetNames: Seq[String], // Columnnames
      values: HashMap[String, String] // ROWID -> VALUE
  )

  def updateSchemaProperties(
      schema: HashMap[String, HashMap[String, Column]],
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
  ): HashMap[String, HashMap[String, Column]] = {
    val schema = HashMap[String, HashMap[String, Column]]()

    logEntries.foreach(logEntry => {
      if (!schema.contains(logEntry.tableID)) {
        schema += (logEntry.tableID -> HashMap[String, Column]())
      }
      val table = schema(logEntry.tableID)
      val rowId = logEntry.rowID
      val affectedTableId = logEntry.tableID
      val affectedColumnIds = (logEntry.statement match {
        case _: InsertStatement =>
          val insertStatement =
            logEntry.statement.asInstanceOf[InsertStatement]
          insertStatement.insertedAttributesAndValues.map(_._1)
        case _: UpdateStatement =>
          Seq(
            (logEntry.statement.asInstanceOf[UpdateStatement]).affectedAttribute
          )
        case _: DeleteStatement =>
          val deleteStatement =
            logEntry.statement.asInstanceOf[DeleteStatement]
          deleteStatement.identifyingAttributesAndValues.map(_._1)
      }).toSeq

      logEntry.statement match {
        case _: InsertStatement =>
          val insertStatement =
            logEntry.statement.asInstanceOf[InsertStatement]
          insertStatement.insertedAttributesAndValues.foreach(entry => {
            if (!table.contains(entry._1)) {
              table += (entry._1 -> Column(
                true,
                Seq(),
                HashMap(rowId -> entry._2)
              ))
            } else {
              table(entry._1).values += (rowId -> entry._2)
            }
          })
        case _: UpdateStatement =>
          val updateStatement =
            logEntry.statement.asInstanceOf[UpdateStatement]
          if (!table.contains(updateStatement.affectedAttribute)) {
            table += (updateStatement.affectedAttribute -> Column(
              true,
              Seq(),
              HashMap(rowId -> updateStatement.oldAttributeValue)
            ))
          }
          table(updateStatement.affectedAttribute).values(rowId) =
            updateStatement.newAttributeValue
        case _: DeleteStatement =>
          val deleteStatement =
            logEntry.statement.asInstanceOf[DeleteStatement]
          deleteStatement.identifyingAttributesAndValues
            .filter(entry => entry._1 == "ROWID")
            .map(_._1)
            .foreach(attribute => {
              if (!table.contains(attribute)) {
                table += (attribute -> Column(
                  true,
                  Seq(),
                  HashMap[String, String]()
                ))
              } else {
                table(attribute).values -= rowId
              }
            })
      }

      updateSchemaProperties(schema, affectedTableId, affectedColumnIds)
    })

    schema
  }
}
