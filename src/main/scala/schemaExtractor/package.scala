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
      columnIsSubsetOf: Seq[Column],
      columnValues: mutable.HashMap[String, String] // ROWID -> VALUE
  ) {
    val name = columnName
    val table = columnTable
    var isPrimaryKey = columnIsPrimaryKey
    var isSubsetOf = columnIsSubsetOf
    val values = columnValues

    override def toString(): String = {
      val primaryKey = isPrimaryKey match {
        case true  => " (PRIMARY KEY)"
        case false => ""
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

    override def toString(): String = {
      val columnsString =
        columns.map(column => column._2.toString()).mkString("\n")
      s"TABLE $name\n$columnsString"
    }
  }

  def checkForPrimaryKeyDuplicates(column: Column): Unit = {
    val values = column.values.map(_._2).toList
    if (values.size > values.distinct.size) {
      column.isPrimaryKey = false
    }
  }

  /**
    * Check for every column in a table whether all its values are included
    * in another column in another table
    *
    * @param schema
    */
  def updateColumnRelations(schema: mutable.HashMap[String, Table]): Unit = {
    if (schema.toList.size > 1) {
      // TODO: Make this algorithm nice
      schema.toList
        .map(_._2)
        .permutations
        .map(tables => (tables(0), tables.tail))
        .foreach(permutation => {
          val (table, otherTables) = permutation
          val tableName = table.name
          val otherTableNames = otherTables.map(_.name).mkString(", ")
          table.columns
            .map(_._2)
            .foreach(column => {
              var isSubsetOf = Seq[Column]()
              val distinctValues = column.values.map(_._2).toList.distinct
              otherTables.foreach(otherTable => {
                val otn = otherTable.name
                otherTable.columns
                  .map(_._2)
                  .foreach(otherColumn => {
                    val on = otherColumn.name
                    val cn = column.name
                    val otherDistinctValues =
                      otherColumn.values.map(_._2).toList.distinct
                    if (distinctValues.forall(otherDistinctValues.contains)) {
                      isSubsetOf = isSubsetOf :+ otherColumn
                    }
                  })
              })
              column.isSubsetOf = isSubsetOf
            })
        })
    }
  }

  def updateSchemaProperties(
      schema: mutable.HashMap[String, Table],
      table: Table,
      affectedColumnIds: Seq[String]
  ): Unit = {
    // Add rules
    affectedColumnIds.foreach(columnId => {
      checkForPrimaryKeyDuplicates(table.columns(columnId))
    })

    updateColumnRelations(schema)
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
