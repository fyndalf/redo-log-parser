import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import schema.{Column, Table}

import scala.collection.mutable
import scala.util.matching.Regex

package object parser {

  // todo: make this method with optional parameter passed via cli
  lazy val formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .appendPattern("dd-MMM-yyyy HH:mm:ss")
    .toFormatter

  val statementTypePattern: Regex = "^(insert|update|delete)\\s.*".r
  val insertPattern: Regex =
    "insert into \".+\"[(](.+)[)] values [(](.+)[)];$".r
  val deletePattern: Regex = "delete from \".+\" where (.+);$".r
  val updatePattern: Regex =
    "update \".+\" set (.+) = (.+) where (.+) = (.+) and .*;$".r
  val attributeValuePattern: Regex = "(.+) = (.+)".r
  val tablePattern: Regex =
    "^(insert|update|delete)(\\s|\\sinto\\s|\\sfrom\\s)(\".*\")([(]|\\sset|\\swhere).*;$".r

  case class ExtractedLogEntry(
      redoStatement: String,
      rowID: String,
      timestamp: LocalDateTime
  )

  case class LogEntryWithRedoStatement(
      statement: ParsedStatement,
      rowID: String,
      tableID: String,
      timestamp: LocalDateTime
  )

  sealed abstract class ParsedStatement()

  object ParsedStatement {

    final case class UpdateStatement(
        affectedAttribute: String,
        newAttributeValue: String,
        oldAttributeValue: String
    ) extends ParsedStatement()

    final case class InsertStatement(
        insertedAttributesAndValues: mutable.HashMap[String, String]
    ) extends ParsedStatement()

    final case class DeleteStatement(
        identifyingAttributesAndValues: mutable.HashMap[String, String]
    ) extends ParsedStatement()

  }

  case class RootElement(
      tableID: String
  )

  type LogEntriesForTrace = Seq[LogEntryWithRedoStatement]

  case class Relation(
      table: Table,
      relatingTables: Set[Table]
  )
  case class BinaryRelation(
      leftTable: Table,
      rightTable: Table
  ) {
    def swap(): BinaryRelation = {
      BinaryRelation(rightTable, leftTable)
    }
  }

  case class ColumnRelation(
      leftColumn: Column,
      rightColumn: Column
  )

  case class LogEntriesForTable(
      tableID: String,
      logEntries: Seq[LogEntryWithRedoStatement]
  )

  case class LogEntriesForTableAndEntity(
      tableID: String,
      entriesForEntities: Seq[LogEntriesForEntity]
  )

  case class LogEntriesForEntity(
      rowID: String,
      logEntries: Seq[LogEntryWithRedoStatement]
  )

  case class RowWithBucketIdentifier(
      tableID: String,
      rowID: String,
      bucketID: Int
  )

  case class TableEntityRelation(
      leftTable: Table,
      rightTable: Table,
      relatingEntities: Seq[EntityRelation]
  )

  case class EntityRelation(
      leftEntityID: String,
      rightEntityID: String
  )

}
