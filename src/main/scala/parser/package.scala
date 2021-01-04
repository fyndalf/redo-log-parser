import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import schema.{Column, Table}

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * A companion object, which provides various utilities and case classes for the parsing of redo logs.
  */
package object parser {

  // todo: make this method with optional parameter passed via cli
  lazy val formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .appendPattern("dd-MMM-yyyy HH:mm:ss")
    .toFormatter

  // Regex patterns for parsing the different properties out of a redo log
  val statementTypePattern: Regex = "^(insert|update|delete)\\s.*".r
  val insertPattern: Regex =
    "insert into \".+\"[(](.+)[)] values [(](.+)[)];$".r
  val deletePattern: Regex = "delete from \".+\" where (.+);$".r
  val updatePattern: Regex =
    "update \".+\" set (.+) = (.+) where (.+) = (.+) and .*;$".r
  val attributeValuePattern: Regex = "(.+) = (.+)".r
  val tablePattern: Regex =
    "^(insert|update|delete)(\\s|\\sinto\\s|\\sfrom\\s)(\".*\")([(]|\\sset|\\swhere).*;$".r

  /**
    * A case class for a log entry that has ben parsed out of a redo log
    * @param redoStatement The complete log statement
    * @param rowID The affected row id
    * @param timestamp The timestamp of the log entry
    */
  case class ExtractedLogEntry(
      redoStatement: String,
      rowID: String,
      timestamp: LocalDateTime
  )

  /**
    * A case class containing a parsed statement out of an ExtractedLogEntry.
    * @param statement The ParsedStatement from the ExtractedLogEntry
    * @param rowID The affected row id
    * @param tableID The affected table id
    * @param timestamp The timestamp of the log entry
    */
  case class LogEntryWithRedoStatement(
      statement: ParsedStatement,
      rowID: String,
      tableID: String,
      timestamp: LocalDateTime
  )

  /**
    * An abstract class providing different concrete classes for encapsulating parsed log statements
    */
  sealed abstract class ParsedStatement()

  object ParsedStatement {

    /**
      * An Update statement parsed from an ExtractedLogEntry.
      * @param affectedAttribute The attribute affected by the update
      * @param newAttributeValue The new value of the attribute
      * @param oldAttributeValue The old value of the attribute
      */
    final case class UpdateStatement(
        affectedAttribute: String,
        newAttributeValue: String,
        oldAttributeValue: String
    ) extends ParsedStatement()

    /**
      * An Insert statement parsed from an ExtractedLogEntry
      * @param insertedAttributesAndValues The inserted attributes and their values
      */
    final case class InsertStatement(
        insertedAttributesAndValues: mutable.HashMap[String, String]
    ) extends ParsedStatement()

    /**
      * A Delete statement parsed from an ExtractedLogEntry
      * @param identifyingAttributesAndValues The attributes and values used for identifying the entity to be deleted
      */
    final case class DeleteStatement(
        identifyingAttributesAndValues: mutable.HashMap[String, String]
    ) extends ParsedStatement()

  }

  /**
    * Holds the Root Class for creation of the XES log
    * @param tableID ID of the root entity's table
    */
  case class RootClass(
      tableID: String
  )

  type LogEntriesForTrace = Seq[LogEntryWithRedoStatement]

  /**
    * Holds a 1:m relation for a Table
    */
  case class Relation(
      table: Table,
      relatingTables: Set[Table]
  )

  /**
    * Holds a binary relation between two Tables
    */
  case class BinaryRelation(
      leftTable: Table,
      rightTable: Table
  ) {
    def swap(): BinaryRelation = {
      BinaryRelation(rightTable, leftTable)
    }
  }

  /**
    * Holds a relation between two columns
    */
  case class ColumnRelation(
      leftColumn: Column,
      rightColumn: Column
  )

  /**
    * Holds log entries for a specific table
    */
  case class LogEntriesForTable(
      tableID: String,
      logEntries: Seq[LogEntryWithRedoStatement]
  )

  /**
    * Contains all log entries for a table, grouped by entities
    * @param tableID The ID of the table
    * @param entriesForEntities The set of log entries for each entity of the table
    */
  case class LogEntriesForTableAndEntity(
      tableID: String,
      entriesForEntities: Seq[LogEntriesForEntity]
  )

  /**
    * Holds parsed log entries for a specific entity
    * @param rowID The ID of the specific entity
    * @param logEntries The corresponding log entries
    */
  case class LogEntriesForEntity(
      rowID: String,
      logEntries: Seq[LogEntryWithRedoStatement]
  )

  /**
    * A row of a table (i.e., entity), with an ID determining which trace the row will belong to.
    * @param tableID The ID of the row's table
    * @param rowID The ID of the row
    * @param bucketID The ID of the trace bucket the row belongs to
    */
  case class RowWithBucketIdentifier(
      tableID: String,
      rowID: String,
      bucketID: Int
  )

  /**
    * A relation between two tables. Holds the relation between each table's entities.
    * @param leftTable The left table of the relation
    * @param rightTable The right table of the relation
    * @param relatingEntities The set of binary relations between the table's entities
    */
  case class TableEntityRelation(
      leftTable: Table,
      rightTable: Table,
      relatingEntities: Seq[EntityRelation]
  )

  /**
    * A binary relation between two entities of two tables
    * @param leftEntityID ID of the left entity of the relation
    * @param rightEntityID ID of the right entity of the relation
    */
  case class EntityRelation(
      leftEntityID: String,
      rightEntityID: String
  )

}
