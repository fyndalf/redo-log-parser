package parser.file

import java.nio.file.Path
import java.time.LocalDateTime

import parser._

import scala.collection.mutable

// todo: take variable separator into account
/**
  * An object that provides methods for parsing the corresponding log statements out of a previously read redo log file.
  * It assumes that update statement occupy two lines and blocks are separated by a single empty line
  */
object FileParser {

  /**
    * Reads and parses the log file from disk, by grouping the log based on empty lines
    */
  def getAndParseLogFile(path: Path): Seq[ExtractedLogEntry] = {
    val logFileContents = FileReader.readLogFile(path)
    // group the log based on empty lines, and group each two lines of a redo log statement together
    val splitLogFile = logFileContents.filterNot(_.isBlank).grouped(2).toSeq
    parseLogFileChunks(splitLogFile)
  }

  /**
    * For each chunk of the read log, a single log entry is extracted
    */
  def parseLogFileChunks(chunks: Seq[Seq[String]]): Seq[ExtractedLogEntry] = {
    chunks.map(entry => translateChunkToLogEntry(entry))
  }

  /**
    * Takes extracted log entries and parses them to extract more information
    */
  def parseLogEntries(
      entries: Seq[ExtractedLogEntry]
  ): Seq[LogEntryWithRedoStatement] = {
    entries.map(entry =>
      LogEntryWithRedoStatement(
        parseLogStatement(entry.redoStatement),
        entry.rowID,
        parseTableIdentifier(entry.redoStatement),
        entry.timestamp
      )
    )
  }

  /**
    * Translates a chunk of the log into a single extracted log entry
    */
  private def translateChunkToLogEntry(
      chunk: Seq[String]
  ): ExtractedLogEntry = {
    val statement = chunk.head.trim()
    val rowIDAndTimestamp = chunk(1).split(" ", 2)
    val rowID = rowIDAndTimestamp(0)
    val timestampString = rowIDAndTimestamp(1).trim().replaceAll(" +", " ")
    val timestamp = LocalDateTime.parse(timestampString, formatter)
    ExtractedLogEntry(statement, rowID, timestamp)
  }

  /**
    * Extracts a more detailed statement out of the parsed log entry statement based on the statement type
    */
  private def parseLogStatement(statement: String): ParsedStatement = {
    val statementTypePattern(statementType) = statement
    statementType match {
      case "insert" =>
        extractInsert(statement)
      case "update" =>
        extractUpdate(statement)
      case "delete" =>
        extractDeleteStatement(statement)
      case _ =>
        throw new Exception(
          "Could not identify statement type for " + statement
        )
    }
  }

  /**
    * Extracts an insert statement and its inserted attributes and values
    */
  private def extractInsert(
      statement: String
  ): ParsedStatement.InsertStatement = {
    val insertPattern(columnsString, valuesString) = statement
    val columns = columnsString
      .split(",")
      .map(column => column.substring(1, column.length() - 1))
    val values = valuesString
      .split(",")
      .map(value => value.substring(1, value.length() - 1))
    if (columns.length != values.length) {
      throw new Exception(
        "Encountered insert statement with not the same number of values as attributes: " + statement
      )
    }
    val inserts: mutable.HashMap[String, String] = mutable.HashMap()
    for (i <- columns.indices) {
      inserts += (columns(i) -> values(i))
    }
    ParsedStatement.InsertStatement(inserts)
  }

  /**
    * Extracts an update statement, its affected attribute, and the old and new values.
    */
  private def extractUpdate(
      statement: String
  ): ParsedStatement.UpdateStatement = {
    val updatePattern(attribute1, newValue, attribute2, oldValue) = statement
    if (attribute1 != attribute2) {
      throw new Exception(
        "Encountered update statement with unexpected behaviour: " + statement
      )
    }
    ParsedStatement.UpdateStatement(
      attribute1.substring(1, attribute1.length() - 1),
      newValue.substring(1, newValue.length() - 1),
      oldValue.substring(1, oldValue.length() - 1)
    )
  }

  /**
    * Extracts a delete statement and the attributes and values used for identifying the entity to be deleted.
    */
  private def extractDeleteStatement(
      statement: String
  ): ParsedStatement.DeleteStatement = {
    val deletePattern(wherePart) = statement
    val identifiers: mutable.HashMap[String, String] = mutable.HashMap()

    wherePart
      .split(" and ")
      .foreach(attributeValue => {
        val attributeValuePattern(attribute, value) = attributeValue
        val cleanAttribute =
          if (attribute.startsWith("\"") || attribute.startsWith("'"))
            attribute.substring(1, attribute.length() - 1)
          else attribute
        identifiers += (cleanAttribute -> value
          .substring(1, value.length() - 1))
      })
    ParsedStatement.DeleteStatement(identifiers)
  }

  /**
    * Parses the table identifier out of a redo log statement
    */
  private def parseTableIdentifier(statement: String): String = {
    val tablePattern(_, _, table, _) = statement
    table
  }
}
