package parser.file

import java.nio.file.Path
import java.time.LocalDateTime

import parser._

import scala.collection.mutable

// assumes that update statement is single line and blocks are separated by a single empty line
// todo: take variable separator into account
object FileParser {

  def getAndParseLogFile(path: Path): Seq[ExtractedLogEntry] = {
    val logFileContents = FileReader.readLogFile(path)
    val splitLogFile = logFileContents.filterNot(_.isBlank).grouped(2).toSeq
    parseLogFileChunks(splitLogFile)
  }

  // todo: make pattern a parameter
  def parseLogFileChunks(chunks: Seq[Seq[String]]): Seq[ExtractedLogEntry] = {
    chunks.map(entry => translateChunkToLogEntry(entry))
  }

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

  private def parseTableIdentifier(statement: String): String = {
    val tablePattern(_, _, table, _) = statement
    table
  }
}