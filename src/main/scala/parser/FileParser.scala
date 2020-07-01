package parser

import java.nio.file.Path
import java.time.LocalDateTime
import scala.collection.mutable.HashMap

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

  private def translateChunkToLogEntry(chunk: Seq[String]): ExtractedLogEntry = {
    val statement = chunk.head.trim()
    val rowIDAndTimestamp = chunk(1).split(" ", 2)
    val rowID = rowIDAndTimestamp(0)
    val timestampString = rowIDAndTimestamp(1).trim().replaceAll(" +", " ")
    val timestamp = LocalDateTime.parse(timestampString, formatter)
    ExtractedLogEntry(statement, rowID, timestamp)
  }

  def pareseLogEntries(entries: Seq[ExtractedLogEntry]): Seq[LogEntryWithRedoStatement] = {
    entries.map(entry => LogEntryWithRedoStatement(
      parseLogStatement(entry.redoStatement),
      entry.rowID,
      parseTableIdentifier(entry.redoStatement),
      entry.timestamp,
      )
    )
  }

  def parseLogStatement(statement: String): ParsedStatement = {
    val statementTypePattern = "^(insert|update|delete)\\s.*".r
    val statementTypePattern(statementType) = statement
    val result = statementType match {
      case "insert" => 
        val insertPattern = "insert into \".+\"[(](.+)[)] values [(](.+)[)];$".r
        val insertPattern(columnsString, valuesString) = statement
        val columns = columnsString.split(",").map(column => column.substring(1, column.length() -1))
        val values = valuesString.split(",").map(value => value.substring(1, value.length() -1))
        if (columns.length != values.length) {
          throw new Exception("Encountered insert statement with not the same number of values as attributes: " + statement)
        }
        val inserts: HashMap[String, String] = HashMap()
        for (i <- 0 until columns.length) {
          inserts += (columns(i) -> values(i))
        }
        ParsedStatement.InsertStatement(inserts)
      case "update" => 
        // TODO: Improve regex
        val updatePattern = "update \".+\" set (.+) = (.+) where (.+) = (.+) and .*;$".r
        val updatePattern(attribute1, newValue, attribute2, oldValue) = statement
        if (attribute1 != attribute2) {
          throw new Exception("Encountered update statement with unexpected behaviour: " + statement)
        }
        ParsedStatement.UpdateStatement(attribute1.substring(1, attribute1.length() - 1), newValue.substring(1, newValue.length() - 1), oldValue.substring(1, oldValue.length() - 1))
      case "delete" => 
        val deletePattern = "delete from \".+\" where (.+);$".r
        val deletePattern(wherePart) = statement
        val identifiers: HashMap[String, String] = HashMap()

        wherePart.split(" and ").foreach(attributeValue => {
          val attributeValuePattern = "(.+) = (.+)".r
          val attributeValuePattern(attribute, value) = attributeValue
          val cleanAttribute = if (attribute.startsWith("\"") || attribute.startsWith("'")) attribute.substring(1, attribute.length() - 1) else attribute
          identifiers += (cleanAttribute -> value.substring(1, value.length() - 1))
        })

        ParsedStatement.DeleteStatement(identifiers)
      case _ => throw new Exception("Could not identify statement type for " + statement) 
    }
    return result
  }

  def parseTableIdentifier(statement: String): String = {
    // TODO: Maybe improve regex
    val tablePattern = "^(insert|update|delete)(\\s|\\sinto\\s|\\sfrom\\s)(\".*\")([(]|\\sset|\\swhere).*;$".r
    val tablePattern(g1, g2, table, g4) = statement
    return table
  }
}
