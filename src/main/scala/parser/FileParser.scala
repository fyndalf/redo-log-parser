package parser

import java.nio.file.Path
import java.time.LocalDateTime

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
    ???
  }

  def parseTableIdentifier(statement: String): String = {
    ???
  }
}
