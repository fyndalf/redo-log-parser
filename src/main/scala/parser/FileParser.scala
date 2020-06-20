package parser

import java.nio.file.Path
import java.time.LocalDateTime

// assumes that update statement is single line and blocks are separated by a single empty line
// todo: take variable separator into account
object FileParser {
  def getAndParseLogFile(path: Path): Seq[ExtractedLogEntry] = {
    val logFileContents = FileReader.readLogFile(path)
    val splitLogFile = logFileContents.filterNot(_.isBlank).grouped(3).toSeq
    parseLogFileChunks(splitLogFile)
  }

  // todo: make pattern a parameter
  def parseLogFileChunks(chunks: Seq[Seq[String]]): Seq[ExtractedLogEntry] = {
    chunks.map(entry => ExtractedLogEntry(LocalDateTime.parse(entry.head, formatter), entry(1), entry(2)))
  }

  def pareseLogEntries(entries: Seq[ExtractedLogEntry]): Seq[LogEntryWithRedoStatement] = {
    entries.map(entry => LogEntryWithRedoStatement(
      entry.timestamp,
      entry.tableIdentifier,
      parseLogStatement(entry.redoStatement))
    )
  }

  def parseLogStatement(statement: String): ParsedStatement = {
    ???
  }
}
