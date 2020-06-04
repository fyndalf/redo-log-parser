package parser

import java.nio.file.Path
import java.time.LocalDateTime

// assumes that update statement is single line and blocks are separated by a single empty line
// todo: take variable separator into account
object FileParser {
  def getAndParseLogFile(path: Path): Seq[LogEntry] = {
    val logFileContents = FileReader.readLogFile(path)
    val splitLogFile = logFileContents.filterNot(_.isBlank).grouped(3).toSeq
    parseLogFileChunks(splitLogFile)
  }

  // todo: make pattern a parameter
  def parseLogFileChunks(chunks: Seq[Seq[String]]): Seq[LogEntry] = {

    chunks.map(entry => LogEntry(LocalDateTime.parse(entry.head, formatter), entry(1), entry(2)))
  }
}
