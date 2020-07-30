package parser

import java.time.LocalDateTime

import org.scalatest._
import parser.file.FileParser

class FileParserSpec extends FlatSpec with Matchers {
  private val entryA = Seq("some statement", "firstIdentifier 27-MAY-2020 14:50:03")
  private val entryB = Seq("other statement", "secondIdentifier 27-MAY-2020 14:50:06")
  private val chunks = Seq(entryA, entryB)

  private val logA = ExtractedLogEntry("some statement", "firstIdentifier", LocalDateTime.parse("27-MAY-2020 14:50:03", formatter))
  private val logB = ExtractedLogEntry("other statement", "secondIdentifier", LocalDateTime.parse("27-MAY-2020 14:50:06", formatter))
  "The File Parser Object" should "parse strings to log entries" in {
    FileParser.parseLogFileChunks(chunks) should contain only(logA, logB)
  }
}
