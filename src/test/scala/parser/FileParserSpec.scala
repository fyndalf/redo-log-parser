package parser

import java.time.LocalDateTime

import org.scalatest._

class FileParserSpec extends FlatSpec with Matchers {
  private val entryA = Seq("27-MAY-2020 14:50:03", "some Table", "do something")
  private val entryB = Seq("27-MAY-2020 14:50:06", "another Table", "other update")
  private val chunks = Seq(entryA, entryB)

  private val logA = ExtractedLogEntry("do something", "some Table", LocalDateTime.parse("27-MAY-2020 14:50:03", formatter))
  private val logB = ExtractedLogEntry("other update", "another Table", LocalDateTime.parse("27-MAY-2020 14:50:06", formatter))
  "The File Parser Object" should "parse strings to log entries" in {
    FileParser.parseLogFileChunks(chunks) should contain only(logA, logB)
  }
}
