import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

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

  case class TraceIDPatternPart(
      tableID: String,
      attribute: String
  )

  type TraceIDPattern = Seq[TraceIDPatternPart]

  type LogEntriesForTrace = Seq[LogEntryWithRedoStatement]

}
