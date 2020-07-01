import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import scala.collection.mutable

package object parser {

  // todo: make this method with optional parameter passed via cli
  lazy val formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .appendPattern("dd-MMM-yyyy HH:mm:ss")
    .toFormatter

  case class ExtractedLogEntry(
                                redoStatement: String,
                                rowID: String,
                                timestamp: LocalDateTime
                              )

  case class LogEntryWithRedoStatement(
                                        statement: ParsedStatement,
                                        rowID: String,
                                        tableID: String,
                                        timestamp: LocalDateTime,
                                      )

  sealed abstract class ParsedStatement()

  object ParsedStatement {

    final case class UpdateStatement(
                                      affectedAttribute: String,
                                      newAttributeValue: String,
                                      oldAttributeValue: String,
                                    ) extends ParsedStatement()

    final case class InsertStatement(
                                      insertedAttributesAndValues: mutable.HashMap[String, String]
                                    ) extends ParsedStatement()

    final case class DeleteStatement(
                                      identifyingAttributesAndValues: mutable.HashMap[String, String],
                                    ) extends ParsedStatement()

  }

}
