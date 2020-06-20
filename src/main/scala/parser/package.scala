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
                       timestamp: LocalDateTime,
                       tableIdentifier: String,
                       redoStatement: String
                     )

  case class LogEntryWithRedoStatement(
                                      timestamp: LocalDateTime,
                                      tableIdentifier: String,
                                      statement: ParsedStatement,
                                      )

  sealed abstract case class ParsedStatement()
  object ParsedStatement {
    final case class UpdateStatement(
                                      affectedAttribute: String,
                                      newAttributeValue: String,
                                      oldAttributeValue: String,
                                      rowId: String
                              )
    final case class InsertStatement(
                                    updatedAttributesAndValues: mutable.HashMap[String, String]
                                    )
    final case class DeleteStatement(
                                    identifyingAttributesAndValues: mutable.HashMap[String, String],
                                    rowId: String
                                    )
  }
}
