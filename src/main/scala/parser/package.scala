import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

package object parser {

  // todo: make this method with optional parameter passed via cli
  lazy val formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .appendPattern("dd-MMM-yyyy HH:mm:ss")
    .toFormatter

  // todo: make redoStatement own case class and parse String into it
  case class LogEntry(
                       timestamp: LocalDateTime,
                       tableIdentifier: String,
                       redoStatement: String
                     )

}
