import java.nio.file.Path

import parser.{ExtractedLogEntry, LogEntryWithRedoStatement}
import schema.DatabaseSchema

/**
  * A companion object holding additional print methods for the CLI tool,
  * and storing how strict the primary key check should be
  */
package object cli {

  // Should a strict check be used for determining primary key columns?
  var strictPrimaryKeyChecking: Boolean = false

  def printEntries(
      logEntries: Seq[ExtractedLogEntry]
  )(implicit path: Path, verbose: Boolean): Unit = {
    if (verbose) {
      println(
        s"\n\nRead following log entries from ${path.toAbsolutePath.toString}:"
      )
      logEntries.toList.foreach(println)
    }
  }

  def printParsedLogEntries(
      parsedLogEntries: Seq[LogEntryWithRedoStatement]
  )(implicit path: Path, verbose: Boolean): Unit = {
    if (verbose) {
      println(
        "\n\nExtracted the following statements out of the log entries:"
      )
      parsedLogEntries.toList.foreach(println)
    }
  }

  def printTransformedLogEntries(
      transformedLogEntries: Seq[LogEntryWithRedoStatement]
  )(implicit path: Path, verbose: Boolean): Unit = {
    if (verbose) {
      println(
        "\n\nTransformed the following log entries and their row identifiers:\n"
      )
      transformedLogEntries.toList.foreach(println)
    }
  }

  def printDatabaseSchema(
      databaseSchema: DatabaseSchema
  )(implicit path: Path, verbose: Boolean): Unit = {
    println(
      "\nExctracted the following database schema from transformed log entries:"
    )
    println(
      databaseSchema
        .map(table => table._2.toString)
        .mkString("\n\n")
    )
  }

  def printPath()(implicit path: Path, verbose: Boolean): Unit = {
    if (verbose)
      println(s"\n\nGetting log file from ${path.toAbsolutePath.toString}")
  }
}
