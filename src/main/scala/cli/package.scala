import parser.{ExtractedLogEntry, LogEntryWithRedoStatement, RootClass}
import schema.DatabaseSchema

import java.nio.file.Path
import scala.annotation.tailrec

/**
  * A companion object holding additional print methods for the CLI tool,
  * and storing how strict the primary key check should be
  */
package object cli {

  // Should a strict check be used for determining primary key columns?
  var strictPrimaryKeyChecking: Boolean = false

  // Should update values generate events with values as a part of their event descriptor?
  var includeUpdateValues: Boolean = false

  // The expected format of the redo log's timestamps
  var dateFormatString: String = "dd-MMM-yyyy HH:mm:ss"

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

  @tailrec
  def getRootClassInput(schema: DatabaseSchema): RootClass = {
    val rootClassInput =
      scala.io.StdIn.readLine("\nPlease enter a root class:")
    val rootClass = RootClass(rootClassInput)
    if (schema.keySet.contains(rootClass.tableID)) {
      rootClass
    } else {
      println(
        s"\nThe class $rootClassInput you entered does not seem to exist. Please try again!"
      )
      getRootClassInput(schema)
    }
  }
}
