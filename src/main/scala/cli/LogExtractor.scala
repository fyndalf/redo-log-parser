package cli

import java.nio.file.Path

import cats.implicits._
import com.monovore.decline._
import parser.FileParser

object LogExtractor extends CommandApp(
  name = "redo-log-extractor",
  header = "Extract Event logs from redo logs",
  main = {

    val filePath = Opts.argument[Path](metavar = "file")

    val verboseOpt = Opts
      .flag("verbose", help = "Print extra metadata to the console.")
      .orFalse

    (filePath, verboseOpt).mapN {
      (path, verbose) =>
        if (verbose)
          println(s"Getting log file from ${path.toAbsolutePath.toString}")

        // todo: make block separator a parameter
        // todo: make date time format a parameter
        val logEntries = FileParser.getAndParseLogFile(path)
        if (verbose) {
          println(s"Read following log entries from ${path.toAbsolutePath.toString}:")
          logEntries.toList.foreach(println)
        }
        val parsedLogEntries = FileParser.parseLogEntries(logEntries)
        if (verbose) {
          println("Extracted the following statements out of the log entries:")
          parsedLogEntries.toList.foreach(println)
        }
        val transformedLogEntries = eventExtractor.transformRowIdentifiers(parsedLogEntries)
        if (verbose) {
          println("Transformed the following log entries and their row identifiers:")
          transformedLogEntries.toList.foreach(println)
        }
        val databaseSchema = schemaExtractor.extractDatabaseSchema(transformedLogEntries)
        if (verbose) {
          println("Exctracted the following database schema from transformed log entries:")
          databaseSchema.toList.foreach(println)
        }

      /**
       * TODO: Add table and column class
       * A table contains several columns that are discovered in the process
       * A column contains a flag indicating whether it could be a primary key,
       * a list of all distinct values found,
       * and a list of all columns that could be a foreign key that refers to this column
       */

      //TODO: Replay the log events and find tables, columns and the relation between them
    }
  }
)
