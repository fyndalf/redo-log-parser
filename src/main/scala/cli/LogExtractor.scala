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

    // todo: remove this example data
    val userOpt =
      Opts
        .option[String]("target", help = "Person to greet.")
        .withDefault("world")

    val verboseOpt = Opts
      .flag("verbose", help = "Print extra metadata to the console.")
      .orFalse

    (filePath, userOpt, verboseOpt).mapN {
      (path, user, verbose) =>
        if (verbose)
          println(s"Getting log file from ${path.toAbsolutePath.toString}")
        else println(s"Hello $user!")

        // todo: make block separator a parameter
        // todo: make date time format a parameter
        val logEntries = FileParser.getAndParseLogFile(path)
        if (verbose) {
          println(s"Read following log entries from ${path.toAbsolutePath.toString}:")
          logEntries.toList.map(println)
        }
        val parsedLogEntries = FileParser.parseLogEntries(logEntries)
        if (verbose) {
          println("Extracted the following statements out of the log entries:")
          parsedLogEntries.toList.map(println)
        }

        //todo: derive unique row ids for each lifetime of a row id
        // new row id for insert and updates until we see delete or new insert

    }
  }
)
