package cli

import java.nio.file.Path

import cats.implicits._
import com.monovore.decline._
import parser.file.{EventExtractor, FileParser}
import parser.trace.TraceIDParser
import parser.RootClass
import parser.trace.TraceIDParser.generateXMLLog
import schema.SchemaExtractor

/**
  * The main object of the CLI tool - it is used to run the tool.
  */
object Main
    extends CommandApp(
      name = "redo-log-extractor",
      header = "Extract Event logs from redo logs",
      main = {

        val filePath = Opts.argument[Path](metavar = "file")

        val verboseOpt = Opts
          .flag("verbose", help = "Print detailed information the console.")
          .orFalse

        (filePath, verboseOpt).mapN {
          (pathParam, verboseParam) =>
            implicit val verbose: Boolean = verboseParam
            implicit val path: Path = pathParam

            printPath()
            // todo: make block separator a parameter
            // todo: make date time format a parameter

            println("Reading and parsing redo log...")

            val logEntries = FileParser.getAndParseLogFile(path)
            printEntries(logEntries)
            val parsedLogEntries = FileParser.parseLogEntries(logEntries)
            printParsedLogEntries(parsedLogEntries)
            val transformedLogEntries =
              EventExtractor.transformRowIdentifiers(parsedLogEntries)
            printTransformedLogEntries(transformedLogEntries)

            println("Done.\nExtracting database schema...")

            val databaseSchema =
              SchemaExtractor.extractDatabaseSchema(transformedLogEntries)

            println("Done.")

            printDatabaseSchema(databaseSchema)

            val rootClassInput =
              scala.io.StdIn.readLine("\nPlease enter a root class:")
            val rootClass = RootClass(rootClassInput)

            println("Start creating traces from the redo log ...")

            val traces = TraceIDParser.createTracesForPattern(
              rootClass,
              databaseSchema,
              transformedLogEntries
            )

            println("Done.\nGenerating XES log and serialising it to disk ...")

            val log = generateXMLLog(traces, rootClass)
            val resultPath = path.toString + s"_${rootClass.tableID}_result.xes"

            TraceIDParser.serializeLogToDisk(
              log,
              resultPath
            )

            println(s"Done.\nThe event log is stored in $resultPath ")
        }
      }
    )
