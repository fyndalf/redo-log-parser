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
object LogExtractor
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
            val logEntries = FileParser.getAndParseLogFile(path)
            printEntries(logEntries)
            val parsedLogEntries = FileParser.parseLogEntries(logEntries)
            printParsedLogEntries(parsedLogEntries)
            val transformedLogEntries =
              EventExtractor.transformRowIdentifiers(parsedLogEntries)
            printTransformedLogEntries(transformedLogEntries)
            val databaseSchema =
              SchemaExtractor.extractDatabaseSchema(transformedLogEntries)
            printDatabaseSchema(databaseSchema)

            val rootClassInput =
              scala.io.StdIn.readLine("Please enter a root element:")
            val rootClass = RootClass(rootClassInput)

            println("Start creating traces from the redo log ...")

            val traces = TraceIDParser.createTracesForPattern(
              rootClass,
              databaseSchema,
              transformedLogEntries
            )

            println("Done.\nGenerating XES log and serialising it to disk ...")

            val log = generateXMLLog(traces, rootClass)

            TraceIDParser.serializeLogToDisk(
              log,
              path.toString + s"_${rootClass.tableID}_result.xes"
            )

            println("Done.")
        }
      }
    )
