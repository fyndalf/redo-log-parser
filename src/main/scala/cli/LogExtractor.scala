package cli

import java.nio.file.Path

import cats.implicits._
import com.monovore.decline._
import parser.file.{EventExtractor, FileParser}
import parser.trace.TraceIDParser
import parser.RootElement
import parser.trace.TraceIDParser.generateXMLLog
import schema.SchemaExtractor

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

            val rootElementInput =
              scala.io.StdIn.readLine("Please enter a root element:")
            val rootElement = RootElement(rootElementInput)

            println("Start creating traces from the redo log ...")

            val traces = TraceIDParser.createTracesForPattern(
              rootElement,
              databaseSchema,
              transformedLogEntries
            )

            println("Done.\nGenerating XES log and serialising it to disk ...")

            val log = generateXMLLog(traces)

            TraceIDParser.serializeLogToDisk(
              log,
              path.toString + "_result.xes"
            )

            println("Done.")
        }
      }
    )
