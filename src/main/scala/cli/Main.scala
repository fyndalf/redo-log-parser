package cli

import cats.implicits._
import com.monovore.decline._
import parser.file.{EventExtractor, FileParser}
import parser.trace.TraceIDParser
import parser.trace.TraceIDParser.generateXMLLog
import schema.SchemaExtractor

import java.io.FileNotFoundException
import java.nio.file.Path

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

        val includeUpdateValuesOpt = Opts
          .flag(
            "includeUpdateValues",
            help =
              "Generate events from update statements that contain the updated values in the event name."
          )
          .orFalse

        val strongPKOpt = Opts
          .flag(
            "strict",
            help =
              "Only allow strong Primary Key candidates by checking whether " +
                "the column names indicate a primary key and the values only ever increase "
          )
          .orFalse

        (filePath, verboseOpt, includeUpdateValuesOpt, strongPKOpt).mapN {
          (pathParam, verboseParam, includeUpdateValuesParam, strongPKParam) =>
            implicit val verbose: Boolean = verboseParam
            implicit val path: Path = pathParam

            // determine strictness of PK checking
            cli.strictPrimaryKeyChecking = strongPKParam

            // determine whether updated values should be included
            cli.includeUpdateValues = includeUpdateValuesParam

            if (verbose && strongPKParam)
              println("Strong PK Checking has been enabled!")

            printPath()
            // todo: make block separator a parameter
            // todo: make date time format a parameter

            println("Reading and parsing redo log...")
            var logEntries: Seq[parser.ExtractedLogEntry] = null
            try {
              logEntries = FileParser.getAndParseLogFile(path)
            } catch {
              case _: FileNotFoundException =>
                println(
                  "The file you provided could not be found. Please ensure that the path to the redo log is correct, and that the log exists."
                )
                System.exit(1)

            }
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

            val rootClass = getRootClassInput(databaseSchema).get

            println("\nStart creating traces from the redo log ...")

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
