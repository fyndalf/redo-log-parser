package cli

import java.nio.file.Path

import cats.implicits._
import com.monovore.decline._
import parser.{EventExtractor, FileParser}
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
          (pathParam, veboseParam) =>
            implicit val verbose: Boolean = veboseParam
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

          // TODO: Implement log generator according to paper
        }
      }
    )
