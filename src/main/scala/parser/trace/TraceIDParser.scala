package parser.trace

import parser.ParsedStatement.{
  DeleteStatement,
  InsertStatement,
  UpdateStatement
}
import parser._
import parser.relations.RelationsExtractor.{
  extractRelations,
  extractTableEntityRelations,
  extractUniqueBinaryRelations
}
import parser.trace.TraceBinningAssigner.determineRowBuckets
import parser.trace.TraceIDParserHelper.{
  assignLogEntriesToBuckets,
  gatherRootLogBuckets,
  gatherRowsFromLogBuckets,
  initializeLogEntryBucketsForRows
}
import schema.DatabaseSchema

import scala.xml.XML

object TraceIDParser {

  // assumes: primary keys and foreign keys are not updated
  def createTracesForPattern(
      rootElement: RootElement,
      schema: DatabaseSchema,
      logEntries: Seq[LogEntryWithRedoStatement]
  ): Seq[LogEntriesForTrace] = {
    // group entries by table and row id
    val logEntriesForTable = logEntries
      .groupBy(_.tableID)
      .map(e => LogEntriesForTable(e._1, e._2))
    val logEntriesForEntity = logEntriesForTable
      .map(x => {
        LogEntriesForTableAndEntity(
          x.tableID,
          x.logEntries
            .groupBy(_.rowID)
            .map(e => LogEntriesForEntity(e._1, e._2))
            .toSeq
        )
      })
      .toSeq

    val tableRelations = extractRelations(schema)

    val uniqueRelations = extractUniqueBinaryRelations(tableRelations)

    val tableEntityRelations =
      extractTableEntityRelations(uniqueRelations, schema, logEntriesForEntity)

    val logBuckets = gatherRootLogBuckets(rootElement, logEntriesForEntity)

    val relevantRows = gatherRowsFromLogBuckets(logBuckets)

    val rootTable = schema(rootElement.tableID)

    val rowBucketMapping =
      determineRowBuckets(
        rootTable,
        Set(rootTable),
        tableEntityRelations,
        relevantRows
      )

    val logEntryBuckets = initializeLogEntryBucketsForRows(relevantRows)

    val rowIDsWithBuckets: Map[String, Set[RowWithBucketIdentifier]] =
      rowBucketMapping.groupBy(_.rowID)

    assignLogEntriesToBuckets(logEntryBuckets, logEntries, rowIDsWithBuckets)
  }

  def parseTraceToXML(events: LogEntriesForTrace): scala.xml.Node = {
    val eventNodes = events.map(e => {
      val table = e.tableID
      val eventName = e.statement match {
        case _: InsertStatement => s"Insert into $table"
        case update: UpdateStatement =>
          val affectedAttribute = update.affectedAttribute
          s"Update $affectedAttribute from $table"
        case _: DeleteStatement => s"Delete $table"
      }
      <event>
        <string key="concept:name" value={eventName}/>
        <date key="time:timestamp" value={e.timestamp.toString}/>
      </event>
    })

    <trace>
      {eventNodes}
    </trace>
  }

  def serializeLogToDisk(
      traces: Seq[scala.xml.Node],
      filename: String
  ): Unit = {
    val logContent = traces.flatten[scala.xml.Node].head
    XML.save(filename, logContent, "UTF-8", xmlDecl = true, null)
  }

}
