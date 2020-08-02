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

import scala.xml.{Elem, XML}

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
      .map(table => LogEntriesForTable(table._1, table._2))
    val logEntriesForEntity = logEntriesForTable
      .map(logEntriesForTable => {
        LogEntriesForTableAndEntity(
          logEntriesForTable.tableID,
          logEntriesForTable.logEntries
            .groupBy(_.rowID)
            .map(row => LogEntriesForEntity(row._1, row._2))
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

  def generateXMLLog(traces: Seq[LogEntriesForTrace]): Elem = {
    val xmlTraces = traces
      .map(parseTraceEventsToXML)

    <log xes.version="2.0" xmlns="http://www.xes-standard.org/">
      {xmlTraces.map(t => <trace>{t}</trace>)}
    </log>
  }

  private def parseTraceEventsToXML(events: LogEntriesForTrace): Seq[Elem] = {
    val eventNodes = events.map(e => {
      val table = e.tableID
      val eventName = e.statement match {
        case _: InsertStatement => s"Add $table entity"
        case update: UpdateStatement =>
          val affectedAttribute = update.affectedAttribute
          val newAttributeValue = update.newAttributeValue
          s"Update $affectedAttribute value of $table entity to $newAttributeValue"
        case _: DeleteStatement => s"Delete entity from $table"
      }
      <string key="concept:name" value={eventName}/>
        <date key="time:timestamp" value={e.timestamp.toString}/>
    })

    { eventNodes.map(e => <event>{e}</event>) }
  }

  def serializeLogToDisk(
      traces: Elem,
      filename: String
  ): Unit = {
    val prettyPrinter = new scala.xml.PrettyPrinter(200, 2)
    val prettyXml = prettyPrinter.format(traces)
    XML.save(filename, XML.loadString(prettyXml), "UTF-8", xmlDecl = true, null)
  }

}
