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

/**
  * Provides functionality for generating log entry traces for the chosen root class,
  * and for generating and serializing an XML XES log. Generally, it is assumed that
  * primary keys and foreign keys are not updated.
  */
object TraceIDParser {

  /**
    * Generates a sequence of sequences, where each sequence holds redo log entries for a trace
    * based on the selected root class and schema
    * @param rootClass The root class selected by the user
    * @param schema The schema discovered from the redo log
    * @param logEntries All log entries parsed out of the redo log
    * @return
    */
  def createTracesForPattern(
      rootClass: RootClass,
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

    val logBuckets = gatherRootLogBuckets(rootClass, logEntriesForEntity)

    val relevantRows = gatherRowsFromLogBuckets(logBuckets)

    val rootTable = schema(rootClass.tableID)

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

  /**
    * Takes all traces and their log entries and generates an XML Element containing the whole event log
    * @param traces A Sequence containing Sequences of Log Entries, each Seq being a single trace
    * @param rootClass The root class selected by the user
    * @return An XML Element containing the event log
    */
  def generateXMLLog(
      traces: Seq[LogEntriesForTrace],
      rootClass: RootClass
  ): Elem = {
    val xmlTraces = traces
      .map(parseTraceEventsToXML)

    val logName = s"${rootClass.tableID}_XES_Log"

    <log xes.version="2.0" xmlns="http://www.xes-standard.org/">
      <extension name="Time" prefix="time" uri="http://www.xes-standard.org/time.xesext"/>
      <extension name="Concept" prefix="concept" uri="http://www.xes-standard.org/concept.xesext"/>
      <string key="concept:name" value={logName} />
      {xmlTraces.map(traceNode => <trace>{traceNode}</trace>)}
    </log>
  }

  /**
    * Parses a Sequence of Log Entries to a Sequence containing XML Events
    * @param events A list of Log Entries to be transformed to Events
    * @return A Sequence containing the translated XML Events
    */
  private def parseTraceEventsToXML(events: LogEntriesForTrace): Seq[Elem] = {
    val eventNodes = events.map(event => {

      val eventName = event.statement match {
        case _: InsertStatement => s"Add ${event.tableID} entity"
        case update: UpdateStatement if cli.includeUpdateValues =>
          s"Update ${update.affectedAttribute} value of ${event.tableID} entity to ${update.newAttributeValue}"
        case update: UpdateStatement if !cli.includeUpdateValues =>
          s"Update ${update.affectedAttribute} value of ${event.tableID} entity"
        case _: DeleteStatement => s"Delete entity from ${event.tableID}"
      }
      <string key="concept:name" value={eventName}/>
          <date key="time:timestamp" value={
        event.timestamp.toString + ":000+00:00"
      }/>
    })

    { eventNodes.map(eventNode => <event>{eventNode}</event>) }
  }

  /**
    * Serializes the Event Log XML to disk
    * @param traces The Event Log XML
    * @param filename The File Name with which the Log should be stored
    */
  def serializeLogToDisk(
      traces: Elem,
      filename: String
  ): Unit = {
    val prettyPrinter = new scala.xml.PrettyPrinter(200, 2)
    val prettyXml = prettyPrinter.format(traces)
    XML.save(filename, XML.loadString(prettyXml), "UTF-8", xmlDecl = true, null)
  }

}
