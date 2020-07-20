package parser

import schema.DatabaseSchema

import scala.xml.XML;

object TraceIDParser {

  def parseTraceIDPatternFromInput(input: String): TraceIDPattern = {
    ???
  }

  def createTracesForPattern(
      pattern: TraceIDPattern,
      schema: DatabaseSchema,
      logEntries: Seq[LogEntryWithRedoStatement]
  ): Seq[LogEntriesForTrace] = {
    ???
  }

  def parseTraceToXML(events: LogEntriesForTrace): scala.xml.Node = {
    ???
  }

  def serializeLogToDisk(
      traces: Seq[scala.xml.Node],
      filename: String
  ): Unit = {
    val logContent = traces.flatten[scala.xml.Node].head
    XML.save(filename, logContent, "UTF-8", xmlDecl = true, null)
  }

}
