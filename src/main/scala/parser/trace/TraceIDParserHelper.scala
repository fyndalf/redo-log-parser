package parser.trace

import parser.{
  LogEntriesForTableAndEntity,
  LogEntryWithRedoStatement,
  RootClass,
  RowWithBucketIdentifier
}

/**
  * Provides methods for handling the binning of log entries to traces.
  */
object TraceIDParserHelper {

  /**
    * For the root class, assign entries to root element buckets
    */
  def gatherRootLogBuckets(
      rootClass: RootClass,
      logEntriesForEntity: Seq[LogEntriesForTableAndEntity]
  ): Seq[Seq[LogEntryWithRedoStatement]] = {
    logEntriesForEntity
      .filter(_.tableID.equalsIgnoreCase(rootClass.tableID))
      .flatMap(_.entriesForEntities.map(_.logEntries))
  }

  /**
    * Extracts rows and and tables with a bucket identifier from sequences of log entry statements
    */
  def gatherRowsFromLogBuckets(
      logBuckets: Seq[Seq[LogEntryWithRedoStatement]]
  ): Seq[RowWithBucketIdentifier] = {
    logBuckets.zipWithIndex.map(bucket => {
      val tableID = bucket._1.head.tableID
      val rowID = bucket._1.head.rowID
      RowWithBucketIdentifier(tableID, rowID, bucket._2)
    })
  }

  /**
    * Assigns each log entry to the trace buckets it belongs to, based on the the table id and the determined bucket id
    */
  def assignLogEntriesToBuckets(
      logEntryBuckets: Array[Seq[LogEntryWithRedoStatement]],
      logEntries: Seq[LogEntryWithRedoStatement],
      rowIDsWithBuckets: Map[String, Set[RowWithBucketIdentifier]]
  ): Seq[Seq[LogEntryWithRedoStatement]] = {
    logEntries.foreach(entry => {
      if (rowIDsWithBuckets.contains(entry.rowID)) {
        rowIDsWithBuckets(entry.rowID)
          .filter(_.tableID.equals(entry.tableID))
          .foreach(b => {
            logEntryBuckets(b.bucketID) = logEntryBuckets(b.bucketID) :+ entry
          })
      }
    })

    logEntryBuckets.toSeq
  }

  /**
    * Initializes an array for holding log entry buckets in order to assign them to different traces
    */
  def initializeLogEntryBucketsForRows(
      rows: Seq[RowWithBucketIdentifier]
  ): Array[Seq[LogEntryWithRedoStatement]] = {
    val bucketSize = rows.length
    new Array[Seq[LogEntryWithRedoStatement]](bucketSize).map(_ =>
      Seq[LogEntryWithRedoStatement]()
    )
  }
}
