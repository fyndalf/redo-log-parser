package parser.trace

import parser.{
  LogEntriesForTableAndEntity,
  LogEntryWithRedoStatement,
  RootElement,
  RowWithBucketIdentifier
}

object TraceIDParserHelper {

  // assign entries to root element buckets
  def gatherRootLogBuckets(
      rootElement: RootElement,
      logEntriesForEntity: Seq[LogEntriesForTableAndEntity]
  ): Seq[Seq[LogEntryWithRedoStatement]] = {
    logEntriesForEntity
      .filter(_.tableID.equalsIgnoreCase(rootElement.tableID))
      .flatMap(_.entriesForEntities.map(_.logEntries))
  }

  def gatherRowsFromLogBuckets(
      logBuckets: Seq[Seq[LogEntryWithRedoStatement]]
  ): Seq[RowWithBucketIdentifier] = {
    logBuckets.zipWithIndex.map(bucket => {
      val tableID = bucket._1.head.tableID
      val rowID = bucket._1.head.rowID
      RowWithBucketIdentifier(tableID, rowID, bucket._2)
    })
  }

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

  def initializeLogEntryBucketsForRows(
      rows: Seq[RowWithBucketIdentifier]
  ): Array[Seq[LogEntryWithRedoStatement]] = {
    val bucketSize = rows.length
    new Array[Seq[LogEntryWithRedoStatement]](bucketSize).map(_ =>
      Seq[LogEntryWithRedoStatement]()
    )
  }
}
