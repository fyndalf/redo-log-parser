package parser.file

import parser.ParsedStatement.{
  DeleteStatement,
  InsertStatement,
  UpdateStatement
}
import parser.{LogEntryWithRedoStatement, ParsedStatement}

import scala.collection.mutable
import scala.util.Random

object EventExtractor {

  /**
    * Transforms all log entries to ensure that now row id is reused across records
    */
  def transformRowIdentifiers(
      logEntries: Seq[LogEntryWithRedoStatement]
  ): Seq[LogEntryWithRedoStatement] = {
    val knownRowIDAndStatement =
      new mutable.HashMap[String, Seq[ParsedStatement]]
    val rowIDLookup = new mutable.HashMap[String, String]
    logEntries.map(logEntry => {
      val newRowID =
        determineNewRowID(logEntry, knownRowIDAndStatement, rowIDLookup)
      val entriesPerID =
        if (knownRowIDAndStatement.contains(newRowID))
          knownRowIDAndStatement(newRowID) :+ logEntry.statement
        else Seq(logEntry.statement)
      knownRowIDAndStatement += (newRowID -> entriesPerID)
      LogEntryWithRedoStatement(
        logEntry.statement,
        newRowID,
        logEntry.tableID,
        logEntry.timestamp
      )
    })
  }

  // todo: ensure uniqueness across entities
  /**
    * Translates row IDs of log entries to new ones,
    * based on what kind of statements have already been seen for that row
    * if lookup is empty: add normal row to lookup and use that
    *  if lookup is not empty:
    *     if current statement is insert: use new id and add to lookup
    *     if current statement is update or delete, and known does not contain delete: use lookup
    *     if known contains delete: use new id and add to lookup
    */
  private def determineNewRowID(
      logEntry: LogEntryWithRedoStatement,
      knownRowIDs: mutable.HashMap[String, Seq[ParsedStatement]],
      rowIDLookup: mutable.HashMap[String, String]
  ): String = {
    val oldRowID = logEntry.rowID
    if (!rowIDLookup.contains(oldRowID)) {
      rowIDLookup(oldRowID) = oldRowID
      oldRowID
    } else {
      logEntry.statement match {
        case _: InsertStatement =>
          val newRowID = generateNewRowID(oldRowID, rowIDLookup)
          rowIDLookup(oldRowID) = newRowID
          newRowID
        case _: UpdateStatement | _: DeleteStatement
            if !knownRowIDs(rowIDLookup(oldRowID)).exists(
              _.isInstanceOf[DeleteStatement]
            ) =>
          rowIDLookup(oldRowID)
        case _ =>
          if (
            knownRowIDs(rowIDLookup(oldRowID)).exists(
              _.isInstanceOf[DeleteStatement]
            )
          ) {
            val newRowID = generateNewRowID(oldRowID, rowIDLookup)
            rowIDLookup(oldRowID) = newRowID
            newRowID
          } else {
            throw new NoSuchElementException
          }
      }
    }
  }

  /**
    * Generates a new row id and ensures that it has not already been used
    * @param oldRowID
    * @param rowIDLookup
    * @return
    */
  private def generateNewRowID(
      oldRowID: String,
      rowIDLookup: mutable.HashMap[String, String]
  ): String = {
    var newRowID = oldRowID
    while (
      rowIDLookup.values.exists(_.equals(newRowID)) || rowIDLookup.keys.exists(
        _.equals(newRowID)
      )
    ) {
      newRowID = newRowID + "_" + Random.alphanumeric
        .filter(_.isLetterOrDigit)
        .take(4)
        .mkString
    }
    newRowID
  }
}
