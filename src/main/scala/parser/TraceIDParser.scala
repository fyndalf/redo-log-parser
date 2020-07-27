package parser

import parser.ParsedStatement.InsertStatement
import schema.{DatabaseSchema, Table}

import scala.xml.XML;

object TraceIDParser {

  // assumes: primary keys and foreign keys are not updated
  def createTracesForPattern(
      rootElement: RootElement,
      schema: DatabaseSchema,
      logEntries: Seq[LogEntryWithRedoStatement]
  ): Seq[LogEntriesForTrace] = {
    // group entries by table and row id
    val logEntriesForTable = logEntries.groupBy(_.tableID)
    val logEntriesForEntity = logEntriesForTable.map(x => {
      (x._1, x._2.groupBy(_.rowID))
    })

    // extract referenced tables for each table
    val tableRelations = schema.values.map(table => {
      var referencedTables = Set[Table]()
      table.columns.values.foreach(col => {
        referencedTables = referencedTables ++ col.isSubsetOf.map(_.table)
      })
      (table, referencedTables)
    })

    // identify all unique binary relations
    val allBinaryRelations =
      tableRelations.flatMap(x => x._2.map(y => (x._1, y))).toSet
    var uniqueRelations = Set[(Table, Table)]()
    allBinaryRelations.foreach(relation => {
      if (
        !uniqueRelations.contains(relation) && !uniqueRelations
          .contains(relation.swap)
      ) {
        uniqueRelations = uniqueRelations + relation
      }
    })

    // find all relevant attributes mapping to other table
    // for each of relation: look at both columns and take matching values
    // save combination of row id

    val tableEntityRelations = uniqueRelations.map(relation => {
      val (leftTable, rightTable) = relation
      val rightReferences = schema(leftTable.name).columns.values
        .flatMap(col => {
          col.isSubsetOf = col.isSubsetOf
            .filter(_.table.name.equalsIgnoreCase(rightTable.name))
            .filter(_.isPrimaryKey)
          val columnPairs =
            col.isSubsetOf.map(referencedColumn => (col, referencedColumn))
          columnPairs
        })
        .toSet
      val leftReferences = schema(rightTable.name).columns.values
        .flatMap(col => {
          col.isSubsetOf = col.isSubsetOf
            .filter(_.table.name.equalsIgnoreCase(leftTable.name))
            .filter(_.isPrimaryKey)
          val columnPairs =
            col.isSubsetOf.map(referencedColumn => (referencedColumn, col))
          columnPairs
        })
        .toSet
      val allReferences = rightReferences ++ leftReferences

      var currentTableEntityRelation =
        TableEntityRelation(leftTable, rightTable, Seq())

      allReferences.foreach(reference => {
        val (leftColumn, rightColumn) = reference
        val leftTableEntries = logEntriesForEntity.toSeq
          .filter(_._1.equals(leftTable.name))
          .flatMap(_._2)
        val rightTableEntries = logEntriesForEntity.toSeq
          .filter(_._1.equals(rightTable.name))
          .flatMap(_._2)
        leftTableEntries.foreach(entry => {
          val leftInsertStatments =
            entry._2.filter(_.statement.isInstanceOf[InsertStatement])
          if (leftInsertStatments.nonEmpty) {
            val insertStatement = leftInsertStatments.head
            val statement =
              insertStatement.statement.asInstanceOf[InsertStatement]
            if (
              statement.insertedAttributesAndValues.contains(leftColumn.name)
            ) {
              val leftValue =
                statement.insertedAttributesAndValues(leftColumn.name)
              val rightInsertStatements = rightTableEntries.flatMap(e =>
                e._2.filter(_.statement.isInstanceOf[InsertStatement])
              )
              rightInsertStatements.foreach(s => {
                val rightStatement = s.statement.asInstanceOf[InsertStatement]
                if (
                  rightStatement.insertedAttributesAndValues.contains(
                    rightColumn.name
                  ) &&
                  rightStatement
                    .insertedAttributesAndValues(rightColumn.name)
                    .equals(leftValue)
                ) {
                  val existingEntities =
                    currentTableEntityRelation.relatingEntities
                  val newRelatingEntities =
                    existingEntities :+ (insertStatement.rowID, s.rowID)
                  currentTableEntityRelation =
                    currentTableEntityRelation.copy(relatingEntities =
                      newRelatingEntities
                    )

                }
              })
            }
          }
        })
      })
      currentTableEntityRelation
    })

    var logBuckets: Seq[Seq[LogEntryWithRedoStatement]] = Seq()
    val rootLogEntries = logEntriesForEntity
      .filter(_._1.equalsIgnoreCase(rootElement.tableID))
      .values
    rootLogEntries.foreach(x => {
      logBuckets = logBuckets ++ x.values
    })

    val relevantRows = logBuckets.zipWithIndex.map(bucket => {
      val tableID = bucket._1.head.tableID
      val rowID = bucket._1.head.rowID
      (tableID, rowID, bucket._2)
    })

    val rootTable = schema(rootElement.tableID)

    val bucketMapping =
      traverse(rootTable, Set(rootTable), tableEntityRelations, relevantRows)

    val bucketSize = relevantRows.length

    val logEntryBuckets =
      new Array[Seq[LogEntryWithRedoStatement]](bucketSize).map(x =>
        Seq[LogEntryWithRedoStatement]()
      )

    val rowIDs = bucketMapping.groupBy(_._2)
    logEntries.foreach(entry => {
      rowIDs(entry.rowID)
        .filter(_._1.equals(entry.tableID))
        .foreach(b => {
          logEntryBuckets(b._3) = logEntryBuckets(b._3) :+ entry
        })
    })

    logEntryBuckets.toSeq
  }

  def traverse(
      currentEntity: Table,
      path: Set[Table],
      tableEntityRelations: Set[TableEntityRelation],
      relevantRows: Seq[(String, String, Int)]
  ): Set[(String, String, Int)] = {
    // todo: make tuples case classes
    val leftEntityRelations = tableEntityRelations.filter(x =>
      x.leftTable.name.equals(currentEntity.name) && !path.contains(
        x.rightTable
      )
    )
    val rightEntityRelations = tableEntityRelations.filter(y =>
      y.rightTable.name.equals(currentEntity.name) && !path.contains(
        y.leftTable
      )
    )

    val leftResult = leftEntityRelations.flatMap(x => {
      val newPath = path + x.leftTable
      val newIntermediateRowIDs =
        relevantRows.filter(rr => x.relatingEntities.map(_._1).contains(rr._2))
      var newRelevantRowIDs = Seq[(String, String, Int)]()
      newIntermediateRowIDs.foreach(rr => {
        val newRowIDs = x.relatingEntities.filter(xr => xr._1.equals(rr._2))
        newRowIDs.foreach(newRowID => {
          newRelevantRowIDs =
            newRelevantRowIDs :+ (x.rightTable.name, newRowID._2, rr._3)
        })
      })
      traverse(x.rightTable, newPath, tableEntityRelations, newRelevantRowIDs)
    })

    val rightResult = rightEntityRelations.flatMap(y => {
      val newPath = path + y.rightTable
      val newIntermediateRowIDs =
        relevantRows.filter(rr => y.relatingEntities.map(_._2).contains(rr._2))
      var newRelevantRowIDs = Seq[(String, String, Int)]()
      newIntermediateRowIDs.foreach(rr => {
        val newRowIDs = y.relatingEntities.filter(yr => yr._2.equals(rr._2))
        newRowIDs.foreach(newRowID => {
          newRelevantRowIDs =
            newRelevantRowIDs :+ (y.leftTable.name, newRowID._1, rr._3)
        })
      })
      traverse(y.leftTable, newPath, tableEntityRelations, newRelevantRowIDs)
    })

    leftResult ++ relevantRows ++ rightResult
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
