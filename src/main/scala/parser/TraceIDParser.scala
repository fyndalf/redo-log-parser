package parser

import parser.ParsedStatement.InsertStatement
import schema.{DatabaseSchema, Table}

import scala.xml.XML;

object TraceIDParser {

  def parseTraceIDPatternFromInput(input: String): TraceIDPattern = {
    val splitTables = input.split(",")

    val splitAttibutes = splitTables.map(s => {
      val lastInstance = s.lastIndexOf(".")
      s.splitAt(lastInstance)
    })
    splitAttibutes.map(entry => TraceIDPatternPart(entry._1, entry._2)).toSeq
  }

  // assumes: primary keys and foreign keys are not updated
  def createTracesForPattern(
      pattern: TraceIDPattern,
      schema: DatabaseSchema,
      logEntries: Seq[LogEntryWithRedoStatement]
  ): Seq[LogEntriesForTrace] = {
    // group entries by table and row id
    val logEntriesForTable = logEntries.groupBy(_.tableID)
    val logEntriesForEntity = logEntriesForTable.map(x => {
      (x._1, x._2.groupBy(_.rowID))
    })

    println(logEntriesForEntity)
    logEntriesForEntity.foreach(println(_))

    // extract referenced tables for each table
    val tableRelations = schema.values.map(table => {
      var referencedTables = Set[Table]()
      table.columns.values.foreach(col => {
        referencedTables = referencedTables ++ col.isSubsetOf.map(_.table)
      })
      (table, referencedTables)
    })

    println("")
    println(tableRelations)

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

    println("")
    println(uniqueRelations)

    // find all relevant attributes mapping to other table
    // for each of relation: look at both columns and take matching values
    // save combination of row id

    uniqueRelations.map(relation => {
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

      println("")
      println(allReferences)

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
                if (rightStatement.insertedAttributesAndValues.contains(
                  rightColumn.name
                ) &&
                  rightStatement
                    .insertedAttributesAndValues(rightColumn.name)
                    .equals(leftValue)) {
                  val existingEntities = currentTableEntityRelation.relatingEntities
                  val newRelatingEntities = existingEntities :+ (insertStatement.rowID, s.rowID)
                  currentTableEntityRelation = currentTableEntityRelation.copy(relatingEntities =  newRelatingEntities)

                }
              })
            }
        }})
      })
      println(currentTableEntityRelation)
      currentTableEntityRelation
    })

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
