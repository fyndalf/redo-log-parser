package parser.relations

import parser.ParsedStatement.InsertStatement
import parser._
import schema.DatabaseSchema

object RelationsExtractor {
  // todo: improve naming

  // extract referenced tables for each table
  def extractRelations(schema: DatabaseSchema): Seq[Relation] = {
    schema.values
      .map(table => {
        val referencedTables = table.columns.values
          .flatMap(col => {
            col.isSubsetOf.map(_.table)
          })
          .toSet
        Relation(table, referencedTables)
      })
      .toSeq
  }

  // identify all unique binary relations
  def extractUniqueBinaryRelations(
      relations: Seq[Relation]
  ): Set[BinaryRelation] = {
    val allBinaryRelations =
      relations
        .flatMap(relation =>
          relation.relatingTables.map(relatingTable =>
            BinaryRelation(relation.table, relatingTable)
          )
        )
        .toSet

    var uniqueRelations = Set[BinaryRelation]()
    allBinaryRelations.foreach(relation => {
      if (
        !uniqueRelations.contains(relation) && !uniqueRelations
          .contains(relation.swap())
      ) {
        uniqueRelations = uniqueRelations + relation
      }
    })
    uniqueRelations
  }

  // find all relevant attributes mapping to other table
  // for each of relation: look at both columns and take matching values
  // save combination of row id
  def extractTableEntityRelations(
      uniqueRelations: Set[BinaryRelation],
      schema: DatabaseSchema,
      logEntriesForEntity: Seq[LogEntriesForTableAndEntity]
  ): Set[TableEntityRelation] = {
    uniqueRelations.map(relation => {
      val allReferences = extractReferencesOfRelation(relation, schema)
      extractEntityRelationsForRelation(
        relation,
        allReferences,
        logEntriesForEntity
      )
    })
  }

  private def extractEntityRelationsForRelation(
      relation: BinaryRelation,
      allReferences: Set[ColumnRelation],
      logEntriesForEntity: Seq[LogEntriesForTableAndEntity]
  ): TableEntityRelation = {
    var currentTableEntityRelation =
      TableEntityRelation(relation.leftTable, relation.rightTable, Seq())

    allReferences.foreach(columnRelation => {

      val leftTableEntries = logEntriesForEntity
        .filter(_.tableID.equals(relation.leftTable.name))
        .flatMap(_.entriesForEntities)

      val rightTableEntries = logEntriesForEntity
        .filter(_.tableID.equals(relation.rightTable.name))
        .flatMap(_.entriesForEntities)

      leftTableEntries.foreach(logEntriesForEntity => {
        currentTableEntityRelation = determineRelationForEntity(
          logEntriesForEntity,
          columnRelation,
          rightTableEntries,
          currentTableEntityRelation
        )
      })
    })
    currentTableEntityRelation
  }

  private def determineRelationForEntity(
      entriesForEntity: LogEntriesForEntity,
      columnRelation: ColumnRelation,
      rightTableEntries: Seq[LogEntriesForEntity],
      currentTableEntityRelation: TableEntityRelation
  ): TableEntityRelation = {
    var updatedTableEntityRelation = currentTableEntityRelation
    val leftInsertStatements =
      entriesForEntity.logEntries.filter(
        _.statement.isInstanceOf[InsertStatement]
      )
    if (leftInsertStatements.nonEmpty) {
      val leftInsertStatement = leftInsertStatements.head
      val statement =
        leftInsertStatement.statement.asInstanceOf[InsertStatement]
      if (
        statement.insertedAttributesAndValues
          .contains(columnRelation.leftColumn.name)
      ) {
        val leftAttributeValue =
          statement.insertedAttributesAndValues(columnRelation.leftColumn.name)
        val rightInsertStatements = rightTableEntries.flatMap(e =>
          e.logEntries.filter(_.statement.isInstanceOf[InsertStatement])
        )
        rightInsertStatements.foreach(logEntry => {
          updatedTableEntityRelation = updateEntityRelationForRightStatements(
            logEntry,
            columnRelation,
            leftAttributeValue,
            leftInsertStatement,
            updatedTableEntityRelation
          )
        })
      }
    }
    updatedTableEntityRelation
  }

  private def updateEntityRelationForRightStatements(
      logEntry: LogEntryWithRedoStatement,
      columnRelation: ColumnRelation,
      leftAttributeValue: String,
      leftInsertStatement: LogEntryWithRedoStatement,
      currentTableEntityRelation: TableEntityRelation
  ): TableEntityRelation = {
    var updatedTableEntityRelation = currentTableEntityRelation
    val rightStatement = logEntry.statement.asInstanceOf[InsertStatement]
    if (
      rightStatement.insertedAttributesAndValues.contains(
        columnRelation.rightColumn.name
      ) &&
      rightStatement
        .insertedAttributesAndValues(columnRelation.rightColumn.name)
        .equals(leftAttributeValue)
    ) {
      val existingEntities =
        currentTableEntityRelation.relatingEntities
      val newRelatingEntities =
        existingEntities :+ EntityRelation(
          leftInsertStatement.rowID,
          logEntry.rowID
        )
      updatedTableEntityRelation =
        updatedTableEntityRelation.copy(relatingEntities = newRelatingEntities)
    }
    updatedTableEntityRelation
  }

  private def extractReferencesOfRelation(
      relation: BinaryRelation,
      schema: DatabaseSchema
  ): Set[ColumnRelation] = {
    val rightReferences = extractRightReferencesOfRelation(schema, relation)
    val leftReferences = extractLeftReferencesOfRelation(schema, relation)
    rightReferences ++ leftReferences
  }

  private def extractLeftReferencesOfRelation(
      schema: DatabaseSchema,
      relation: BinaryRelation
  ): Set[ColumnRelation] = {
    val leftReferences = schema(relation.rightTable.name).columns.values
      .flatMap(col => {
        col.isSubsetOf = col.isSubsetOf
          .filter(_.table.name.equalsIgnoreCase(relation.leftTable.name))
          .filter(_.isPrimaryKey)
        val columnPairs =
          col.isSubsetOf.map(referencedColumn =>
            ColumnRelation(referencedColumn, col)
          )
        columnPairs
      })
      .toSet
    leftReferences
  }

  private def extractRightReferencesOfRelation(
      schema: DatabaseSchema,
      relation: BinaryRelation
  ): Set[ColumnRelation] = {
    val rightReferences = schema(relation.leftTable.name).columns.values
      .flatMap(col => {
        col.isSubsetOf = col.isSubsetOf
          .filter(_.table.name.equalsIgnoreCase(relation.rightTable.name))
          .filter(_.isPrimaryKey)
        val columnPairs =
          col.isSubsetOf.map(referencedColumn =>
            ColumnRelation(col, referencedColumn)
          )
        columnPairs
      })
      .toSet
    rightReferences
  }
}
