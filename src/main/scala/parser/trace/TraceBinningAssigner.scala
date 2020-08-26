package parser.trace

import parser.{RowWithBucketIdentifier, TableEntityRelation}
import schema.Table

object TraceBinningAssigner {

  /**
   * Determines the trace bucket for each row (i.e., entity) of the redo log
   * @param currentEntity The currently considered entity
   * @param path All entities that have already been considered
   * @param tableEntityRelations All relating entities that have been discovered
   * @param relevantRows All relevant rows and bucket identifiers
   * @return All rows and their bucket identifiers
   */
  def determineRowBuckets(
      currentEntity: Table,
      path: Set[Table],
      tableEntityRelations: Set[TableEntityRelation],
      relevantRows: Seq[RowWithBucketIdentifier]
  ): Set[RowWithBucketIdentifier] = {
    val leftEntityRelations = tableEntityRelations.filter(tableEntityRelation =>
      tableEntityRelation.leftTable.name.equals(currentEntity.name) && !path
        .contains(
          tableEntityRelation.rightTable
        )
    )

    val rightEntityRelations =
      tableEntityRelations.filter(tableEntityRelation =>
        tableEntityRelation.rightTable.name.equals(currentEntity.name) && !path
          .contains(
            tableEntityRelation.leftTable
          )
      )

    val leftResult = leftEntityRelations.flatMap(tableEntityRelation => {
      val newPath = path + tableEntityRelation.leftTable
      val newIntermediateRowIDs =
        relevantRows.filter(rr =>
          tableEntityRelation.relatingEntities
            .map(_.leftEntityID)
            .contains(rr.rowID)
        )
      val newRelevantRowIDs = determineRelevantRowIDsForLeftResult(
        newIntermediateRowIDs,
        tableEntityRelation
      )
      determineRowBuckets(
        tableEntityRelation.rightTable,
        newPath,
        tableEntityRelations,
        newRelevantRowIDs
      )
    })

    val rightResult = rightEntityRelations.flatMap(tableEntityRelation => {
      val newPath = path + tableEntityRelation.rightTable
      val newIntermediateRowIDs =
        relevantRows.filter(rr =>
          tableEntityRelation.relatingEntities
            .map(_.rightEntityID)
            .contains(rr.rowID)
        )
      val newRelevantRowIDs = determineRelevantRowIDsForRightResult(
        newIntermediateRowIDs,
        tableEntityRelation
      )
      determineRowBuckets(
        tableEntityRelation.leftTable,
        newPath,
        tableEntityRelations,
        newRelevantRowIDs
      )
    })

    leftResult ++ relevantRows ++ rightResult
  }

  /**
   * Determines all relevant row ids for the left-hand side of the relation
   */
  private def determineRelevantRowIDsForLeftResult(
      newIntermediateRows: Seq[RowWithBucketIdentifier],
      tableEntityRelation: TableEntityRelation
  ): Seq[RowWithBucketIdentifier] = {
    newIntermediateRows.flatMap(rowWithBucket => {
      val newRowIDs =
        tableEntityRelation.relatingEntities.filter(entityRelation =>
          entityRelation.leftEntityID.equals(rowWithBucket.rowID)
        )
      newRowIDs.map(newRowID => {
        RowWithBucketIdentifier(
          tableEntityRelation.rightTable.name,
          newRowID.rightEntityID,
          rowWithBucket.bucketID
        )
      })
    })
  }

  /**
   * Determines all relevant row ids for the right-hand side of the relation
   */
  private def determineRelevantRowIDsForRightResult(
      newIntermediateRows: Seq[RowWithBucketIdentifier],
      tableEntityRelation: TableEntityRelation
  ): Seq[RowWithBucketIdentifier] = {
    newIntermediateRows.flatMap(rowWithBucket => {
      val newRowIDs =
        tableEntityRelation.relatingEntities.filter(entityRelation =>
          entityRelation.rightEntityID.equals(rowWithBucket.rowID)
        )
      newRowIDs.map(newRowID => {
        RowWithBucketIdentifier(
          tableEntityRelation.leftTable.name,
          newRowID.leftEntityID,
          rowWithBucket.bucketID
        )
      })
    })
  }
}
