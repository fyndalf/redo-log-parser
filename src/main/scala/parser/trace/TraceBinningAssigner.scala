package parser.trace

import parser.{RowWithBucketIdentifier, TableEntityRelation}
import schema.Table

object TraceBinningAssigner {
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
