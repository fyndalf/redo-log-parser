package schema

import scala.collection.mutable

object SchemaDeriver {

  def updateSchemaProperties(
      schema: mutable.HashMap[String, Table],
      table: Table,
      affectedColumnIds: Seq[String]
  ): Unit = {
    // Add rules
    affectedColumnIds.foreach(columnId => {
      checkForPrimaryKeyDuplicates(table.columns(columnId))
    })
    updateColumnRelations(schema)
  }

  private def checkForPrimaryKeyDuplicates(column: Column): Unit = {
    val values = column.values.values.toList
    if (values.size > values.distinct.size) {
      column.isPrimaryKey = false
    }
  }

  /**
    * Check for every column in a table whether all its values are included
    * in another column in another table
    *
    * @param schema
    */
  private def updateColumnRelations(
      schema: mutable.HashMap[String, Table]
  ): Unit = {
    if (schema.toList.size > 1) {
      // TODO: Make this algorithm nice
      schema.toSeq
        .map(_._2)
        .permutations
        .map(tables => (tables.head, tables.tail))
        .foreach(determineColumnRelation)
    }
  }

  private def determineColumnRelation(
      permutation: (Table, Seq[Table])
  ): Unit = {
    val (table, otherTables) = permutation
    table.columns.values
      .foreach(column => {
        var isSubsetOf = Seq[Column]()
        val distinctValues = column.values.values.toList.distinct
        otherTables.foreach(otherTable => {
          otherTable.columns.values
            .foreach(otherColumn => {
              val otherDistinctValues =
                otherColumn.values.values.toList.distinct
              if (distinctValues.forall(otherDistinctValues.contains)) {
                isSubsetOf = isSubsetOf :+ otherColumn
              }
            })
        })
        column.isSubsetOf = isSubsetOf
      })
  }
}
