package schema

import scala.collection.mutable

object SchemaDeriver {

  def updateSchemaProperties(
      schema: DatabaseSchema,
      previousSchema: DatabaseSchema,
      table: Table,
      affectedColumnIds: Seq[String]
  ): Unit = {
    // Add rules
    affectedColumnIds.foreach(columnId => {
      checkForPrimaryKeyDuplicates(table.columns(columnId))
    })
    updateColumnRelations(schema, previousSchema);
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
      schema: DatabaseSchema,
      previousSchema: DatabaseSchema
  ): Unit = {
    if (schema.toList.size > 1) {
      // TODO: Make this algorithm nice
      schema.toSeq
        .map(_._2)
        .permutations
        .map(tables => (tables.head, tables.tail))
        .foreach(permutation =>
          determineColumnRelation(permutation, previousSchema)
        )
    }
  }

  private def determineColumnRelation(
      permutation: (Table, Seq[Table]),
      previousSchema: mutable.HashMap[String, Table]
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

        if (column.isSubsetOf != isSubsetOf) {
          val similarColumns = isSubsetOf.filter(newColumn =>
            column.isSubsetOf.contains(newColumn)
          );

          val newColumns = isSubsetOf
            .filter(targetColumn =>
              // Target column is not included in previous subset of considered column
              !column.isSubsetOf.contains(targetColumn)
            )
            .filter(targetColumn =>
              // Table of target column did not exist in the previous step
              !previousSchema.contains(targetColumn.table.name) ||
                // Table of considered column did not exist in the previous step
                !previousSchema.contains(table.name) ||
                // Target column did not exist in previous step
                !previousSchema(targetColumn.table.name).columns
                  .contains(targetColumn.name) ||
                // Considered column did not exist in previous step
                !previousSchema(table.name).columns
                  .contains(column.name)
            )

          column.isSubsetOf = similarColumns ++ newColumns
        }
      })
  }
}
