package schema

object SchemaDeriver {

  /**
    * Takes a schema and tables and checks for primary and foreign key relations
    */
  def updateSchemaProperties(
      schema: DatabaseSchema,
      previousSchema: DatabaseSchema,
      table: Table,
      affectedColumnIds: Seq[String]
  ): Unit = {
    // Apply table relation rules

    // Check primary key columns
    affectedColumnIds.foreach(columnId => {
      checkForPrimaryKeyDuplicates(table.columns(columnId))
    })

    // Check foreign key relations
    updateColumnRelations(schema, previousSchema)
  }

  /**
    * Checks whether a column contains duplicate values or not, and determines whether it can be a primary key or not
    */
  private def checkForPrimaryKeyDuplicates(column: Column): Unit = {
    val values = column.values.values.toList
    if (values.size > values.distinct.size) {
      column.canBePrimaryKey = false
    }
  }

  /**
    * Check for every column in a table whether all its values are included
    * in another column in another table
    */
  private def updateColumnRelations(
      schema: DatabaseSchema,
      previousSchema: DatabaseSchema
  ): Unit = {
    if (schema.toList.size > 1) {
      schema.toSeq
        .map(_._2)
        .permutations
        .map(tables => (tables.head, tables.tail))
        .foreach(permutation =>
          determineColumnRelation(permutation, previousSchema)
        )
    }
  }

  /**
    * Determines the relation between a table and all other tables
    */
  private def determineColumnRelation(
      permutation: (Table, Seq[Table]),
      previousSchema: DatabaseSchema
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
          )

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
