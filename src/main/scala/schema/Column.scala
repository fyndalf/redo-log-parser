package schema

import scala.collection.mutable

class Column(
    columnName: String,
    columnTable: Table,
    columnIsPrimaryKey: Boolean,
    areColumnValuesIncreasing: Boolean,
    columnIsSubsetOf: Seq[Column],
    columnValues: mutable.HashMap[String, String] // ROWID -> VALUE
) {
  val name: String = columnName
  val table: Table = columnTable
  var canBePrimaryKey: Boolean = columnIsPrimaryKey
  var areValuesIncreasing: Boolean = areColumnValuesIncreasing

  var isSubsetOf: Seq[Column] = columnIsSubsetOf
  val values: mutable.HashMap[String, String] = columnValues

  override def clone(): Column = {
    new Column(
      name,
      table,
      canBePrimaryKey,
      areValuesIncreasing,
      isSubsetOf,
      values.clone()
    )
  }

  override def toString: String = {
    val primaryKey = if (isPrimaryKeyCandidate) {
      " (PRIMARY KEY)"
    } else {
      ""
    }
    val foreignKeyTargets = isSubsetOf.map(column => {
      column.table.name + "." + column.name
    })
    val foreignKeyString = foreignKeyTargets.size match {
      case 0 => ""
      case _ => "FK CANDIDATE FOR: " + foreignKeyTargets.mkString(" AND ")
    }
    s"$name$primaryKey $foreignKeyString"
  }

  /**
    * Determines, depending on the required strength of the primary key check,
    * whether this column is a primary key or a strong primary key candidate.
    * @return Is this column a primary key candidate, depending on the required strength of the check.
    */
  def isPrimaryKeyCandidate: Boolean = {
    if (cli.strictPrimaryKeyChecking) {
      isStrongPrimaryKey
    } else {
      canBePrimaryKey
    }
  }

  /**
    * Checks whether a column contains duplicate values or not.
    * Duplicate values make it impossible for a column to be a primary key column.
    */
  def verifyValueUniqueness(): Unit = {
    val columnValues = values.values.toList
    if (columnValues.size > columnValues.distinct.size) {
      canBePrimaryKey = false
    }
  }

  /**
    * Verifies whether up until now all values of the column have been increasing.
    * If they have not been monotonous at least once, this check will always return false,
    * ensuring that even for deletions the history of the column's values is retained in this check.
    *
    * This is a good indicator for a primary key column.
    *
    * Calling this method on every value change of the column ensures that this property is verified correctly.
    */
  def verifyIncreasingValuesOnChange(): Unit = {
    // if values have not been increasing before, we don't need to check again
    if (!areValuesIncreasing) return

    val valuesAreMonotonous = values.values.toSeq.sliding(2).forall {
      case x :: y :: _ => x < y
      case _           => true
    }
    if (valuesAreMonotonous && areValuesIncreasing) {
      areValuesIncreasing = true
    } else {
      areValuesIncreasing = false
    }
  }

  /**
    * Indicates whether the values of the column indicate a strong primary key,
    * and the values have always been increasing over the time of the redo log.
    * @return Is this column a strong primary key candidate?
    */
  private def isStrongPrimaryKey: Boolean = {
    canBePrimaryKey && areValuesIncreasing && isColumnNamePotentiallyPrimaryKey
  }

  /**
    * Returns whether the column name potentially indicates a good primary key column based on it's name.
    * Suffixes that are a good fit are -id, -nr, -key, -no, without regard to the actual case.
    */
  private def isColumnNamePotentiallyPrimaryKey = {
    columnName.matches("(?i:.*id)") ||
    columnName.matches("(?i:.*nr)") ||
    columnName.matches("(?i:.*key)") ||
    columnName.matches("(?i:.*no)")

  }

}
