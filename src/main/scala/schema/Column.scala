package schema

import scala.collection.mutable

/**
  * Represents a column of a table, and provides various methods for determining whether
  * this column is a good primary key as observed over the time of the log. The value verification needs to
  * be done every time values are inserted or updated for ensuring a correct strong primary key check.
  * @param columnName The name of the column
  * @param columnTable The table this column belongs to
  * @param columnCanBePrimaryKey Indicates whether up until now, this column can be a primary key based on it's values
  * @param areColumnValuesIncreasing Indicates whether all values inserted or updated have been increasing over time
  * @param columnIsSubsetOf Provides a list of columns which this column appears to be a subset of
  * @param columnValues Contains a map of Row IDs to values, storing the current values of the column
  */
class Column(
    columnName: String,
    columnTable: Table,
    columnCanBePrimaryKey: Boolean,
    areColumnValuesIncreasing: Boolean,
    columnIsSubsetOf: Seq[Column],
    columnValues: mutable.HashMap[String, String]
) {
  val name: String = columnName
  val table: Table = columnTable
  var canBePrimaryKey: Boolean = columnCanBePrimaryKey
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
    val primaryKey =
      if (isPrimaryKeyCandidate && cli.strictPrimaryKeyChecking) {
        " (STRONG PRIMARY KEY)"
      } else if (isPrimaryKeyCandidate) {
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

    var valuesAreMonotonous = false

    // try to interpret column values as doubles and assert that they only ever increase
    // if this fails, we can still interpret them as strings and assert a certain ordering,
    // which can't deal with numbers-only strings all to well, but works for any other string.
    try {
      valuesAreMonotonous = values
        .map(v => (v._1.toDouble, v._2.toDouble))
        .values
        .toSeq
        .sliding(2)
        .forall {
          case x :: y :: _ => x < y
          case _           => true
        }
    } catch {
      case _: NumberFormatException =>
        // compare strings with comparator, and require x to be before y
        valuesAreMonotonous = values.values.toSeq
          .sliding(2)
          .forall {
            case x :: y :: _ => x.compareToIgnoreCase(y) < 0
            case _           => true
          }
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
