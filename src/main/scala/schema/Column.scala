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

  // determines how a primary key can be determined:
  // strict or lenient
  def isPrimaryKeyCandidate: Boolean = {
    if (cli.strictPrimaryKeyChecking) {
      isStrongPrimaryKey
    } else {
      canBePrimaryKey
    }
  }

  // verify whether up until now all values have been increasing
  // ==> good indicator for a primary key
  def verifyIncreasingValuesOnChange(): Unit = {
    // if values have not been increasing before, we don't need to check again
    if (!areValuesIncreasing) return;

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

  // indicates whether the values indicate a strong primary key and the values are always increasing
  private def isStrongPrimaryKey: Boolean = {
    canBePrimaryKey && areValuesIncreasing && isColumnNamePotentiallyPrimaryKey
  }

  // verify whether the name indicates a good primary key candidate
  private def isColumnNamePotentiallyPrimaryKey = {
    columnName.matches("(?i:.*id)") ||
    columnName.matches("(?i:.*nr)") ||
    columnName.matches("(?i:.*key)") ||
    columnName.matches("(?i:.*no)")

  }

}
