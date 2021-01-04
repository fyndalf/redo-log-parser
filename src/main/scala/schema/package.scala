import scala.collection.mutable

/**
  * This companion object provides additional type definitions for schema-related procedures.
  */
package object schema {
  type DatabaseSchema = mutable.HashMap[String, Table]
  type TableColumns = mutable.HashMap[String, Column]
}
