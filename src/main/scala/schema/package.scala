import scala.collection.mutable

package object schema {
  type DatabaseSchema = mutable.HashMap[String, Table]
  type TableColumns = mutable.HashMap[String, Column]
}
