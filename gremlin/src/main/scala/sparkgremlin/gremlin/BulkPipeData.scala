package sparkgremlin.gremlin

/**
 * Created by kellrott on 2/8/14.
 */
trait BulkPipeData[T] {
   def extract() : Iterator[T]
   def count() : scala.Long
   def dataType() : BulkDataType.Value
 }
