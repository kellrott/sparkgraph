package sparkgraph.gremlin.pipe

import scala.collection.mutable.ArrayBuffer
import sparkgraph.gremlin.{BulkDataType, BulkPipeData}

/**
 * Created by kellrott on 2/8/14.
 */
class SingleElementBulkPipe[E](data:E, dtype : BulkDataType.Value) extends BulkPipeData[E] {
   def extract(): Iterator[E] = {
     val o = new ArrayBuffer[E]();
     o += data;
     return o.toIterator
   };

   def count() : Long = 1L

  override def dataType(): BulkDataType.Value = dtype
}
