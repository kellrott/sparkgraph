package sparkgremlin.gremlin.pipe

import scala.collection.mutable.ArrayBuffer
import sparkgremlin.gremlin.BulkPipeData

/**
 * Created by kellrott on 2/8/14.
 */
class SingleElementBulkPipe[E](data:E) extends BulkPipeData[E] {
   def extract(): Iterator[E] = {
     val o = new ArrayBuffer[E]();
     o += data;
     return o.toIterator
   };
 }
