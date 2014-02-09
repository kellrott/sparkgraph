package sparkgremlin.gremlin

import scala.collection.mutable.ArrayBuffer

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
