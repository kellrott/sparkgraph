package sparkgraph.blueprints.io.build

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue

/**
 * Created by kellrott on 2/8/14.
 */
class GraphMLIterator(is: InputStream) extends Iterator[BuildElement] {

   val queue = new LinkedBlockingQueue[Option[BuildElement]](100);

   val reader = new Thread(new ParserThread(is, queue));
   reader.start();

   var done = false;
   var current : BuildElement = null;

   def hasNext() : Boolean = {
     if (done) return false;
     if (current == null) {
       val n = queue.take();
       if (n.isDefined)
         current = n.get;
       else
         current = null;
     }
     if (current == null) {
       done = true;
     }
     return !done;
   }

   def next() : BuildElement = {
     val o = current;
     current = null;
     return o;
   }

 }
