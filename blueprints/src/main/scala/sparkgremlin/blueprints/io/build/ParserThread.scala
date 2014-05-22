package sparkgremlin.blueprints.io.build

import java.io.InputStream
import java.util.concurrent.BlockingQueue
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader

/**
 * Created by kellrott on 2/8/14.
 */
class ParserThread(is:InputStream, queue:BlockingQueue[Option[BuildElement]]) extends Runnable {
   def run() {
     try {
       val inputGraph = new GraphInputPrinter(queue);
       val reader = new GraphMLReader(inputGraph);
       reader.inputGraph(is);
     } catch {
       case e: Throwable => {println("Exception" + e)}
     } finally {
       while (queue.remainingCapacity() <= 0) {
         Thread.sleep(1)
       }
       queue.add(None)
     }
   }
 }
