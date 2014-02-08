package sparkgremlin.blueprints.io.build

import java.util.concurrent.BlockingQueue
import com.tinkerpop.blueprints.{Direction, Vertex, Edge}
import sparkgremlin.blueprints.SparkGraph

/**
 * Created by kellrott on 2/8/14.
 */
class EdgeInputPrinter(val edgeId : AnyRef, val outVertex:Vertex, val inVertex:Vertex, queue:BlockingQueue[Option[BuildElement]]) extends InputElement with Edge {
   def getVertex(direction: Direction): Vertex = {
     throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
   }

   def setProperty(key: String, value: scala.Any) = {
     queue.add(Some(new EdgePropertyBuild(edgeId, outVertex.getId, inVertex.getId, key, value)))
   }

   def getLabel: String = ""
   def getId : AnyRef = edgeId;
 }
