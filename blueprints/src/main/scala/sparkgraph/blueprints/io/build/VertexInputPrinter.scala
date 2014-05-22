package sparkgraph.blueprints.io.build

import java.util.concurrent.BlockingQueue
import com.tinkerpop.blueprints.{VertexQuery, Direction, Edge, Vertex}
import sparkgraph.blueprints.SparkGraph

/**
 * Created by kellrott on 2/8/14.
 */
class VertexInputPrinter(val id:Long, queue:BlockingQueue[Option[BuildElement]]) extends InputElement with Vertex {

   def getId : AnyRef = id.asInstanceOf[AnyRef];

   def addEdge(label: String, inVertex: Vertex): Edge = {
     //println("Vertex Add Edge: " + label + " " + vertex);
     /*
     val out = new EdgeInputPrinter(this, inVertex, queue);
     queue.add(Some(new EdgeBuild(id, inVertex.getId, label)));
     return out;
     */
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def setProperty(key: String, value: scala.Any) = {
     queue.add(Some(new VertexPropertyBuild(id, key, value)));
   }
   def getVertices(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Vertex] = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def query(): VertexQuery = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def getEdges(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Edge] = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

 }
