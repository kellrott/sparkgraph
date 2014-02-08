package sparkgremlin.blueprints.io.build

import java.util.concurrent.BlockingQueue
import com.tinkerpop.blueprints._
import sparkgremlin.blueprints.SparkGraph
import scala.Some

/**
 * Created by kellrott on 2/8/14.
 */
class GraphInputPrinter(queue:BlockingQueue[Option[BuildElement]]) extends Graph {

   def getFeatures: Features = null;

   def getEdge(id: scala.Any): Edge = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def getVertex(id: scala.Any): Vertex = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def shutdown() = {

   }

   def removeEdge(edge: Edge) {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def removeVertex(vertex: Vertex) {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def addVertex(id: scala.AnyRef): Vertex =  {
     //println("AddingVertex: " + id);
     val out = new VertexInputPrinter(id, queue);
     queue.put(Some(new VertexBuild(id)));
     return out;
   }

   def getVertices(key: String, value: scala.Any): java.lang.Iterable[Vertex] = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def addEdge(id: scala.AnyRef, outVertex: Vertex, inVertex: Vertex, label: String): Edge = {
     //println("AddingEdge: " + id);
     val out = new EdgeInputPrinter(id, outVertex, inVertex, queue);
     queue.put(Some(new EdgeBuild(id, outVertex.getId, inVertex.getId, label)));
     return out;
   }

   def getEdges: java.lang.Iterable[Edge] = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def getEdges(key: String, value: scala.Any): java.lang.Iterable[Edge] = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def query(): GraphQuery = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }

   def getVertices: java.lang.Iterable[Vertex] = {
     throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
   }
 }
