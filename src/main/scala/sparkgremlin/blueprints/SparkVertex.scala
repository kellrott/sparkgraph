package sparkgremlin.blueprints

import collection.JavaConverters._

import org.apache.spark.SparkContext._
import com.tinkerpop.blueprints.{Direction, VertexQuery, Edge, Vertex}
import scala.collection.mutable.ArrayBuffer
import sparkgremlin.blueprints.io.build.{EdgeBuild, VertexRemoveBuild, VertexPropertyBuild}
import scala.util.Random

/**
 *
 * @param id
 * @param inGraph
 */
class SparkVertex(override val id:Long, @transient inGraph:SparkGraph) extends SparkGraphElementBase(id, inGraph) with Vertex with Serializable {
  val edgeSet = new ArrayBuffer[SparkEdge]();

  override def setProperty(key:String, value:AnyRef) = {
    if (key == null || key.length == 0 || key == "id") {
      throw new IllegalArgumentException("Bad Property Key");
    }
    super.setProperty(key,value);
    if (graph != null) {
      graph.updates += new VertexPropertyBuild(id, key, value);
    }
  }

  override def equals(other: Any) = other match {
    case that: SparkVertex => (this.id == that.id)
    case _ => false
  }

  override def hashCode() = id.hashCode

  def remove() = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    graph.updates += new VertexRemoveBuild(id);
  }

  def addEdge(label: String, inVertex: Vertex): Edge = {
    if (label == null) {
      throw new IllegalArgumentException("Cannot add unlabeled edge");
    }
    val edgeId = new java.lang.Long(Random.nextLong());
    if (graph != null) {
      graph.updates += new EdgeBuild(edgeId, id, inVertex.getId.asInstanceOf[Long], label);
    }
    val e = new SparkEdge(edgeId, id, inVertex.getId.asInstanceOf[Long], label, graph, this, inVertex);
    edgeSet += e;
    return e;
  }

  def query(): VertexQuery = {
    return new SparkVertexQuery(this, graph);
  }

  def getVertices(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Vertex] = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    val idSet = new ArrayBuffer[Long]();
    if ( direction == Direction.IN || direction == Direction.BOTH ) {
      var incoming = graph.graph.edges.filter( _.dstId == id ).map( _.attr );
      if (labels.length > 0) {
        incoming = incoming.filter( _.labelMatch(labels:_*) );
      }
      idSet ++= incoming.map( _.outVertexId ).collect();
    }
    if ( direction == Direction.OUT || direction == Direction.BOTH ) {
      var outgoing = graph.graph.edges.filter( _.srcId == id ).map( _.attr );
      if (labels.length > 0) {
        outgoing = outgoing.filter(_.labelMatch(labels:_*));
      }
      idSet ++= outgoing.map( _.inVertexId ).collect()
    }
    val verts = graph.graph.vertices.filter( x => idSet.contains(x._1)  ).collect()
    val out = idSet.flatMap( x => verts.filter( y => x == y._1) );
    return out.map( x => { val y = x._2; y.graph = graph; y.asInstanceOf[Vertex]; } ).toIterable.asJava;
  }


  def getEdges(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Edge] = {
    if (graph != null) {
      graph.flushUpdates();
    }

    val out = new ArrayBuffer[SparkEdge]();
    if (direction == Direction.OUT || direction == Direction.BOTH) {
      if (graph != null) {
        var outgoing = graph.graph.edges.filter( x => x.srcId == id ).map( _.attr );
        if (labels.length > 0) {
          outgoing = outgoing.filter( x=>labels.contains(x.label) );
        }
        out ++= outgoing.collect();
      } else {
        if (labels.length > 0) {
          out ++= edgeSet.filter( x => labels.contains(x.label) && x.outVertexId == id )
        } else {
          out ++= edgeSet.filter( x => x.outVertexId == id )
        }
      }
    }
    if (direction == Direction.IN || direction == Direction.BOTH) {
      if (graph == null) {
        throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
      }
      var incoming = graph.graph.edges.filter( x => x.dstId == id ).map( _.attr )
      if (labels.length > 0) {
        incoming = incoming.filter( x=>labels.contains(x.label) );
      }
      out ++= incoming.collect();
    }
    return out.map( x => { x.graph = graph; x.asInstanceOf[Edge]; } ).toIterable.asJava;
  }
}
