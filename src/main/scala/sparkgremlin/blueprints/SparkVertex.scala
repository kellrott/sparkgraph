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
class SparkVertex(override val id:AnyRef, @transient inGraph:SparkGraph) extends SparkGraphElementBase(id, inGraph) with Vertex with Serializable {
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
      graph.updates += new EdgeBuild(edgeId, id, inVertex.getId, label);
    }
    val e = new SparkEdge(edgeId, id, inVertex.getId, label, graph, this, inVertex);
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
    val idSet = new ArrayBuffer[AnyRef]();
    if ( direction == Direction.IN || direction == Direction.BOTH ) {
      var incoming = graph.curgraph.flatMap( x => x._2.edgeSet.filter( _.inVertexId == id ) );
      if (labels.length > 0) {
        incoming = incoming.filter( _.labelMatch(labels:_*) );
      }
      idSet ++= incoming.map( _.outVertexId ).collect();
    }
    if ( direction == Direction.OUT || direction == Direction.BOTH ) {
      var outgoing = graph.curgraph.lookup(id).head.edgeSet;
      if (labels.length > 0) {
        outgoing = outgoing.filter(_.labelMatch(labels:_*));
      }
      idSet ++= outgoing.map( _.inVertexId )
    }
    val verts = graph.curgraph.filter( x => idSet.contains(x._1)  ).collect()
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
        var outgoing = graph.curgraph.filter( x => x._1 == id ).flatMap( x => x._2.edgeSet );
        if (labels.length > 0) {
          outgoing = outgoing.filter( x=>labels.contains(x.label) );
        }
        out ++= outgoing.collect();
      } else {
        if (labels.length > 0) {
          out ++= edgeSet.filter( x => labels.contains(x.label) )
        } else {
          out ++= edgeSet
        }
      }
    }
    if (direction == Direction.IN || direction == Direction.BOTH) {
      if (graph == null) {
        throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
      }
      var incoming = graph.curgraph.flatMap( x => x._2.edgeSet.filter(x => x.inVertexId == id ))
      if (labels.length > 0) {
        incoming = incoming.filter( x=>labels.contains(x.label) );
      }
      out ++= incoming.collect();
    }
    return out.map( x => { x.graph = graph; x.asInstanceOf[Edge]; } ).toIterable.asJava;
  }
}
