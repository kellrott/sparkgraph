package sparkgremlin.blueprints

import collection.JavaConverters._

import com.tinkerpop.blueprints.{Direction, Edge, Vertex}
import sparkgremlin.blueprints.io.build.{EdgeRemoveBuild, EdgePropertyBuild}
import scala.collection.mutable.HashMap
import org.apache.spark.graphx.{Edge => GraphXEdge}

/**
 *
 * @param id
 * @param outVertexId
 * @param inVertexId
 * @param label
 * @param outVertexCache
 * @param inVertexCache
 */
class SparkEdge(
                 val id:Long,
                 val outVertexId: Long,
                 val inVertexId: Long,
                 val label:String,
                 @transient var graph:SparkGraph,
                 @transient outVertexCache : Vertex = null,
                 @transient inVertexCache : Vertex = null
                 ) extends GraphXEdge[HashMap[String,Any]](outVertexId, inVertexId, new HashMap[String,Any]()) with SparkGraphElement with Edge with Serializable {

  def getId() : AnyRef = id.asInstanceOf[AnyRef];

  def setGraph(inGraph:SparkGraph) = { graph = inGraph };
  def getGraph() = graph;

  override def equals(other: Any) = other match {
    case that: SparkEdge => (this.id == that.id)
    case that: Edge => (this.id == that.getId)
    case _ => false
  }

  override def hashCode() = id.hashCode

  override def setProperty(key:String, value:AnyRef) = {
    if (key == "id" || key == "label") {
      throw new IllegalArgumentException("Invalid Key String");
    }
    if (key == null || key.length == 0) {
      throw new IllegalArgumentException("Invalid Key String");
    }
    if (value == null) {
      throw new IllegalArgumentException("Invalid Property Value");
    }
    attr(key) = value;
    if (graph != null) {
      graph.updates += new EdgePropertyBuild(id, outVertexId, inVertexId, key, value);
    }
  }

  def remove() = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    graph.updates += new EdgeRemoveBuild(id, outVertexId);
  }

  /**
   *
   * @param direction
   * @return
   */
  def getVertex(direction: Direction): Vertex = {
    if (graph == null) {
      if (direction == Direction.IN) {
        if (inVertexCache == null) {
          return new SparkVertex(inVertexId, null);
        } else {
          return inVertexCache
        }
      } else if (direction == Direction.OUT) {
        if (outVertexCache == null) {
          return new SparkVertex(outVertexId, null);
        } else {
          return outVertexCache
        }
      }
      throw new IllegalArgumentException("Bad Edge Direction")
    } else {
      if (direction == Direction.IN) {
        return graph.getVertex(inVertexId);
      } else if (direction == Direction.OUT) {
        return graph.getVertex(outVertexId);
      }
      throw new IllegalArgumentException("Bad Edge Direction")
    }
  }

  def labelMatch(args:String*) : Boolean = {
    if (args.length == 1)
      return label == args(0)
    if (args.length == 0) {
      return true;
    }
    if (args.length == 2) {
      return attr.getOrElse(args(0), null) == args(1);
    }
    return false;
  }

  def getLabel: String = label;

  def getProperty[T](key: String): T = {
    attr.get(key).getOrElse(null).asInstanceOf[T];
  }

  def getPropertyKeys: java.util.Set[String] = attr.keySet.asJava;

  def removeProperty[T](key: String): T = {
    return attr.remove(key).orNull.asInstanceOf[T];
  }

}
