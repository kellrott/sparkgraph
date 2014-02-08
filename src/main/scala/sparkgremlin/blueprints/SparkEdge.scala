package sparkgremlin.blueprints

import com.tinkerpop.blueprints.{Direction, Edge, Vertex}
import sparkgremlin.blueprints.io.build.{EdgeRemoveBuild, EdgePropertyBuild}

/**
 *
 * @param id
 * @param outVertexId
 * @param inVertexId
 * @param label
 * @param inGraph
 * @param outVertexCache
 * @param inVertexCache
 */
class SparkEdge(
                 override val id:AnyRef,
                 val outVertexId: AnyRef,
                 val inVertexId:AnyRef,
                 val label:String,
                 @transient inGraph:SparkGraph,
                 @transient outVertexCache : Vertex = null,
                 @transient inVertexCache : Vertex = null
                 ) extends SparkGraphElement(id, inGraph) with Edge with Serializable {

  def setGraph(inGraph:SparkGraph) = { graph = inGraph };

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
    super.setProperty(key,value);
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

  override def labelMatch(args:String*) : Boolean = {
    if (args.length == 1)
      return label == args(0)
    return super.labelMatch(args:_*);
  }

  def getLabel: String = label;

}
