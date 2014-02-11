package sparkgremlin.blueprints

import collection.JavaConverters._
import org.apache.spark.SparkContext._
import com.tinkerpop.blueprints._
import java.lang.Iterable
import scala.Iterable

/**
 * Created by kellrott on 2/8/14.
 */
object SparkVertexQuery {
  def idCmp(left:AnyRef, right:AnyRef) : Boolean = {
    return left==right;
  }
}



class SparkVertexQuery(val vertex:SparkVertex, val graph:SparkGraph)  extends BaseQuery with VertexQuery {

  def direction(direction: Direction): VertexQuery = {
    directionValue = direction;
    return this;
  }

  def labels(inLabels:String*): VertexQuery = {
    labelSet = inLabels.toArray;
    return this;
  }

  def count(): Long = {
    var i = 0L;
    for ( c <- edges().asScala ) {
      i += 1;
    }
    return i;
  }

  def vertexIds(): AnyRef = {
    throw new IllegalArgumentException("Null ID value");
  }

  override def has(key: String): VertexQuery = {
    super.has(key);
    return this;
  }

  override def hasNot(key: String): VertexQuery = {
    super.hasNot(key);
    return this;
  }

  override def has(key: String, value: scala.AnyRef): VertexQuery = {
    super.has(key, value);
    return this;
  }

  override def hasNot(key: String, value: scala.AnyRef): VertexQuery = {
    super.hasNot(key, value);
    return this;
  }

  override def has(key: String, predicate: Predicate, value: scala.AnyRef): VertexQuery = {
    super.has(key, predicate, value);
    return this;
  }

  override def has[T <: Comparable[T]](key: String, value: T, compare: Query.Compare): VertexQuery = {
    super.has(key, value, compare);
    return this;
  }

  override def interval[T <: Comparable[_]](key: String, startValue: T, endValue: T): VertexQuery = {
    super.interval(key, startValue, endValue);
    return this;
  }

  override def limit(limit: Int): VertexQuery = {
    super.limit(limit);
    return this;
  }

  def edges(): java.lang.Iterable[Edge] = {
    graph.flushUpdates();
    var outEdges = if (directionValue == Direction.OUT || directionValue == Direction.BOTH) {
      val nodes = graph.curgraph.lookup(vertex.id);
      if (nodes.length == 0) {
        Array[SparkEdge]();
      } else {
        nodes.head.edgeSet.toArray
      }
    } else {
      Array[SparkEdge]();
    }

    val vert_id = vertex.id;
    val inEdges = if (directionValue == Direction.IN || directionValue == Direction.BOTH) {
      graph.curgraph.flatMap( x => x._2.edgeSet.filter( y => y.inVertexId == vert_id ) ).collect();
    } else {
      Array[SparkEdge]();
    }

    var edgeSet = outEdges ++ inEdges;
    if (labelSet.length > 0) {
      edgeSet = edgeSet.filter( x => labelSet.contains(x.label) );
    }
    for ( has <- hasContainers ) {
      edgeSet = has.predicate match {
        case Compare.EQUAL => {
          has.value match {
            case _ => edgeSet.filter( _.getProperty(has.key) == has.value )
          }
        }
        case Compare.NOT_EQUAL => {
          has.value match {
            case _ => edgeSet.filter( _.getProperty(has.key) != has.value);
          }
        }
        case Compare.GREATER_THAN | Compare.GREATER_THAN_EQUAL | Compare.LESS_THAN | Compare.LESS_THAN_EQUAL  => {
          edgeSet.filter( x => has.predicate.evaluate(x.getProperty(has.key), has.value) )
        }
        case Contains.IN | Contains.NOT_IN => {
          edgeSet.filter( x => SparkGraphQuery.containCheck(x.getProperty(has.key), has.predicate, has.value) )
        }
        case _ => {
          throw new IllegalArgumentException( "Missing Comparison: " + has.predicate); // + " " + has.value.getClass  )
        }
      }
    }
    return edgeSet.slice(0, limit).map( x => { x.graph = graph; x.asInstanceOf[Edge]} ).toIterable.asJava;
  }
  def vertices(): java.lang.Iterable[Vertex] = {
    var edgeSet = edges().asScala;
    var nodeIds = edgeSet.map( _.asInstanceOf[SparkEdge] ).map( x => {
      if ( x.inVertexId == vertex.id ) {
        (x.outVertexId, true)
      } else {
        (x.inVertexId, true)
      }
    } ).toSeq;
    val out = graph.curgraph.context.parallelize(nodeIds).join( graph.curgraph ).map( x => (x._2._2 ) ).collect();
    return out.map( x => {x.graph = graph; x.asInstanceOf[Vertex]} ).toIterable.asJava;
  }

}
