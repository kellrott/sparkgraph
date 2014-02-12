package sparkgremlin.blueprints

import collection.JavaConverters._
import com.tinkerpop.blueprints._
import java.lang.Iterable
import scala.Iterable

/**
 * Created by kellrott on 2/8/14.
 */
object SparkGraphQuery {
  def containCheck(value:Any, predicate : Predicate, valueSet:AnyRef) : Boolean = {
    val out = valueSet match {
      case _ : List[_] => {
        valueSet.asInstanceOf[List[String]].contains(value)
      }
      case _ : java.util.List[_] => {
        valueSet.asInstanceOf[java.util.List[String]].asScala.contains(value)
      }
      case _ =>  {
        throw new IllegalArgumentException( "Missing Comparison: " + valueSet.getClass  )
      }
    }
    if (predicate == Contains.NOT_IN)
      return !out;
    return out;
  }
}


class SparkGraphQuery(val graph:SparkGraph) extends BaseQuery with GraphQuery {

  override def has(key: String): GraphQuery = {
    super.has(key);
    return this;
  }

  override def has(key: String, value: scala.AnyRef): GraphQuery = {
    super.has(key,value);
    return this;
  }

  override def has(key: String, predicate: Predicate, value: scala.AnyRef): GraphQuery = {
    super.has(key,predicate,value);
    return this;
  }

  override def has[T <: Comparable[T]](key: String, value: T, compare: Query.Compare): GraphQuery = {
    super.has(key, compare, value.asInstanceOf[AnyRef]);
    return this;
  }

  override def limit(count: Int): GraphQuery = {
    super.limit(count)
    return this;
  }

  override def hasNot(key: String, value: scala.AnyRef): GraphQuery = {
    super.hasNot(key, value)
    return this;
  }

  override def interval[T <: Comparable[_]](key: String, startValue: T, endValue: T): GraphQuery = {
    super.interval(key, startValue, endValue);
    return this;
  }

  override def hasNot(key: String): GraphQuery = {
    super.hasNot(key);
    return this;
  }

  def edges(): java.lang.Iterable[Edge] = {
    graph.flushUpdates();
    //hasContainers.foreach( println );
    var rdd = graph.graphX().edges.map( _.asInstanceOf[SparkEdge] )
    for ( has <- hasContainers ) {
      rdd = has.predicate match {
        case Compare.EQUAL => {
          /*
          has.value match {
            case null => rdd.filter( !_.propMap.contains(has.key) );
            case _ => rdd.filter( _.propMap.getOrElse(has.key, null) == has.value )
          }   */
          rdd.filter( _.getProperty(has.key) == has.value )
        }
        case Compare.NOT_EQUAL => {
          /*
          has.value match {
            case null => rdd.filter( _.propMap.contains(has.key));
            case _ => rdd.filter( _.propMap.getOrElse(has.key, null) != has.value);
          }
          */
          rdd.filter( _.getProperty(has.key) != has.value)
        }
        case Contains.IN => {
          println("Container:" + has.value)
          rdd.filter( x => SparkGraphQuery.containCheck(x.getProperty(has.key).asInstanceOf[String], has.predicate, has.value) )
        }
        case Compare.GREATER_THAN_EQUAL | Compare.GREATER_THAN  | Compare.LESS_THAN | Compare.LESS_THAN_EQUAL  => {
          rdd.filter( x => has.predicate.evaluate(x.getProperty(has.key), has.value) )
        }
        case _ => {
          throw new IllegalArgumentException( "Missing Comparison: " + has.predicate); // + " " + has.value.getClass  )
        }
      }
    }
    return rdd.collect().slice(0, limit).map( x => { x.graph = graph; x.asInstanceOf[Edge]} ).toIterable.asJava;
  }

  def vertices(): java.lang.Iterable[Vertex] = {
    graph.flushUpdates();
    //hasContainers.foreach( println );
    var rdd = graph.graphX().vertices.map(_._2);
    for ( has <- hasContainers ) {
      rdd = has.predicate match {
        case Compare.EQUAL => {
          /*
          has.value match {
            case null => rdd.filter( !_.propMap.contains(has.key) );
            case _ => rdd.filter( _.propMap.getOrElse(has.key, null) == has.value )
          } */
          rdd.filter( _.getProperty(has.key) == has.value )
        }
        case Compare.NOT_EQUAL => {
          /*
          has.value match {
            case null => rdd.filter( _.propMap.contains(has.key));
            case _ => rdd.filter( _.propMap.getOrElse(has.key, null) != has.value);
          }
          */
          rdd.filter( _.getProperty(has.key) != has.value )
        }
        case Contains.IN | Contains.NOT_IN => {
          rdd.filter( x => SparkGraphQuery.containCheck(x.getProperty(has.key).asInstanceOf[String], has.predicate, has.value) )
        }
        case Compare.GREATER_THAN | Compare.GREATER_THAN_EQUAL| Compare.LESS_THAN | Compare.LESS_THAN_EQUAL  => {
          rdd.filter( x => has.predicate.evaluate(x.getProperty(has.key), has.value) )
        }
        case _ => {
          throw new IllegalArgumentException( "Missing Comparison: " + has.predicate); // + " " + has.value.getClass  )
        }
      }
    }
    return rdd.collect().slice(0, limit).map( x => { x.graph = graph; x.asInstanceOf[Vertex]} ).toIterable.asJava;
  }
}