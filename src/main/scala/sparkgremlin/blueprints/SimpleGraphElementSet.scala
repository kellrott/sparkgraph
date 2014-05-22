package sparkgremlin.blueprints

import org.apache.spark.rdd.RDD
import com.tinkerpop.blueprints.Element
import org.apache.spark.graphx.{Graph => GraphXGraph, VertexRDD, EdgeRDD}
import scala.reflect.ClassTag

/**
 * Created by kellrott on 2/8/14.
 */
class SimpleGraphElementSet[E <: SparkGraphElement,O <: Element](var inGraph:SparkGraph, inElementClass : Class[E], filter : (E) => Boolean)(implicit m:ClassTag[E]) extends SparkGraphElementSet[O] {

  def elementClass() : Class[_] = inElementClass
  def flushUpdates() : Boolean = inGraph.flushUpdates()

  def graphX() : GraphXGraph[SparkVertex, SparkEdge] = inGraph.graphX()

  def remove() = {}

  override def process(in: Any): O = {
    if (in.isInstanceOf[SparkGraphElement]) {
      val o =in.asInstanceOf[SparkGraphElement]
      o.setGraph(inGraph)
      return o.asInstanceOf[O]
    } else {
      return in.asInstanceOf[O]
    }
  }

  override def getRDD(): RDD[E] = {
    val local_filter = filter //you need to you a local copy, otherwise the whole class gets passed to the closure
    if (inElementClass == classOf[SparkVertex]) {
      return inGraph.graphX().vertices.filter( x => local_filter(x._2.asInstanceOf[E]) ).map(_._2.asInstanceOf[E])
    }
    if (inElementClass == classOf[SparkEdge]) {
      return inGraph.graphX().edges.filter( x => local_filter(x.attr.asInstanceOf[E]) ).map(_.attr.asInstanceOf[E])
    }
    return null
  }

  def selectVertexRDD(): VertexRDD[Boolean] = {
    if (elementClass() != classOf[SparkVertex]) {
      throw new RuntimeException("Selecting Vertices in non-vertex element set")
    }
    val local_filter = filter
    graphX().vertices.mapValues( x => local_filter(x.asInstanceOf[E]) )
  }

  def selectEdgeRDD(): EdgeRDD[Boolean,SparkVertex] = {
    if (elementClass() != classOf[SparkEdge]) {
      throw new RuntimeException("Selecting Edges in non-edge element set")
    }
    val local_filter = filter
    graphX().edges.mapValues( x => local_filter(x.attr.asInstanceOf[E]) )
  }
}
