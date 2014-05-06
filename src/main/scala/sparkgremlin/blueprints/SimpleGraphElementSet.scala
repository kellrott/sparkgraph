package sparkgremlin.blueprints

import org.apache.spark.rdd.RDD
import com.tinkerpop.blueprints.Element
import org.apache.spark.graphx.{Graph => GraphXGraph}

/**
 * Created by kellrott on 2/8/14.
 */
class SimpleGraphElementSet[E <: Element](var inGraph:SparkGraph, var rdd:RDD[E], inElementClass : Class[_]) extends SparkGraphElementSet[E] {

  def elementClass() : Class[_] = inElementClass
  def flushUpdates() : Boolean = inGraph.flushUpdates()
  def elementRDD(): RDD[E] = rdd

  def graphX() : GraphXGraph[SparkVertex, SparkEdge] = inGraph.graphX()

  def remove() = {}

  override def process(in: Any): E = {
    if (in.isInstanceOf[SparkGraphElement]) {
      val o =in.asInstanceOf[SparkGraphElement]
      o.setGraph(inGraph)
      return o.asInstanceOf[E]
    } else {
      return in.asInstanceOf[E]
    }
  }

  override def getRDD(): RDD[E] = rdd
}
