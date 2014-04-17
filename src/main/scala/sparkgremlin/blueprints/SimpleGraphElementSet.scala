package sparkgremlin.blueprints

import org.apache.spark.rdd.RDD
import com.tinkerpop.blueprints.Element
import org.apache.spark.graphx.{Graph => GraphXGraph}

/**
 * Created by kellrott on 2/8/14.
 */
class SimpleGraphElementSet[E <: Element](var inGraph:SparkGraph, var rdd:RDD[E], inElementClass : Class[_]) extends SparkGraphElementSet[E] {

  def elementClass() : Class[_] = inElementClass;
  def flushUpdates() : Boolean = inGraph.flushUpdates();
  def elementRDD(): RDD[E] = rdd;

  def graphX() : GraphXGraph[SparkVertex, SparkEdge] = inGraph.graphX()

  def remove() = {};

  override def process(in: E): E = in

  override def getRDD(): RDD[E] = rdd
}
