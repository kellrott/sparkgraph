package sparkgremlin.blueprints

import org.apache.spark.rdd.RDD
import com.tinkerpop.blueprints.Element

/**
 * Created by kellrott on 2/8/14.
 */
class SimpleGraphElementSet[E <: Element](var inGraph:SparkGraph, var rdd:RDD[E], inElementClass : Class[_]) extends SparkGraphElementSet[E] {

  def elementClass() : Class[_] = inElementClass;
  def flushUpdates() : Boolean = inGraph.flushUpdates();
  def elementRDD(): RDD[E] = rdd;
  def graphRDD(): RDD[(AnyRef,SparkVertex)] = {
    flushUpdates();
    inGraph.curgraph
  };

  var rddCollect : Array[E] = null;
  var rddCollectIndex = 0;

  def hasNext: Boolean = {
    if (rddCollect == null) {
      rddCollect = rdd.collect();
      rddCollectIndex = 0;
    }
    return rddCollectIndex < rddCollect.length;
  }

  def next(): E = {
    if (!hasNext) {
      return null.asInstanceOf[E];
    }
    if (rddCollect != null) {
      val out = rddCollect(rddCollectIndex).asInstanceOf[SparkGraphElement];
      out.graph = inGraph
      rddCollectIndex += 1
      out.asInstanceOf[E];
    } else {
      null.asInstanceOf[E]
    }
  }

  def remove() = {};

  def iterator(): java.util.Iterator[E] = {
    rddCollect = null;
    this
  };
}
