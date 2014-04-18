package sparkgremlin.blueprints

import org.apache.spark.graphx.{Graph => GraphXGraph}
import org.apache.spark.rdd.RDD

/**
 * Created by kellrott on 2/8/14.
 */
abstract class SparkGraphElementSet[E] extends RDDIterator[E] {
  def graphX() : GraphXGraph[SparkVertex, SparkEdge];
}
