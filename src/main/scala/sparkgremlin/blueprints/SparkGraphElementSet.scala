package sparkgremlin.blueprints

import org.apache.spark.graphx.{Graph => GraphXGraph}

/**
 * Created by kellrott on 2/8/14.
 */
trait SparkGraphElementSet[E] extends RDDKeySet[E] {
  def graphX() : GraphXGraph[SparkVertex, SparkEdge];
}
