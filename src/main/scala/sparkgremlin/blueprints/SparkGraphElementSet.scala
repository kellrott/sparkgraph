package sparkgremlin.blueprints

import org.apache.spark.rdd.RDD

/**
 * Created by kellrott on 2/8/14.
 */
trait SparkGraphElementSet[E] extends RDDKeySet[E] {
  def graphRDD() : RDD[(Long,SparkVertex)];
}
