package sparkgremlin.blueprints

import org.apache.spark.rdd.RDD

/**
 * Created by kellrott on 2/8/14.
 */
trait RDDKeySet[E] extends java.util.Iterator[E] with java.lang.Iterable[E] {
  def elementRDD() : RDD[E];
  def elementClass() : Class[_];
}
