package sparkgraph.gremlin

import org.apache.spark.rdd.RDD
import collection.JavaConverters._

/**
 * Created by kellrott on 5/22/14.
 */
object RDDUtil {

  def pipe2rdd[I,O](in:SparkGraphBulkData[I]): RDD[O] = {
    in.stateGraph.vertices.flatMap[AnyRef]( x => {
      if (x._2._2.travelers != null) {
        x._2._2.travelers.map(x=> x.asColumnMap.map(y => (y._1, y._2.value)).toMap ).toIterator
      } else {
        Iterator.empty
      }
    }).asInstanceOf[RDD[O]]
  }

}
