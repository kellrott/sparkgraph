package sparkgremlin.gremlin

import sparkgremlin.blueprints.SparkGraphElementSet
import org.apache.spark.rdd.RDD

/**
 * Created by kellrott on 2/8/14.
 */
abstract class SparkGraphBulkData[E](
                                      val graphData : SparkGraphElementSet[_],
                                      val graphState : RDD[(Long, GremlinVertex)],
                                      val asColumns: Array[String],
                                      val elementType : BulkDataType.Value,
                                      val extractKey : String) extends BulkPipeData[E]
{

  def extract() : Iterator[E] = {
    return currentRDD().collect().toIterator.asInstanceOf[Iterator[E]];
  }

  def currentRDD() : RDD[E];

}
