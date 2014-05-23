package sparkgraph.gremlin

import sparkgraph.blueprints.{SparkEdge, SparkVertex, SparkGraphElementSet}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx

/**
 * Created by kellrott on 2/8/14.
 */
abstract class SparkGraphBulkData[E](
                                      val graphData : SparkGraphElementSet[_],
                                      val stateGraph : graphx.Graph[(SparkVertex,GremlinVertex), (SparkEdge,Boolean)],
                                      val asColumns: Array[(String, BulkDataType.Value)],
                                      val elementType : BulkDataType.Value,
                                      val extractKey : String) extends BulkPipeData[E]
{

  def extract() : Iterator[E] = {
    return currentRDD().collect().toIterator.asInstanceOf[Iterator[E]];
  }

  def count() : scala.Long = {
    return currentRDD().count()
  }

  def currentRDD() : RDD[E]

  def dataType() : BulkDataType.Value = elementType

}
