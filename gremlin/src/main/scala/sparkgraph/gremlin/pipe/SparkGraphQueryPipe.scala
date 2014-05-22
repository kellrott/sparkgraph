package sparkgraph.gremlin.pipe

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import sparkgraph.blueprints.{SparkEdge, SparkVertex, SparkGraphElement}
import com.tinkerpop.blueprints.Graph
import sparkgraph.gremlin._
import org.apache.spark.graphx

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphQueryPipe[E <: SparkGraphElement](cls : BulkDataType.Value) extends BulkPipe[Graph,E] {
  def bulkReader(input: java.util.Iterator[Graph]) : BulkPipeData[Graph] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess() : BulkPipeData[E] = {
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[E]];
    if (cls == BulkDataType.VERTEX_DATA) {
      return new SparkGraphBulkData[E](
        bs.graphData, bs.stateGraph,
        bs.asColumns, cls, null) {
        def currentRDD(): RDD[E] = bs.graphData.graphX().vertices.map( _._2 ).asInstanceOf[RDD[E]];
      }
    }
    if (cls == BulkDataType.EDGE_DATA) {
      return new SparkGraphBulkData[E](
        bs.graphData, bs.stateGraph,
        bs.asColumns, cls, null) {
        def currentRDD(): RDD[E] = bs.graphData.graphX().edges.map( _.attr ).asInstanceOf[RDD[E]];
      }
    }
    return null;
  }
}
