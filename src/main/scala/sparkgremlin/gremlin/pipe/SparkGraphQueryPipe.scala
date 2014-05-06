package sparkgremlin.gremlin.pipe

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import sparkgremlin.blueprints.{SparkEdge, SparkVertex, SparkGraphElement}
import com.tinkerpop.blueprints.Graph
import sparkgremlin.gremlin._
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
      val active_ids = bs.graphData.elementRDD().map( x => (x.asInstanceOf[SparkVertex].id, true) )
      val new_verts = bs.graphData.graphX().vertices.cogroup( active_ids ).map( x => {
        if (x._2._2.toSeq.length > 0) {
          (x._1, (x._2._1.head, new GremlinVertex(1)) )
        } else{
          (x._1, (x._2._1.head, new GremlinVertex(0)) )
        }
      } )
      return new SparkGraphBulkData[E](
        bs.graphData,
        graphx.Graph(new_verts, bs.graphData.graphX().edges.map( x => new graphx.Edge(x.srcId, x.dstId, (x.attr, true)) )),
        bs.asColumns, BulkDataType.VERTEX_DATA, null ) {
        def currentRDD(): RDD[E] = bs.graphData.graphX().vertices.map( _._2 ).asInstanceOf[RDD[E]]
      }
    } else if (cls == BulkDataType.EDGE_DATA) {
      val new_verts = bs.graphData.graphX().vertices.map( x => (x._1, (x._2, new GremlinVertex(1))) )
      if (bs.elementType == BulkDataType.EDGE_DATA) {
        return new SparkGraphBulkData[E](
          bs.graphData,
          graphx.Graph(new_verts, bs.graphData.elementRDD().asInstanceOf[RDD[SparkEdge]].map( x => new graphx.Edge(x.outVertexId, x.inVertexId, (x, true)))),
          bs.asColumns, cls, null
        ) {
          def currentRDD(): RDD[E] = bs.graphData.graphX().edges.map( _.attr ).asInstanceOf[RDD[E]];
        }
      } else {
        return new SparkGraphBulkData[E](
          bs.graphData,
          graphx.Graph(new_verts, bs.graphData.graphX().edges.map( x => new graphx.Edge(x.srcId, x.dstId, (x.attr, true)))),
          bs.asColumns, cls, null
        ) {
          def currentRDD(): RDD[E] = bs.graphData.graphX().edges.map( _.attr ).asInstanceOf[RDD[E]];
        }
      }
    }
    return null;
  }
}
