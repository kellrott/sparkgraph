package sparkgremlin.gremlin.pipe

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import sparkgremlin.blueprints.{SparkVertex, SparkGraphElement}
import com.tinkerpop.blueprints.Graph
import sparkgremlin.gremlin._

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphQueryPipe[E <: SparkGraphElement](cls : BulkDataType.Value) extends BulkPipe[Graph,E] {
   def bulkReader(input: java.util.Iterator[Graph]) : BulkPipeData[Graph] = {
     throw new SparkPipelineException(SparkPipelineException.NON_READER);
   }

   def bulkProcess() : BulkPipeData[E] = {
     throw new RuntimeException("FIX THIS CLASS!!!")
     /*
     val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[E]];
     if (cls == BulkDataType.VERTEX_DATA) {
       val active_ids = bs.graphData.elementRDD().map( x => (x.asInstanceOf[SparkVertex].id, true) );
       return new SparkGraphBulkData[E](
         bs.graphData,
         bs.graphData.graphRDD().join( active_ids ).map( x => (x._1, new GremlinVertex(1)) ),
         bs.asColumns, BulkDataType.VERTEX_DATA, null ) {
         def currentRDD(): RDD[E] = bs.graphData.graphRDD().map( _._2 ).asInstanceOf[RDD[E]]
       }
     } else if (cls == BulkDataType.EDGE_DATA) {
       return new SparkGraphBulkData[E](
         bs.graphData,
         bs.graphData.graphRDD().map( x => (x._1, new GremlinVertex(1)) ),
         bs.asColumns, cls, null
       ) {
         def currentRDD(): RDD[E] = bs.graphData.graphRDD().flatMap( _._2.edgeSet ).asInstanceOf[RDD[E]];
       }
     }
     return null;
     */
   }

 }
