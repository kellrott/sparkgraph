package sparkgremlin.gremlin.pipe

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import sparkgremlin.blueprints.SparkVertex
import com.tinkerpop.blueprints.Direction
import sparkgremlin.gremlin._

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphConnectedVertex[S](val direction:Direction, val max_branch : Int, val labels:Array[String]) extends BulkPipe[S,SparkVertex] {
   def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
     throw new SparkPipelineException(SparkPipelineException.NON_READER);
   }

   def bulkProcess(): BulkPipeData[SparkVertex] = {
     val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkVertex]]
     throw new RuntimeException("FIX THIS AREA!!!!")
     /*
     val edges = bs.graphData.graphRDD().map( x => (x._1, x._2.edgeSet.map(y => y.inVertexId) ) );
     val n = edges.join( bs.graphState ).flatMap( x => x._2._1.map( y => (y, x._2._2)) );
     val reduced = n.reduceByKey( (x,y) => GremlinVertex.merge(x, y) );
     return new SparkGraphBulkData[SparkVertex](
       bs.graphData, reduced, bs.asColumns, BulkDataType.VERTEX_DATA, null
     ) {
       def currentRDD(): RDD[SparkVertex] = graphData.graphRDD().map( _._2 )
     }
     */
   }
 }
