package sparkgremlin.gremlin.pipe

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import sparkgremlin.blueprints.SparkVertex
import com.tinkerpop.blueprints.Direction
import sparkgremlin.gremlin._
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphConnectedVertex[S](val direction:Direction, val max_branch : Int, val labels:Array[String]) extends BulkPipe[S,SparkVertex] {
   def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
     throw new SparkPipelineException(SparkPipelineException.NON_READER);
   }

   def bulkProcess(): BulkPipeData[SparkVertex] = {
     val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkVertex]]
     //bs.stateGraph.vertices.collect().foreach(println)
     //bs.stateGraph.triplets.collect().foreach(println(_))
     val outvert = bs.stateGraph.mapReduceTriplets[(SparkVertex,GremlinVertex)](
       x => {
         if (x.srcAttr._2 != null) {
           Iterator( (x.dstId, (x.dstAttr._1, x.srcAttr._2)), (x.srcId, (x.srcAttr._1, new GremlinVertex())) )
         } else {
           Iterator.empty
         }
       },
       (y,z) => (y._1, GremlinVertex.merge(y._2, z._2))
     )
     return new SparkGraphBulkData[SparkVertex](
     bs.graphData, graphx.Graph(outvert, bs.stateGraph.edges), bs.asColumns, bs.elementType, bs.extractKey
     ) {
       def currentRDD(): RDD[SparkVertex] = outvert.map( _._2._1 )
     }
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
