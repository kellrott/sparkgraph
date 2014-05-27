package sparkgraph.gremlin.pipe

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import sparkgraph.blueprints.SparkVertex
import com.tinkerpop.blueprints.Direction
import sparkgraph.gremlin._
import org.apache.spark.graphx.{VertexRDD, VertexId}
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
     val local_labels = labels
     val outstate : VertexRDD[(SparkVertex,GremlinVertex)] = if (direction == Direction.IN) {
       bs.stateGraph.mapReduceTriplets[(SparkVertex,GremlinVertex)](
         x => {
           if (local_labels == null || local_labels.length == 0 || local_labels.contains(x.attr._1.getLabel) ) {
             Iterator( (x.dstId, (x.dstAttr._1, new GremlinVertex())), (x.srcId, (x.srcAttr._1, x.dstAttr._2)) )
           } else {
             Iterator( (x.dstId, (x.dstAttr._1, new GremlinVertex())), (x.srcId, (x.srcAttr._1, new GremlinVertex())) )
           }
         },
         (y,z) => (y._1, GremlinVertex.merge(y._2, z._2))
       )

     } else if (direction == Direction.OUT) {
       bs.stateGraph.mapReduceTriplets[(SparkVertex,GremlinVertex)](
         x => {
           if (local_labels == null || local_labels.length == 0 || local_labels.contains(x.attr._1.getLabel) ) {
             Iterator( (x.dstId, (x.dstAttr._1, x.srcAttr._2)), (x.srcId, (x.srcAttr._1, new GremlinVertex())) )
           } else {
             Iterator( (x.dstId, (x.dstAttr._1, new GremlinVertex())), (x.srcId, (x.srcAttr._1, new GremlinVertex())) )
           }
         },
         (y,z) => (y._1, GremlinVertex.merge(y._2, z._2))
       )
     } else if(direction == Direction.BOTH) {
       bs.stateGraph.mapReduceTriplets[(SparkVertex,GremlinVertex)](
         x => {
           if (local_labels == null || local_labels.length == 0 || local_labels.contains(x.attr._1.getLabel) ) {
             Iterator( (x.dstId, (x.dstAttr._1, x.srcAttr._2)), (x.srcId, (x.srcAttr._1, x.dstAttr._2)) )
           } else {
             Iterator( (x.dstId, (x.dstAttr._1, new GremlinVertex())), (x.srcId, (x.srcAttr._1, new GremlinVertex())) )
           }
         },
         (y,z) => (y._1, GremlinVertex.merge(y._2, z._2))
       )
     } else {
      throw new RuntimeException("Unknown Direction:" + direction)
     }
     //println("next")
     //outstate.collect().foreach(println)

     return new SparkGraphBulkData[SparkVertex](
     bs.graphData, graphx.Graph(outstate, bs.stateGraph.edges), bs.asColumns, bs.elementType, bs.extractKey
     ) {
       def currentRDD(): RDD[SparkVertex] = stateGraph.vertices.filter(_._2._2.travelerCount > 0).map( _._2._1 )
     }
   }
 }