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
     val outstate : VertexRDD[GremlinVertex] = if (direction == Direction.IN) {
       bs.stateGraph.mapReduceTriplets[GremlinVertex](
         x => {
           if (local_labels == null || local_labels.length == 0 || local_labels.contains(x.attr._1.getLabel) ) {
             Iterator( (x.dstId, new GremlinVertex()), (x.srcId, x.dstAttr._2))
           } else {
             Iterator( (x.dstId, new GremlinVertex()), (x.srcId, new GremlinVertex()))
           }
         },
         (y,z) => GremlinVertex.merge(y, z)
       )
     } else if (direction == Direction.OUT) {
       bs.stateGraph.mapReduceTriplets[GremlinVertex](
         x => {
           if (local_labels == null || local_labels.length == 0 || local_labels.contains(x.attr._1.getLabel) ) {
             Iterator( (x.dstId,  x.srcAttr._2) , (x.srcId, new GremlinVertex()) )
           } else {
             Iterator( (x.dstId, new GremlinVertex()), (x.srcId, new GremlinVertex()) )
           }
         },
         (y,z) => (GremlinVertex.merge(y, z))
       )
     } else if(direction == Direction.BOTH) {
       bs.stateGraph.mapReduceTriplets[GremlinVertex](
         x => {
           if (local_labels == null || local_labels.length == 0 || local_labels.contains(x.attr._1.getLabel) ) {
             Iterator( (x.dstId, x.srcAttr._2), (x.srcId, (x.dstAttr._2)) )
           } else {
             Iterator( (x.dstId, new GremlinVertex()), (x.srcId, new GremlinVertex()) )
           }
         },
         (y,z) => GremlinVertex.merge(y, z)
       )
     } else {
      throw new RuntimeException("Unknown Direction:" + direction)
     }
     //println("next")
     //outstate.collect().foreach(println)
     val outgraph = bs.stateGraph.outerJoinVertices(outstate)( (vid,x,y) => {
       if (y.isDefined) {
         (x._1, y.get)
       } else {
         (x._1, new GremlinVertex(0))
       }
     })
     return new SparkGraphBulkData[SparkVertex](
     bs.graphData, outgraph, bs.asColumns, bs.elementType, bs.extractKey
     ) {
       def currentRDD(): RDD[SparkVertex] = stateGraph.vertices.filter(_._2._2.travelerCount > 0).map( _._2._1 )
     }
   }
 }
