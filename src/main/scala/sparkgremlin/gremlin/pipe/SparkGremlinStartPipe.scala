package sparkgremlin.gremlin.pipe

import sparkgremlin.blueprints.{SparkEdge, SparkVertex, SparkGraphElementSet, SparkGraphElement}
import org.apache.spark.rdd.RDD
import sparkgremlin.gremlin._
import org.apache.spark.graphx
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.EdgeRDD

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGremlinStartPipe[E <: SparkGraphElement](startGraph : SparkGraphElementSet[E]) extends BulkPipe[E,E] {

  /*
  Because SparkGremlinStartPipe is at the start of the pipeline, and has it's inputs passed
  passed to it in the constructor, it does it's processing immedately.
  This way, it the SparkGremlinPipeline is re-used, the work has already been done
  and cached.
   */
  val cached_start = init()

  def init() : BulkPipeData[E] = {
    var local_start = startGraph
    val state_graph_1 = if (startGraph.elementClass() == classOf[SparkVertex]) {
      startGraph.graphX().mapVertices( (vid,x) => {
        if (local_start.filter(x))
          (x, new GremlinVertex(1))
        else
          (x, new GremlinVertex(0))
      })
    } else {
      startGraph.graphX().mapVertices( (vid,x) => {
        (x, new GremlinVertex(0))
      })
    }

    val state_graph_2 = if (startGraph.elementClass() == classOf[SparkEdge]) {
      state_graph_1.mapEdges( x => {
        if (local_start.filter(x.attr))
          (x.attr, true)
        else
          (x.attr, false)
      })
    } else {
      state_graph_1.mapEdges( x => (x.attr,true) )
    }


    return new SparkGraphBulkData[E](
      startGraph,
      state_graph_2.cache(),
      null, null, null) {
      def currentRDD(): RDD[E] = startGraph.graphX().vertices.map(_._2).asInstanceOf[RDD[E]]
    }
  }


  def bulkReader(input: java.util.Iterator[E]): BulkPipeData[E] = {
    null
  }

  override def bulkProcessStart() : BulkPipeData[E] = {
    starts = startGraph;
    return bulkProcess();
  }

  def bulkProcess(): BulkPipeData[E] = {
    cached_start
  }

}
