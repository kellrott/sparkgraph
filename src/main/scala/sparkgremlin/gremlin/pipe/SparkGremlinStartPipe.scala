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
    val nv = if (startGraph.elementClass() == classOf[SparkVertex]) {
      startGraph.graphX().vertices.leftJoin(startGraph.selectVertexRDD())((x, y, z) => {
        if (z.get) {
          (y, new GremlinVertex(1))
        } else {
          (y, new GremlinVertex(0))
        }
      })
    } else {
      startGraph.graphX().vertices.map(x => {
        (x._1, (x._2, new GremlinVertex(0)))
      })
    }

    val ne: EdgeRDD[(SparkEdge, Boolean),SparkVertex] = if (startGraph.elementClass() == classOf[SparkEdge]) {
      startGraph.graphX().edges.innerJoin(startGraph.selectEdgeRDD())((a, b, x, y) => {
        if (y) {
          (x, true)
        } else {
          (x, false)
        }
      })
    } else {
      startGraph.graphX().edges.mapValues(x => {
        (x.attr, false)
      })
    }

    return new SparkGraphBulkData[E](
      startGraph,
      graphx.Graph(nv, ne).cache(),
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
