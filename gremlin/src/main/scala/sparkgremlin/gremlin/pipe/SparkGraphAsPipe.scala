package sparkgremlin.gremlin.pipe

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import sparkgremlin.gremlin._

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphAsPipe[S,E](name: String) extends BulkPipe[S,E] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[E] = {
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[E]];
    val tname = name;
    val tkey = bs.extractKey;
    val out = bs.elementType match {
      case BulkDataType.VERTEX_DATA => bs.stateGraph.mapVertices( (x,y) => (y._1, GremlinVertex.addAsColumn( y._2, tname, y._1)) );
      case BulkDataType.VERTEX_PROP_DATA => bs.stateGraph.mapVertices( (x,y) => (y._1, GremlinVertex.addAsColumn( y._2, tname, y._1.getProperty(tkey) )) )
      case _ => throw new RuntimeException("Don't know what to do here")
    }

    if (bs.asColumns != null ) {
      return new SparkGraphBulkData[E](bs.graphData, out, bs.asColumns ++ Array(name), bs.elementType, bs.extractKey) {
        def currentRDD(): RDD[E] = bs.currentRDD()
      }
    }
    return new SparkGraphBulkData[E](bs.graphData, out, Array(name), bs.elementType, bs.extractKey) {
      def currentRDD(): RDD[E] = bs.currentRDD()
    }

  }
}
