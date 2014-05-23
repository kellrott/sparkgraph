package sparkgraph.gremlin.pipe

import sparkgraph.gremlin.pipe.{SingleElementBulkPipe, BulkSideEffectPipe}
import sparkgraph.gremlin._
import sparkgraph.blueprints.{SparkVertex, SparkGraphElement}
import org.apache.spark.graphx
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag


class SparkGraphBackPipe[S,E](var stepName:String) extends BulkPipe[S,E] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[E] = {
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkGraphElement]];

    val matches = bs.asColumns.filter(_._1 == stepName)
    if (matches.size == 0 ) {
      throw new RuntimeException("Can't find step " + stepName)
    }
    val step_data_type = matches(0)._2

    var local_stepName = stepName
    val new_travelers = bs.stateGraph.vertices.flatMap( x => {
      if (x._2._2.travelers != null)
        x._2._2.travelers.map( y => (y.asColumnMap(local_stepName).vertexID, new GremlinVertex(y)) )
      else
        Iterator.empty
    }).reduceByKey( (x,y) => {
      GremlinVertex.merge(x,y)
    } )

    val new_states = bs.stateGraph.outerJoinVertices(new_travelers)((vid,x,y) => {
      if (y.isDefined)
        (x._1, y.get)
      else
        (x._1, new GremlinVertex(0))
    } )

    return new SparkGraphBulkData[E](
      bs.graphData, new_states, bs.asColumns, step_data_type, bs.extractKey
    ) {
      def currentRDD(): RDD[E] = stateGraph.vertices.filter(_._2._2.travelerCount > 0).map( _._2._1 ).asInstanceOf[RDD[E]]
    }

  }

}
