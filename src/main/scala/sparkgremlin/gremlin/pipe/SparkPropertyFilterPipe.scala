package sparkgremlin.gremlin.pipe

//import collection.JavaConverters._

import sparkgremlin.blueprints.{SparkVertex, SparkGraphElement}
import com.tinkerpop.blueprints.Predicate
import com.tinkerpop.pipes.util.PipeHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import sparkgremlin.gremlin._

/**
 * Created by kellrott on 2/8/14.
 */
object SparkPropertyFilterPipe {
   def filterVertex( vert : SparkVertex, state : GremlinVertex, key:String, predicate : Predicate, value : AnyRef ) : Boolean = {
     return predicate.evaluate(vert.getProperty(key), value)
   }

   def filterVertexID( vert:SparkVertex, state: GremlinVertex, predicate: Predicate, value: AnyRef) : Boolean = {
     val vid : java.lang.Long = value match {
       case x : java.lang.Long => x;
       case _ => value.toString.toLong
     }
     val out = predicate.evaluate(vert.id, vid);
     return out;
   }

   def filterEdges( vert: SparkVertex, state : GremlinVertex, key:String, predicate: Predicate, value: AnyRef) : GremlinVertex = {
     val newState = new GremlinVertex();
     newState.travelers = state.travelers
     newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.getProperty(key), value) ).map( _.id.asInstanceOf[AnyRef] ).toArray
     return newState;
   }

   def filterEdgesLabels( vert: SparkVertex, state : GremlinVertex, predicate: Predicate, value: AnyRef) : GremlinVertex = {
     val newState = new GremlinVertex();
     newState.travelers = state.travelers
     newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.label, value) ).map( _.id.asInstanceOf[AnyRef] ).toArray
     return newState;
   }

   def filterEdgesID( vert:SparkVertex, state: GremlinVertex, predicate: Predicate, value: AnyRef) : GremlinVertex = {
     val newState = new GremlinVertex();
     val vid : java.lang.Long = value match {
       case x : java.lang.Long => x;
       case _ => value.toString.toLong
     }
     newState.travelers = state.travelers
     newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.id, vid) ).map( _.id.asInstanceOf[AnyRef] ).toArray
     return newState;
   }
 }



class SparkPropertyFilterPipe extends BulkPipe[SparkGraphElement, SparkGraphElement] {
  val id : AnyRef = null;
  val label : String = null;
  var key : String = null;
  var value: AnyRef = null
  var predicate: Predicate = null

  def this(key: String, predicate: Predicate, value: AnyRef) {
    this()
    this.key = key;
    this.value = value
    this.predicate = predicate
  }

  override def toString: String = {
    return PipeHelper.makePipeString(this, this.predicate, this.key)
  }

  def bulkReader(input: java.util.Iterator[SparkGraphElement]) : BulkPipeData[SparkGraphElement] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess() : SparkGraphBulkData[SparkGraphElement] = {
    val y = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkGraphElement]];
    val searchKey = key;
    val searchPredicate = predicate;

    if (y.elementType == BulkDataType.VERTEX_DATA) {
      if (this.id != null || this.key == "id") {
        val searchValue = if (id != null) {
          this.id
        } else {
          this.value
        }
        val step = y.vertexData.filter( x => SparkPropertyFilterPipe.filterVertexID( x._2._1, x._2._2, searchPredicate, searchValue ))
        return new SparkGraphBulkData[SparkGraphElement](y.graphData, step, y.asColumns, BulkDataType.VERTEX_DATA, null ) {
          def currentRDD(): RDD[SparkGraphElement] = y.graphData.graphX().vertices.join(step).map(_._2._1)
        }
      } else {
        val searchValue = value
        val step = y.vertexData.filter( x => SparkPropertyFilterPipe.filterVertex(x._2._1, x._2._2, searchKey, searchPredicate, searchValue ))
        return new SparkGraphBulkData[SparkGraphElement](y.graphData, step, y.asColumns, BulkDataType.VERTEX_DATA, null ) {
          def currentRDD(): RDD[SparkGraphElement] = y.graphData.graphX.vertices.join(step).map(_._2._1)
        }
      }
    }
    if (y.elementType == BulkDataType.EDGE_DATA) {
      val step = if (label != null || key == "label") {
        val searchValue = if (label != null) {
          label;
        } else {
          value ;
        }
        y.vertexData.map( x=> (x._1, (x._2._1, SparkPropertyFilterPipe.filterEdgesLabels(x._2._1, x._2._2, searchPredicate, searchValue)) ));
      } else if (id != null || key == "id") {
        val searchValue = if (id != null) {
          id;
        } else {
          value;
        }
        y.vertexData.map( x=> (x._1, (x._2._1, SparkPropertyFilterPipe.filterEdgesID(x._2._1, x._2._2, searchPredicate, searchValue)) ) );
      } else {
        val searchValue = value
        y.vertexData.map( x=> (x._1, (x._2._1, SparkPropertyFilterPipe.filterEdges(x._2._1, x._2._2, searchKey, searchPredicate, searchValue)) ));
      }
      return new SparkGraphBulkData[SparkGraphElement](
        y.graphData, step, y.asColumns, BulkDataType.EDGE_DATA, null
      ) {
        //def currentRDD(): RDD[SparkGraphElement] = y.graphData.graphX().vertices.join(step).flatMap( x => x._2._1.edgeSet.filter( y => x._2._2.validEdges.contains(y.id)  ) )
        def currentRDD() : RDD[SparkGraphElement] = null;
      }
    }
    return null;
  }
}
