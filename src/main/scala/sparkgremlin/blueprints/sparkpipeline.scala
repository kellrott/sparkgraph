package sparkgremlin.blueprints

import com.tinkerpop.blueprints._
import com.tinkerpop.pipes.util.PipeHelper
import com.tinkerpop.pipes.{AbstractPipe, Pipe, PipeFunction}
import com.tinkerpop.pipes.util.structures.{Row, Pair, Table, Tree}
import java.util.Map
import java.util.List
import java.util.Map.Entry
import com.tinkerpop.pipes.transform.TransformPipe.Order
import com.tinkerpop.gremlin.Tokens
import java.lang._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import collection.JavaConverters._
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle
import com.tinkerpop.pipes.util.iterators.HistoryIterator
import java.lang.RuntimeException
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SparkPipelineException {
  val NON_READER: String = "Bulk Pipeline Step does not implement reader"

}

class SparkPipelineException(msg:String) extends RuntimeException(msg) {

}

trait BulkPipeData[T] {

  def extract() : Iterator[T];

}

abstract class BulkPipe[S,E] extends AbstractPipe[S,E] {

  var bulkStartPipe : BulkPipe[_,S] = null;
  var bulkStarts : BulkPipeData[S] = null;

  def bulkReader(input: java.util.Iterator[S]) : BulkPipeData[S];
  def bulkProcess() : BulkPipeData[E];

  var collectedOutput : Iterator[E] = null;

  override def setStarts(starts: java.util.Iterator[S]) = {
    if (starts.isInstanceOf[BulkPipe[_,S]]) {
      this.starts = null
      this.bulkStartPipe = starts.asInstanceOf[BulkPipe[_,S]];
    } else if (starts.isInstanceOf[Pipe[_, _]]) {
      this.starts = starts
      this.bulkStartPipe = null;
    } else {
      this.starts = new HistoryIterator[S](starts)
      this.bulkStartPipe = null;
    }
  }

  def bulkProcessStart() : BulkPipeData[E] = {
    if (bulkStartPipe != null) {
      bulkStarts = bulkStartPipe.bulkProcessStart();
      return bulkProcess();
    } else if (starts != null) {
      bulkStarts = bulkReader(starts);
      return bulkProcess();
    } else {
      throw new RuntimeException("Unable to link Bulk Pipe")
    }
    return null;
  }

  def processNextStart(): E = {
    if (collectedOutput == null) {
      if (bulkStartPipe != null) {
        bulkStarts = bulkStartPipe.bulkProcessStart();
        collectedOutput = bulkProcess().extract();
      } else if (starts != null) {
        bulkStarts = bulkReader(starts);
        collectedOutput = bulkProcess().extract();
      } else {
        throw new RuntimeException("Unable to link Bulk Pipe")
      }
    }
    if (!collectedOutput.hasNext) {
      throw new NoSuchElementException();
    }
    return collectedOutput.next()
  }
}


class SparkGremlinStartPipe[E <: SparkGraphElement](startGraph : SparkGraphElementSet[E]) extends BulkPipe[E,E] {
  def bulkReader(input: java.util.Iterator[E]): BulkPipeData[E] = {
    val y = input.asInstanceOf[SparkGraphElementSet[E]];
    return new SparkGraphBulkData[E](y, null, null )
  }
  override def bulkProcessStart() : BulkPipeData[E] = {
    starts = startGraph;
    bulkStarts = bulkReader(starts);
    return bulkProcess();
  }
  def bulkProcess(): BulkPipeData[E] = bulkStarts
}

object GremlinState {
  def merge(a: GremlinState, b:GremlinState) : GremlinState = {
    val out = new GremlinState();
    out.startVertices = a.startVertices ++ b.startVertices;
    return out;
  }
  def clearEdges(a: GremlinState, b:SparkVertex) : GremlinState = {
    val out = new GremlinState();
    out.startVertices = a.startVertices;
    out.validEdges = b.edgeSet.map( _.id ).toArray;
    return out;
  }
}

class GremlinState extends Serializable {
  def this(vertex:SparkVertex) = {
    this()
    startVertices = Array(vertex.id);
    validEdges = vertex.edgeSet.map( _.id ).toArray
  }
  var validEdges : Array[AnyRef] = null;
  var startVertices : Array[AnyRef] = null;
}

class SparkGraphBulkData[E <: SparkGraphElement](val graphData : SparkGraphElementSet[E], val graphState : RDD[(AnyRef, GremlinState)], val elementClass : Class[_]) extends BulkPipeData[E] {

  def extract() : Iterator[E] = {
    if (elementClass == classOf[SparkVertex]) {
      val out = graphData.graphRDD().join( graphState ).map( x => x._2._1 );
      return out.collect().toIterator.asInstanceOf[Iterator[E]];
    }
    if (elementClass == classOf[SparkEdge]) {
      val validEdges = graphState.flatMap( x => x._2.validEdges.map( x => (x, true)) )
      return graphData.graphRDD().flatMap(_._2.edgeSet.map( x => (x.id, x) )).join( validEdges ).map( _._2._1 ).collect().toIterator.asInstanceOf[Iterator[E]];
    }
    return null;
  }

  def createStep(nextState : RDD[(AnyRef, GremlinState)], newElementClass : Class[_]) : SparkGraphBulkData[E] = {
    return new SparkGraphBulkData[E](graphData, nextState, newElementClass);
  }

}

class SparkGraphQueryPipe[E <: SparkGraphElement](cls : Class[_]) extends BulkPipe[Graph,E] {
  def bulkReader(input: java.util.Iterator[Graph]) : BulkPipeData[Graph] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess() : BulkPipeData[E] = {
    val y = bulkStarts.asInstanceOf[SparkGraphBulkData[E]];
    val out = y.createStep( y.graphData.graphRDD().map( x => (x._1, new GremlinState(x._2)) ), cls );
    return out;
  }

}


object SparkPropertyFilterPipe  {
  def filterVertex( vert : SparkVertex, state : GremlinState, key:String, predicate : Predicate, value : AnyRef ) : Boolean = {
    return predicate.evaluate(vert.getProperty(key), value)
  }

  def filterVertexID( vert:SparkVertex, state: GremlinState, predicate: Predicate, value: AnyRef) : Boolean = {
    val vid : java.lang.Long = value match {
      case x : java.lang.Long => x;
      case _ => value.toString.toLong
    }
    val out = predicate.evaluate(vert.id, vid);
    return out;
  }

  def filterEdges( vert: SparkVertex, state : GremlinState, key:String, predicate: Predicate, value: AnyRef) : GremlinState = {
    val newState = new GremlinState();
    newState.startVertices = state.startVertices
    newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.getProperty(key), value) ).map( _.id ).toArray
    return newState;
  }

  def filterEdgesLabels( vert: SparkVertex, state : GremlinState, predicate: Predicate, value: AnyRef) : GremlinState = {
    val newState = new GremlinState();
    newState.startVertices = state.startVertices
    newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.label, value) ).map( _.id ).toArray
    return newState;
  }

  def filterEdgesID( vert:SparkVertex, state: GremlinState, predicate: Predicate, value: AnyRef) : GremlinState = {
    val newState = new GremlinState();
    val vid : java.lang.Long = value match {
      case x : java.lang.Long => x;
      case _ => value.toString.toLong
    }
    newState.startVertices = state.startVertices
    newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.id, vid) ).map( _.id ).toArray
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

    if (y.elementClass == classOf[SparkVertex]) {
      if (this.id != null || this.key == "id") {
        val searchValue = if (id != null) {
          this.id
        } else {
          this.value
        }
        val step = y.graphState.join( y.graphData.graphRDD() ).filter( x => SparkPropertyFilterPipe.filterVertexID(x._2._2, x._2._1, searchPredicate, searchValue ))
        return y.createStep( step.map( x => (x._1, x._2._1) ), y.elementClass )
      } else {
        val searchValue = value
        val step = y.graphState.join( y.graphData.graphRDD() ).filter( x => SparkPropertyFilterPipe.filterVertex(x._2._2, x._2._1, searchKey, searchPredicate, searchValue ))
        return y.createStep( step.map( x => (x._1, x._2._1) ), y.elementClass )
      }
    }
    if (y.elementClass == classOf[SparkEdge]) {
      val step = if (label != null || key == "label") {
        val searchValue = if (label != null) {
          label;
        } else {
          value ;
        }
        y.graphState.join( y.graphData.graphRDD() ).map( x=> (x._1, SparkPropertyFilterPipe.filterEdgesLabels(x._2._2, x._2._1, searchPredicate, searchValue)) );
      } else if (id != null || key == "id") {
        val searchValue = if (id != null) {
          id;
        } else {
          value;
        }
        y.graphState.join( y.graphData.graphRDD() ).map( x=> (x._1, SparkPropertyFilterPipe.filterEdgesID(x._2._2, x._2._1, searchPredicate, searchValue)) );
      } else {
        val searchValue = value
        y.graphState.join( y.graphData.graphRDD() ).map( x=> (x._1, SparkPropertyFilterPipe.filterEdges(x._2._2, x._2._1, searchKey, searchPredicate, searchValue)) );
      }
      return y.createStep( step, y.elementClass );
    }
    return null;
  }
}

class SparkIdFilterPipe extends SparkPropertyFilterPipe {
  def this(predicate: Predicate, value: AnyRef) {
    this()
    this.key = "id"
    this.value = value
    this.predicate = predicate
  }
}


class SparkLabelFilterPipe extends SparkPropertyFilterPipe {
  def this(predicate: Predicate, value: AnyRef) {
    this()
    this.value = value
    this.predicate = predicate
    this.key = "label"
  }
}


class SparkGraphConnectedVertex[S](val direction:Direction, val max_branch : Int, val labels:Array[String]) extends BulkPipe[S,SparkVertex] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[SparkVertex] = {
    //pretty sure this is the least optimal way to do things
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkVertex]]
    val edges = bs.graphData.graphRDD().map( x => (x._1, x._2.edgeSet.map(y => y.inVertexId) ) );
    val n = edges.join( bs.graphState ).flatMap( x => x._2._1.map( y => (y, x._2._2)) );
    val reduced = n.reduceByKey( (x,y) => GremlinState.merge(x, y) );
    val out = reduced.join( bs.graphData.graphRDD() ).map( x => (x._1, GremlinState.clearEdges( x._2._1, x._2._2 ) ) );
    return bs.createStep(out, classOf[SparkVertex]);
  }
}


object SparkGremlinPipeline {
  val NOT_IMPLEMENTED_ERROR = "Not yet implemented"
}

class SparkGremlinPipeline[S, E](val start: AnyRef) extends SparkGremlinPipelineBase[S, E] {

  pipes = new java.util.ArrayList[Pipe[_, _]]();
  if (start.isInstanceOf[SparkGraphElementSet[_]]) {
    val sparkElementSet = start.asInstanceOf[SparkGraphElementSet[_]];
    if (sparkElementSet.getElementClass() == classOf[SparkVertex]) {
      pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkVertex]]));
      pipes.add(new SparkGraphQueryPipe[SparkVertex](classOf[SparkVertex]));

    } else if (sparkElementSet.getElementClass() == classOf[SparkEdge]) {
      pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkEdge]]));
      pipes.add(new SparkGraphQueryPipe[SparkEdge](classOf[SparkEdge]));
    }
  } else if (start.isInstanceOf[SparkGraphElementSet[SparkGraphElement]]) {
    pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkGraphElement]]));
  } else if (start.isInstanceOf[SparkVertex]) {
    val sparkStart = start.asInstanceOf[SparkVertex]
    val startID = sparkStart.getId
    val startRDD = sparkStart.graph.curgraph.filter( x => x._1 == startID  ).map(_._2);
    pipes.add(new SparkGremlinStartPipe(new SimpleGraphElementSet[SparkVertex](sparkStart.graph, startRDD, classOf[SparkVertex])));
    pipes.add(new SparkGraphQueryPipe[SparkVertex](classOf[SparkVertex]));
  } else {
    throw new RuntimeException("Unable to init pipeline")
  }

  def optimize(v: Boolean): SparkGremlinPipeline[S, E] = {
    return this;
  }

  /**
   * Add an arbitrary pipe to the GremlinPipeline
   *
   * @param pipe the pipe to add to the pipeline
   * @return the extended Pipeline
   */
  def add[T](pipe: Pipe[_, T]): SparkGremlinPipeline[S, T] = {
    this.addPipe(pipe)
    return this.asInstanceOf[SparkGremlinPipeline[S, T]]
  }

  /**
    */
  def V: SparkGremlinPipeline[S, SparkVertex] = {
    add( new SparkGraphQueryPipe[SparkVertex](classOf[SparkVertex]) )
  }


  /**
    */
  def E: SparkGremlinPipeline[S, SparkEdge] = {
    add( new SparkGraphQueryPipe[SparkEdge](classOf[SparkEdge]) )
  }

  def step(function: PipeFunction[_, _]): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def step[T](pipe: Pipe[E, T]): SparkGremlinPipeline[S, T] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def copySplit(pipes: Pipe[E, _]*): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def exhaustMerge(): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def fairMerge(): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def ifThenElse(ifFunction: PipeFunction[E, Boolean], thenFunction: PipeFunction[E, _], elseFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def loop(numberedStep: Int, whileFunction: PipeFunction[LoopBundle[E], Boolean]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def loop(namedStep: String, whileFunction: PipeFunction[LoopBundle[E], Boolean]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def loop(numberedStep: Int, whileFunction: PipeFunction[LoopBundle[E], Boolean], emitFunction: PipeFunction[LoopBundle[E], Boolean]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def loop(namedStep: String, whileFunction: PipeFunction[LoopBundle[E], Boolean], emitFunction: PipeFunction[LoopBundle[E], Boolean]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def and(pipes: Pipe[E, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def back(numberedStep: Int): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def back(namedStep: String): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def dedup(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def dedup(dedupFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def except(collection: java.util.Collection[E]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def except(namedSteps: String*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def filter(filterFunction: PipeFunction[E, Boolean]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def or(pipes: Pipe[E, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def random(bias: Double): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def range(low: Int, high: Int): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def retain(collection: java.util.Collection[E]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def retain(namedSteps: String*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def simplePath(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  /**
   * Check if the element has a property with provided key.
   *
   * @param key the property key to check
   * @return the extended Pipeline
   */
  def has(key: String): SparkGremlinPipeline[S, _ <: Element] = {
    return this.has(key, Tokens.T.neq, null)
  }

  /**
   * Add an IdFilterPipe, LabelFilterPipe, or PropertyFilterPipe to the end of the Pipeline.
   * If the incoming element has the provided key/value as check with .equals(), then let the element pass.
   * If the key is id or label, then use respect id or label filtering.
   *
   * @param key   the property key to check
   * @param value the object to filter on (in an OR manner)
   * @return the extended Pipeline
   */
  def has(key: String, value: AnyRef): SparkGremlinPipeline[S, _ <: Element] = {
    return this.has(key, Tokens.T.eq, value)
  }

  /**
   * Add an IdFilterPipe, LabelFilterPipe, or PropertyFilterPipe to the end of the Pipeline.
   * If the incoming element has the provided key/value as check with .equals(), then let the element pass.
   * If the key is id or label, then use respect id or label filtering.
   *
   * @param key          the property key to check
   * @param compareToken the comparison to use
   * @param value        the object to filter on
   * @return the extended Pipeline
   */
  def has(key: String, compareToken: Tokens.T, value: AnyRef): SparkGremlinPipeline[S, _ <: Element] = {
    return this.has(key, Tokens.mapPredicate(compareToken), value)
  }

  /**
   * Add an IdFilterPipe, LabelFilterPipe, or PropertyFilterPipe to the end of the Pipeline.
   * If the incoming element has the provided key/value as check with .equals(), then let the element pass.
   * If the key is id or label, then use respect id or label filtering.
   *
   * @param key       the property key to check
   * @param predicate the comparison to use
   * @param value     the object to filter on
   * @return the extended Pipeline
   */
  def has(key: String, predicate: Predicate, value: AnyRef): SparkGremlinPipeline[S, _ <: Element] = {
    if ( key == Tokens.ID ) {
      return this.add(new SparkIdFilterPipe(predicate, value))
    } else if (key == Tokens.LABEL) {
      return this.add(new SparkLabelFilterPipe(predicate, value))
    } else {
      val pipe: Pipe[_, _ <: Element] = new SparkPropertyFilterPipe(key, predicate, value)
      return this.add(pipe)
    }
  }

  /**
   * Check if the element does not have a property with provided key.
   *
   * @param key the property key to check
   * @return the extended Pipeline
   */
  def hasNot(key: String): SparkGremlinPipeline[S, _ <: Element] = {
    return this.has(key, Tokens.T.eq, null)
  }

  /**
   * Add an IdFilterPipe, LabelFilterPipe, or PropertyFilterPipe to the end of the Pipeline.
   * If the incoming element has the provided key/value as check with .equals(), then filter the element.
   * If the key is id or label, then use respect id or label filtering.
   *
   * @param key   the property key to check
   * @param value the objects to filter on (in an OR manner)
   * @return the extended Pipeline
   */
  def hasNot(key: String, value: AnyRef): SparkGremlinPipeline[S, _ <: Element] = {
    return this.has(key, Tokens.T.neq, value)
  }


  def interval(key: String, startValue: Comparable[_], endValue: Comparable[_]): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def gather(): SparkGremlinPipeline[S, List[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def gather(function: PipeFunction[List[_], _]): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(namedStep: String): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(numberedStep: Int): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(namedStep: String, map: Map[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(numberedStep: Int, map: Map[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def order(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def order(order: Order): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def order(compareFunction: PipeFunction[Pair[E, E], Integer]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def path(pathFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, List[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def scatter(): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def select(stepNames: java.util.Collection[String], columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, Row[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def select(columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, Row[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def select(): SparkGremlinPipeline[S, Row[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def shuffle(): SparkGremlinPipeline[S, List[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def cap(): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def orderMap(order: Order): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def orderMap(compareFunction: PipeFunction[Pair[Entry[_, _], Entry[_, _]], Integer]): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def transform[T](function: PipeFunction[E, T]): SparkGremlinPipeline[S, T] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def bothE(labels: String*): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def bothE(branchFactor: Int, labels: String*): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def both(labels: String*): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def both(branchFactor: Int, labels: String*): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def bothV(): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def idEdge(graph: Graph): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def id(): SparkGremlinPipeline[S, AnyRef] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def idVertex(graph: Graph): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def inE(labels: String*): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def inE(branchFactor: Int, labels: String*): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def in(labels: String*): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def in(branchFactor: Int, labels: String*): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def inV(): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def label(): SparkGremlinPipeline[S, String] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def outE(labels: String*): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def outE(branchFactor: Int, labels: String*): SparkGremlinPipeline[S, Edge] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def out() : SparkGremlinPipeline[S, Vertex] = {
    return this.add(new SparkGraphConnectedVertex(Direction.OUT, Integer.MAX_VALUE, null)).asInstanceOf[SparkGremlinPipeline[S,Vertex]];
  }

  def out(labels: String*): SparkGremlinPipeline[S, Vertex] = {
    return this.out(Integer.MAX_VALUE, labels:_*)
  }

  def out(branchFactor: Int, labels: String*): SparkGremlinPipeline[S, Vertex] = {
    return this.add(new SparkGraphConnectedVertex(Direction.OUT, branchFactor, labels.toArray)).asInstanceOf[SparkGremlinPipeline[S,Vertex]];
  }

  def outV(): SparkGremlinPipeline[S, Vertex] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def map(keys: String*): SparkGremlinPipeline[S, Map[String, AnyRef]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def property(key: String): SparkGremlinPipeline[S, AnyRef] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def aggregate(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def aggregate(aggregate: java.util.Collection[E]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def aggregate(aggregate: java.util.Collection[_], aggregateFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def aggregate(aggregateFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def optional(numberedStep: Int): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def optional(namedStep: String): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(map: Map[_, List[_]], keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(reduceMap: Map[_, _], keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _], reduceFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _], reduceFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(map: Map[_, Number], keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[Pair[_, Number], Number]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[Pair[_, Number], Number]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(map: Map[_, Number], keyFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(keyFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(map: Map[_, Number]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def sideEffect(sideEffectFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def store(storage: java.util.Collection[E]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def store(storage: java.util.Collection[_], storageFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def store(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def store(storageFunction: PipeFunction[E, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(table: Table, stepNames: java.util.Collection[String], columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(table: Table, columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(table: Table): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def tree(tree: Tree[_], branchFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def tree(branchFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def linkOut(label: String, namedStep: String): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def linkIn(label: String, namedStep: String): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def linkBoth(label: String, namedStep: String): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def linkOut(label: String, other: Vertex): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def linkIn(label: String, other: Vertex): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def linkBoth(label: String, other: Vertex): SparkGremlinPipeline[S, _ <: Element] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def as(name: String): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  override def `_`(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def start(`object`: S): SparkGremlinPipeline[S, S] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def enablePath(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def identity(): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

}