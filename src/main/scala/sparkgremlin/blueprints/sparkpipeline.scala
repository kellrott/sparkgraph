package sparkgremlin.blueprints

import com.tinkerpop.blueprints._
import com.tinkerpop.pipes.util.PipeHelper
import com.tinkerpop.pipes.{AbstractPipe, Pipe, PipeFunction}
import com.tinkerpop.pipes.util.structures.{Row, Pair, Table, Tree}
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
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

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

trait BulkSideEffectPipe[S,T] extends BulkPipe[S,S] {
  def getSideEffect() : T;
}


object GremlinTraveler {
  def addAsColumn(t: GremlinTraveler, name:String, element: SparkGraphElement) : GremlinTraveler = {
    val out = new GremlinTraveler();
    out.asColumnMap = if (t.asColumnMap != null) {
      (t.asColumnMap ++ Map[String,SparkGraphElement](name -> element));
    } else {
      Map(name -> element);
    }
    return out;
  }
}

class GremlinTraveler extends Serializable {
  var asColumnMap : Map[String,AnyRef] = null;
}

object GremlinVertex {
  def merge(a: GremlinVertex, b:GremlinVertex) : GremlinVertex = {
    val out = new GremlinVertex();
    out.travelers = if (a.travelers != null && b.travelers != null) {
      a.travelers ++ b.travelers
    } else if (a.travelers != null) {
      a.travelers
    } else if (b.travelers != null) {
      b.travelers
    } else {
      null
    }
    out.travelerCount = a.travelerCount + b.travelerCount;
    return out;
  }

  def addAsColumn(a: GremlinVertex, name: String, element : SparkGraphElement) : GremlinVertex = {
    val out = new GremlinVertex()
    out.travelerCount = a.travelerCount;
    out.travelers = if (a.travelers != null) {
      a.travelers.map( x => GremlinTraveler.addAsColumn(x, name, element) )
    } else if (a.travelerCount > 0) {
      0.until(a.travelerCount).map( x => GremlinTraveler.addAsColumn(new GremlinTraveler, name, element) ).toArray
    } else {
      null
    }
    return out;
  }
}

class GremlinVertex extends Serializable {
  def this(count:Int) = {
    this();
    travelerCount = count;
  }
  var validEdges : Array[AnyRef] = null;
  var travelers : Array[GremlinTraveler] = null;
  var travelerCount = 0;
}

class SparkGraphBulkData[E](val graphData : SparkGraphElementSet[_], val graphState : RDD[(AnyRef, GremlinVertex)], val states: Array[String], val elementClass : Class[_]) extends BulkPipeData[E] {

  def extract() : Iterator[E] = {
    if (elementClass == classOf[SparkVertex]) {
      val out = graphData.graphRDD().join( graphState ).map( x => x._2._1 );
      return out.collect().toIterator.asInstanceOf[Iterator[E]];
    }
    if (elementClass == classOf[SparkEdge]) {
      val validEdges = graphData.graphRDD().join( graphState ).flatMap( x => {
        if (x._2._2.validEdges != null)
          x._2._2.validEdges.map( x => (x, true))
        else
          x._2._1.edgeSet.map( x => (x.id, true) )
      } )
      return graphData.graphRDD().flatMap(_._2.edgeSet.map( x => (x.id, x) )).join( validEdges ).map( _._2._1 ).collect().toIterator.asInstanceOf[Iterator[E]];
    }
    return null;
  }

  def createStep(nextState : RDD[(AnyRef, GremlinVertex)], newElementClass : Class[_]) : SparkGraphBulkData[E] = {
    return new SparkGraphBulkData[E](graphData, nextState, states, newElementClass);
  }

  def createStep(nextState : RDD[(AnyRef, GremlinVertex)], newStates : Array[String], newElementClass : Class[_]) : SparkGraphBulkData[E] = {
    return new SparkGraphBulkData[E](graphData, nextState, newStates, newElementClass);
  }

}

class SparkGremlinStartPipe[E <: SparkGraphElement](startGraph : SparkGraphElementSet[E]) extends BulkPipe[E,E] {
  def bulkReader(input: java.util.Iterator[E]): BulkPipeData[E] = {
    val y = input.asInstanceOf[SparkGraphElementSet[E]];
    return new SparkGraphBulkData[E](y, null, null, null);
  }
  override def bulkProcessStart() : BulkPipeData[E] = {
    starts = startGraph;
    bulkStarts = bulkReader(starts);
    return bulkProcess();
  }
  def bulkProcess(): BulkPipeData[E] = bulkStarts
}


class SparkGraphQueryPipe[E <: SparkGraphElement](cls : Class[_]) extends BulkPipe[Graph,E] {
  def bulkReader(input: java.util.Iterator[Graph]) : BulkPipeData[Graph] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess() : BulkPipeData[E] = {
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[E]];
    if (cls == classOf[SparkVertex]) {
      val active_ids = bs.graphData.elementRDD().map( x => (x.asInstanceOf[SparkVertex].id, true) );
      val out = bs.createStep( bs.graphData.graphRDD().join( active_ids ).map( x => (x._1, new GremlinVertex(1)) ), cls );
      return out;
    } else {
      val out = bs.createStep( bs.graphData.graphRDD().map( x => (x._1, new GremlinVertex(1)) ), cls );
      return out;
    }
  }

}


object SparkPropertyFilterPipe  {
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
    newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.getProperty(key), value) ).map( _.id ).toArray
    return newState;
  }

  def filterEdgesLabels( vert: SparkVertex, state : GremlinVertex, predicate: Predicate, value: AnyRef) : GremlinVertex = {
    val newState = new GremlinVertex();
    newState.travelers = state.travelers
    newState.validEdges = vert.edgeSet.filter( x => predicate.evaluate(x.label, value) ).map( _.id ).toArray
    return newState;
  }

  def filterEdgesID( vert:SparkVertex, state: GremlinVertex, predicate: Predicate, value: AnyRef) : GremlinVertex = {
    val newState = new GremlinVertex();
    val vid : java.lang.Long = value match {
      case x : java.lang.Long => x;
      case _ => value.toString.toLong
    }
    newState.travelers = state.travelers
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
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkVertex]]
    //println("Before", bs.graphState.filter(_._2.travelers != null).count)
    val edges = bs.graphData.graphRDD().map( x => (x._1, x._2.edgeSet.map(y => y.inVertexId) ) );
    val n = edges.join( bs.graphState ).flatMap( x => x._2._1.map( y => (y, x._2._2)) );
    val reduced = n.reduceByKey( (x,y) => GremlinVertex.merge(x, y) );
    //println("After", reduced.filter(_._2.travelers != null).count)
    return bs.createStep(reduced, classOf[SparkVertex]);
  }
}


class SparkGraphAsPipe[S,E](name: String) extends BulkPipe[S,E] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[E] = {
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[E]];
    val tname = name;
    val out = bs.graphData.graphRDD().join( bs.graphState ).map( x => (x._1, GremlinVertex.addAsColumn( x._2._2, tname, x._2._1 )) );
    if (bs.states != null )
      return bs.createStep(out, bs.states ++ Array(name), bs.elementClass);
    return bs.createStep(out, Array(name), bs.elementClass);
  }
}



class SparkGraphPropertyPipe[S](name:String) extends BulkPipe[S,AnyRef] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[AnyRef] = {
    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkVertex]];
    throw new RuntimeException("I have no idea what to do here");
  }
}


class SparkGraphTablePipe[S](table: Table, columnFunctions: SparkGremlinPipelineBase.PipeFunctionWrapper) extends BulkSideEffectPipe[S,Table] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[S] = {
    var currentFunction = 0;

    val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[S]];
    table.setColumnNames(bs.states:_*);
    val data = bs.graphState.filter(_._2.travelers != null ).flatMap( _._2.travelers ).collect()
      data.foreach( x => {
      val y = bs.states.map( z => x.asColumnMap(z) );
      val row = columnFunctions.rowCalc(currentFunction, new java.util.ArrayList(y.toList.asJava) );
      currentFunction += y.length;
      table.addRow(row)
    } )
    return bulkStarts;
  }



  def getSideEffect(): Table = {
    return table;
  }
}

class SingleElementBulkPipe[E](data:E) extends BulkPipeData[E] {
  def extract(): Iterator[E] = {
    val o = new ArrayBuffer[E]();
    o += data;
    return o.toIterator
  };
}

class SparkGraphCapPipe[S,E]() extends BulkPipe[S,E] {
  def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
    throw new SparkPipelineException(SparkPipelineException.NON_READER);
  }

  def bulkProcess(): BulkPipeData[E] = {
    if (!bulkStartPipe.isInstanceOf[BulkSideEffectPipe[S,E]]) {
      throw new RuntimeException("Cap not connected to side effect pipe");
    }
    return new SingleElementBulkPipe(bulkStartPipe.asInstanceOf[BulkSideEffectPipe[S,E]].getSideEffect());
  }

}
object SparkGremlinPipeline {
  val NOT_IMPLEMENTED_ERROR = "Not yet implemented"
}

class SparkGremlinPipeline[S, E](val start: AnyRef) extends SparkGremlinPipelineBase[S, E] with Iterable[E] {

  pipes = new java.util.ArrayList[Pipe[_, _]]();
  if (start.isInstanceOf[SparkGraphElementSet[_]]) {
    val sparkElementSet = start.asInstanceOf[SparkGraphElementSet[_]];
    if (sparkElementSet.elementClass() == classOf[SparkVertex]) {
      pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkVertex]]));
      pipes.add(new SparkGraphQueryPipe[SparkVertex](classOf[SparkVertex]));

    } else if (sparkElementSet.elementClass() == classOf[SparkEdge]) {
      pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkEdge]]));
      pipes.add(new SparkGraphQueryPipe[SparkEdge](classOf[SparkEdge]));
    }
  /*
  } else if (start.isInstanceOf[SparkGraphElementSet[SparkGraphElement]]) {
    pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkGraphElement]]));
    */
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

  def gather(): SparkGremlinPipeline[S, java.util.List[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def gather(function: PipeFunction[java.util.List[_], _]): SparkGremlinPipeline[S, _] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(namedStep: String): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(numberedStep: Int): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(namedStep: String, map: java.util.Map[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def memoize(numberedStep: Int, map: java.util.Map[_, _]): SparkGremlinPipeline[S, E] = {
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

  def path(pathFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, java.util.List[_]] = {
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

  def shuffle(): SparkGremlinPipeline[S, java.util.List[_]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  override def cap(): SparkGremlinPipeline[S, _] = {
    return this.add(new SparkGraphCapPipe());
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

  def map(keys: String*): SparkGremlinPipeline[S, java.util.Map[String, AnyRef]] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def property(key: String): SparkGremlinPipeline[S, AnyRef] = {
    return this.add(new SparkGraphPropertyPipe(key));
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

  def groupBy(map: java.util.Map[_, java.util.List[_]], keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(reduceMap: java.util.Map[_, _], keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _], reduceFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupBy(keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[_, _], reduceFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(map: java.util.Map[_, Number], keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[Pair[_, Number], Number]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(keyFunction: PipeFunction[_, _], valueFunction: PipeFunction[Pair[_, Number], Number]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(map: java.util.Map[_, Number], keyFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(keyFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def groupCount(map: java.util.Map[_, Number]): SparkGremlinPipeline[S, E] = {
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


  /*
  def table(inTable: Table, columnFunction: PipeFunction[_, _]): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR);
  }

  def table(table: Table, columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }
  */

  def table(columnFunctions: PipeFunction[_, _]*): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(table: Table): SparkGremlinPipeline[S, E] = {
    throw new RuntimeException(SparkGremlinPipeline.NOT_IMPLEMENTED_ERROR)
  }

  def table(): SparkGremlinPipeline[S, E] = {
    return __table(null, null);
  }

  def __table(table: Table, columnFunctions: SparkGremlinPipelineBase.PipeFunctionWrapper): SparkGremlinPipeline[S, E] = {
    return this.add(new SparkGraphTablePipe(table, columnFunctions) );
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
    return this.add(new SparkGraphAsPipe(name));
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