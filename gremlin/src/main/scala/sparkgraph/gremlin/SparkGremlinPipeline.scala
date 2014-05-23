package sparkgraph.gremlin

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
import sparkgraph.blueprints._
import sparkgraph.gremlin.pipe._


object SparkGremlinPipeline {
  val NOT_IMPLEMENTED_ERROR = "Not yet implemented"
}

class SparkGremlinPipeline[S, E](val start: AnyRef) extends SparkGremlinPipelineBase[S, E] with Iterable[E] {

  pipes = new java.util.ArrayList[Pipe[_, _]]();
  if (start != null) {
    if (start.isInstanceOf[SparkGraphElementSet[_]]) {
      if (start.isInstanceOf[SparkGraph]) {
        pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkVertex]]));
      } else {
        val sparkElementSet = start.asInstanceOf[SparkGraphElementSet[_]];
        if (sparkElementSet.elementClass() == classOf[SparkVertex]) {
          pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkVertex]]));
          pipes.add(new SparkGraphQueryPipe[SparkVertex](BulkDataType.VERTEX_DATA));
        } else if (sparkElementSet.elementClass() == classOf[SparkEdge]) {
          pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkEdge]]));
          pipes.add(new SparkGraphQueryPipe[SparkEdge](BulkDataType.EDGE_DATA));
        } else {
          throw new RuntimeException("Unable to init pipeline")
        }
      }
      /*
    } else if (start.isInstanceOf[SparkGraphElementSet[SparkGraphElement]]) {
      pipes.add(new SparkGremlinStartPipe(start.asInstanceOf[SparkGraphElementSet[SparkGraphElement]]));
      */
    } else if (start.isInstanceOf[SparkVertex]) {
      val sparkStart = start.asInstanceOf[SparkVertex]
      val startID = sparkStart.getId
      pipes.add(new SparkGremlinStartPipe(new SimpleGraphElementSet[SparkVertex,SparkVertex](sparkStart.graph, classOf[SparkVertex], x => x.getID == startID)));
      pipes.add(new SparkGraphQueryPipe[SparkVertex](BulkDataType.VERTEX_DATA));
    } else {
      throw new RuntimeException("Unable to init pipeline")
    }
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
    val out = new SparkGremlinPipeline[S,T](null)
    this.pipes.asScala.foreach(out.addPipe(_))
    out.addPipe(pipe)
    return out
  }

  /**
    */
  def V: SparkGremlinPipeline[S, SparkVertex] = {
    add( new SparkGraphQueryPipe[SparkVertex](BulkDataType.VERTEX_DATA) )
  }


  /**
    */
  def E: SparkGremlinPipeline[S, SparkEdge] = {
    add( new SparkGraphQueryPipe[SparkEdge](BulkDataType.EDGE_DATA) )
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
    return this.add(new SparkGraphBackPipe(namedStep))
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
    return this.add(new SparkGraphConnectedVertex(Direction.IN, Integer.MAX_VALUE, null)).asInstanceOf[SparkGremlinPipeline[S,Vertex]];
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

  def out(label: String): SparkGremlinPipeline[S,Vertex] = {
    return this.out(Integer.MAX_VALUE, label)
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

  def rdd[S]() : RDD[Map[String,AnyRef]] = {
    if (endPipe.isInstanceOf[BulkPipe[_,S]]) {
      return RDDUtil.pipe2rdd(endPipe.asInstanceOf[BulkPipe[_,S]].bulkProcessStart().asInstanceOf[SparkGraphBulkData[E]])
    }
    return null
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

  def __underscore() : SparkGremlinPipeline[S, E] = {
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

  override def count() : scala.Long = {
    if (endPipe.isInstanceOf[BulkPipe[_,_]]) {
      return endPipe.asInstanceOf[BulkPipe[_,_]].bulkProcessStart().count()
    }
    return super.count()
  }

}