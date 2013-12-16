package sparkgremlin.blueprints

import com.tinkerpop.blueprints._
import com.tinkerpop.blueprints.util.wrappers.readonly.{ReadOnlyGraph,ReadOnlyVertex}
import com.tinkerpop.blueprints.Compare

import java.lang.Iterable

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import collection.JavaConverters._

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import scala.util.Random




class PropertyMapper(val id:AnyRef) extends Serializable {

  val propMap = new HashMap[String,Any]();

  def setProperty(key: String, value: scala.AnyRef) = {
    if (key == null || key.length == 0) {
      throw new IllegalArgumentException("Invalid Key String");
    }
    if (value == null) {
      throw new IllegalArgumentException("Invalid Property Value");
    }
    propMap(key) = value;
  }

  def getProperty[T](key: String): T = {
    propMap.get(key).getOrElse(null).asInstanceOf[T];
  }

  def getPropertyKeys: java.util.Set[String] = propMap.keySet.asJava;

  def removeProperty[T](key: String): T = {
    return propMap.remove(key).orNull.asInstanceOf[T];
  }

  def labelMatch(args:String*) : Boolean = {
    if (args.length == 0) {
      return true;
    }
    if (args.length == 1) {
      return id == args(0);
    }
    if (args.length == 2) {
      return propMap.getOrElse(args(0), null) == args(1);
    }
    return false;
  }

}

class SparkEdge(override val id:AnyRef, val outVertexId: AnyRef, val inVertexId:AnyRef, val label:String, @transient inGraph:SparkGraph ) extends PropertyMapper(id) with Edge with Serializable {
  @transient var graph : SparkGraph = inGraph;
  def getId: AnyRef = id;

  def setGraph(inGraph:SparkGraph) = { graph = inGraph };

  override def equals(other: Any) = other match {
    case that: SparkEdge => (this.id == that.id)
    case _ => false
  }

  override def hashCode() = id.hashCode

  override def setProperty(key:String, value:AnyRef) = {
    if (key == "id" || key == "label") {
      throw new IllegalArgumentException("Invalid Key String");
    }
    super.setProperty(key,value);
    if (graph != null) {
      graph.updates += new EdgePropertyBuild(id, outVertexId, inVertexId, key, value);
    }
  }

  def remove() = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    graph.updates += new EdgeRemoveBuild(id, outVertexId);
  }

  def getVertex(direction: Direction): Vertex = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    val out = new ArrayBuffer[Vertex]();
    if (direction == Direction.IN) {
      return graph.getVertex(inVertexId);
    } else if (direction == Direction.OUT) {
      return graph.getVertex(outVertexId);
    }
    throw new IllegalArgumentException("Bad Edge Direction")
  }

  override def labelMatch(args:String*) : Boolean = {
    if (args.length == 1)
      return label == args(0)
    return super.labelMatch(args:_*);
  }

  def getLabel: String = label;

}

class SparkVertex(override val id:AnyRef, @transient inGraph:SparkGraph) extends PropertyMapper(id) with Vertex with Serializable {
  @transient var graph : SparkGraph = inGraph;
  val edgeSet = new ArrayBuffer[SparkEdge]();

  override def setProperty(key:String, value:AnyRef) = {
    if (key == null || key.length == 0 || key == "id") {
      throw new IllegalArgumentException("Bad Property Key");
    }
    super.setProperty(key,value);
    if (graph != null) {
      graph.updates += new VertexPropertyBuild(id, key, value);
    }
  }

  override def equals(other: Any) = other match {
    case that: SparkVertex => (this.id == that.id)
    case _ => false
  }

  override def hashCode() = id.hashCode

  def getId: java.lang.Object = id;

  def remove() = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    graph.updates += new VertexRemoveBuild(id);
  }

  def addEdge(label: String, inVertex: Vertex): Edge = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    if (label == null) {
      throw new IllegalArgumentException("Adding unlabed edge");
    }
    val edgeId = new java.lang.Long(Random.nextLong());
    graph.updates += new EdgeBuild(edgeId, id, inVertex.getId, label);
    return new SparkEdge(edgeId, id, inVertex.getId, label, graph);
  }

  def query(): VertexQuery = {
    return new SparkVertexQuery(id, graph);
  }

  def getVertices(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Vertex] = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.NOT_READY_MESSAGE);
    }
    val idSet = new ArrayBuffer[AnyRef]();
    if ( direction == Direction.IN || direction == Direction.BOTH ) {
      var incoming = graph.curgraph.flatMap( x => x._2.edgeSet.filter( _.inVertexId == id ) );
      if (labels.length > 0) {
        incoming = incoming.filter( _.labelMatch(labels:_*) );
      }
      idSet ++= incoming.map( _.outVertexId ).collect();
      //println("OUT IDS:" + idSet)
    }
    if ( direction == Direction.OUT || direction == Direction.BOTH ) {
      var outgoing = graph.curgraph.lookup(id).head.edgeSet;
      if (labels.length > 0) {
        outgoing = outgoing.filter(_.labelMatch(labels:_*));
      }
      idSet ++= outgoing.map( _.inVertexId )
      //println("IN IDS:" + idSet)
    }
    val verts = graph.curgraph.filter( x => idSet.contains(x._1)  ).collect()
    val out = idSet.flatMap( x => verts.filter( y => x == y._1) );
    return out.map( x => { val y = x._2; y.graph = graph; y.asInstanceOf[Vertex]; } ).toIterable.asJava;
  }


  def getEdges(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Edge] = {
    if (graph == null) {
      throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
    }
    graph.flushUpdates();

    val out = new ArrayBuffer[SparkEdge]();
    if (direction == Direction.OUT || direction == Direction.BOTH) {
      var outgoing = graph.curgraph.filter( x => x._1 == id ).flatMap( x => x._2.edgeSet );
      if (labels.length > 0) {
        outgoing = outgoing.filter( x=>labels.contains(x.label) );
      }
      //println("Outgoing:" + outgoing.count())
      out ++= outgoing.collect();
    }
    if (direction == Direction.IN || direction == Direction.BOTH) {
      var incoming = graph.curgraph.flatMap( x => x._2.edgeSet.filter(x => x.inVertexId == id ))
      if (labels.length > 0) {
        incoming = incoming.filter( x=>labels.contains(x.label) );
      }
      //println("Incoming:" + incoming.count())
      out ++= incoming.collect();
    }
    return out.map( x => { x.graph = graph; x.asInstanceOf[Edge]; } ).toIterable.asJava;
  }
}

class SparkVertexQuery(val vertexId:AnyRef, val graph:SparkGraph) extends VertexQuery {
  def direction(direction: Direction): VertexQuery = null

  def labels(labels:String*): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def count(): Long = {
    throw new IllegalArgumentException("Null ID value");
  }

  def vertexIds(): AnyRef = {
    throw new IllegalArgumentException("Null ID value");
  }

  def has(key: String): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def hasNot(key: String): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def has(key: String, value: scala.Any): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def hasNot(key: String, value: scala.Any): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def has(key: String, predicate: Predicate, value: scala.Any): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def has[T <: Comparable[T]](key: String, value: T, compare: Query.Compare): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def interval[T <: Comparable[_]](key: String, startValue: T, endValue: T): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def limit(limit: Int): VertexQuery = {
    throw new IllegalArgumentException("Null ID value");
  }

  def edges(): Iterable[Edge] = {
    throw new IllegalArgumentException("Null ID value");
  }

  def vertices(): Iterable[Vertex] = {
    throw new IllegalArgumentException("Null ID value");
  }
}


class HasContainer(val key : String, val predicate: Predicate, val value:AnyRef) extends Serializable {
  def isLegal(element:Element) : Boolean = {
    return this.predicate.evaluate(element.getProperty(this.key), this.value);
  }
}


object SparkGraphQuery {
  def containCheck(value:String, valueSet:AnyRef) : Boolean = {
    valueSet match {
      case _ : List[String] => {
        valueSet.asInstanceOf[List[String]].contains(value)
      }
      case _ : java.util.List[String] => {
        valueSet.asInstanceOf[java.util.List[String]].asScala.contains(value)
      }
      case _ =>  {
        throw new IllegalArgumentException( "Missing Comparison: " + valueSet.getClass  )
      }
    }
  }
}

class SparkGraphQuery(val graph:SparkGraph) extends GraphQuery {

  //private static final String[] EMPTY_LABELS = new String[]{};

  var direction : Direction = Direction.BOTH;
  var labels = ArrayBuffer[String]();
  var limit = Integer.MAX_VALUE;
  var hasContainers = new ArrayBuffer[HasContainer]();

  def has(key: String): GraphQuery = {
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.NOT_EQUAL, null));
    return this;
  }

  def hasNot(key: String): GraphQuery = {
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.EQUAL, null));
    return this;
  }

  def has(key: String, value: scala.AnyRef): GraphQuery = {
    this.hasContainers += new HasContainer(key, com.tinkerpop.blueprints.Compare.EQUAL, value);
    return this;
  }

  def hasNot(key: String, value: scala.AnyRef): GraphQuery = {
    this.hasContainers += new HasContainer(key, com.tinkerpop.blueprints.Compare.NOT_EQUAL, value);
    return this;
  }

  def has(key: String, predicate: Predicate, value: scala.AnyRef): GraphQuery = {
    this.hasContainers += (new HasContainer(key, predicate, value));
    return this;
  }

  def has[T <: Comparable[T]](key: String, value: T, compare: Query.Compare): GraphQuery = {
    return this.has(key, compare, value);
  }

  def interval[T <: Comparable[_]](key: String, startValue: T, endValue: T): GraphQuery = {
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.GREATER_THAN_EQUAL, startValue));
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.LESS_THAN, endValue));
    return this;
  }

  def limit(count: Int): GraphQuery = {
    this.limit = count;
    return this;
  }



  def edges(): Iterable[Edge] = {
    graph.flushUpdates();
    hasContainers.foreach( println );
    var rdd = graph.curgraph.flatMap( _._2.edgeSet );
    println("Count:" + rdd.count())
    for ( has <- hasContainers ) {
      rdd = has.predicate match {
        case Compare.EQUAL => {
          has.value match {
            case null => rdd.filter( !_.propMap.contains(has.key) );
            case _ => rdd.filter( _.propMap(has.key) == has.value )
          }
        }
        case Compare.NOT_EQUAL => {
          has.value match {
            case null => rdd.filter( _.propMap.contains(has.key));
            case _ => rdd.filter( _.propMap(has.key) != has.value);
          }
        }
        case Contains.IN => {
          println("Container:" + has.value)
          rdd.filter( x => SparkGraphQuery.containCheck(x.propMap(has.key).asInstanceOf[String], has.value) )
        }
        case _ => {
          throw new IllegalArgumentException( "Missing Comparison: " + has.predicate); // + " " + has.value.getClass  )
        }
      }
      println("Count:" + rdd.count())
    }
    return rdd.collect().slice(0, limit).map( x => { x.graph = graph; x.asInstanceOf[Edge]} ).toIterable.asJava;
  }

  def vertices(): Iterable[Vertex] = {
    throw new IllegalArgumentException("Not Yet Implemented");
  }
}

object SparkGraph {
  val FEATURES = new Features();
  FEATURES.supportsDuplicateEdges = true;
  FEATURES.supportsSelfLoops = true;
  FEATURES.isPersistent = false;
  FEATURES.supportsVertexIteration = true;
  FEATURES.supportsEdgeIteration = true;
  FEATURES.supportsVertexIndex = false;
  FEATURES.supportsEdgeIndex = false;
  FEATURES.ignoresSuppliedIds = true;
  FEATURES.supportsEdgeRetrieval = true;
  FEATURES.supportsVertexProperties = true;
  FEATURES.supportsEdgeProperties = true;
  FEATURES.supportsTransactions = false;
  FEATURES.supportsIndices = false;

  FEATURES.supportsSerializableObjectProperty = true;
  FEATURES.supportsBooleanProperty = true;
  FEATURES.supportsDoubleProperty = true;
  FEATURES.supportsFloatProperty = true;
  FEATURES.supportsIntegerProperty = true;
  FEATURES.supportsPrimitiveArrayProperty = true;
  FEATURES.supportsUniformListProperty = true;
  FEATURES.supportsMixedListProperty = true;
  FEATURES.supportsLongProperty = true;
  FEATURES.supportsMapProperty = true;
  FEATURES.supportsStringProperty = true;

  FEATURES.isWrapper = true;
  FEATURES.supportsKeyIndices = false;
  FEATURES.supportsVertexKeyIndex = false;
  FEATURES.supportsEdgeKeyIndex = false;
  FEATURES.supportsThreadedTransactions = false;

  val READ_ONLY_MESSAGE = "SparkGraph is ReadOnly";
  val WRITE_ONLY_MESSAGE = "InputGraph for Write Only"
  val NOT_READY_MESSAGE = "Element not connected to Graph"

  def generate(sc:SparkContext) : SparkGraph = {
    return new SparkGraph(sc.parallelize(Array[(AnyRef,SparkVertex)]()));
  }
}
class SparkGraph(graph:RDD[(AnyRef,SparkVertex)]) extends Graph {
  println("Setting up Graph")
  var curgraph : RDD[(AnyRef,SparkVertex)] = graph.persist();

  var updates = new ArrayBuffer[BuildElement]();

  override def toString() = "sparkgraph[nodes=" + curgraph.count + "]"

  def getFeatures: Features = SparkGraph.FEATURES;

  def flushUpdates() : Boolean = {
    //updates.foreach(x => println("UPDATE: " + x + " " + x.getVertexId))
    if (updates.length == 0) {
      return false;
    }
    val u = graph.sparkContext.parallelize(updates).map( x => (x.getVertexId, x) ).groupByKey().map( x => (x._1, SparkGraphBuilder.vertexBuild(x._1, x._2)) );
    val nextgraph = curgraph.cogroup( u ).map( x => (x._1, SparkGraphBuilder.mergeVertex( x._2._1, x._2._2 ) ) ).filter(x => x._2 != null).persist();
    //val nextgraph = curgraph.union(u).groupByKey().map( x => (x._1, SparkGraphBuilder.mergeVertex(x._2)) ).filter(x => x._2 != null).persist();
    nextgraph.count();
    curgraph.unpersist();
    curgraph = nextgraph;
    updates.clear();
    return true;
  }

  def getEdge(id: scala.Any): Edge = {
    if (id == null) {
      throw new IllegalArgumentException("Null ID value");
    }
    flushUpdates();
    val set = curgraph.flatMap( x => x._2.edgeSet.filter( _.id == id ) ).collect();
    if (set.length == 0) {
      return null;
    }
    val out = set.head;
    out.graph = this;
    return out.asInstanceOf[Edge];
  }

  def getVertex(id: AnyRef): Vertex = {
    if (id == null) {
      throw new IllegalArgumentException();
    }
    flushUpdates();
    val set = curgraph.lookup(id);
    if (set.length > 0) {
      val out = set.head;
      out.graph = this;
      return out;
    }
    return null;
  }

  def shutdown() = curgraph.unpersist()

  def removeEdge(edge: Edge) {
    updates += new EdgeRemoveBuild(edge.getId, edge.asInstanceOf[SparkEdge].outVertexId);
  }

  def removeVertex(vertex: Vertex) {
    //println("Remove: " + vertex.getId )
    updates += new VertexRemoveBuild(vertex.getId);
  }

  def addVertex(id: scala.AnyRef): Vertex =  {
    //println("Add Vertex: " + id)
    val u : AnyRef = id match {
      case null => (new java.lang.Long(Random.nextLong())).toString;
      case _ => id;
    }
    updates += new VertexBuild(u);
    return new SparkVertex(u, this);
  }

  def getVertices(key: String, value: scala.Any): Iterable[Vertex] = {
    flushUpdates();
    return curgraph.filter(x => x._2.getProperty(key) == value).map(_._2.asInstanceOf[Vertex]).collect().toIterable.asJava;
  }

  def addEdge(id: scala.AnyRef, outVertex: Vertex, inVertex: Vertex, label: String): Edge = {
    //println("Add Edge: " + id)
    if (label == null) {
      throw new IllegalArgumentException("Null Label");
    }
    val u : AnyRef = id match {
      case null => new java.lang.Long(Random.nextLong());
      case _ => id;
    }
    updates += new EdgeBuild(u, outVertex.getId, inVertex.getId, label)
    return new SparkEdge(u, outVertex.getId, inVertex.getId, label, this);
  }

  def getEdges: Iterable[Edge] = {
    flushUpdates()
    return curgraph.flatMap( x => x._2.edgeSet ).collect.map( x => {
      x.graph = this;
      x.asInstanceOf[Edge]
    }
    ).toIterable.asJava;
  }

  def getEdges(key: String, value: scala.Any): Iterable[Edge] = {
    flushUpdates()
    return curgraph.flatMap( x => x._2.edgeSet ).filter( _.labelMatch(key, value.toString) ).collect.map( x => {
      x.graph = this;
      x.asInstanceOf[Edge]
    }
    ).toIterable.asJava;
  }

  def query(): GraphQuery = {
    return new SparkGraphQuery(this);
  }

  def getVertices: Iterable[Vertex] = {
    flushUpdates();
    return curgraph.map(x => x._2.asInstanceOf[Vertex]).collect().map( x => {
      val y = x.asInstanceOf[SparkVertex];
      y.graph = this;
      y.asInstanceOf[Vertex]
    }
    ).toIterable.asJava;
  }

}
