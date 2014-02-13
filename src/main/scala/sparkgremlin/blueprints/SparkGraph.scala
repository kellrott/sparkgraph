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
import org.apache.spark.storage.StorageLevel
import sparkgremlin.blueprints.io._
import sparkgremlin.blueprints.io.build._

import org.apache.spark.graphx.impl.{GraphImpl => GraphXImplGraph, EdgePartitionBuilder}
import org.apache.spark.graphx.{Graph => GraphXGraph, Edge => GraphXEdge, VertexRDD, EdgeRDD}
import scala.reflect.ClassTag
import com.tinkerpop.blueprints.Edge

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
    return new SparkGraph(sc.parallelize(Array[(Long,SparkVertex)]()), sc.parallelize(Array[SparkEdge]()));
  }
}

class SparkGraph(vertices:RDD[(Long,SparkVertex)], edges:RDD[SparkEdge], defaultStorage: StorageLevel) extends Graph with SparkGraphElementSet[SparkGraphElement] {

  def this(vertices:RDD[(Long,SparkVertex)], edges:RDD[SparkEdge]) = {
    this(vertices, edges, StorageLevel.MEMORY_ONLY)
  }
  //val vertRDD = VertexRDD(vertices);
  //val edgeRDD = createEdgeRDD(edges)
  var graph : GraphXGraph[SparkVertex,SparkEdge] = GraphXGraph(vertices, edges.asInstanceOf[RDD[GraphXEdge[AnyRef]]]).asInstanceOf[GraphXGraph[SparkVertex,SparkEdge]];

  /*
  def this(graph:RDD[(Long,SparkVertex)]) = {
    this(graph, StorageLevel.MEMORY_ONLY)
  }
  */

  //var curgraph : RDD[(Long,SparkVertex)] = graph.persist(defaultStorage);
  var updates = new ArrayBuffer[BuildElement]();

  override def toString() = "sparkgraph[nodes=" + graph.vertices.count + "]"

  def getFeatures: Features = SparkGraph.FEATURES;

  def flushUpdates() : Boolean = {
    if (updates.length == 0) {
      return false;
    }
    val u = graph.vertices.sparkContext.parallelize(updates)

    val newVertex = u.filter( ! _.isEdge ).map( x => (x.getVertexId.asInstanceOf[Long], x) ).groupByKey().map( x => (x._1, SparkGraphBuilder.vertexBuild(x._1, x._2)) );
    val newEdges =  u.filter( ! _.isEdge ).map( x => (x.getEdgeId.asInstanceOf[Long], x)).groupByKey().map( x => (x._1, SparkGraphBuilder.edgeBuild(x._1, x._2)));

    val nextVerts = graph.vertices.cogroup( newVertex ).map( x => (x._1, SparkGraphBuilder.mergeVertex( x._2._1, x._2._2 ) ) ).filter(x => x._2 != null)
    val nextEdges = graph.edges.map( x => (x.asInstanceOf[SparkEdge].id, x.asInstanceOf[SparkEdge]) ).cogroup(newEdges).map( x => SparkGraphBuilder.mergeEdge(x._2._1, x._2._2) ).filter( _ != null )

    graph = GraphXGraph(nextVerts, nextEdges.asInstanceOf[RDD[GraphXEdge[AnyRef]]]).asInstanceOf[GraphXGraph[SparkVertex,SparkEdge]];
    updates.clear();
    return true;
  }

  /**
   *
   * @param id
   * @return
   */
  def getEdge(id: Any): Edge = {
    if (id == null) {
      throw new IllegalArgumentException("Null ID value");
    }
    flushUpdates();
    val set = graph.edges.filter( x => x.asInstanceOf[SparkEdge].getId() == id ).collect();
    if (set.length == 0) {
      return null;
    }
    val out = set.head.asInstanceOf[SparkEdge]
    out.graph = this;
    return out.asInstanceOf[Edge];
  }

  /**
   *
   * @param id
   * @return
   */
  def getVertex(id: Any): Vertex = {
    if (id == null) {
      throw new IllegalArgumentException();
    }
    flushUpdates();
    try {
      val lid : java.lang.Long = id match {
        case x : java.lang.Long => id.asInstanceOf[java.lang.Long]
        case _ => id.toString.toLong
      }
      val set = graph.vertices.lookup(lid);
      if (set.length > 0) {
        val out = set.head;
        out.graph = this;
        return out;
      }
    } catch {
      case _ : java.lang.NumberFormatException => return null;
    }

    return null;
  }

  def shutdown() = graph = null

  /**
   *
   * @param edge
   */
  def removeEdge(edge: Edge) {
    updates += new EdgeRemoveBuild(edge.getId.asInstanceOf[Long], edge.asInstanceOf[SparkEdge].outVertexId);
  }

  /**
   *
   * @param vertex
   */
  def removeVertex(vertex: Vertex) {
    updates += new VertexRemoveBuild(vertex.getId.asInstanceOf[Long]);
  }

  /**
   *
   * @param id
   * @return The newly created vertex
   */
  def addVertex(id: scala.AnyRef): Vertex =  {
    val u : java.lang.Long = id match {
      case x : java.lang.Long => x;
      case null => new java.lang.Long(Random.nextLong()); //(new java.lang.Long(Random.nextLong())).toString;
      case _ => {
        try {
          id.toString.toLong
        } catch {
          case _ : java.lang.NumberFormatException => new java.lang.Long(Random.nextLong())
        }
      }
    }
    updates += new VertexBuild(u);
    return new SparkVertex(u, this);
  }

  def getVertices(key: String, value: scala.Any): Iterable[Vertex] = {
    flushUpdates();
    return graph.vertices.filter(x => x._2.getProperty(key) == value).map(_._2.asInstanceOf[Vertex]).collect().toIterable.asJava;
  }

  def addEdge(id: Any, outVertex: Vertex, inVertex: Vertex, label: String): Edge = {
    //println("Add Edge: " + id)
    if (label == null) {
      throw new IllegalArgumentException("Null Label");
    }
    val u : Any = id match {
      case null => new java.lang.Long(Random.nextLong());
      case _ => {
        try {
          id.toString.toLong
        } catch {
          case _ : java.lang.NumberFormatException => new java.lang.Long(Random.nextLong())
        }
      }
    }
    updates += new EdgeBuild(u.asInstanceOf[Long], outVertex.getId.asInstanceOf[Long], inVertex.getId.asInstanceOf[Long], label)
    return new SparkEdge(u.asInstanceOf[Long], outVertex.getId.asInstanceOf[Long], inVertex.getId.asInstanceOf[Long], label, this);
  }

  def getEdges: Iterable[Edge] = {
    flushUpdates();
    val out = graph.edges.map( x => x.attr );
    return new SimpleGraphElementSet[SparkEdge](this, out, classOf[SparkEdge]).asInstanceOf[Iterable[Edge]];
  }

  def getEdges(key: String, value: scala.Any): Iterable[Edge] = {
    flushUpdates();
    val out = graph.edges.filter( _.attr.labelMatch(key, value.toString) );
    return new SimpleGraphElementSet[Edge](this, out.map(_.asInstanceOf[SparkEdge]), classOf[SparkEdge]);
  }

  def query(): GraphQuery = {
    return new SparkGraphQuery(this);
  }

  def getVertices: java.lang.Iterable[Vertex] = {
    flushUpdates();
    return new SimpleGraphElementSet[Vertex](this, graph.vertices.map( _._2.asInstanceOf[SparkVertex] ), classOf[SparkVertex] );
  }

  def elementClass() : Class[_] = classOf[SparkVertex];

  def iterator(): java.util.Iterator[SparkGraphElement] = this;

  var graphCollect : Array[SparkGraphElement] = null;
  var graphCollectIndex = 0;

  def hasNext: Boolean = {
    if (graphCollect == null) {
      graphCollect = elementRDD().collect();
      graphCollectIndex = 0;
    }
    return graphCollectIndex < graphCollect.length;
  }

  def next(): SparkGraphElement = {
    if (graphCollect != null) {
      val out = graphCollect(graphCollectIndex);
      graphCollectIndex += 1
      out;
    } else {
      null
    }
  }

  def remove() = {};
  def graphX(): GraphXGraph[SparkVertex,SparkEdge] = {
    flushUpdates()
    graph
  };

  def elementRDD(): RDD[SparkGraphElement] = {
    //return curgraph.flatMap( x => x._2.edgeSet.map( _.asInstanceOf[SparkGraphElement] ) ).union( curgraph.map( _.asInstanceOf[SparkGraphElement]) );
    return graph.vertices.map( _._2 )
  }
}
