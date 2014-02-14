package sparkgremlin.blueprints.io.build

import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader
import com.tinkerpop.blueprints._
import java.lang.Iterable


import collection.JavaConverters._
import java.io.{InputStream, File, FileInputStream}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashSet
import sparkgremlin.blueprints.{SparkEdge, SparkVertex, SparkGraph}


object SparkGraphBuilder {

  /**
   * This takes a set of built instructions and compiles them into a 'BuiltVertex' or a 'DeletedVertex'
   * These are later merged with the existing set of vertices in 'mergeVertex', with the 'DeletedVertex'
   * objects acting as an instruction to remove vertices
   * @param id
   * @param buildSeq
   * @return
   */
  def vertexBuild(id:Long, buildSeq:Seq[BuildElement]) : SparkVertex = {
    for (b <- buildSeq) {
      if (b.isRemoval && !b.isEdge) {
        return new DeletedVertex(id);
      }
    }

    //Check if there was an actual request to build the vertex, because it
    //could be a bunch of property additions
    val wasBuilt = buildSeq.filter( _.isInstanceOf[VertexBuild] ).length > 0;
    val out = new BuiltVertex(id, wasBuilt);
    for (b <- buildSeq) {
      if (!b.isEdge) {
        if (b.isProperty) {
          out.setProperty(b.getKey, b.getValue.asInstanceOf[AnyRef]);
        }
      }
    }
    return out;
  }

  def edgeBuild(id:Long, buildSeq:Seq[BuildElement]) : SparkEdge = {
    for (b <- buildSeq) {
      if (b.isRemoval && b.isEdge) {
        return new DeletedEdge(id);
      }
    }

    val wasBuilt = buildSeq.filter( _.isInstanceOf[EdgeBuild] ).length > 0;
    val out = if (wasBuilt) {
      val b = buildSeq.filter( _.isInstanceOf[EdgeBuild] )(0);
      new BuiltEdge(id, true, b.getVertexId.asInstanceOf[Long], b.getVertexInId.asInstanceOf[Long], b.getLabel)
    } else {
      new BuiltEdge(id, false);
    }

    for (b <- buildSeq) {
      if (b.isEdge) {
        if (b.isProperty) {
          out.setProperty(b.getKey, b.getValue.asInstanceOf[AnyRef]);
        }
      }
    }
    return out;
  }

  def mergeVertex(originalVertexSet:Seq[SparkVertex], newVertexSet:Seq[SparkVertex]) : SparkVertex = {
    if ( !(originalVertexSet.length == 0 || originalVertexSet.length == 1) || !(newVertexSet.length == 0 || newVertexSet.length == 1) ) {
      return null;
    }

    val newVertex = newVertexSet.length match {
      case 0 => null : SparkVertex;
      case 1 => newVertexSet.head
    }

    val originalVertex = originalVertexSet.length match {
      case 0 => null : SparkVertex;
      case 1 => originalVertexSet.head
    }

    if (newVertex != null && newVertex.isInstanceOf[DeletedVertex]) {
      return null;
    }
    if (originalVertex == null && newVertex != null && !newVertex.asInstanceOf[BuiltVertex].wasBuilt ) {
      return null;
    }

    var out : SparkVertex = if (originalVertex == null) {
      new SparkVertex(newVertex.getId.asInstanceOf[Long], null);
    } else {
      val tmp = new SparkVertex(originalVertex.getId.asInstanceOf[Long], null);
      for ( k <- originalVertex.getPropertyKeys.asScala) {
        tmp.setProperty(k, originalVertex.getProperty(k));
      }
      tmp;
    }
    if (newVertex != null) {
      for ( k <- newVertex.getPropertyKeys.asScala) {
        out.setProperty(k, newVertex.getProperty(k));
      }
    }
    return out;
  }

  def mergeEdge(originalEdgeSet:Seq[SparkEdge], newEdgeSet:Seq[SparkEdge]) : SparkEdge = {
    if ( !(originalEdgeSet.length == 0 || originalEdgeSet.length == 1) || !(newEdgeSet.length == 0 || newEdgeSet.length == 1) ) {
      return null;
    }

    val newEdge = newEdgeSet.length match {
      case 0 => null : SparkEdge;
      case 1 => newEdgeSet.head
    }

    val originalEdge = originalEdgeSet.length match {
      case 0 => null : SparkEdge;
      case 1 => originalEdgeSet.head
    }

    if (newEdge != null && newEdge.isInstanceOf[DeletedEdge]) {
      return null;
    }
    if (originalEdge == null && newEdge != null && !newEdge.asInstanceOf[BuiltEdge].wasBuilt ) {
      return null;
    }

    var out : SparkEdge = if (originalEdge == null) {
      new SparkEdge(newEdge.getId.asInstanceOf[Long], newEdge.outVertexId, newEdge.inVertexId, newEdge.label, null);
    } else {
      val tmp = new SparkEdge(originalEdge.getId.asInstanceOf[Long], originalEdge.outVertexId, originalEdge.inVertexId, originalEdge.label, null);
      for ( k <- originalEdge.getPropertyKeys.asScala) {
        tmp.setProperty(k, originalEdge.getProperty(k));
      }
      tmp;
    }
    if (newEdge != null) {
      for ( k <- newEdge.getPropertyKeys.asScala) {
        out.setProperty(k, newEdge.getProperty(k));
      }
    }
    return out;
  }

  def buildGraph(sc:SparkContext, input:Iterator[BuildElement]) : SparkGraph = {
    val u = sc.parallelize(input.toSeq)
    val newVertex = u.filter( ! _.isEdge ).map( x => (x.getVertexId.asInstanceOf[Long], x) ).groupByKey().map( x => (x._1, SparkGraphBuilder.vertexBuild(x._1, x._2)) );
    val newEdges =  u.filter( ! _.isEdge ).map( x => (x.getEdgeId.asInstanceOf[Long], x)).groupByKey().map( x => SparkGraphBuilder.edgeBuild(x._1, x._2));
    return new SparkGraph(newVertex, newEdges);
  }
}