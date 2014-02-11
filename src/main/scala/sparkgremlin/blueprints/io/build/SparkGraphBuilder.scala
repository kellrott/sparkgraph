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
  def vertexBuild(id:AnyRef, buildSeq:Seq[BuildElement]) : SparkVertex = {
    //println("VertexBuild: " + id )
    val wasBuilt = buildSeq.filter( _.isInstanceOf[VertexBuild] ).length > 0;
    val out = new SparkVertexBuilt(id, wasBuilt);

    val edgeremove = new HashSet[AnyRef]();
    for (b <- buildSeq) {
      if (b.isRemoval && b.isEdge) {
        edgeremove += b.getEdgeId;
      }
    }

    for (b <- buildSeq) {
      if (b.isRemoval && !b.isEdge) {
        return new DeletedVertex(id);
      }
      if (b.isEdge && !b.isProperty) {
        if ( !edgeremove.contains(b.getEdgeId)) {
          out.edgeSet += new SparkEdge(b.getEdgeId, b.getVertexId, b.getVertexInId, b.getLabel, null, null, null);
        } else {
          out.edgeSet += new DeletedEdge(b.getEdgeId);
        }
      }
    }

    for (b <- buildSeq) {
      if (b.isProperty) {
        if (b.isEdge) {
          out.edgeSet.foreach( x => {
            if (x.id == b.getEdgeId) {
              x.setProperty(b.getKey, b.getValue.asInstanceOf[AnyRef]);
            }
          });
        } else {
          out.setProperty(b.getKey, b.getValue.asInstanceOf[AnyRef]);
        }
      }
    }
    //println(out.getPropertyKeys.toArray().mkString(" "))
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
    if (originalVertex == null && newVertex != null && !newVertex.asInstanceOf[SparkVertexBuilt].wasBuilt ) {
      return null;
    }
    val rmSet = if (newVertex != null)
      newVertex.edgeSet.filter( _.isInstanceOf[DeletedEdge]).map(_.id).toSet;
    else
      Set[AnyRef]()
    var out : SparkVertex = if (originalVertex == null) {
      new SparkVertex(newVertex.getId, null);
    } else {
      val tmp = new SparkVertex(originalVertex.getId, null);
      for ( k <- originalVertex.getPropertyKeys.asScala) {
        tmp.setProperty(k, originalVertex.getProperty(k));
      }
      tmp.edgeSet ++= originalVertex.edgeSet.filter( x => !rmSet.contains(x.id) );
      tmp;
    }
    if (newVertex != null) {
      for ( k <- newVertex.getPropertyKeys.asScala) {
        out.setProperty(k, newVertex.getProperty(k));
      }
      out.edgeSet ++= newVertex.edgeSet.filter( x => !rmSet.contains(x.id) );
    }
    return out;
  }

  def buildGraph(sc:SparkContext, input:Iterator[BuildElement]) : SparkGraph = {
    val buildGraph = sc.parallelize(input.toSeq).map( x => (x.getVertexId, x));
    val vertexSet = buildGraph.groupByKey().map( x => (x._1, vertexBuild(x._1, x._2)) );
    return new SparkGraph(vertexSet);
  }
}