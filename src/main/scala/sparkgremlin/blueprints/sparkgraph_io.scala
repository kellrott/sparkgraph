package sparkgremlin.blueprints

import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader
import com.tinkerpop.blueprints._
import java.lang.Iterable


import collection.JavaConverters._
import java.io.{InputStream, File, FileInputStream}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashSet

/**
 * Created by kellrott on 12/7/13.
 */


trait BuildElement extends Serializable  {
  def getVertexId : AnyRef;
  def isEdge : Boolean;
  def isProperty : Boolean;
  def isRemoval : Boolean;
  def getKey : String;
  def getValue : Any;
  def getEdgeId : AnyRef;
  def getVertexInId : AnyRef;
  def getLabel : String;
}


abstract class InputElement extends Element {

  def getProperty[T](key: String): T = {
    throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
  }

  def getPropertyKeys: java.util.Set[String] = {
    throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
  }

  def removeProperty[T](key: String): T = {
    throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
  }

  def remove() = {
    throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
  }
}

class EdgePropertyBuild(val id:AnyRef, val outVertexId:AnyRef, val inVertexId:AnyRef, val key:String, val value:Any) extends BuildElement {
  if ( id == null) {
    throw new IllegalArgumentException("Null Edge ID")
  }
  def getVertexId : AnyRef = outVertexId;
  def isEdge = true;
  def isProperty = true;
  def getKey : String  = key;
  def getValue : Any = value;
  def getEdgeId : AnyRef = id;
  def getVertexInId : AnyRef = inVertexId;
  def isRemoval = false;
  def getLabel : String = null;
}

class EdgeBuild(val id:AnyRef, val outVertexId:AnyRef, val inVertexId:AnyRef, val label:String) extends BuildElement {
  if ( id == null) {
    throw new IllegalArgumentException("Null Edge ID")
  }
  def getVertexId : AnyRef = outVertexId;
  def isEdge : Boolean = true;
  def isProperty : Boolean = false;
  def isRemoval = false;
  def getKey : String  = null;
  def getValue : Any = null;
  def getEdgeId : AnyRef = id;
  def getVertexInId : AnyRef = inVertexId;
  def getLabel : String = label;
}



class EdgeRemoveBuild(val id:AnyRef, val outVertexId:AnyRef) extends BuildElement {
  def getVertexId : AnyRef = outVertexId;
  def isEdge : Boolean = true;
  def isProperty : Boolean = false;
  def isRemoval = true;
  def getKey : String  = null;
  def getValue : Any = null;
  def getEdgeId : AnyRef = id;
  def getVertexInId : AnyRef = null;
  def getLabel : String = null;
}

class DeletedEdge(override val id:AnyRef) extends SparkEdge(id, null, null, null, null);

class EdgeInputPrinter(val edgeId : AnyRef, val outVertex:Vertex, val inVertex:Vertex, queue:BlockingQueue[Option[BuildElement]]) extends InputElement with Edge {
  def getVertex(direction: Direction): Vertex = {
    throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
  }

  def setProperty(key: String, value: scala.Any) = {
    queue.add(Some(new EdgePropertyBuild(edgeId, outVertex.getId, inVertex.getId, key, value)))
  }

  def getLabel: String = ""
  def getId : AnyRef = edgeId;
}


class VertexPropertyBuild(val vertexId:AnyRef, val key:String, val value:Any) extends BuildElement {
  def getVertexId : AnyRef = vertexId;
  def isEdge : Boolean = false;
  def isProperty : Boolean = true;
  def isRemoval = false;
  def getKey : String  = key;
  def getValue : Any = value;
  def getEdgeId : AnyRef = null;
  def getVertexInId : AnyRef = null;
  def getLabel : String = null;
}

class VertexBuild(vertexId:AnyRef) extends BuildElement {
  def getVertexId : AnyRef = vertexId;
  def isEdge : Boolean = false;
  def isProperty : Boolean = false;
  def isRemoval = false;
  def getKey : String  = null;
  def getValue : Any = null;
  def getEdgeId : AnyRef = null;
  def getVertexInId : AnyRef = null;
  def getLabel : String = null;
}



class VertexRemoveBuild(val vertexId:AnyRef) extends BuildElement {
  def getVertexId : AnyRef = vertexId;
  def isEdge : Boolean = false;
  def isProperty : Boolean = false;
  def isRemoval = true;
  def getKey : String  = null;
  def getValue : Any = null;
  def getEdgeId : AnyRef = null;
  def getVertexInId : AnyRef = null;
  def getLabel : String = null;
}

class DeletedVertex(override val id: AnyRef) extends SparkVertex(id, null);

class VertexInputPrinter(val id:AnyRef, queue:BlockingQueue[Option[BuildElement]]) extends InputElement with Vertex {

  def getId : AnyRef = id;

  def addEdge(label: String, inVertex: Vertex): Edge = {
    //println("Vertex Add Edge: " + label + " " + vertex);
    /*
    val out = new EdgeInputPrinter(this, inVertex, queue);
    queue.add(Some(new EdgeBuild(id, inVertex.getId, label)));
    return out;
    */
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def setProperty(key: String, value: scala.Any) = {
    queue.add(Some(new VertexPropertyBuild(id, key, value)));
  }
  def getVertices(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Vertex] = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def query(): VertexQuery = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def getEdges(direction: Direction, labels: java.lang.String*): java.lang.Iterable[Edge] = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

}

class GraphInputPrinter(queue:BlockingQueue[Option[BuildElement]]) extends Graph {

  def getFeatures: Features = null;

  def getEdge(id: scala.Any): Edge = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def getVertex(id: scala.Any): Vertex = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def shutdown() = {

  }

  def removeEdge(edge: Edge) {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def removeVertex(vertex: Vertex) {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def addVertex(id: scala.AnyRef): Vertex =  {
    //println("AddingVertex: " + id);
    val out = new VertexInputPrinter(id, queue);
    queue.put(Some(new VertexBuild(id)));
    return out;
  }

  def getVertices(key: String, value: scala.Any): Iterable[Vertex] = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def addEdge(id: scala.AnyRef, outVertex: Vertex, inVertex: Vertex, label: String): Edge = {
    //println("AddingEdge: " + id);
    val out = new EdgeInputPrinter(id, outVertex, inVertex, queue);
    queue.put(Some(new EdgeBuild(id, outVertex.getId, inVertex.getId, label)));
    return out;
  }

  def getEdges: Iterable[Edge] = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def getEdges(key: String, value: scala.Any): Iterable[Edge] = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def query(): GraphQuery = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }

  def getVertices: Iterable[Vertex] = {
    throw new UnsupportedOperationException(SparkGraph.WRITE_ONLY_MESSAGE);
  }
}



class ParserThread(is:InputStream, queue:BlockingQueue[Option[BuildElement]]) extends Runnable {
  def run() {
    try {
      val inputGraph = new GraphInputPrinter(queue);
      val reader = new GraphMLReader(inputGraph);
      reader.inputGraph(is);
    } catch {
      case e => {println("Exception" + e)}
    } finally {
      while (queue.remainingCapacity() <= 0) {
        Thread.sleep(1)
      }
      queue.add(None)
    }
  }
}


class GraphMLIterator(is: InputStream) extends Iterator[BuildElement] {

  val queue = new LinkedBlockingQueue[Option[BuildElement]](100);

  val reader = new Thread(new ParserThread(is, queue));
  reader.start();

  var done = false;
  var current : BuildElement = null;

  def hasNext() : Boolean = {
    if (done) return false;
    if (current == null) {
      val n = queue.take();
      if (n.isDefined)
        current = n.get;
      else
        current = null;
    }
    if (current == null) {
      done = true;
    }
    return !done;
  }

  def next() : BuildElement = {
    val o = current;
    current = null;
    return o;
  }

}

class SparkVertexBuilt(val inID:AnyRef, built : Boolean) extends SparkVertex(inID, null) {
  def wasBuilt = built;
}

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
          out.edgeSet += new SparkEdge(b.getEdgeId, b.getVertexId, b.getVertexInId, b.getLabel, null);
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
      tmp.propMap ++= originalVertex.propMap;
      tmp.edgeSet ++= originalVertex.edgeSet.filter( x => !rmSet.contains(x.id) );
      tmp;
    }
    if (newVertex != null) {
      out.propMap ++= newVertex.propMap;
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