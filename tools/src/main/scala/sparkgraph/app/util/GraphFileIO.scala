package sparkgraph.app.util


import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import org.openrdf.rio.rdfxml.RDFXMLParser
import com.tinkerpop.blueprints.Vertex
import org.openrdf.rio.turtle.TurtleParser
import org.openrdf.rio.RDFParser
import org.openrdf.rio.ParserConfig
import org.openrdf.rio.{RDFParser, ParserConfig}
import org.openrdf.rio.rdfxml.RDFXMLParser
import org.openrdf.rio.turtle.TurtleParser

import org.openrdf.rio.RDFHandler
import org.openrdf.model.Statement
import org.openrdf.model.Resource
import org.openrdf.model.URI
import scala.collection.mutable.HashMap
import org.openrdf.model.impl.URIImpl
import org.openrdf.rio.RDFParser
import java.io.InputStream
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import org.openrdf.model.impl.StatementImpl
import org.openrdf.model.BNode
import org.openrdf.model.Value
import scala.collection.mutable.ArrayBuffer
import org.openrdf.model.Literal

import scala.collection.JavaConversions._

import org.openrdf.rio.RDFParser
import java.io.InputStream
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.openrdf.model.{Value, Literal, Resource, URI}
import com.tinkerpop.blueprints.{Direction, Vertex}

import sparkgraph.blueprints.{SparkEdge, SparkVertex}
import scala.util.Random




object GraphFileIO {

    val FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    val FNV_prime = 1099511628211L;

    def fnv(value : Int, seed : Int = 0x811C9DC5) = (seed ^ value) * 0x1000193

    def URLHash(url:String) : Long = {
      /*
      hash = FNV_offset_basis
      for each octet_of_data to be hashed
        hash = hash × FNV_prime
        hash = hash XOR octet_of_data
      return hash
      */
      var hash = FNV_OFFSET_BASIS;
      url.getBytes.foreach( x => {
        hash = hash * FNV_prime
        hash = hash ^ x;}
      )
      return hash;
    }

  def fileRead(filePath:String, gzipped: Boolean, baseURL:String, reader:String, allVertices:Boolean) : Iterator[Vertex] = {
    reader match {
      case "rdf" | "rdfxml" => {
        val fis = if (gzipped)
          new GZIPInputStream(new FileInputStream(filePath))
        else
          new FileInputStream(filePath);
        val parser = new RDFXMLParser();
        parser.setParserConfig(new ParserConfig(true, false, false, RDFParser.DatatypeHandling.VERIFY))
        return new RIO2GraphVertex(parser, fis, baseURL, URLHash, allVertices, true);
      }
      case "ttl" => {
        val fis = if (gzipped)
          new GZIPInputStream(new FileInputStream(filePath))
        else
          new FileInputStream(filePath);
        val parser = new TurtleParser();
        parser.setParserConfig( new ParserConfig(true, false, false,  RDFParser.DatatypeHandling.VERIFY) );
        return new RIO2GraphVertex(parser, fis, baseURL, URLHash, allVertices, true);
      }
    }
    return Array[Vertex]().iterator
  }
}




class RIO2GraphVertex(parser:RDFParser, is:InputStream, baseUrl:String, urlHash: (String)=>Long, allVertices : Boolean = false, closeOnEnd:Boolean = false, type2Param:Boolean=true) extends Iterator[Vertex] {

  val reader = new RIOIterator(parser, is, baseUrl)

  var curEdges = new ArrayBuffer[(URI,Resource)]()
  var curValues = new  ArrayBuffer[(URI,AnyRef)]()

  val vertex_visited = new HashMap[Long, Resource]()

  var curSubject : Resource = null
  var first = true

  var next_val : Vertex = null

  def hasNext() : Boolean = {
    queueNext()
    return next_val != null
  }

  def genVertex(src:Resource, edges:Array[(URI,Resource)], props:Array[(URI,AnyRef)]) : Vertex = {
    val out = new SparkVertex(urlHash(src.toString), null)
    val edges = new ArrayBuffer[SparkEdge]()
    curEdges.foreach( x => {
      val e = new SparkEdge(new java.lang.Long(Random.nextLong()), urlHash(src.toString), urlHash(x._2.toString), x._1.toString, null )
      edges += e
    } )
    curValues.foreach( x => {
      out.setProperty(x._1.toString, x._2.toString)
    })
    out.setProperty("name", src.toString)
    out.edgeSet = edges.toArray
    return out
  }

  def next() : Vertex = {
    queueNext()
    val out = next_val
    next_val = null
    return out
  }

  def queueNext() : Unit = {
    if (next_val != null)
      return
    while (reader.hasNext()) {
      val next_statement = reader.next()
      val subj = next_statement.getSubject()
      val pred = next_statement.getPredicate()
      val obj = next_statement.getObject()

      if (subj != curSubject) {
        if (!first) {
          next_val = genVertex(curSubject, curEdges.toArray, curValues.toArray)
        }
        curEdges.clear()
        curValues.clear()
        curSubject = subj
        first = false;
      }
      addEdge(pred, obj)
      if (allVertices) {
        if (obj.isInstanceOf[Resource]) {
          val uid = urlHash(obj.toString)
          if (!vertex_visited.contains(uid)) {
            vertex_visited(uid) = obj.asInstanceOf[Resource]
          }
        }
        vertex_visited(urlHash(curSubject.toString)) = null
      }
      if (next_val != null)
        return
    }

    if (curSubject != null) {
      next_val = genVertex(curSubject, curEdges.toArray, curValues.toArray)
      curSubject = null
      curEdges.clear()
      curValues.clear()
      return
    }

    while (vertex_visited.size > 0) {
      val n = vertex_visited.iterator.next()
      vertex_visited.remove(n._1)
      if (n._2 != null) {
        next_val = genVertex(n._2, Array(), Array())
        return
      }
    }
    if (closeOnEnd) {
      is.close()
    }
    return
  }

  def addEdge(pred:URI, obj:Value) = {
    if (type2Param && pred.toString == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
      curValues += Tuple2(pred, obj.asInstanceOf[Resource].stringValue())
    } else {
      if (obj.isInstanceOf[Literal]) {
        curValues += Tuple2(pred, obj.asInstanceOf[Literal].stringValue())
      } else {
        curEdges += Tuple2(pred, obj.asInstanceOf[Resource])
      }
    }
  }

}



class IteratorHandler(queue:BlockingQueue[Option[Statement]], baseURL:String) extends RDFHandler {
  val nsMap = new scala.collection.mutable.HashMap[String,String]();
  def endRDF() = {
    while (queue.remainingCapacity() <= 0) {
      Thread.sleep(1)
    }
    queue.add(None)
  }
  def handleComment(comment:String) = {

  }
  def handleNamespace(prefix:String, uri:String) = {
    nsMap(prefix + ":") = uri
  }

  def nsClean(r:Resource) : Resource = {
    if (r.isInstanceOf[URI]) {
      val u = r.asInstanceOf[URI];
      return if (nsMap.contains(u.getNamespace())) {
        new URIImpl(nsMap(u.getNamespace()) + u.getLocalName())
      } else {
        r
      }
    }
    if (r.isInstanceOf[BNode]) {
      val u = r.asInstanceOf[BNode];
      return new URIImpl(baseURL + u.getID())
    }
    return r;
  }

  def handleStatement(st:Statement) = {
    val b = st.getSubject();
    val subject = nsClean(b);
    val predicate = nsClean(st.getPredicate());
    val obj = if (st.getObject().isInstanceOf[URI]) {
      nsClean(st.getObject().asInstanceOf[URI])
    } else {
      st.getObject()
    }
    while (queue.remainingCapacity() <= 0) {
      Thread.sleep(1)
    }
    queue.add( Some(new StatementImpl(subject, predicate.asInstanceOf[URI], st.getObject())) )
  }

  def startRDF() = {

  }
}




class ParserThread(parser:RDFParser, is:InputStream, baseURL:String, queue:BlockingQueue[Option[Statement]]) extends Runnable {
  def run() {
    try {
      val rdfHandler = new IteratorHandler(queue, baseURL)
      parser.setRDFHandler(rdfHandler)
      parser.parse(is , baseURL)
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



class RIOIterator(parser:RDFParser, is: InputStream, baseURL:String) extends Iterator[Statement] {

  val queue = new LinkedBlockingQueue[Option[Statement]](100);

  val reader = new Thread(new ParserThread(parser, is, baseURL, queue));
  reader.start();

  var done = false;
  var current : Statement = null;

  def hasNext() : Boolean = {
    //println(done)
    if (done) return false;
    if (current == null) {
      val n = queue.take();
      //println(n)
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

  def next() : Statement = {
    val o = current;
    current = null;
    return o;
  }

}


