package sparkgremlin.blueprints.io

/**
 * Created by kellrott on 2/8/14.
 */

import collection.JavaConverters._
import java.io.StringWriter
import com.fasterxml.jackson.core.{JsonGenerator, JsonFactory}
import com.fasterxml.jackson.databind.ObjectMapper
import com.tinkerpop.blueprints.Direction
import sparkgremlin.blueprints.{SparkEdge, SparkVertex}
import scala.collection.mutable.ArrayBuffer


class SparkGraphSONParser {
  val JSON_MAPPER = new ObjectMapper();
  val JSON_FACTORY = new JsonFactory(JSON_MAPPER);
  JSON_FACTORY.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
  JSON_FACTORY.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
  JSON_FACTORY.disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);

  def fromJSON(text:String) : SparkVertex = {
    val data = JSON_MAPPER.readValue(text, classOf[java.util.Map[String,AnyRef]])
    val id = data.get("_id").toString.toLong;
    val out = new SparkVertex(id.asInstanceOf[Long], null);
    val edges = new ArrayBuffer[SparkEdge]()
    for ( (k,v) <- data.asScala ) {
      if (k == "_outE") {
        val edgeArray = v.asInstanceOf[java.util.ArrayList[AnyRef]];
        for ( edgeElement <- edgeArray.asScala ) {
          val edgeData = edgeElement.asInstanceOf[java.util.Map[String,AnyRef]]
          val outEdge = new SparkEdge(
            edgeData.get("_id").toString.toLong,
            id, edgeData.get("_inV").toString.toLong,
            edgeData.get("_label").asInstanceOf[String], null )
          //val outEdge = out.addEdge(edgeData.get("_label").asInstanceOf[String], new SparkVertex(edgeData.get("_inV").toString.toLong, null))
          for ( (ek,ev) <- edgeData.asScala ) {
            if (ek == "_id") {
            } else if (ek == "_label") {
            } else if (ek == "_inV") {
            } else if (ek == "_outV") {
            } else {
              outEdge.setProperty(ek,ev)
            }
          }
          edges += outEdge
        }
      } else if ( k == "_id") {
      } else if ( k == "_inE") {
      } else {
        out.setProperty(k,v);
      }
    }
    out.edgeSet = edges.toArray
    return out;
  }

  def toJSON(vertex: SparkVertex) : String = {
    val out = new java.util.HashMap[String,AnyRef]();
    out.put("_id", vertex.getId())
    vertex.getPropertyKeys.asScala.foreach( x => out.put(x, vertex.getProperty(x)) )
    val outE = new java.util.ArrayList[AnyRef]();
    vertex.getEdges(Direction.OUT).asScala.foreach( x => {
      val e = new java.util.HashMap[String,AnyRef]()
      e.put("_id", x.getId)
      e.put("_label", x.getLabel)
      e.put("_inV",  x.getVertex(Direction.IN).getId)
      x.getPropertyKeys.asScala.foreach( key => e.put(key,x.getProperty(key)))
      outE.add(e)
    } )

    if (outE.asScala.length > 0) {
      out.put("_outE", outE)
    }
    val sw = new StringWriter()
    val jw = JSON_FACTORY.createGenerator(sw);
    jw.writeObject(out)
    return sw.toString;
  }
}
