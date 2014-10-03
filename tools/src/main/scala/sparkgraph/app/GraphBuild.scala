package sparkgraph.app

import scala.io.Source._
import java.io._
import collection.JavaConverters._


import java.util.zip.GZIPInputStream

import org.openrdf.rio.rdfxml.RDFXMLParser
import org.openrdf.rio.turtle.TurtleParser
import org.openrdf.rio.{RDFParser, ParserConfig}
import org.openrdf.model.{Value, Literal, Resource, URI}

import com.tinkerpop.blueprints.{Direction, Vertex}
import sparkgraph.blueprints.{SparkGraph, SparkVertex, SparkEdge}

import com.fasterxml.jackson.core.{JsonGenerator, JsonFactory}
import com.fasterxml.jackson.databind.ObjectMapper
import java.util
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer
import sparkgraph.app.util.GraphFileIO
import org.rogach.scallop
import org.apache.spark.storage.StorageLevel
import sparkgraph.blueprints.parquet.BlueprintParquet
import sparkgraph.blueprints.hadoop.GraphSON
import org.apache.spark.rdd.RDD


object GraphBuild {

  def nsRename(nsMap : Map[String,String], url:String) : String = {
    if (url == null) {
      return url
    }
    for ( (ns,base) <- nsMap) {
      if (url.startsWith(base)) {
        return url.replaceFirst(base, ns + ":")
      }
    }
    return url
  }

  def edge_rename(nsMap : Map[String,String], edge: SparkEdge) : SparkEdge = {
    val out = new SparkEdge(edge.getID(), edge.outVertexId, edge.inVertexId, nsRename(nsMap, edge.label), null)
    edge.propMap.foreach( x => {
      out.propMap(nsRename(nsMap, x._1)) = x._2
    })
    return out
  }

  def vector_rename(nsMap : Map[String,String], vertex: SparkVertex) : SparkVertex = {
    val out = new SparkVertex(vertex.getID, null)
    out.edgeSet = vertex.edgeSet.map( x => edge_rename(nsMap, x))
    vertex.propMap.foreach( x => {
      if (x._1 == "name") {
        out.propMap("name") = nsRename(nsMap, x._2.asInstanceOf[String])
      } else if (x._1 == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
        out.propMap("type") = nsRename(nsMap, x._2.asInstanceOf[String])
      } else {
        out.propMap(nsRename(nsMap, x._1)) = x._2
      }
    })
    return out
  }


  def main(args: Array[String]) {

    object conf extends scallop.ScallopConf(args) {
      val baseURL : scallop.ScallopOption[String] = opt[String]("baseURL", default = Some("http://purl.org/bmeg/import/"))
      val spark = opt[String]("spark", default=Some("local"))
      val localdisk = opt[String]("localdisk", default=Some("/tmp"))
      val nsMap : scallop.ScallopOption[File] = opt[File]("nsMap")
      val outdir : scallop.ScallopOption[String] = opt[String]("outdir", default=Some("bmeg.data"))
      val typereduce : scallop.ScallopOption[Boolean] = toggle("typereduce")
      val fragcount : scallop.ScallopOption[Int] = opt[Int]("fragcount", default=Some(50))
      val filetype : scallop.ScallopOption[String] = opt[String]("filetype", default=Some("work"))
      val files = trailArg[List[String]]();
    }

    val sconf = new SparkConf().setMaster(conf.spark()).setAppName("GraphBuild")
    sconf.set("spark.local.dir", conf.localdisk())
    sconf.set("spark.serializer.objectStreamReset", "1000")
    //sconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //sconf.set("spark.kryo.registrator", "sparkgremlin.common.SparkGremlinKyroRegistrator")

    val sourcePath = GraphBuild.getClass.getProtectionDomain.getCodeSource.getLocation.getPath;
    if ( sourcePath.endsWith(".jar") ) {
      sconf.setJars( Seq(sourcePath) )
    }

    val sc = new SparkContext(sconf)

    val requests_rdd : RDD[Map[String,AnyRef]] = if (conf.filetype() == "work") {
      val workFile = new File(conf.files()(0))
      val objMapper = new ObjectMapper()
      val requests = fromFile(workFile).getLines().map( x => objMapper.readValue(x, classOf[java.util.Map[String,AnyRef]]).asScala.toMap ).toSeq
      sc.parallelize(requests, math.min(requests.length, conf.fragcount()))
    } else if (conf.filetype() == "rdfxml") {
      val requests = conf.files().map(x => Map[String,AnyRef]( ("path", x), ("format", "rdfxml") ) ).toSeq
      sc.parallelize(requests, math.min(requests.length, conf.fragcount()))
    } else {
      null
    }

    val nsFile = new File(conf.nsMap().getAbsolutePath)
    val nsMap = fromFile(nsFile).getLines().map( _.stripLineEnd.split("\t")).filter( _.length == 2 ).map( x => (x(0), x(1)) ).toMap
    var nsMap_bc = sc.broadcast(nsMap)

    val baseURL = conf.baseURL()
    val vertex_rdd = requests_rdd.flatMap(
      x => GraphFileIO.fileRead(x("path").asInstanceOf[String],
        false, baseURL,
        x("format").toString, true ) ).
      map( x => vector_rename(nsMap_bc.value, x.asInstanceOf[SparkVertex]))

    val read_rdd = vertex_rdd.map( x => (x.asInstanceOf[SparkVertex].getID(),x) ).persist(StorageLevel.DISK_ONLY);
    val reduced_rdd  = read_rdd.reduceByKey( (x,y) => SparkGraph.reduceVertex(x.asInstanceOf[SparkVertex],y.asInstanceOf[SparkVertex])).
      persist(StorageLevel.DISK_ONLY)
    read_rdd.unpersist()

    printf( "Vertex Count: %s\n", reduced_rdd.count() )

    val graph = SparkGraph.cachedVertices2Graph(reduced_rdd.map(_._2.asInstanceOf[SparkVertex]))

    //print("Unamed Vertices: ", graph.vertices.filter( x => !x._2.propMap.contains("name")).map(_._2).collect().mkString((",")))

    BlueprintParquet.save(conf.outdir(), new SparkGraph(graph))
  }

}