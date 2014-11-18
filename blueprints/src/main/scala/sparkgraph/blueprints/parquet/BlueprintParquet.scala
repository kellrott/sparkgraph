package sparkgraph.blueprints.parquet

import org.apache.spark
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import org.apache.spark.{graphx, SparkContext}
import spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import parquet.avro.{AvroParquetInputFormat, AvroParquetOutputFormat, AvroWriteSupport, AvroReadSupport}
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import java.lang.Iterable
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import com.google.common.io.Files
import java.io.File
import sparkgraph.blueprints.{SparkEdge, SparkVertex, SparkGraph}
import sparkgraph.blueprints.avro.{ElementType, AvroElement, AvroEdge, AvroProperty}

import org.apache.hadoop.fs.Path

import collection.JavaConverters._
import org.apache.spark.storage.StorageLevel


object BlueprintParquet {

  def sparkVertex2Avro(inV:SparkVertex) : AvroElement = {
    val out = new AvroElement()
    out.setType(ElementType.VERTEX)
    out.setId(inV.getId.asInstanceOf[Long])
    out.setProps( inV.propMap.toList.map( x => new AvroProperty(x._1, x._2) ).asJava )
    return out
  }

  def avroVertex2Spark(inV:AvroElement) : SparkVertex = {
    val out = new SparkVertex(inV.getId, null)
    inV.getProps.asScala.foreach( x => out.setProperty(x.getKey.toString, x.getValue) )
    return out
  }

  def sparkEdge2Avro(inE:SparkEdge) : AvroElement = {
    val out = new AvroElement()
    out.setType(ElementType.EDGE)
    out.setId(inE.getId.asInstanceOf[Long])
    val edge = new AvroEdge()
    edge.setSrc( inE.outVertexId )
    edge.setDest( inE.inVertexId )
    edge.setLabel(inE.getLabel)
    out.setEdge(edge)
    val props = inE.propMap.toList.map( x => new AvroProperty(x._1, x._2) ).asJava
    out.setProps( props )
    return out
  }

  def avroEdge2Spark(inE:AvroElement) : SparkEdge = {
    val label_name = inE.getEdge.getLabel
    val out = new SparkEdge(inE.getId, inE.getEdge.getSrc, inE.getEdge.getDest, label_name.toString, null)
    inE.getProps.asScala.foreach( x => out.setProperty(x.getKey.toString, x.getValue) )
    return out
  }


  def save(path:String, gr:SparkGraph) = {
    val job = new Job()
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    AvroParquetOutputFormat.setSchema(job, AvroElement.SCHEMA$)

    val vertexRDD = gr.graphX().vertices.map(x => (null, sparkVertex2Avro(x._2)) )
    val edgeRDD = gr.graphX().edges.map( x => (null, sparkEdge2Avro(x.attr)) )
    vertexRDD.union(edgeRDD).saveAsNewAPIHadoopFile(path, classOf[Void], classOf[AvroElement],
      classOf[ParquetOutputFormat[AvroElement]], job.getConfiguration)
  }

  def load(path:String, sc:SparkContext, defaultStorage: StorageLevel = StorageLevel.MEMORY_ONLY) : SparkGraph = {
    val job = new Job()
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroElement]])
    val elements = sc.newAPIHadoopFile(path.toString, classOf[ParquetInputFormat[AvroElement]],
      classOf[Void], classOf[AvroElement], job.getConfiguration).cache()


    val graph = graphx.Graph[SparkVertex,SparkEdge](
      elements.filter( _._2.getEdge == null ).map( x => (x._2.getId, avroVertex2Spark(x._2)) ),
      elements.filter( _._2.getEdge != null ).map( x => new graphx.Edge(x._2.getEdge.getSrc, x._2.getEdge.getDest, avroEdge2Spark(x._2) ) ),
      edgeStorageLevel = defaultStorage,
      vertexStorageLevel = defaultStorage
    )

    return new SparkGraph(graph)
  }


}