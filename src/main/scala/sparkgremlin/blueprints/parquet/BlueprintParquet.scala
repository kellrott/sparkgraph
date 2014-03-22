package sparkgremlin.blueprints.parquet

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
import sparkgremlin.blueprints.{SparkEdge, SparkVertex, SparkGraph}
import sparkgremlin.blueprints.avro.{AvroEdge, AvroVertex, Property}

import org.apache.hadoop.fs.Path

import collection.JavaConverters._


object BlueprintParquet {

  def sparkVertex2Avro(inV:SparkVertex) : AvroVertex = {
    val out = new AvroVertex()
    out.setId(inV.getId.asInstanceOf[Long])
    out.setProps( inV.propMap.toList.map( x => new Property(x._1, x._2) ).asJava )
    return out
  }

  def avroVertex2Spark(inV:AvroVertex) : SparkVertex = {
    val out = new SparkVertex(inV.getId, null)

    return out
  }

  def sparkEdge2Avro(inE:SparkEdge) : AvroEdge = {
    val out = new AvroEdge()
    out.setId(inE.getId.asInstanceOf[Long])
    out.setSrc( inE.outVertexId )
    out.setDest( inE.inVertexId )
    out.setProps( inE.propMap.toList.map( x => new Property(x._1, x._2) ).asJava )
    return out
  }


  def avroEdge2Spark(inE:AvroEdge) : SparkEdge = {
    val out = new SparkEdge(inE.getId, inE.getSrc, inE.getDest, inE.getLabel.toString, null)
    inE.getProps.asScala.foreach( x => out.setProperty(x.getKey.toString, x.getValue) )
    return out
  }


  def save(path:String, gr:SparkGraph) = {
    val dpath = new Path(path)
    val fs = dpath.getFileSystem(gr.graphX().vertices.context.hadoopConfiguration)
    if (!fs.exists(dpath)) {
      fs.mkdirs(dpath)
    }

    val job = new Job()
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    AvroParquetOutputFormat.setSchema(job, AvroVertex.SCHEMA$)

    val vertexRDD = gr.graphX().vertices.map(x => (null, sparkVertex2Avro(x._2)) )


    vertexRDD.saveAsNewAPIHadoopFile(new Path(dpath, "vertices").toString, classOf[Void], classOf[AvroVertex],
      classOf[ParquetOutputFormat[AvroVertex]], job.getConfiguration)

    val edgeRDD = gr.graphX().edges.map( x => (null, sparkEdge2Avro(x.attr)) )
    AvroParquetOutputFormat.setSchema(job, AvroEdge.SCHEMA$)
    edgeRDD.saveAsNewAPIHadoopFile(new Path(dpath, "edges").toString, classOf[Void], classOf[AvroEdge],
      classOf[ParquetOutputFormat[AvroEdge]], job.getConfiguration)
  }

  def load(path:String, sc:SparkContext) : SparkGraph = {
    val dpath = new Path(path)
    val fs = dpath.getFileSystem(sc.hadoopConfiguration)
    if (!fs.exists(dpath)) {
      fs.mkdirs(dpath)
    }

    val job = new Job()
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroVertex]])
    val verticies = sc.newAPIHadoopFile(new Path(dpath, "vertices").toString, classOf[ParquetInputFormat[AvroVertex]],
      classOf[Void], classOf[AvroVertex], job.getConfiguration)

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroEdge]])
    val edges = sc.newAPIHadoopFile(new Path(dpath, "edges").toString, classOf[ParquetInputFormat[AvroEdge]],
      classOf[Void], classOf[AvroEdge], job.getConfiguration)

    val graph = graphx.Graph[SparkVertex,SparkEdge]( verticies.map( x => (x._2.getId, avroVertex2Spark(x._2)) ), edges.map( x => new graphx.Edge(x._2.getSrc, x._2.getDest, avroEdge2Spark(x._2) ) ))

    return new SparkGraph(graph)
  }


}