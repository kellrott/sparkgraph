package sparkgremlin.blueprints


import collection.JavaConverters._
import org.apache.spark.SparkContext._

import org.apache.hadoop.mapreduce.{RecordWriter, RecordReader, TaskAttemptContext, InputSplit}
import org.apache.hadoop.mapreduce.lib.input.{LineRecordReader, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.{StringWriter, DataOutputStream}
import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodec}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.LongWritable
import com.tinkerpop.blueprints.Direction
import com.fasterxml.jackson.core.{JsonGenerator, JsonFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object SparkGraphHadoop {
  def saveAsHadoopGraphSON(path : String, sg : SparkGraph) = {
    sg.graphRDD().saveAsNewAPIHadoopFile(path, classOf[LongWritable], classOf[SparkVertex], classOf[SparkGraphSONOutputFormat]);
  }

  def loadHadoopGraphSON(path: String, sc : SparkContext, defaultStorage: StorageLevel = StorageLevel.MEMORY_ONLY) : SparkGraph = {
    val rdd = sc.newAPIHadoopFile[Long, SparkVertex, SparkGraphSONInputFormat](path);
    val gr = new SparkGraph(rdd.asInstanceOf[RDD[(AnyRef,SparkVertex)]], defaultStorage);
    return gr;
  }
}

class SparkGraphSONParser {
  val JSON_MAPPER = new ObjectMapper();
  val JSON_FACTORY = new JsonFactory(JSON_MAPPER);
  JSON_FACTORY.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
  JSON_FACTORY.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
  JSON_FACTORY.disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);

  def fromJSON(text:String) : SparkVertex = {
    val data = JSON_MAPPER.readValue(text, classOf[java.util.Map[String,AnyRef]])
    val id = data.get("_id").toString.toLong;
    val out = new SparkVertex(id.asInstanceOf[AnyRef], null);
    for ( (k,v) <- data.asScala ) {
      if (k == "_outE") {
        val edgeArray = v.asInstanceOf[java.util.ArrayList[AnyRef]];
        for ( edgeElement <- edgeArray.asScala ) {
          val edgeData = edgeElement.asInstanceOf[java.util.Map[String,AnyRef]]
          val outEdge = out.addEdge(edgeData.get("_label").asInstanceOf[String], new SparkVertex(edgeData.get("_inV"), null))
          for ( (ek,ev) <- edgeData.asScala ) {
            if (ek == "_id") {
            } else if (ek == "_label") {
            } else {
              outEdge.setProperty(ek,ev)
            }
          }
        }
      } else if ( k == "_id") {
      } else {
        out.setProperty(k,v);
      }
    }
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

class SparkGraphSONInputFormat extends FileInputFormat[Long,SparkVertex] {
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) : GraphSONRecordReader = { //RecordReader[LongWritable, SparkVertex] {
    return new GraphSONRecordReader();
  }
}

class GraphSONRecordReader extends RecordReader[Long,SparkVertex]  {

  //var pathEnabled : Boolean;
  var lineRecordReader : LineRecordReader = new LineRecordReader();
  var vertex : SparkVertex = null;
  var parser = new SparkGraphSONParser()

  def initialize(genericSplit: InputSplit , context :TaskAttemptContext) = {
    this.lineRecordReader.initialize(genericSplit, context);
    //this.pathEnabled = false; //context.getConfiguration().getBoolean(SparkGraphCompiler.PATH_ENABLED, false);
  }

  def nextKeyValue() : Boolean = {
    if (!this.lineRecordReader.nextKeyValue())
      return false;

    this.vertex = parser.fromJSON(this.lineRecordReader.getCurrentValue().toString());
    //this.vertexQuery.defaultFilter(this.vertex);
    //this.vertex.enablePath(this.pathEnabled);
    return true;
  }

  def getCurrentKey() : Long = {
    return this.vertex.getId().asInstanceOf[Long];
  }

  def getCurrentValue() : SparkVertex = {
    return this.vertex;
  }

  def getProgress() : Float = {
    return this.lineRecordReader.getProgress();
  }

  def close() = {
    this.lineRecordReader.close();
  }

}


class SparkGraphSONOutputFormat extends FileOutputFormat[Long, SparkVertex] {


  def getDataOuputStream(job: TaskAttemptContext) : DataOutputStream = {
    val conf = job.getConfiguration();
    val isCompressed = FileOutputFormat.getCompressOutput(job);
    var codec : CompressionCodec = null;
    var extension = "";
    if (isCompressed) {
      val codecClass = FileOutputFormat.getOutputCompressorClass(job, classOf[DefaultCodec]);
      codec = ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    var file = super.getDefaultWorkFile(job, extension);
    var fs = file.getFileSystem(conf);
    if (!isCompressed) {
      return new DataOutputStream(fs.create(file, false));
    } else {
      return new DataOutputStream(codec.createOutputStream(fs.create(file, false)));
    }
  }

  def getRecordWriter(job: TaskAttemptContext) : RecordWriter[Long, SparkVertex] = {
    return new GraphSONRecordWriter(getDataOuputStream(job));
  }
}

class GraphSONRecordWriter(val out : DataOutputStream) extends RecordWriter[Long, SparkVertex] {

  val UTF8 = "UTF-8";
  val NEWLINE = "\n".getBytes(UTF8);
  val parser = new SparkGraphSONParser

  def write(key: Long, vertex : SparkVertex) = {
    if (null != vertex) {
      this.out.write(parser.toJSON(vertex).toString().getBytes(UTF8));
      this.out.write(NEWLINE);
    }
  }

  def close(context : TaskAttemptContext) = {
    this.out.close();
  }
}