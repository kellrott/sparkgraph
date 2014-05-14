package sparkgremlin.blueprints.parquet

import collection.JavaConverters._

import sparkgremlin.blueprints.{SparkEdge, SparkVertex, SparkGraphElement}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import sparkgremlin.blueprints.avro.{AvroEdge, AvroVertex}
import scala.collection.mutable.HashMap
import com.tinkerpop.blueprints
import org.apache.hadoop.conf.Configuration

/**
 * Created by kellrott on 5/12/14.
 */

abstract class BlueprintParquetReader[I, O <: SparkGraphElement](var conf:Configuration, var splits:List[InputSplit]) extends java.util.Iterator[O] with java.lang.Iterable[O] {

  var curIndex = 0
  val parquet_reader = new ParquetInputFormat[I]()
  var cur_record_reader : RecordReader[Void,I] = null
  var next_record : O = null.asInstanceOf[O]

  def getRecordReader(i:Int) : RecordReader[Void,I] = {
    val at = new TaskAttemptContext(conf, new TaskAttemptID())
    val record_reader = parquet_reader.createRecordReader(splits(i), at)
    record_reader.initialize(splits(i),at)
    return record_reader
  }

  def process(in:I) : O

  def remove() = {}

  def queue_next_record() = {
    if (next_record == null) {
      if (cur_record_reader == null) {
        cur_record_reader = getRecordReader(curIndex)
        curIndex += 1
      }
      if (cur_record_reader.nextKeyValue()) {
        val i = cur_record_reader.getCurrentValue
        next_record = process(i)
      } else {
        curIndex += 1
        if (curIndex < splits.length) {
          cur_record_reader = getRecordReader(curIndex)
          cur_record_reader.nextKeyValue()
          val i = cur_record_reader.getCurrentValue
          next_record = process(i)
        }
      }
    }
  }

  override def iterator: java.util.Iterator[O] = {
    curIndex = 0
    cur_record_reader = null
    next_record = null.asInstanceOf[O]
    return this
  }

  override def next(): O = {
    queue_next_record()
    val out = next_record
    next_record = null.asInstanceOf[O]
    return out
  }

  override def hasNext: Boolean = {
    queue_next_record()
    return next_record != null
  }
}


object BlueprintParquetReader {

  def openVertices(path:String) : BlueprintParquetReader[AvroVertex, SparkVertex] = {
    val job = new Job()
    val vertices_path = new Path(new Path(path), "vertices")
    FileInputFormat.addInputPath(job, vertices_path)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroVertex]])

    val vert_reader = new ParquetInputFormat[AvroVertex]()
    val splits = vert_reader.getSplits(job).asScala

    return new BlueprintParquetReader[AvroVertex,SparkVertex](job.getConfiguration, splits.toList) {
      override def process(in: AvroVertex): SparkVertex = BlueprintParquet.avroVertex2Spark(in)
    }

  }


  def openEdges(path:String) : BlueprintParquetReader[AvroEdge, SparkEdge] = {
    val job = new Job()
    val edge_path = new Path(new Path(path), "edges")
    FileInputFormat.addInputPath(job, edge_path)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroEdge]])

    val edge_reader = new ParquetInputFormat[AvroEdge]()
    val splits = edge_reader.getSplits(job).asScala

    return new BlueprintParquetReader[AvroEdge,SparkEdge](job.getConfiguration, splits.toList) {
      override def process(in: AvroEdge): SparkEdge = BlueprintParquet.avroEdge2Spark(in)
    }
  }

}