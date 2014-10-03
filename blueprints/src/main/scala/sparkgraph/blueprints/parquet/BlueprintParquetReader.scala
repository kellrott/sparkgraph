package sparkgraph.blueprints.parquet

import collection.JavaConverters._

import sparkgraph.blueprints.{SparkEdge, SparkVertex, SparkGraphElement}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import sparkgraph.blueprints.avro.{ElementType, AvroElement, AvroEdge}
import parquet.filter.ColumnRecordFilter._
import parquet.filter.ColumnPredicates._
import org.apache.hadoop.conf.Configuration
import parquet.filter.{RecordFilter, UnboundRecordFilter}
import java.lang
import parquet.column.ColumnReader

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
        var done = false
        while (!done && curIndex < splits.length) {
          cur_record_reader = getRecordReader(curIndex)
          curIndex += 1
          cur_record_reader.nextKeyValue()
          val i = cur_record_reader.getCurrentValue
          if (i != null ) {
            next_record = process(i)
            done = true
          }
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


class EdgeFilter extends UnboundRecordFilter() {
  override def bind(readers: lang.Iterable[ColumnReader]): RecordFilter = {
    column("type", equalTo(ElementType.EDGE)).bind(readers)
  }
}

class VertexFilter extends UnboundRecordFilter() {
  override def bind(readers: lang.Iterable[ColumnReader]): RecordFilter = {
    column("type", equalTo(ElementType.VERTEX)).bind(readers)
  }
}


object BlueprintParquetReader {

  def openVertices(path:String) : BlueprintParquetReader[AvroElement, SparkVertex] = {
    val job = new Job()
    val vertices_path = new Path(path)
    FileInputFormat.addInputPath(job, vertices_path)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroElement]])
    ParquetInputFormat.setUnboundRecordFilter(job, classOf[VertexFilter])

    val vert_reader = new ParquetInputFormat[AvroElement]()
    val splits = vert_reader.getSplits(job).asScala

    return new BlueprintParquetReader[AvroElement,SparkVertex](job.getConfiguration, splits.toList) {
      override def process(in: AvroElement): SparkVertex = BlueprintParquet.avroVertex2Spark(in)
    }
  }


  def openEdges(path:String) : BlueprintParquetReader[AvroElement, SparkEdge] = {
    val job = new Job()
    val edge_path = new Path(path)
    FileInputFormat.addInputPath(job, edge_path)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[AvroElement]])
    ParquetInputFormat.setUnboundRecordFilter(job, classOf[EdgeFilter])

    val edge_reader = new ParquetInputFormat[AvroElement]()
    val splits = edge_reader.getSplits(job).asScala

    return new BlueprintParquetReader[AvroElement,SparkEdge](job.getConfiguration, splits.toList) {
      override def process(in: AvroElement): SparkEdge = BlueprintParquet.avroEdge2Spark(in)
    }
  }

}