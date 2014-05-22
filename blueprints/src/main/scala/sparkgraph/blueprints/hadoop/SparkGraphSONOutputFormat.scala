package sparkgraph.blueprints.hadoop

import collection.JavaConverters._
import org.apache.spark.SparkContext._

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext, InputSplit}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.io.{DataOutputStream}
import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodec}
import org.apache.hadoop.util.ReflectionUtils
import sparkgraph.blueprints.SparkVertex


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

