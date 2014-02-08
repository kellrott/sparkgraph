package sparkgremlin.blueprints.hadoop

import java.io.DataOutputStream
import sparkgremlin.blueprints.{SparkVertex}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import sparkgremlin.blueprints.io.SparkGraphSONParser

/**
 * Created by kellrott on 2/8/14.
 */
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
