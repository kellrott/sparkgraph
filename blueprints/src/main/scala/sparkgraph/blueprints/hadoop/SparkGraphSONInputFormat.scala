package sparkgraph.blueprints.hadoop

/**
 * Created by kellrott on 2/8/14.
 */

import org.apache.hadoop.mapreduce.lib.input.{LineRecordReader, FileInputFormat}
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit}
import sparkgraph.blueprints.{SparkVertex}

class SparkGraphSONInputFormat extends FileInputFormat[Long,SparkVertex] {
  def createRecordReader(split: InputSplit, context: TaskAttemptContext) : GraphSONRecordReader = { //RecordReader[LongWritable, SparkVertex] {
    return new GraphSONRecordReader();
  }
}
