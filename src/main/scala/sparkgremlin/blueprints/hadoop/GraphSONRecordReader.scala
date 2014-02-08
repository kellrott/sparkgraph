package sparkgremlin.blueprints.hadoop

/**
 * Created by kellrott on 2/8/14.
 */

import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit}
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import sparkgremlin.blueprints.{SparkVertex}
import sparkgremlin.blueprints.io.SparkGraphSONParser


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
