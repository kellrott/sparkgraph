package sparkgremlin.blueprints.hadoop

/**
 * Created by kellrott on 2/8/14.
 */

import org.apache.spark.SparkContext._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import sparkgremlin.blueprints.{SparkVertex, SparkGraph}

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
