package sparkgremlin.blueprints.hadoop

/**
 * Created by kellrott on 2/8/14.
 */

import org.apache.spark.SparkContext._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{graphx, SparkContext}
import org.apache.spark.storage.StorageLevel
import sparkgremlin.blueprints.{SparkEdge, SparkVertex, SparkGraph}

object GraphSON {
  def save(path : String, sg : SparkGraph) = {
    val flatRDD = sg.graphX().mapReduceTriplets[(SparkVertex)](  x => {
      val src = new SparkVertex(x.srcAttr.id, null)
      x.srcAttr.propMap.foreach( x => src.propMap(x._1) = x._2 )
      src.edgeSet = Array(x.attr)

      val dst = new SparkVertex(x.dstAttr.id, null)
      x.dstAttr.propMap.foreach( x => dst.propMap(x._1) = x._2 )
      dst.edgeSet = Array(x.attr)

      Iterator((x.srcId, src),(x.dstId,dst))
    },
      (y,z) => {
         y.edgeSet ++= z.edgeSet;
         y
      })
    flatRDD.saveAsNewAPIHadoopFile(path, classOf[LongWritable], classOf[SparkVertex], classOf[SparkGraphSONOutputFormat]);
  }

  def load(path: String, sc : SparkContext, defaultStorage: StorageLevel = StorageLevel.MEMORY_ONLY) : SparkGraph = {
    val rdd = sc.newAPIHadoopFile[Long, SparkVertex, SparkGraphSONInputFormat](path);
    val edges = rdd.flatMap( x => x._2.edgeSet ).map( x => graphx.Edge( x.outVertexId, x.inVertexId, x ) )
    val gr = new SparkGraph( graphx.Graph(rdd, edges).mapVertices( (vid,attr) => {
      if (attr != null) {
        attr
      } else {
        new SparkVertex(vid, null)
      }
    }) )
    return gr;
  }
}
