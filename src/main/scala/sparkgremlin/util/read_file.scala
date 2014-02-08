
package sparkgremlin.blueprints.util

import java.io.{File, FileInputStream}

import collection.JavaConverters._

import org.apache.spark.SparkContext
import sparkgremlin.blueprints.io.build.{SparkGraphBuilder, GraphMLIterator}


object ReadFile {
  def main(args:Array[String]) = {
    val sc = new SparkContext("local", "GremlinTest");
    val fis = new FileInputStream(new File(args(0)));
    val iter = new GraphMLIterator(fis);
    val graph = SparkGraphBuilder.buildGraph(sc, iter);
    graph.getVertices().asScala.foreach( x => println( "Hello " + x.getId + " " + x.getPropertyKeys.toArray().mkString(" ")) );
    graph.getEdges().asScala.foreach( x => println(x.getId))
    //graph.getVertices().asScala.foreach( x => println( x.prop ) );

  }
}