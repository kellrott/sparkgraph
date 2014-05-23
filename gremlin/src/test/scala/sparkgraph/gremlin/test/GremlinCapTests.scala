package sparkgraph.gremlin.test

import com.tinkerpop.blueprints._
import sparkgraph.blueprints.SparkGraph
import sparkgraph.blueprints.test.SparkGraphTestFactory
import sparkgraph.gremlin.SparkGremlinPipeline
import org.apache.spark.{SparkConf, SparkContext}
import junit.framework.{Assert, TestCase}

class GremlinCapTests extends BaseTest {

  var sc: SparkContext = null
  var g: Graph = null

  override def setUp {
    System.err.println("Setting Up")
    if (sc == null) {
      sc = new SparkContext("local", "SparkGraphTest", new SparkConf)
      g = SparkGraphTestFactory.createSparkGraph(sc)
    }
  }

  override def tearDown {
    if (sc != null) {
      sc.stop
      sc = null
      g = null
      try {
        Thread.sleep(1000)
      } catch {
        case e: InterruptedException => {
        }
      }
    }
  }

  def test_call() = {
    val sg: SparkGraph = SparkGraphTestFactory.createSparkGraph(sc)
    val gr = new SparkGremlinPipeline(sg)
    var rdd = gr.V.has("name", "marko").as("src").out().property("name").as("friend").rdd()
    var out = rdd.map( x => x("friend").asInstanceOf[String]).collect()
    Assert.assertEquals(3, out.length)
    assert(out.contains("josh"))
    assert(out.contains("lop"))
    assert(out.contains("vadas"))
  }

  def test_in_cap() = {
    val sg: SparkGraph = SparkGraphTestFactory.createSparkGraph(sc)
    val gr = new SparkGremlinPipeline(sg)
    var rdd = gr.V.has("name", "vadas").as("src").in("knows").property("name").as("friend").rdd()
    var out = rdd.map( x => x("friend").asInstanceOf[String]).collect()
    Assert.assertEquals(1, out.length)
    assert(out(0) == "marko")
  }

  def test_prop_back_cap() = {
    val sg: SparkGraph = SparkGraphTestFactory.createSparkGraph(sc)
    val gr = new SparkGremlinPipeline(sg)
    var rdd = gr.V.has("name", "marko").as("src").property("age").as("src_age").back("src").
      property("name").as("src_name").back("src").
      out().property("name").as("friend_name").rdd()
    var out = rdd.map( x => (x("src_name").asInstanceOf[String], x("friend_name").asInstanceOf[String]) ).collect()
    out.foreach( x => {
      assert(x._1 == "marko")
      assert(Array("josh", "lop", "vadas").contains(x._2))
    })
  }


}

