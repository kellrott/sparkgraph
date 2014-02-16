package sparkgremlin.test;

import com.tinkerpop.blueprints.*;
import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.storage.StorageLevel;
import sparkgremlin.blueprints.SparkGraph;
import sparkgremlin.blueprints.hadoop.GraphSON;


import java.util.Iterator;

public class SparkHadoopFileTest extends TestCase {

    private SparkContext sc = null;
    SparkGraph g = null;

    public void setUp() {
        System.err.println("Setting Up");
        if (sc == null) {
            sc = new SparkContext("local", "SparkGraphTest", new SparkConf());
            g = SparkGraphTestFactory.createSparkGraph(sc);
        }
    }

    public void tearDown() {
        if (sc != null) {
            sc.stop();
            sc = null;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) { }
        }
    }

    int iteratorCount(Iterator e) {
       int out = 0;
       while (e.hasNext()) {
           out++;
           e.next();
       }
       return out;
    }

    public void testFileSaveLoad() throws Exception {
        GraphSON.save("test_graph", g);
        SparkGraph ng = GraphSON.load("test_graph", sc, StorageLevel.MEMORY_ONLY());
        Iterator<Vertex> o1 = ng.getVertices("name", "marko").iterator();
        assertTrue(o1.hasNext());
        Vertex v1 = o1.next();
        assertEquals(v1.getProperty("age"), 29);
        assertEquals(iteratorCount(g.getVertices().iterator()), iteratorCount(ng.getVertices().iterator()));
        assertEquals(iteratorCount(g.getEdges().iterator()), iteratorCount(ng.getEdges().iterator()));
    }
}