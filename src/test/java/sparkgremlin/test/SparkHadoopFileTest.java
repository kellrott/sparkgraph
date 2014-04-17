package sparkgremlin.test;

import com.tinkerpop.blueprints.*;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.storage.StorageLevel;
import sparkgremlin.blueprints.SparkGraph;
import sparkgremlin.blueprints.hadoop.GraphSON;
import sparkgremlin.blueprints.parquet.BlueprintParquet;

import java.io.File;
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

    public void testGraphSONFileSaveLoad() throws Exception {
        if (new File("test_graph").exists()) {
            FileUtils.deleteDirectory(new File("test_graph"));
        }
        GraphSON.save("test_graph", g);
        SparkGraph ng = GraphSON.load("test_graph", sc, StorageLevel.MEMORY_ONLY());
        for (Vertex v : ng.getVertices()) {
            System.out.println(v);
        }
        for (Edge e : ng.getEdges()) {
            System.out.println(e);
        }
        Iterator<Vertex> o1 = ng.getVertices("name", "marko").iterator();
        assertTrue(o1.hasNext());
        Vertex v1 = o1.next();
        assertEquals(v1.getProperty("age"), 29);
        assertEquals(iteratorCount(g.getVertices().iterator()), iteratorCount(ng.getVertices().iterator()));
        assertEquals(iteratorCount(g.getEdges().iterator()), iteratorCount(ng.getEdges().iterator()));
        FileUtils.deleteDirectory(new File("test_graph"));
    }

    public void testParquetFileSaveLoad() throws Exception {
        if (new File("test_graph").exists()) {
            FileUtils.deleteDirectory(new File("test_graph"));
        }
        BlueprintParquet.save("test_graph", g);
        SparkGraph ng = BlueprintParquet.load("test_graph", sc, StorageLevel.MEMORY_ONLY());
        for (Vertex v : ng.getVertices()) {
            System.out.println(v);
        }
        for (Edge e : ng.getEdges()) {
            System.out.println(e);
        }
        Iterator<Vertex> o1 = ng.getVertices("name", "marko").iterator();
        assertTrue(o1.hasNext());
        Vertex v1 = o1.next();
        assertEquals(v1.getProperty("age"), 29);
        assertEquals(iteratorCount(g.getVertices().iterator()), iteratorCount(ng.getVertices().iterator()));
        assertEquals(iteratorCount(g.getEdges().iterator()), iteratorCount(ng.getEdges().iterator()));
        FileUtils.deleteDirectory(new File("test_graph"));
    }
}