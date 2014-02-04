package sparkgremlin.test;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;
import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import sparkgremlin.blueprints.SparkGraph;
import sparkgremlin.blueprints.SparkGraphHadoop;


import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;

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

    public void testFileSaveLoad() throws Exception {
        System.out.println("Howdy");
        SparkGraphHadoop.saveAsHadoopGraphSON("test_data", g);

    }
}