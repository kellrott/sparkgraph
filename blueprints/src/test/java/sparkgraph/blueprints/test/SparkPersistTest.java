package sparkgraph.blueprints.test;

import com.tinkerpop.blueprints.*;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.storage.StorageLevel;
import sparkgraph.blueprints.SparkGraph;
import sparkgraph.blueprints.hadoop.GraphSON;
import sparkgraph.blueprints.parquet.BlueprintParquet;

import java.io.File;
import java.util.Iterator;

public class SparkPersistTest extends TestCase {

    private SparkContext sc = null;
    SparkGraph g = null;

    public void setUp() {
        System.err.println("Setting Up");
        if (sc == null) {
            sc = new SparkContext("local", "SparkGraphTest", new SparkConf());
            g = SparkGraphTestFactory.createSparkGraph(sc, StorageLevel.MEMORY_ONLY_SER() );
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


    public void testPersist() throws Exception {
        SparkGraph next = new SparkGraph(g.graphX());
        next.graphX().edges().count();
    }

}