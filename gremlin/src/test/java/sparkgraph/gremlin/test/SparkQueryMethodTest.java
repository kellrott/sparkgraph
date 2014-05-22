package sparkgraph.gremlin.test;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.PipeHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import sparkgraph.blueprints.SparkGraph;
import sparkgraph.blueprints.test.SparkGraphTestFactory;
import sparkgraph.gremlin.SparkGremlinPipeline;
import sparkgraph.gremlin.pipe.SparkGraphQueryPipe;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kellrott on 5/19/14.
 */
public class SparkQueryMethodTest extends BaseTest {

    private SparkContext sc = null;
    public void setUp() {
        System.err.println("Setting Up");
        if (sc == null) {
            sc = new SparkContext("local", "SparkGraphTest", new SparkConf());
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

    public void test_vertex_count() throws Exception {
        SparkGraph sg = SparkGraphTestFactory.createSparkGraph(sc);
        long count = new SparkGremlinPipeline(sg).V().count();
        assertEquals(count, 6);
    }

    public void test_edge_count() throws Exception {
        SparkGraph sg = SparkGraphTestFactory.createSparkGraph(sc);
        long count = new SparkGremlinPipeline(sg).E().count();
        assertEquals(count, 6);
    }


    public void test_multiple_use() throws Exception {
        SparkGraph sg = SparkGraphTestFactory.createSparkGraph(sc);
        SparkGremlinPipeline gr = new SparkGremlinPipeline(sg);
        Iterator<Edge> e_pipe = gr.E().has("label", Tokens.T.eq, "knows");
        int counter = 0;
        while (e_pipe.hasNext()) {
            counter++;
            assertEquals(e_pipe.next().getLabel(), "knows");
        }
        assertEquals(counter, 2);

        Iterator<Vertex> v_pipe = gr.V().has("age", Tokens.T.gt, 30);
        List<Vertex> list = new ArrayList<Vertex>();
        PipeHelper.fillCollection(v_pipe, list);
        assertEquals(list.size(), 2);
        for (Vertex v : list) {
            assertTrue((Integer) v.getProperty("age") > 30);
        }

        List<Pipe> pipes = gr.V().getPipes();
        int query_class_count = 0;
        for (Pipe p : pipes) {
            if (p instanceof SparkGraphQueryPipe) {
                query_class_count++;
            }
        }
        assertEquals(1, query_class_count);
    }


    public void test_link_property_count() throws Exception {
        SparkGraph sg = SparkGraphTestFactory.createSparkGraph(sc);
        SparkGremlinPipeline gr = new SparkGremlinPipeline(sg);
        long count = gr.V().has("name", "marko").out("created").property("lang").count();
        assertEquals(1, count);
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testSparkGraph");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }
    }
}