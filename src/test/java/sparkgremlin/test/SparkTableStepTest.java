package sparkgremlin.test;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import com.tinkerpop.gremlin.java.GremlinFluentPipeline;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.gremlin.test.ComplianceTest;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.structures.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import sparkgremlin.blueprints.SparkGremlinPipeline;
import sparkgremlin.blueprints.SparkGremlinPipelineBase;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkTableStepTest extends com.tinkerpop.gremlin.test.sideeffect.TableStepTest {

    SparkContext sc = null;
    Graph g = null;

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
            g = null;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) { }
        }
    }

    public void test_g_v1_asXaX_out_properyXnameX_asXbX_table_cap() {
        super.test_g_v1_asXaX_out_properyXnameX_asXbX_table_cap(new SparkGremlinPipeline(g.getVertex(1)).as("a").out().property("name").as("b").table().cap());
        super.test_g_v1_asXaX_out_properyXnameX_asXbX_table_cap(new SparkGremlinPipeline(g.getVertex(1)).optimize(false).as("a").out().property("name").as("b").table().cap());
    }

    public void test_g_v1_asXaX_out_asXbX_tableXnameX_cap() {
        super.test_g_v1_asXaX_out_asXbX_tableXnameX_cap(new SparkGremlinPipeline(g.getVertex(1)).as("a").out().as("b").table(new Table(), new PipeFunction<Vertex, String>() {
            public String compute(Vertex vertex) {
                return (String) vertex.getProperty("name");
            }
        }).cap());

        super.test_g_v1_asXaX_out_asXbX_tableXnameX_cap(new SparkGremlinPipeline(g.getVertex(1)).optimize(false).as("a").out().as("b").table(new Table(), new PipeFunction<Vertex, String>() {
            public String compute(Vertex vertex) {
                return (String) vertex.getProperty("name");
            }
        }).cap());
    }


    public void test_g_v1_asXaX_out_propertyXnameX_asXbX_tableXname_lengthX_cap() {
        super.test_g_v1_asXaX_out_propertyXnameX_asXbX_tableXname_lengthX_cap(new SparkGremlinPipeline(g.getVertex(1)).as("a").out().property("name").as("b").table(new Table(), new PipeFunction<Vertex, String>() {
                    public String compute(Vertex vertex) {
                        return (String) vertex.getProperty("name");
                    }
                }, new PipeFunction<String, Integer>() {
                    public Integer compute(String name) {
                        return name.length();
                    }
                }
        ).cap());

        super.test_g_v1_asXaX_out_propertyXnameX_asXbX_tableXname_lengthX_cap(new SparkGremlinPipeline(g.getVertex(1)).optimize(false).as("a").out().property("name").as("b").table(new Table(), new PipeFunction<Vertex, String>() {
                    public String compute(Vertex vertex) {
                        return (String) vertex.getProperty("name");
                    }
                }, new PipeFunction<String, Integer>() {
                    public Integer compute(String name) {
                        return name.length();
                    }
                }
        ).cap());
    }

}