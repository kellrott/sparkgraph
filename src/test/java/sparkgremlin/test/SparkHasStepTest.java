
package sparkgremlin.test;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.gremlin.test.ComplianceTest;
import com.tinkerpop.gremlin.Tokens.T;
import org.apache.spark.SparkContext;
import java.util.Arrays;
import sparkgremlin.blueprints.SparkGremlinPipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkHasStepTest extends com.tinkerpop.gremlin.test.filter.HasStepTest {

    SparkContext sc = null;
    Graph g = null;

    public void setUp() {
        System.err.println("Setting Up");
        if (sc == null) {
            sc = new SparkContext("local", "SparkGraphTest", null, null, null, null);
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

    public void testCompliance() {
        ComplianceTest.testCompliance(this.getClass());
    }


    public void test_g_V_hasXname_markoX() {
        super.test_g_V_hasXname_markoX(new SparkGremlinPipeline(g).V().has("name", "marko"));
        super.test_g_V_hasXname_markoX(new SparkGremlinPipeline(g.getVertices()).optimize(false).has("name", "marko"));
    }

    public void test_g_V_hasXname_blahX() {
        super.test_g_V_hasXname_blahX(new SparkGremlinPipeline(g.getVertices()).has("name", "blah"));
        super.test_g_V_hasXname_blahX(new SparkGremlinPipeline(g.getVertices()).optimize(false).has("name", "blah"));
    }

    public void test_g_V_hasXblahX() {
        super.test_g_V_hasXblahX(new SparkGremlinPipeline(g.getVertices()).has("blah"));
        super.test_g_V_hasXblahX(new SparkGremlinPipeline(g).V().optimize(false).has("blah"));
    }

    /*
    public void test_g_v1_out_hasXid_2X() {
        super.test_g_v1_out_hasXid_2X(new SparkGremlinPipeline(g.getVertex(1)).out().has("id", "2"));
        super.test_g_v1_out_hasXid_2X(new SparkGremlinPipeline(g.getVertex(1)).optimize(false).out().has("id", "2"));
    }
    */

    public void test_g_V_hasXage_gt_30X() {
        super.test_g_V_hasXage_gt_30X(new SparkGremlinPipeline(g).V().has("age", T.gt, 30));
        super.test_g_V_hasXage_gt_30X(new SparkGremlinPipeline(g).V().optimize(false).has("age", T.gt, 30));
    }

    public void test_g_E_hasXlabelXknowsX() {
        super.test_g_E_hasXlabelXknowsX(new SparkGremlinPipeline(g).E().has("label", T.eq, "knows"));
        super.test_g_E_hasXlabelXknowsX(new SparkGremlinPipeline(g).optimize(false).E().has("label", T.eq, "knows"));
    }

    public void test_g_E_hasXlabelXknows_createdX() {
        super.test_g_E_hasXlabelXknows_createdX(new SparkGremlinPipeline(g.getEdges()).has("label", T.in, Arrays.asList("knows", "created")));
        super.test_g_E_hasXlabelXknows_createdX(new SparkGremlinPipeline(g).E().optimize(false).has("label", T.in, Arrays.asList("knows", "created")));

    }

}