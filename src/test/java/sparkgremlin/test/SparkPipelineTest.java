package sparkgremlin.test;

/**
 * Created by kellrott on 12/17/13.
 */


import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.gremlin.java.GremlinStartPipe;
import com.tinkerpop.pipes.IdentityPipe;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.filter.FilterPipe;
import com.tinkerpop.pipes.filter.PropertyFilterPipe;
import com.tinkerpop.pipes.filter.RangeFilterPipe;
import com.tinkerpop.pipes.sideeffect.SideEffectFunctionPipe;
import com.tinkerpop.pipes.transform.*;
import junit.framework.TestCase;
import org.apache.spark.SparkContext;
import sparkgremlin.blueprints.SparkGraph;
import sparkgremlin.blueprints.SparkGremlinPipeline;
import sparkgremlin.blueprints.SparkGremlinStartPipe;


public class SparkPipelineTest extends TestCase {

    private SparkContext sc = null;
    public void setUp() {
        System.err.println("Setting Up");
        if (sc == null) {
            sc = new SparkContext("local", "SparkGraphTest", null, null, null, null);
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

    /*
    public void testNoGraphInPath() {
        SparkGraph graph = SparkGraphTestFactory.createSparkGraph(sc);

        Pipe pipe = new SparkGremlinStartPipe(graph);
        pipe.enablePath(true);
        System.out.println(pipe.getCurrentPath());
        assertEquals(pipe.getCurrentPath().size(), 0);
    }
    */

    /*
    public void testVertexQueryOptimization() {
        Graph graph = SparkGraphTestFactory.createSparkGraph(sc);

        GremlinPipeline pipeline = new SparkGremlinPipeline(graph.getVertex(1)).outE().inV();
        // System.out.println(pipeline);
        assertEquals(pipeline.size(), 3);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE()._().inV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE("knows").has("weight", 0.5).inV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE("knows").has("weight", 0.5).interval("since", 10, 2).inV();
        // System.out.println(pipeline);
        assertEquals(pipeline.size(), 5);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);
        assertTrue(pipeline.get(4) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE("knows").has("weight", 0.5).interval("since", 10, 2).outV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 5);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);
        assertTrue(pipeline.get(4) instanceof OutVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).inE("knows", "created").has("weight", 0.5).interval("since", 10, 2).outV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 5);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);
        assertTrue(pipeline.get(4) instanceof OutVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).bothE("knows", "created").has("weight", 0.5).interval("since", 10, 2).bothV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 5);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);
        assertTrue(pipeline.get(4) instanceof BothVerticesPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).bothE("knows", "created").has("weight", 0.5)._().interval("since", 10, 2).bothV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 6);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);
        assertTrue(pipeline.get(4) instanceof IdentityPipe);
        assertTrue(pipeline.get(5) instanceof BothVerticesPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE("knows", "created").has("weight", 0.5)._().interval("since", 10, 2).range(1, 10).bothV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 7);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);
        assertTrue(pipeline.get(4) instanceof IdentityPipe);
        assertTrue(pipeline.get(5) instanceof IdentityPipe);
        assertTrue(pipeline.get(6) instanceof BothVerticesPipe);
    }

    public void testNoEdgeConstraintOptimizations() {
        Graph graph = SparkGraphTestFactory.createSparkGraph(sc);

        GremlinPipeline pipeline = new GremlinPipeline(graph.getVertex(1)).outE().sideEffect(null).inV();
        // System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof SideEffectFunctionPipe);
        assertTrue(pipeline.get(3) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE().filter(null).inV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof FilterPipe);
        assertTrue(pipeline.get(3) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).outE().has("weight", 0.5).filter(null).has("date", 2012).inV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 6);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof FilterPipe);
        assertTrue(pipeline.get(4) instanceof PropertyFilterPipe);
        assertTrue(pipeline.get(5) instanceof InVertexPipe);


        // TEST OPTIMIZE(BOOLEAN) PARAMETERIZATION
        pipeline = new GremlinPipeline(graph.getVertex(1)).optimize(false).outE("knows").has("weight", 0.5).inV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof OutEdgesPipe);
        assertTrue(pipeline.get(2) instanceof PropertyFilterPipe);
        assertTrue(pipeline.get(3) instanceof InVertexPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).optimize(true).outE("knows").has("weight", 0.5).inV();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof IdentityPipe);
        assertTrue(pipeline.get(3) instanceof InVertexPipe);
    }

    public void testVertexRangeOptimization() {
        Graph graph = SparkGraphTestFactory.createSparkGraph(sc);

        GremlinPipeline pipeline = new GremlinPipeline(graph.getVertex(1)).out().out();
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 3);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof VertexQueryPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).out().out().range(0, 10);
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);

        pipeline = new GremlinPipeline(graph.getVertex(1)).out("knows").out().range(5, 10);
        //System.out.println(pipeline);
        assertEquals(pipeline.size(), 4);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(3) instanceof IdentityPipe);

        // no optimization should occur
        pipeline = new GremlinPipeline(graph.getVertex(1)).out("knows").out().has("name", "marko").range(0, 10);
        // System.out.println(pipeline);
        assertEquals(pipeline.size(), 5);
        assertTrue(pipeline.get(0) instanceof GremlinStartPipe);
        assertTrue(pipeline.get(1) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(2) instanceof VertexQueryPipe);
        assertTrue(pipeline.get(3) instanceof PropertyFilterPipe);
        assertTrue(pipeline.get(4) instanceof RangeFilterPipe);

    }


           */



}


