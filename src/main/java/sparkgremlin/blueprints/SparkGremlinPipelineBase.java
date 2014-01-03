package sparkgremlin.blueprints;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.java.GremlinFluentPipeline;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.util.Pipeline;

/**
 * Created by kellrott on 12/23/13.
 */
public abstract class SparkGremlinPipelineBase <S, E> extends Pipeline<S, E> implements GremlinFluentPipeline<S, E> {

    abstract SparkGremlinPipelineBase<S, E> identity();

    /**
     * Returns the current pipeline with a new end type.
     * Useful if the end type of the pipeline cannot be implicitly derived.
     *
     * @return returns the current pipeline with the new end type.
     */
    @Override
    public <E> SparkGremlinPipelineBase<S, E> cast(Class<E> end) {
        return (SparkGremlinPipelineBase<S, E>) this;
    }


}
