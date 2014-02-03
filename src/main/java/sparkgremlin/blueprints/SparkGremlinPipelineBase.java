package sparkgremlin.blueprints;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.java.GremlinFluentPipeline;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.sideeffect.SideEffectPipe;
import com.tinkerpop.pipes.transform.SideEffectCapPipe;
import com.tinkerpop.pipes.util.FluentUtility;
import com.tinkerpop.pipes.util.Pipeline;
import com.tinkerpop.pipes.util.structures.Row;
import com.tinkerpop.pipes.util.structures.Table;

import java.util.ArrayList;

/**
 * Created by kellrott on 12/23/13.
 */
public abstract class SparkGremlinPipelineBase <S, E> extends Pipeline<S, E> implements GremlinFluentPipeline<S, E>, Iterable<E> {

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

    /**
     * Remap the table to __table to eliminate problems with variable argument methods with going between Java and Scala
     * @param table
     * @param columnFunctions
     * @return
     */
    public SparkGremlinPipelineBase<S,E> table(Table table, PipeFunction ... columnFunctions) {
        return this.__table(table, new PipeFunctionWrapper(columnFunctions));
    }

    static public class PipeFunctionWrapper {
        public PipeFunctionWrapper(PipeFunction []columnFunctions) {
            this.columnFunctions = columnFunctions;
            doFunctions = this.columnFunctions.length > 0;
        }
        PipeFunction []columnFunctions;
        Boolean doFunctions;

        public Row rowCalc(Integer offset, ArrayList values) {
            int currentFunction = offset;
            Row row = new Row();
            for (int i = 0; i < values.size(); i++) {
            if (doFunctions) {
                    row.add(this.columnFunctions[currentFunction++ % columnFunctions.length].compute(values.get(i)));
                } else {
                    row.add(values.get(i));
                }
            }
            return row;
        }
    }

    abstract public SparkGremlinPipelineBase<S,E> __table(Table table, PipeFunctionWrapper wrapper);

    public SparkGremlinPipelineBase<S,E> _() {
        return __underscore();
    }

    abstract public SparkGremlinPipelineBase<S,E> __underscore();

    public SparkGremlinPipelineBase<S, ?> cap() {
        throw new RuntimeException("Unimplemented Method");
    }
}
