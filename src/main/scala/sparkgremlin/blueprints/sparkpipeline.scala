package sparkgremlin.blueprints

import com.tinkerpop.blueprints._
import com.tinkerpop.pipes.util.Pipeline
import com.tinkerpop.gremlin.java.{GremlinPipeline, GremlinFluentPipeline}
import com.tinkerpop.pipes.{IdentityPipe, PipeFunction}
import com.tinkerpop.pipes.util.structures.{Row, Pair, Table, Tree}
import java.util.Map
import java.util.List
import java.util.Map.Entry
import com.tinkerpop.pipes.transform.TransformPipe.Order

class SparkGremlinStartPipe(val start:Any) extends IdentityPipe  {

}

class SparkGremlinPipeline[S, E](val start:Any) extends GremlinPipeline[S, E](start) {

}