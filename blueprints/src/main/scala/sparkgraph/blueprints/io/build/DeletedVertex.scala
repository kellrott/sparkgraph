package sparkgraph.blueprints.io.build

import sparkgraph.blueprints.SparkVertex

/**
 * Created by kellrott on 2/8/14.
 */
class DeletedVertex(override val id: Long) extends SparkVertex(id, null)
