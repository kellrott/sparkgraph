package sparkgremlin.blueprints.io.build

import sparkgremlin.blueprints.SparkEdge

/**
 * Created by kellrott on 2/8/14.
 */
class DeletedEdge(override val id:Long) extends SparkEdge(id, 0, 0, null, null)
