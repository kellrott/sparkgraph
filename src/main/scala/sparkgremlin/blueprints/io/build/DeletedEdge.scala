package sparkgremlin.blueprints.io.build

import sparkgremlin.blueprints.SparkEdge

/**
 * Created by kellrott on 2/8/14.
 */
class DeletedEdge(override val id:AnyRef) extends SparkEdge(id, null, null, null, null)
