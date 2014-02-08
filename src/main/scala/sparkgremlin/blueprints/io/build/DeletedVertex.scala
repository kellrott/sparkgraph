package sparkgremlin.blueprints.io.build

import sparkgremlin.blueprints.SparkVertex

/**
 * Created by kellrott on 2/8/14.
 */
class DeletedVertex(override val id: AnyRef) extends SparkVertex(id, null)
