package sparkgremlin.blueprints.io.build

import sparkgremlin.blueprints.SparkVertex

/**
 * Created by kellrott on 2/8/14.
 */
class SparkVertexBuilt(val inID:AnyRef, built : Boolean) extends SparkVertex(inID, null) {
   def wasBuilt = built;
 }
