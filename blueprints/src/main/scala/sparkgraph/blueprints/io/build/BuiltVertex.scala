package sparkgraph.blueprints.io.build

import sparkgraph.blueprints.SparkVertex

/**
 * Created by kellrott on 2/8/14.
 */
class BuiltVertex(val inID:Long, built : Boolean) extends SparkVertex(inID, null) {
   def wasBuilt = built;
 }
