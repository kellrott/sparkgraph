package sparkgraph.blueprints.io.build

import sparkgraph.blueprints.SparkEdge

/**
 * Created by kellrott on 2/13/14.
 */
class BuiltEdge (override val id:Long, built : Boolean, vertexOutId:Long=0, vertexInId:Long=0, label:String=null  )
  extends SparkEdge(id, vertexOutId, vertexInId, label, null) {
  def wasBuilt = built;
}