package sparkgraph.blueprints.io.build

import scala.Predef._

/**
 * Created by kellrott on 2/8/14.
 */
class EdgeBuild(val id:Long, val outVertexId:Long, val inVertexId:Long, val label:String) extends BuildElement {
   if ( id == null) {
     throw new IllegalArgumentException("Null Edge ID")
   }
   def getVertexId : Any = outVertexId;
   def isEdge : Boolean = true;
   def isProperty : Boolean = false;
   def isRemoval = false;
   def getKey : String  = null;
   def getValue : Any = null;
   def getEdgeId : Any = id;
   def getVertexInId : Any = inVertexId;
   def getLabel : String = label;
 }
