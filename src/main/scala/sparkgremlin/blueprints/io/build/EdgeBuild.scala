package sparkgremlin.blueprints.io.build

import scala.Predef._

/**
 * Created by kellrott on 2/8/14.
 */
class EdgeBuild(val id:AnyRef, val outVertexId:AnyRef, val inVertexId:AnyRef, val label:String) extends BuildElement {
   if ( id == null) {
     throw new IllegalArgumentException("Null Edge ID")
   }
   def getVertexId : AnyRef = outVertexId;
   def isEdge : Boolean = true;
   def isProperty : Boolean = false;
   def isRemoval = false;
   def getKey : String  = null;
   def getValue : Any = null;
   def getEdgeId : AnyRef = id;
   def getVertexInId : AnyRef = inVertexId;
   def getLabel : String = label;
 }
