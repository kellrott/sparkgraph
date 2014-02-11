package sparkgremlin.blueprints.io.build

/**
 * Created by kellrott on 2/8/14.
 */
class EdgeRemoveBuild(val id:Long, val outVertexId:Any) extends BuildElement {
   def getVertexId : Any = outVertexId;
   def isEdge : Boolean = true;
   def isProperty : Boolean = false;
   def isRemoval = true;
   def getKey : String  = null;
   def getValue : Any = null;
   def getEdgeId : Any = id;
   def getVertexInId : AnyRef = null;
   def getLabel : String = null;
 }
