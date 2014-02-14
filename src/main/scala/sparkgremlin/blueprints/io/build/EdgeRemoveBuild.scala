package sparkgremlin.blueprints.io.build

/**
 * Created by kellrott on 2/8/14.
 */
class EdgeRemoveBuild(val id:Long) extends BuildElement {
   def getVertexId : Any = null;
   def isEdge : Boolean = true;
   def isProperty : Boolean = false;
   def isRemoval = true;
   def getKey : String  = null;
   def getValue : Any = null;
   def getEdgeId : Any = id;
   def getVertexInId : AnyRef = null;
   def getLabel : String = null;
 }
