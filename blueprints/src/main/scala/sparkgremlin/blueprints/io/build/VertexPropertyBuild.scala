package sparkgremlin.blueprints.io.build

/**
 * Created by kellrott on 2/8/14.
 */
class VertexPropertyBuild(val vertexId:Long, val key:String, val value:Any) extends BuildElement {
   def getVertexId : Any = vertexId;
   def isEdge : Boolean = false;
   def isProperty : Boolean = true;
   def isRemoval = false;
   def getKey : String  = key;
   def getValue : Any = value;
   def getEdgeId : AnyRef = null;
   def getVertexInId : AnyRef = null;
   def getLabel : String = null;
 }
