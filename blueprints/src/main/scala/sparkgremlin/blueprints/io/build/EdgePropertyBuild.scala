package sparkgremlin.blueprints.io.build

/**
 * Created by kellrott on 2/8/14.
 */
class EdgePropertyBuild(val id:Long, val outVertexId:Any, val inVertexId:Any, val key:String, val value:Any) extends BuildElement {
   if ( id == null) {
     throw new IllegalArgumentException("Null Edge ID")
   }
   def getVertexId : Any = outVertexId;
   def isEdge = true;
   def isProperty = true;
   def getKey : String  = key;
   def getValue : Any = value;
   def getEdgeId : Any = id;
   def getVertexInId : Any = inVertexId;
   def isRemoval = false;
   def getLabel : String = null;
 }
