package sparkgremlin.blueprints.io.build

/**
 * Created by kellrott on 2/8/14.
 */
class EdgePropertyBuild(val id:AnyRef, val outVertexId:AnyRef, val inVertexId:AnyRef, val key:String, val value:Any) extends BuildElement {
   if ( id == null) {
     throw new IllegalArgumentException("Null Edge ID")
   }
   def getVertexId : AnyRef = outVertexId;
   def isEdge = true;
   def isProperty = true;
   def getKey : String  = key;
   def getValue : Any = value;
   def getEdgeId : AnyRef = id;
   def getVertexInId : AnyRef = inVertexId;
   def isRemoval = false;
   def getLabel : String = null;
 }
