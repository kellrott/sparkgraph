package sparkgraph.blueprints.io.build

/**
 * Created by kellrott on 2/8/14.
 */
class VertexRemoveBuild(val vertexId:Long) extends BuildElement {
   def getVertexId : Any = vertexId;
   def isEdge : Boolean = false;
   def isProperty : Boolean = false;
   def isRemoval = true;
   def getKey : String  = null;
   def getValue : Any = null;
   def getEdgeId : AnyRef = null;
   def getVertexInId : AnyRef = null;
   def getLabel : String = null;
 }
