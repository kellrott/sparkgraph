package sparkgremlin.blueprints.io.build

/**
  * Created by kellrott on 12/7/13.
  */


trait BuildElement extends Serializable  {
   def getVertexId : AnyRef;
   def isEdge : Boolean;
   def isProperty : Boolean;
   def isRemoval : Boolean;
   def getKey : String;
   def getValue : Any;
   def getEdgeId : AnyRef;
   def getVertexInId : AnyRef;
   def getLabel : String;
 }
