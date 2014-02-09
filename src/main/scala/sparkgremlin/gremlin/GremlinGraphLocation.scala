package sparkgremlin.gremlin

/**
 * Created by kellrott on 2/8/14.
 */
class GremlinGraphLocation extends  Serializable {
   def this(element: AnyRef) = {
     this();
     value = element;
   }
   var value : AnyRef = null;
 }
