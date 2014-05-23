package sparkgraph.gremlin

/**
 * Created by kellrott on 2/8/14.
 */
class GremlinGraphLocation extends  Serializable {
   def this(vid: Long, element: AnyRef) = {
     this()
     value = element
     vertexID = vid
   }
   var vertexID : Long = 0L
   var value : AnyRef = null
 }
