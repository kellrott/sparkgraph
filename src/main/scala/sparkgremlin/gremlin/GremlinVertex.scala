package sparkgremlin.gremlin

/**
 * Created by kellrott on 2/8/14.
 */
object GremlinVertex {
   def merge(a: GremlinVertex, b:GremlinVertex) : GremlinVertex = {
     val out = new GremlinVertex();
     out.travelers = if (a.travelers != null && b.travelers != null) {
       a.travelers ++ b.travelers
     } else if (a.travelers != null) {
       a.travelers
     } else if (b.travelers != null) {
       b.travelers
     } else {
       null
     }
     out.travelerCount = a.travelerCount + b.travelerCount;
     return out;
   }

   def addAsColumn(a: GremlinVertex, name: String, element : AnyRef) : GremlinVertex = {
     val out = new GremlinVertex()
     out.travelerCount = a.travelerCount;
     out.travelers = if (a.travelers != null) {
       a.travelers.map( x => GremlinTraveler.addAsColumn(x, name, element) )
     } else if (a.travelerCount > 0) {
       0.until(a.travelerCount).map( x => GremlinTraveler.addAsColumn(new GremlinTraveler, name, element) ).toArray
     } else {
       null
     }
     return out;
   }
 }


class GremlinVertex extends Serializable {
  def this(count:Int) = {
    this();
    travelerCount = count;
  }
  var validEdges : Array[AnyRef] = null;
  var travelers : Array[GremlinTraveler] = null;
  var travelerCount = 0;
}
