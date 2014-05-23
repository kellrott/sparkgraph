package sparkgraph.gremlin

import collection.JavaConverters._

/**
 * Created by kellrott on 2/8/14.
 */
object GremlinTraveler {
   def addAsColumn(t: GremlinTraveler, name:String, eid : Long, element: AnyRef) : GremlinTraveler = {
     val out = new GremlinTraveler();
     out.asColumnMap = if (t.asColumnMap != null) {
       (t.asColumnMap ++ Map[String,GremlinGraphLocation](name -> new GremlinGraphLocation(eid, element)));
     } else {
       Map(name -> new GremlinGraphLocation(eid, element));
     }
     return out;
   }
 }


class GremlinTraveler extends Serializable {
  var asColumnMap : Map[String,GremlinGraphLocation] = null;
}