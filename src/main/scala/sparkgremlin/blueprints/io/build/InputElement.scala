package sparkgremlin.blueprints.io.build

import com.tinkerpop.blueprints.Element
import sparkgremlin.blueprints.SparkGraph

/**
 * Created by kellrott on 2/8/14.
 */
abstract class InputElement extends Element {

   def getProperty[T](key: String): T = {
     throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
   }

   def getPropertyKeys: java.util.Set[String] = {
     throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
   }

   def removeProperty[T](key: String): T = {
     throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
   }

   def remove() = {
     throw new UnsupportedOperationException(SparkGraph.READ_ONLY_MESSAGE);
   }
 }
