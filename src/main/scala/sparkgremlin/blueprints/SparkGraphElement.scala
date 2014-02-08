package sparkgremlin.blueprints

import collection.JavaConverters._
import com.tinkerpop.blueprints.Element
import scala.collection.mutable.HashMap

/**
 * Base Graph element
 * @param id
 * @param graph
 */
abstract class SparkGraphElement(val id:AnyRef, @transient var graph:SparkGraph) extends Serializable with Element {

  def getId: AnyRef = id;

  val propMap = new HashMap[String,Any]();

  /**
   *
   * @param key
   * @param value
   */
  def setProperty(key: String, value: scala.AnyRef) = {
    if (key == null || key.length == 0) {
      throw new IllegalArgumentException("Invalid Key String");
    }
    if (value == null) {
      throw new IllegalArgumentException("Invalid Property Value");
    }
    propMap(key) = value;
  }

  def getProperty[T](key: String): T = {
    propMap.get(key).getOrElse(null).asInstanceOf[T];
  }

  def getPropertyKeys: java.util.Set[String] = propMap.keySet.asJava;

  def removeProperty[T](key: String): T = {
    return propMap.remove(key).orNull.asInstanceOf[T];
  }

  def labelMatch(args:String*) : Boolean = {
    if (args.length == 0) {
      return true;
    }
    if (args.length == 1) {
      return id == args(0);
    }
    if (args.length == 2) {
      return propMap.getOrElse(args(0), null) == args(1);
    }
    return false;
  }

}
