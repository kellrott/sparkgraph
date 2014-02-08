package sparkgremlin.blueprints

import com.tinkerpop.blueprints.{Element, Predicate}

/**
 * Created by kellrott on 2/8/14.
 */
class HasContainer(val key : String, val predicate: Predicate, val value:AnyRef) extends Serializable {
  def isLegal(element:Element) : Boolean = {
    return this.predicate.evaluate(element.getProperty(this.key), this.value);
  }
}
