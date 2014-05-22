package sparkgremlin.blueprints

import com.tinkerpop.blueprints.{Predicate, Direction, Query}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by kellrott on 2/8/14.
 */
abstract class BaseQuery extends Query {
  var directionValue : Direction = Direction.BOTH;
  var labelSet = Array[String]();
  var limit = Integer.MAX_VALUE;
  var hasContainers = new ArrayBuffer[HasContainer]();

  def has(key: String): Query = {
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.NOT_EQUAL, null));
    return this;
  }

  def has(key: String, value: scala.AnyRef): Query = {
    this.hasContainers += new HasContainer(key, com.tinkerpop.blueprints.Compare.EQUAL, value);
    return this;
  }

  def has(key: String, predicate: Predicate, value: scala.AnyRef): Query = {
    this.hasContainers += (new HasContainer(key, predicate, value));
    return this;
  }

  def has[T <: Comparable[T]](key: String, value: T, compare: Query.Compare): Query = {
    return this.has(key, compare, value.asInstanceOf[AnyRef]);
  }

  def hasNot(key: String): Query = {
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.EQUAL, null));
    return this;
  }

  def hasNot(key: String, value: scala.AnyRef): Query = {
    this.hasContainers += new HasContainer(key, com.tinkerpop.blueprints.Compare.NOT_EQUAL, value);
    return this;
  }

  def interval[T <: Comparable[_]](key: String, startValue: T, endValue: T): Query = {
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.GREATER_THAN_EQUAL, startValue.asInstanceOf[AnyRef]));
    this.hasContainers += (new HasContainer(key, com.tinkerpop.blueprints.Compare.LESS_THAN, endValue.asInstanceOf[AnyRef]));
    return this;
  }

  def limit(count: Int): Query = {
    this.limit = count;
    return this;
  }
}
