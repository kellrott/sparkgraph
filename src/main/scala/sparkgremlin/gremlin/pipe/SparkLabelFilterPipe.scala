package sparkgremlin.gremlin.pipe

import com.tinkerpop.blueprints.Predicate

/**
 * Created by kellrott on 2/8/14.
 */
class SparkLabelFilterPipe extends SparkPropertyFilterPipe {
   def this(predicate: Predicate, value: AnyRef) {
     this()
     this.value = value
     this.predicate = predicate
     this.key = "label"
   }
 }
