package sparkgremlin.gremlin.pipe

import sparkgremlin.blueprints.{SparkGraphElementSet, SparkGraphElement}
import org.apache.spark.rdd.RDD
import sparkgremlin.gremlin.{SparkGraphBulkData, BulkPipeData, BulkPipe}

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGremlinStartPipe[E <: SparkGraphElement](startGraph : SparkGraphElementSet[E]) extends BulkPipe[E,E] {
   def bulkReader(input: java.util.Iterator[E]): BulkPipeData[E] = {
     val y = input.asInstanceOf[SparkGraphElementSet[E]];
     return new SparkGraphBulkData[E](y, null, null, null, null) {
       def currentRDD(): RDD[E] = y.elementRDD()
     };
   }
   override def bulkProcessStart() : BulkPipeData[E] = {
     starts = startGraph;
     bulkStarts = bulkReader(starts);
     return bulkProcess();
   }
   def bulkProcess(): BulkPipeData[E] = bulkStarts
 }
