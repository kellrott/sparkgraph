package sparkgremlin.gremlin.pipe

import sparkgremlin.blueprints.SparkGraphElement
import org.apache.spark.rdd.RDD
import sparkgremlin.gremlin._

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphPropertyPipe[S <: SparkGraphElement](name:String) extends BulkPipe[S,AnyRef] {
   def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
     throw new SparkPipelineException(SparkPipelineException.NON_READER);
   }

   def bulkProcess(): BulkPipeData[AnyRef] = {
     val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[SparkGraphElement]];
     val elType = bs.elementType match {
       case BulkDataType.VERTEX_DATA => BulkDataType.VERTEX_PROP_DATA
       case BulkDataType.EDGE_DATA => BulkDataType.VERTEX_PROP_DATA
     }
     return new SparkGraphBulkData[AnyRef](
       bs.graphData, bs.vertexData, bs.asColumns, elType, name
     ) {
       override def currentRDD(): RDD[AnyRef] = {
         val n = name;
         return bs.currentRDD().map( _.getProperty(n) )
       }
     }
   }
 }
