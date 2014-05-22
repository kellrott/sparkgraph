package sparkgraph.gremlin.pipe

import collection.JavaConverters._
import com.tinkerpop.pipes.util.structures.Table
import sparkgraph.gremlin.{SparkGraphBulkData, SparkPipelineException, BulkPipeData, SparkGremlinPipelineBase}

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphTablePipe[S](var table: Table, columnFunctions: SparkGremlinPipelineBase.PipeFunctionWrapper) extends BulkSideEffectPipe[S,Table] {
   def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
     throw new SparkPipelineException(SparkPipelineException.NON_READER);
   }

   def bulkProcess(): BulkPipeData[S] = {
     var currentFunction = 0;
     if (table == null) {
       table = new Table();
     }
     val bs = bulkStarts.asInstanceOf[SparkGraphBulkData[S]];
     table.setColumnNames(bs.asColumns:_*);
     val data = bs.stateGraph.vertices.filter( _._2._2.travelers != null ).flatMap( _._2._2.travelers ).collect()
       data.foreach( x => {
       val y = bs.asColumns.map( z => x.asColumnMap(z) );
       if (columnFunctions != null) {
         val row = columnFunctions.rowCalc(currentFunction, new java.util.ArrayList(y.map(_.value).toList.asJava) );
         currentFunction += y.length;
         table.addRow(row)
       } else {
         table.addRow(y.map(_.value):_*);
       }
     } )
     return bulkStarts;
   }

   def getSideEffect(): Table = {
     return table;
   }
 }
