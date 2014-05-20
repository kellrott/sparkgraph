package sparkgremlin.gremlin.pipe

import sparkgremlin.gremlin.{BulkDataType, SparkPipelineException, BulkPipeData, BulkPipe}

/**
 * Created by kellrott on 2/8/14.
 */
class SparkGraphCapPipe[S,E]() extends BulkPipe[S,E] {
   def bulkReader(input: java.util.Iterator[S]): BulkPipeData[S] = {
     throw new SparkPipelineException(SparkPipelineException.NON_READER);
   }

   def bulkProcess(): BulkPipeData[E] = {
     if (!bulkStartPipe.isInstanceOf[BulkSideEffectPipe[S,E]]) {
       throw new RuntimeException("Cap not connected to side effect pipe");
     }
     return new SingleElementBulkPipe(bulkStartPipe.asInstanceOf[BulkSideEffectPipe[S,E]].getSideEffect(), BulkDataType.OTHER_DATA);
   }

 }
