package sparkgremlin.gremlin

import com.tinkerpop.pipes.{Pipe, AbstractPipe}
import com.tinkerpop.pipes.util.iterators.HistoryIterator

/** BulkPipe is the base class that extends on the original AbstractPipe
   * The original Pipe Classes chains togeather 'processNextStart'
   * calls to form a processing pipe that occurs one element at a time.
   * The bulk pipe inverts that add lets each step in the pipe chain operate
   * on the entire set of data before passing it along.
   * The allows for segments of the chain to operate in MapReduce space
   *
   * @tparam S Start Type
   * @tparam E End Type
   */
abstract class BulkPipe[S,E] extends AbstractPipe[S,E] {

   var bulkStartPipe : BulkPipe[_,S] = null;
   var bulkStarts : BulkPipeData[S] = null;

   /**
    * If this pipe element does not implement a reader (ie assumes all input data will
    * be in be in bulk form, throw SparkPipelineException(SparkPipelineException.NON_READER);
    * @param input Input data to this section of the pipe
    * @return I BulkPipeData object
    */
   def bulkReader(input: java.util.Iterator[S]) : BulkPipeData[S];

   /**
    * Override to do bulk processing. The 'bulkStarts' member will be ready when this is called.
    * @return
    */
   def bulkProcess() : BulkPipeData[E];

   var collectedOutput : Iterator[E] = null;

   override def setStarts(starts: java.util.Iterator[S]) = {
     if (starts.isInstanceOf[BulkPipe[_,S]]) {
       this.starts = null
       this.bulkStartPipe = starts.asInstanceOf[BulkPipe[_,S]];
     } else if (starts.isInstanceOf[Pipe[_, _]]) {
       this.starts = starts
       this.bulkStartPipe = null;
     } else {
       this.starts = new HistoryIterator[S](starts)
       this.bulkStartPipe = null;
     }
   }

   def bulkProcessStart() : BulkPipeData[E] = {
     if (bulkStartPipe != null) {
       bulkStarts = bulkStartPipe.bulkProcessStart();
       return bulkProcess();
     } else if (starts != null) {
       bulkStarts = bulkReader(starts);
       return bulkProcess();
     } else {
       throw new RuntimeException("Unable to link Bulk Pipe")
     }
     return null;
   }

   def processNextStart(): E = {
     if (collectedOutput == null) {
       if (bulkStartPipe != null) {
         bulkStarts = bulkStartPipe.bulkProcessStart();
         collectedOutput = bulkProcess().extract();
       } else if (starts != null) {
         bulkStarts = bulkReader(starts);
         collectedOutput = bulkProcess().extract();
       } else {
         throw new RuntimeException("Unable to link Bulk Pipe")
       }
     }
     if (!collectedOutput.hasNext) {
       throw new NoSuchElementException();
     }
     return collectedOutput.next()
   }
 }
