package sparkgremlin.gremlin.pipe

import sparkgremlin.gremlin.BulkPipe

/**
  *
  * @tparam S Start Type
  * @tparam T
  */
trait BulkSideEffectPipe[S,T] extends BulkPipe[S,S] {
   def getSideEffect() : T;
 }
