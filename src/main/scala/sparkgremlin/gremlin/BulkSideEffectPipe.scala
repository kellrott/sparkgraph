package sparkgremlin.gremlin

/**
  *
  * @tparam S Start Type
  * @tparam T
  */
trait BulkSideEffectPipe[S,T] extends BulkPipe[S,S] {
   def getSideEffect() : T;
 }
