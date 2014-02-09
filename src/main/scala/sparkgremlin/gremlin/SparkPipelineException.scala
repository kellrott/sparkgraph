package sparkgremlin.gremlin

import java.lang.String
import scala.Predef.String

/**
 * Created by kellrott on 2/8/14.
 */
object SparkPipelineException {
   val NON_READER: String = "Bulk Pipeline Step does not implement reader"
 }


class SparkPipelineException(msg:String) extends RuntimeException(msg) {

}