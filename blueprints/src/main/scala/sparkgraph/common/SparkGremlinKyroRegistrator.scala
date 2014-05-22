package sparkgraph.common

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import sparkgraph.blueprints.{SparkEdge, SparkVertex}
import org.apache.spark.graphx


class SparkGremlinKyroRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SparkVertex])
    kryo.register(classOf[SparkEdge])
    kryo.register(classOf[graphx.Edge[SparkEdge]])
  }
}
