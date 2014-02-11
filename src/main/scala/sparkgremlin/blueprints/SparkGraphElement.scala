package sparkgremlin.blueprints

import com.tinkerpop.blueprints.Element

trait SparkGraphElement extends Serializable with Element {
  def getGraph() : SparkGraph
  def setGraph(graph: SparkGraph)
}
