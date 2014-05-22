package sparkgraph.blueprints

import com.tinkerpop.blueprints.Element

trait SparkGraphElement extends Serializable with Element {
  def getGraph() : SparkGraph
  def getID() : Long
  def setGraph(graph: SparkGraph)
}
