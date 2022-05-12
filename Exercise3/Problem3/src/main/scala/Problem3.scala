import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Problem3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //(a)
    val file = sc.textFile("./twitter_combined.txt")

    val edges = file.map(elem => {
      val edge = elem.split(" ")
      Edge(edge(0).toLong, edge(1).toLong, 1) // TODO perchè ho messo 1? Boh sandro chi lo sa!
    })

    val vertices = file.flatMap(elem => {
      elem.split(" ")
    }).distinct().map(p => (p.toLong, p.toLong)) // TODO parse the features from the additional files and assign them as the vertex labels
    // TODO prossima volta scrivilo ancora dopo prof

    val graph = Graph(vertices, edges).cache()

    //(b)
    val connectedComponentGraph: Graph[VertexId, Int] = graph.connectedComponents()

  }
}
