import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Problem3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //(a)
    val file = sc.textFile("./twitter_combined.txt")

    // TODO c'è un modo migliore per fare questo cambio senza il parallelize?
    val arrayEdges = file.countByValue().toArray.map(elem => {
      val edge = elem._1.split(" ")
      Edge(edge(0).toLong, edge(1).toLong, elem._2.toInt)
    })

    val edges = sc.parallelize(arrayEdges)

    val vertices = file.flatMap(elem => {
      elem.split(" ")
    }).distinct().map(p => (p.toLong, p))
    // TODO parse the features from the additional files and assign them as the vertex labels

    val graph = Graph(vertices, edges).cache()
    //(b)
    // TODO è sbagliato il risultato
    val connectedComponentGraph: Graph[VertexId, Int] = graph.connectedComponents()

    def sortedConnectedComponents(connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
      val componentCounts = connectedComponents.vertices.map(_._2).countByValue
      componentCounts.toSeq.sortBy(_._2).reverse
    }

    val componentCounts = sortedConnectedComponents(connectedComponentGraph)
    println(componentCounts.size)

    //(c)
    // Degree-distribution
    val degrees: VertexRDD[Int] = graph.degrees.cache()
    println(degrees.map(_._2).stats())

    def topNamesAndDegrees(degrees: VertexRDD[Int],
                           topicGraph: Graph[String, Int]): Array[(String, Int)] = {
      val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
        (topicId, degree, name) => (name, degree) }
      val ord = Ordering.by[(String, Int), Int](_._2)
      namesAndDegrees.map(_._2).top(10)(ord)
    }

    topNamesAndDegrees(degrees, graph).foreach(println)

    // Average clustering-coefficient
    val triangleCountGraph = graph.triangleCount()
    triangleCountGraph.vertices.map(x => x._2).stats()

    val maxTriangleGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoefficientGraph = triangleCountGraph.vertices.
      innerJoin(maxTriangleGraph) {
        (vertexId, triCount, maxTris) => {
          if (maxTris == 0) 0 else triCount / maxTris }
      }
    clusterCoefficientGraph.map(_._2).sum() / graph.vertices.count()

    // TODO average path length e commenti

    //(d)
    // PageRank API
    val ranks = graph.pageRank(0.001, 0.15).vertices
    val namesAndRanks = ranks.innerJoin(graph.vertices) {
      (topicId, rank, name) => (name, rank)
    }
    val ord = Ordering.by[(String, Double), Double](_._2)
    namesAndRanks.map(_._2).top(250)(ord).foreach(println)

    // TODO compara con i gradi normali
  }
}
