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
    /*
    //(b)
    // TODO è sbagliato il risultato
    val connectedComponentGraph: Graph[VertexId, Int] = graph.connectedComponents()

    def sortedConnectedComponents(connectedComponents: Graph[VertexId, _]): Seq[(VertexId, Long)] = {
      val componentCounts = connectedComponents.vertices.map(_._2).countByValue
      componentCounts.toSeq.sortBy(_._2).reverse
    }

    val componentCounts = sortedConnectedComponents(connectedComponentGraph)
    println(componentCounts.size)

     */

    //(c)
    // Degree-distribution
    val degrees: VertexRDD[Int] = graph.degrees.cache()
    println(degrees.map(_._2).stats())

    def topNamesAndDegrees(degrees: VertexRDD[Int], topicGraph: Graph[String, Int]): Array[(String, Int)] = {
      val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
        (vertexId, degree, name) => (name, degree) }
      val ord = Ordering.by[(String, Int), Int](_._2)
      namesAndDegrees.map(_._2).top(10)(ord)
    }

    topNamesAndDegrees(degrees, graph).foreach(println)

    // Average clustering-coefficient
    val triangleCountGraph = graph.triangleCount()
    println(triangleCountGraph.vertices.map(x => x._2).stats())

    val maxTriangleGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoefficientGraph = triangleCountGraph.vertices.innerJoin(maxTriangleGraph) {
        (vertexId, triCount, maxTris) => {if (maxTris == 0) 0 else triCount / maxTris }
      }
    println(clusterCoefficientGraph.map(_._2).sum() / graph.vertices.count())

    // Average Path Length
    def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
      def minThatExists(k: VertexId): Int = {
        math.min(m1.getOrElse(k, Int.MaxValue), m2.getOrElse(k, Int.MaxValue))
      }
      (m1.keySet ++ m2.keySet).map{ k => (k, minThatExists(k))}.toMap
    }

    def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int]) = {
      mergeMaps(state, msg)
    }

    def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId) = {
      val aplus = a.map { case (v, d) => v -> (d + 1) }
      if (b != mergeMaps(aplus, b)) {
        Iterator((bid, aplus))
      } else {
        Iterator.empty
      }
    }

    def iterate(e: EdgeTriplet[Map[VertexId, Int], _]) = {
      checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++ checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
    }

    // TODO togli il sample
    // TODO prova su cluster perchè da java heap error :(
    val sampleVertices = graph.vertices.map(v => v._1).sample(false, 0.01).collect().toSet

    val mapGraph = graph.mapVertices((id, _) => {
      if (sampleVertices.contains(id)) {
        Map(id -> 0)
      } else {
        Map[VertexId, Int]()
      }
    })

    // Start the Pregel-style form of iterative breadth-first search
    val start = Map[VertexId, Int]()
    val res = mapGraph.pregel(start)(update, iterate, mergeMaps)

    val paths = res.vertices.flatMap {
      case (id, m) =>
        m.map {
          // merge symmetric (s,t) and (t,s) pairs into same canonical pair
          case (k, v) => if (id < k) { (id, k, v) } else { (k, id, v) }
        }
    }.distinct()
    paths.cache()
    println(paths.map(_._3).filter(_ > 0).stats())

    val hist = paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)

    //(d)
    // PageRank API
    val ranks = graph.pageRank(0.001, 0.15).vertices
    val namesAndRanks = ranks.innerJoin(graph.vertices) {
      (vertexId, rank, name) => (name, rank)
    }
    val ord = Ordering.by[(String, Double), Double](_._2)
    namesAndRanks.map(_._2).top(250)(ord).foreach(println)

    // TODO compara con i gradi normali (Capisci se è quello scritto qui)

    val degreesInOut: VertexRDD[Int] = graph.degrees.cache()
    val namesAndRanksToCompare = degreesInOut.innerJoin(graph.vertices) {
      (topicId, rank, name) => (name, rank)
    }
    val ordToCompare = Ordering.by[(String, Int), Int](_._2)
    namesAndRanksToCompare.map(_._2).top(250)(ordToCompare).foreach(println)

    //(e)
    
  }
}
