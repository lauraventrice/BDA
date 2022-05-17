import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.SparkSession


object Problem3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("Session")
      .getOrCreate()

    //(a)
    val file = sc.textFile("./twitter_combined.txt")
    val features = sc.wholeTextFiles("./twitter/*.feat")
    val featuresName = sc.wholeTextFiles("./twitter/*.featnames")
    val featuresEGO = sc.wholeTextFiles("./twitter/*.egofeat")

    // Parse the feature for each node
    var results = features.map(elem => (elem._1+"names", elem._2))
      .join(featuresName)
      .map { case (fileName, (featBin, featName)) => (featBin, featName)}

    val featuresEgo = featuresEGO.map(elem => {
      (elem._1.replace(".egofeat", ".featnames"), elem._2.replace("\n", ""))
    }).join(featuresName)
      .map{ case (fileName, (featEgo, featName)) =>
        val tmp = fileName.split("/")
        (tmp(tmp.length-1).replace(".featnames", " ") + featEgo, featName)
      }

    results = results.union(featuresEgo)

    val featureNames = results.flatMap(elem => {
      val featBin = elem._1.split("\n")
      val featName = elem._2.split("\n")
      featBin.map(feat => {
        val features = feat.split(" ")
        var string = features(0)
        features.indices.foreach(index => {
          if (index > 0 && features(index).equals("1")){
            string = string + "," + featName.filter(x => x.split(" ")(0).equals((index-1).toString))
              .take(1)(0)
              .split(" ")(1)
          }
        })
        string
      })
    }).map(elem => {val tmp = elem.split(",")(0)
      (tmp, elem.drop(tmp.length+1))
    }).groupByKey().map(elem => (elem._1, elem._2.toArray))

    // Edges
    val arrayEdges = file.countByValue().toArray.map(elem => {
      val edge = elem._1.split(" ")
      Edge(edge(0).toLong, edge(1).toLong, elem._2.toInt)
    })

    val edges = sc.parallelize(arrayEdges)

    // Vertices
    val vertices = featureNames.map(elem => (elem._1.toLong, elem._2))

    val graph = Graph(vertices, edges).cache()

    /*
    //(b)
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

    /*
    def topNamesAndDegrees(degrees: VertexRDD[Int], topicGraph: Graph[Array[String], Int]): Array[(String, Int)] = {
      val namesAndDegrees = degrees.innerJoin(topicGraph.vertices) {
        (vertexId, degree, name) => (name.mkString("Array(", ", ", ")"), degree) }
      val ord = Ordering.by[(String, Int), Int](_._2)
      namesAndDegrees.map(_._2).top(10)(ord)
    }

    topNamesAndDegrees(degrees, graph).foreach(println)

     */
    /*
    // Average clustering-coefficient
    val triangleCountGraph = graph.triangleCount()
    println(triangleCountGraph.vertices.map(x => x._2).stats())

    val maxTriangleGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoefficientGraph = triangleCountGraph.vertices.innerJoin(maxTriangleGraph) {
        (vertexId, triCount, maxTris) => {if (maxTris == 0) 0 else triCount / maxTris }
      }
    println(clusterCoefficientGraph.map(_._2).sum() / graph.vertices.count())


     */

    /*
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

    val sampleVertices = graph.vertices.map(v => v._1).sample(false, 0.01).collect().toSet

    val mapGraph = graph.mapVertices((id, _) => {
      if (sampleVertices.contains(id)) {
        Map(id -> 0)
      } else {
        Map[VertexId, Int]()
      }
    })
    println("A")

    // Start the Pregel-style form of iterative breadth-first search
    val start = Map[VertexId, Int]()
    val res = mapGraph.pregel(start)(update, iterate, mergeMaps)
    println("B")

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


     */

    /*
    //(d)
    // PageRank API
    val ranks = graph.pageRank(0.001, 0.15).vertices
    val namesAndRanks = ranks.innerJoin(graph.vertices) {
      (vertexId, rank, name) => (name, rank)
    }
    val ord = Ordering.by[(Array[String], Double), Double](_._2)
    val top250 = namesAndRanks.map(_._2).top(250)(ord).map(elem => (elem._1.mkString("Array(", ", ", ")"), elem._2))

    val degreesInOut: VertexRDD[Int] = graph.degrees.cache()
    val namesAndRanksToCompare = degreesInOut.innerJoin(graph.vertices) {
      (vertexId, rank, name) => (name, rank)
    }
    val ordToCompare = Ordering.by[(Array[String], Int), Int](_._2)
    val top250ToCompare = namesAndRanksToCompare.map(_._2).top(250)(ordToCompare).map(elem => (elem._1.mkString("Array(", ", ", ")"), elem._2))

    val equalVertex = top250.filter(elem => top250ToCompare.map(_._1).contains(elem._1))
    println("Equal")
    println(equalVertex.length)

     */

    //(e)
    def inRange(int: Int, string: String) = {
      val tmp = string.replace("[", "").replace(")","").split(",")
      var boolean = false
      if(string.equals("[90,inf)")){
        boolean = int >= tmp(0).toInt
      }
      else {
        boolean = int >= tmp(0).toInt && int < tmp(1).toInt
      }
      boolean
    }

    val bucket = List("[0,10)", "[10,20)", "[20,30)", "[30,40)", "[40,50)", "[50,60)", "[60,70)", "[70,80)", "[80,90)", "[90,inf)")
    val degreesMap = degrees.map(elem => (elem._1, elem._2)).flatMap(x => bucket.collect{
      case i if inRange(x._2, i) => (x._1.toLong, i)
    })

    val featuresMap = featureNames.map(elem => (elem._1, elem._2.length)).flatMap(x => bucket.collect{
      case i if inRange(x._2, i) => (x._1.toLong, i)
    })

    val prova = degreesMap.join(featuresMap).groupBy(elem => elem._2).map(elem => (elem._1.toString(), Vectors.dense(elem._2.size)))

    val a = spark.createDataFrame(prova).toDF()

    val b = new StringIndexer().setInputCol("_1").setOutputCol("label").fit(a).transform(a)

    val chi = ChiSquareTest.test(b, "_2", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")

  }
}
