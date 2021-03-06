import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import java.io.{BufferedWriter, File, FileWriter}
import scala.math._
import scala.collection.mutable.ArrayBuffer

object Problem2 {
  val initializationMode: Array[String] = Array("k-means||", "random")

  // It measures how distant the objects in the cluster are
  def clusterSquareDistance(data: RDD[Vector], meansModel: KMeansModel): Double = {
    val tmp = data.map(vector => {
      val dist = distToCentroid(vector, meansModel)
      dist * dist
    })
    tmp.sum()
  }

  // Normalization function
  def logitFunction(value: Double): Double = 5 / (1 + exp(-value))

  // Distance between two elements
  def distance(a: Vector, b: Vector): Double = math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  // Distance between an element and its centroid
  def distToCentroid(vector: Vector, meansModel: KMeansModel): Double = {
    val cluster = meansModel.predict(vector)
    val centroid = meansModel.clusterCenters(cluster)
    distance(centroid, vector)
  }

  // Give a score for each position (max score 30)
  def getScore(pos: Int, weight: Double): Int = {
    ((30 - (pos * 3)) * weight).toInt
  }

  // Give score for each array
  def scoringFunction(orderedArray: ArrayBuffer[(Int, String, Double)], weight: Double): ArrayBuffer[(Int, String, Int)] = {
    val scoreArray: ArrayBuffer[(Int, String, Int)] = ArrayBuffer()
    orderedArray.indices.foreach(i => {
      scoreArray += ((orderedArray(i)._1, orderedArray(i)._2, getScore(i, weight)))
    })
    scoreArray
  }

  def getBestScore(scoreArrays: ArrayBuffer[(Int, String, Int)]): (Int, (Int, String)) = {
    var max = 0
    var bestParam: (Int, String) = (0, "")
    (1 to 10).foreach(i => {
      initializationMode.foreach(elem => {
        val tmp = scoreArrays.filter(x => x._1.equals(i) && x._2.equals(elem)).map(z => z._3).sum
        if(tmp > max) {
          max = tmp
          bestParam = (i, elem)
        }
      })
    })
    (max, bestParam)
  }

  // Obtain the best number of cluster k
  def getBest(array: ArrayBuffer[(Int, Double)]): (Int, Double) = {
    val threshold = 1.2
    var elem = array(0)
    var flag = true
    var i = 1
    while (flag) {
      if (array(i-1)._2 > array(i)._2 * threshold) elem = array(i)
      else flag = false
      if(i.equals(array.length-1)) flag = false
      i = i+1
    }
    elem
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rawData = sc.textFile("./archive/Run_*")

    // Take the data rdd
    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      val vector = Vectors.dense(buffer.map(x => logitFunction(x.toDouble)).toArray)
      vector
    }.cache()

    val distancesArray: ArrayBuffer[(Int, Double)] = ArrayBuffer()
    val clusterSquareDistanceArray: ArrayBuffer[(Int, Double)] = ArrayBuffer()
    val models: ArrayBuffer[KMeansModel] = ArrayBuffer()

    // Choose the best k for the clustering
    (10 to 100 by 10).foreach(k => {
      val KMeans = new KMeans().setK(k).setEpsilon(1.0e-4)
      val meansModel = KMeans.run(data)
      distancesArray += ((k, data.map(d => distToCentroid(d, meansModel)).mean()))
      clusterSquareDistanceArray += ((k, clusterSquareDistance(data, meansModel)))
      models += meansModel
    })

    val file = new File("./numberK.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    clusterSquareDistanceArray.foreach(x => {
      bw.write(x._1.toString + "," + x._2.toString + "\n")
    })
    bw.close()

    val bestSquareDistance = getBest(clusterSquareDistanceArray)
    val bestDistance = distancesArray.filter(elem => elem._1.equals(bestSquareDistance._1)).take(1)(0)
    val bestModel = models(bestDistance._1/10 - 1)

    // Find anomalies and remove them from the data
    val newData = data.filter(d => distToCentroid(d, bestModel) < bestDistance._2*2)

    val distancesToCentroid: ArrayBuffer[(Int, String, Double)] = ArrayBuffer()
    val wss: ArrayBuffer[(Int, String,  Double)] = ArrayBuffer()
    val squareDistance: ArrayBuffer[(Int, String, Double)] = ArrayBuffer()

    val modelsAnalyze: ArrayBuffer[(Int, String, KMeansModel)] = ArrayBuffer()

    // Run K-Means multiple time to obtain different first centroids
    (1 to 10).foreach(i => {
      initializationMode.foreach(elem => {
        val KMeans = new KMeans().setK(bestDistance._1).setEpsilon(1.0e-4).setInitializationMode(elem)
        val meansModel = KMeans.run(newData)
        distancesToCentroid += ((i, elem, newData.map(vector => distToCentroid(vector, meansModel)).mean()))
        wss += ((i, elem, meansModel.computeCost(newData)))
        squareDistance += ((i, elem, clusterSquareDistance(newData, meansModel)))
        modelsAnalyze += ((i, elem, meansModel))
      })
    })

    println("Clusters Square Distance: ")
    val orderedSquareDistance = squareDistance.sortBy(x => x._3)
    val scoreSquareDistance = scoringFunction(orderedSquareDistance, 1.0)
    orderedSquareDistance.foreach(println)

    println("Distance To Centroid: ")
    val orderedDistance = distancesToCentroid.sortBy(x => x._3)
    val scoreDistance = scoringFunction(orderedDistance, 1.0)
    orderedDistance.foreach(println)

    println("Computational Cost: ")
    val orderedWSS = wss.sortBy(x => x._3)
    val scoreWSS = scoringFunction(orderedWSS, 0.5)
    orderedWSS.foreach(println)

    val best = getBestScore(scoreWSS ++ scoreSquareDistance ++ scoreDistance)
    val bestFinalModel = modelsAnalyze.filter(elem => elem._1.equals(best._2._1) && elem._2.equals(best._2._2))(0)._3

    println("The best is: " + best)
    println("The best cluster square distance is: " + bestSquareDistance)
    println("The best distance is: " + bestDistance)

    val partialExample = newData.map(vector => bestFinalModel.predict(vector) + "," + vector.toArray.mkString(",")).sample(false, 0.01)
    partialExample.repartition(1).saveAsTextFile("./partialExample")

    val totalExample = newData.map(vector => bestFinalModel.predict(vector) + "," + vector.toArray.mkString(","))
    totalExample.repartition(1).saveAsTextFile("./totalExample")
  }
}
