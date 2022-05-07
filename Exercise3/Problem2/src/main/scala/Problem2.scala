import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import scala.math._

import java.io._
import scala.collection.mutable.ArrayBuffer

object Problem2 {
  val initializationMode: Array[String] = Array("k-means||", "random")
  // Cluster cohesion: measures how related the objects in the cluster are
  // TODO nel txt spiega che questo massimizzato permette di non avere la situazione di k = numero di oggetti
  def clusterCohesion(data: RDD[Vector], meansModel: KMeansModel): Double = {
    val tmp = data.map(vector => {
      val dist = distToCentroid(vector, meansModel)
      dist * dist
    })
    tmp.sum()
  }

  // Normalization function
  // TODO la teniamo ?
  // TODO il 5 ok?
  def logitFunction(value: Double): Double = 5 / (1 + exp(-value))

  // Distance between two elements
  def distance(a: Vector, b: Vector): Double = math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  // Distance between an element and its centroid
  def distToCentroid(vector: Vector, meansModel: KMeansModel): Double = {
    val cluster = meansModel.predict(vector)
    val centroid = meansModel.clusterCenters(cluster)
    distance(centroid, vector)
  }

  def getScore(pos: Int, weight: Int): Int = {
    (30 - (pos * 3)) * weight
  }

  def scoringFunction(orderedArray: ArrayBuffer[(Int, String, Double)], weight: Int): ArrayBuffer[(Int, String, Int)] = {
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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val rawData = sc.textFile("./archive/Run_*").sample(withReplacement = false, 0.01, seed = 11)

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      val vector = Vectors.dense(buffer.map(x => logitFunction(x.toDouble)).toArray)
      vector
    }.cache()

    val distancesArray: ArrayBuffer[(Int, Double)] = ArrayBuffer()
    val models: ArrayBuffer[KMeansModel] = ArrayBuffer()

    (10 to 70 by 10).foreach(k => {
      val KMeans = new KMeans().setK(k).setEpsilon(1.0e-4)
      val meansModel = KMeans.run(data)
      distancesArray += ((k, data.map(d => distToCentroid(d, meansModel)).mean()))
      models += meansModel
    })

    val bestDistance = distancesArray.sortBy(_._2).take(1)(0)
    val bestModel = models(bestDistance._1/10 - 1)

    // Decide threshold for anomalies
    val newData = data.filter(d => distToCentroid(d, bestModel) < bestDistance._2*2)

    val distancesToCentroid: ArrayBuffer[(Int, String, Double)] = ArrayBuffer()
    val wss: ArrayBuffer[(Int, String,  Double)] = ArrayBuffer()
    val cohesion: ArrayBuffer[(Int, String, Double)] = ArrayBuffer()

    val modelsAnalyze: ArrayBuffer[(Int, String, KMeansModel)] = ArrayBuffer()

    (1 to 10).foreach(i => {
      initializationMode.foreach(elem => {
        val KMeans = new KMeans().setK(bestDistance._1).setEpsilon(1.0e-4).setInitializationMode(elem)
        val meansModel = KMeans.run(newData)
        distancesToCentroid += ((i, elem, newData.map(vector => distToCentroid(vector, meansModel)).mean()))
        wss += ((i, elem, meansModel.computeCost(newData)))
        cohesion += ((i, elem, clusterCohesion(newData, meansModel)))
        modelsAnalyze += ((i, elem, meansModel))
      })
    })

    println("Clusters Cohesion: ")
    val orderedCohesion = cohesion.sortBy(x => x._3).reverse
    val scoreCohesion = scoringFunction(orderedCohesion, 1)
    orderedCohesion.foreach(println)

    println("Distance To Centroid: ")
    val orderedDistance = distancesToCentroid.sortBy(x => x._3)
    val scoreDistance = scoringFunction(orderedDistance, 1)
    orderedDistance.foreach(println)

    println("Computational Cost: ")
    val orderedWSS = wss.sortBy(x => x._3)
    val scoreWSS = scoringFunction(orderedWSS, 1)
    orderedWSS.foreach(println)

    val best = getBestScore(scoreWSS ++ scoreCohesion ++ scoreDistance)
    println("The best is: " + best)
    val bestFinalModel = modelsAnalyze.filter(elem => elem._1.equals(best._2._1) && elem._2.equals(best._2._2))(0)._3

    val sample = newData.map(vector => bestFinalModel.predict(vector) + "," + vector.toArray.mkString(",")).sample(false, 0.01)
    sample.saveAsTextFile("./kmeans-sample")
  }
}
