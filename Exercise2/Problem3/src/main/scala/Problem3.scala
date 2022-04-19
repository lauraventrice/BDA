import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation._

object Problem3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Problem3")
    val sc = new SparkContext(sparkConf)

    val sampleSize = 0.01 // again use 1 percent sample size for debugging!
    val rawArtistAlias = sc.textFile("./Data/audioscrobbler/artist_alias.txt")
    val rawUserArtistData = sc.textFile("./Data/audioscrobbler/user_artist_data.txt").sample(false, sampleSize)

    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    // Broadcast the local aliases map since it is going to be part of the closure of our training function
    val bArtistAlias = sc.broadcast(artistAlias)

    // Prepare and cache the training data
    val trainData = rawUserArtistData.map {
      line => val Array(userID, artistID, count) =
        line.split(' ').map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
        Rating(userID, finalArtistID, count)
    }.cache()

    //(a)
    //(i)
    val counts = trainData.map(elem => (elem.user, 1))
      .reduceByKey(_ + _)
      .filter(elem => elem._2 >= 50)//TODO metti 100, per il momento lascio 50 per i test poichè ho il dataset rimpicciolito
      .map(c => c._1)
      .collect()

    val trainData100 = trainData.filter(elem => counts contains elem.user)
    println(trainData100.count())

    //(ii)
    val Array(trainData90, testData10) = trainData100.randomSplit(Array(0.9, 0.1)) //TODO sono sicuro che la divisione sia come la vuole il prof (?)

    //TODO va bene questo metodo per training ? Il prof usa questo nel suo file
    val model = ALS.trainImplicit(trainData90, 10, 5, 0.01, 1.0)

  }
}
