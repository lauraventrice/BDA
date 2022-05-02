import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._

object Problem3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Problem3")
    val sc = new SparkContext(sparkConf)

    val rawArtistAlias = sc.textFile("./Data/audioscrobbler/artist_alias.txt")
    val rawUserArtistData = sc.textFile("./Data/audioscrobbler/user_artist_data.txt")

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
    //users who listened to at least 100 distinct artists (72,748)
    val counts = trainData.map(elem => (elem.user, 1))
      .reduceByKey(_ + _)
      .filter(elem => elem._2 >= 100)
      .map(c => c._1)
      .collect()

    //Rating objects of only the users above
    val trainData100 = trainData.filter(elem => counts.contains(elem.user))

    //(ii)
    //Split the rdd in two different rdd in which for each user in trainData100,
    // there are about 90% of the artists this user has listened in trainData90
    // while the remaining 10% are in testData10
    val Array(trainData90, testData10) = trainData100.map(x => (x.user, x))
      .groupByKey()
      .flatMap(x => x._2)
      .randomSplit(Array(0.9, 0.1))

    //Train the model
    val model = ALS.trainImplicit(trainData90, 10, 5, 0.01, 1.0)

    //(iii)
    //Take 100 random users
    val someUsers = testData10.map(x => x.user).distinct().take(100)
    val topArtists = trainData100.map(elem => ((elem.user, elem.product), elem.rating))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    //Top 25 artist for the 100 random users
    val top25Artists = someUsers.map(elem => topArtists.filter(obj => obj._1._1.equals(elem))
      .take(25).map(elem => (elem._1._1, elem._1._2)))

    def actualArtistsForUser(someUser: Int): Set[Int] = {
      val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
        filter { case Array(user,_,_) => user.toInt == someUser }
      rawArtistsForUser.map(x => x(1).toInt).collect().toSet
    }

    val artistsTotalCount = trainData100.map(r => (r.product, r.rating)).reduceByKey(_ + _).collect().sortBy(-_._2)

    def predictMostPopular(user: Int, numArtists: Int) = {
      val topArtists = artistsTotalCount.take(numArtists)
      topArtists.map{case (artist, rating) => Rating(user, artist, rating)}
    }

    def getAUC(someUser: Int, actualArtists: Set[Int], model: MatrixFactorizationModel) = {
        val recommendations = model.recommendProducts(someUser, 25)
        val predictionsAndLabels = recommendations.map {
          case Rating(user, artist, rating) =>
            if (actualArtists.contains(artist)) {
              (rating, 1.0)
            } else {
              (rating, 0.0)
            }
        }

        val metrics = new BinaryClassificationMetrics(sc.parallelize(predictionsAndLabels))
        metrics.areaUnderROC
    }

    def getTotalAUC(model: MatrixFactorizationModel) = {
      var AUC = 0.0
      for(someUser <- someUsers){
        val actualArtists = actualArtistsForUser(someUser)
        AUC += getAUC(someUser, actualArtists, model)
      }
      AUC
    }

    /*
    var AUC1 = 0.0
    var AUC2 = 0.0

    for (someUser <- someUsers) {

      val actualArtists = actualArtistsForUser(someUser)

      AUC1 += getAUC(someUser, actualArtists, model)

      val recommendations2 = predictMostPopular(someUser, 25)
      val predictionsAndLabels2 = recommendations2.map {
        case Rating(user, artist, rating) =>
          if (actualArtists.contains(artist)) {
            (rating, 1.0)
          } else {
            (rating, 0.0)
          }
      }

      val metrics2 = new BinaryClassificationMetrics(sc.parallelize(predictionsAndLabels2))
      AUC2 += metrics2.areaUnderROC
    }

     */
    //(b)

    val evaluations = for(rank <- Array(10, 25, 50);
                          lambda <- Array(1.0, 0.01, 0.001);
                          alpha <- Array(1.0, 10.0, 100.0))
    yield {
      val modelCrossValidation = ALS.trainImplicit(trainData90, rank, 5, lambda, alpha)
      val auc = getTotalAUC(modelCrossValidation)
      ((rank, lambda, alpha), auc)
    }
    evaluations.sortBy(_._2).reverse.foreach(println)

    /*
    //TODO metti i parametri del modello migliore
    val bestModel = ALS.trainImplicit(trainData90, 10, 5, 10, 10)

    val predictionsAndLabels: Array[(Double, Double)] = null
    for(someUser <- someUsers) {
      val actualArtists = actualArtistsForUser(someUser)
      val recommendations = model.recommendProducts(someUser, 25)
      predictionsAndLabels +: recommendations.map {
        case Rating(user, artist, rating) =>
          if (actualArtists.contains(artist)) {
            (rating, 1.0)
          } else {
            (rating, 0.0)
          }
      }
    }
    val metrics = new BinaryClassificationMetrics(sc.parallelize(predictionsAndLabels))
    metrics.areaUnderROC()
    metrics.precisionByThreshold()
    metrics.recallByThreshold()
    //TODO calcola accuracy

    //TODO calcola il runtime
     */
    //(c)
    //New user information
    /*val newUserList = List(Rating(1609994, 7007868, 113), Rating(1609994, 10191561, 53), Rating(1609994, 10308181, 23),
      Rating(1609994, 10588243, 215), Rating(1609994, 9951079, 134), Rating(1609994, 10465886, 312),
      Rating(1609994, 1331600, 124), Rating(1609994, 10236358, 76), Rating(1609994, 2008710, 54),
      Rating(1609994, 9910593, 97))
    val newUser = sc.parallelize(newUserList)

    //New trainData
    val trainDataModified = trainData90.union(newUser)
    //Model with best choice
    //TODO trova le best choice
    // val model = ALS.trainImplicit(trainData90, 10, 5, 0.01, 1.0)*/

/*
    val actualArtistsNewUser = actualArtistsForUser(1609994)

    val recommendationsNewUser = modelNewUser.recommendProducts(1609994, 25)
    */
    //TODO stampa i raccomandati per l'utente e commenta se sono carini o meno
  }
}
