import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._

object Problem3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Problem3")
    val sc = new SparkContext(sparkConf)

    val rawArtistAlias = sc.textFile("./Data/audioscrobbler/artist_alias.txt")
    val rawUserArtistData = sc.textFile("./Data/audioscrobbler/user_artist_data.txt").sample(false, 0.01)

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
    // Users who listened to at least 100 distinct artists (72,748)
    val counts = trainData.map(elem => (elem.user, 1))
      .reduceByKey(_ + _)
      .filter(elem => elem._2 >= 30)
      .map(c => c._1)
      .collect()

    //Rating objects of only the users above
    val trainData100 = trainData.filter(elem => counts.contains(elem.user))

    //(ii)
    //Split the rdd in two different rdd in which for each user in trainData100,
    // there are about 90% of the artists this user has listened in trainData90
    // while the remaining 10% are in testData10
    val newRdd = trainData100.map(x => (x.user, x))
      .groupByKey()
      .map(x => x._2.splitAt((x._2.size*0.9).toInt))
    val trainData90 = newRdd.flatMap(x => x._1)
    val testData10 = newRdd.flatMap(x => x._2)

    //Train the model
    //TODO scommenta
    //val model = ALS.trainImplicit(trainData90, 10, 5, 0.01, 1.0)

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

    //TODO scommenta
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
    /*
    val evaluations = for(rank <- Array(10, 25, 50);
                          lambda <- Array(1.0, 0.01, 0.001);
                          alpha <- Array(1.0, 10.0, 100.0))
    yield {
      val modelCrossValidation = ALS.trainImplicit(trainData90, rank, 5, lambda, alpha)
      val auc = getTotalAUC(modelCrossValidation)
      ((rank, lambda, alpha), auc)
    }

    evaluations.sortBy(_._2).reverse.foreach(println)
     */
    val bestModel = ALS.trainImplicit(trainData90, 10, 5, 1.0, 1.0)

    var predictionsAndLabels: Array[(Double, Double)] = Array()
    for(someUser <- someUsers) {
      val actualArtists = actualArtistsForUser(someUser)
      val recommendations = bestModel.recommendProducts(someUser, 25)
      predictionsAndLabels = predictionsAndLabels.union(recommendations.map {
        case Rating(user, artist, rating) =>
          if (actualArtists.contains(artist)) {
            (rating, 1.0)
          } else {
            (rating, 0.0)
          }
      })
    }

    val rddPredictionAndLabels = sc.parallelize(predictionsAndLabels)

    //Precision
    val predictionComputed = rddPredictionAndLabels
      .filter(pl => pl._1 >= 0.5 && pl._2.equals(1.0)).count()
      .toDouble / rddPredictionAndLabels.filter(pl => pl._1 >= 0.5).count().toDouble
    println("Prediction " + predictionComputed)

    //Recall
    val recallComputed = rddPredictionAndLabels
      .filter(pl => pl._1 >= 0.5 && pl._2.equals(1.0)).count()
      .toDouble / rddPredictionAndLabels.filter(pl => pl._2.equals(1.0)).count().toDouble
    println("Recall " + recallComputed)

    //Accuracy
    val accuracyComputed = rddPredictionAndLabels.filter(pl => {
      val prediction = pl._1
      if(prediction < 0.5){
        pl._2.equals(0.0)
      }else{
        pl._2.equals(1.0)
      }
    }).count().toDouble / rddPredictionAndLabels.count().toDouble
    println("Accuracy: " + accuracyComputed)

    /*
    //(c)
    //New user information
    val newUserList = List(Rating(1609994, 7007868, 113), Rating(1609994, 10191561, 53), Rating(1609994, 10308181, 23),
      Rating(1609994, 10588243, 215), Rating(1609994, 9951079, 134), Rating(1609994, 10465886, 312),
      Rating(1609994, 1331600, 124), Rating(1609994, 10236358, 76), Rating(1609994, 2008710, 54),
      Rating(1609994, 9910593, 97))
    val newUser = sc.parallelize(newUserList)

    //New trainData
    val trainDataModified = trainData.union(newUser)
    //Model with best choice
    val modelNewUser = ALS.trainImplicit(trainDataModified, 10, 5, 1.0, 1.0)

    val recommendationsNewUser = modelNewUser.recommendProducts(1609994, 25)

    recommendationsNewUser.sortBy(_.rating).reverse.foreach(println)*/
  }
}
