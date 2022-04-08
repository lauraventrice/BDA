import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization

import Array._
import twitter4j.conf.ConfigurationBuilder

object SparkTwitterCollector {
  
  def main(args: Array[String]) {
    
    if (args.length < 7) {
      System.err.println("Correct arguments: <output-path> <time-window-secs> <timeout-secs> <twitter-consumer-key> <twitter-consumer-secret> <twitter-access-token> <twitter-access-token-secret>")
      System.exit(1)
    }

    val outputDirectory = args(0)
    val windowSecs = args(1).toLong
    val timeoutSecs = args(2).toInt
    val partitionsPerInterval = 1

    System.setProperty("twitter4j.oauth.consumerKey", args(3))
    System.setProperty("twitter4j.oauth.consumerSecret", args(4))
    System.setProperty("twitter4j.oauth.accessToken", args(5))
    System.setProperty("twitter4j.oauth.accessTokenSecret", args(6))

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(windowSecs))

    val tweet = TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(new ConfigurationBuilder().build())))
    val second = tweet.filter(_.getText.contains("#"))

    val regexH = "[#]([a-z]|[0-9]|[A-Z])+"

    val json = tweet.map(x => x.getText)

    val tmp = second.map(x => (x.getId, x.getText.split(" ")))

    var z:Array[String] = new Array[String](0)

    var result = List()

    val text = json.flatMap(line => line.split(" "))
      .filter(s => s.matches(regexH))

    var d:Array[(Long, String)] = new Array[(Long, String)](0)



    var c:Array[(Long,Array[String])] = new Array[(Long, Array[String])](0)

    var g:Array[Long] = Array(1, 2)

    var i = 0
    var x = 0

    tmp.foreachRDD((rdd,time) =>{
      val count = rdd.count()
      if (count > 0) {
          c = c.union(rdd.take(rdd.count().toInt))

        //rdd.repartition(1).saveAsTextFile(outputDirectory + "/tweet2_" + time.milliseconds.toString)

          //rdd.repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/tweet_" + time.milliseconds.toString)
      }
    })

    /*text.foreachRDD((rdd) => {
      val count = rdd.count()
      if (count > 0) {
        if (i == 0) {
          z = rdd.take(rdd.count().toInt)
          i = i+1
        }
        else {
          z = z.union(rdd.take(rdd.count().toInt))
        }
      }
    })*/

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeoutSecs * 1000)

    var e:Array[String] = new Array[String](0)

    var l:Array[(Long, String)] = new Array[(Long, String)](0)

    for (i <- 1 until c.length){
      e = e.union(c(i)._2)
      l = l :+ (c(i)._1, e(i))
    }

    sc.parallelize(l).repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/top")

    /*val top = sc.parallelize(z).map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1000)

    sc.parallelize(top).repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/top")*/
  }
}