import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
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
    //tweet = tweet.filter(_.getLang == "en")

    val regexH = "[#]([a-z]|[0-9]|[A-Z])+"

    val json = tweet.map(x => x.getText)

    val text = json.flatMap(line => line.split(" "))
      .filter(s => s.matches(regexH))

    var z:Array[String] = new Array[String](0)

    var i = 0

    text.foreachRDD((rdd) => {
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
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeoutSecs * 1000)

    val top = sc.parallelize(z).map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1000)

    sc.parallelize(top).repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/top")
  }
}