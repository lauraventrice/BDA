import org.apache.hadoop.shaded.com.google.gson.Gson
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

    val tweetStream = TwitterUtils.createStream(ssc, Some(new OAuthAuthorization(new ConfigurationBuilder().build())))
    val tweet = tweetStream.filter(_.getText.contains("#"))
    //tweet = tweet.filter(_.getLang == "en")

    val regexH = "#([a-z]|[0-9])*"

    val json = tweet.map(x => { val gson = new Gson();
      val xJson = gson.toJson(x.getText)
      xJson
    })

    val text = json.flatMap(line => line.split(" ")).filter(s => s.matches(regexH))

    text.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        rdd.repartition(partitionsPerInterval).saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
      }
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeoutSecs * 1000)
  }
}