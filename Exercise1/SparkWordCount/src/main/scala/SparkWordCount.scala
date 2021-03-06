import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

    var neg: Boolean => Boolean = !_

    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Correct arguments: <input-directory> <output-directory>")
            System.exit(1)
        }
        val filename = "../Data/stopwords.txt"
        val sparkConf = new SparkConf().setAppName("SparkWordCount")
        val sc = new SparkContext(sparkConf)
        val textFile = sc.textFile(args(0))
        val stopwords = sc.textFile(filename).collect()

        val regexW = "^([a-z]|[\\_]|[\\-]+)(([\\-])|([\\_])|([a-z]))*$"
        val regexN = "\\d+(\\.(\\d+))?"

        val text = textFile.flatMap(line => line.split(" "))

        val bStopwords = sc.broadcast(stopwords)


        val counts = text.map(s => s.toLowerCase())
                    .filter(s => (s.matches(regexW) || s.matches(regexN)) && neg(stopwords.contains(s))) //or bStopwords.value
                    .map(word => (word, 1))
                    .reduceByKey(_ + _).sortBy(_._2, ascending = false)

        counts.persist()

        val (countsN, countsW) = counts.collect().partition(pair => pair._1.matches(regexN))

        val topWords = countsW.take(1000)
        val topNumbers = countsN.take(1000)
        sc.parallelize(topWords).saveAsTextFile(args(1) + "/topWords")
        sc.parallelize(topNumbers).saveAsTextFile(args(1) + "/topNumbers")
        sc.parallelize(countsW).saveAsTextFile(args(1) + "/wordsCount")
        sc.parallelize(countsN).saveAsTextFile(args(1) + "/numbersCount")
        sc.stop()


    }
}
