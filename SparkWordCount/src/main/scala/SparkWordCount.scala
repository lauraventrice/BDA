import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{BufferedSource, Source}

object SparkWordCount {

    var filename = "../../stopwords.txt"
    var res: BufferedSource = Source.fromFile(filename)
    val lines: Seq[String] = res.getLines.toList


    var neg: Boolean => Boolean = !_

    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Correct arguments: <input-directory> <output-directory>")
            System.exit(1)
        }

        val sparkConf = new SparkConf().setAppName("SparkWordCount")
        val ctx = new SparkContext(sparkConf)
        val textFile = ctx.textFile(args(0))
        val regexW = "^([a-z]|[\\_]|[\\-]+)(([\\-])|([\\_])|([a-z]))*$"
        val regexN = "\\d+(\\.(\\d+))?"

        val text = textFile.flatMap(line => line.split(" "))

        val countsW = text.map(s => s.toLowerCase()).filter(s => s.matches(regexW) && neg(lines.contains(s)))
                      .map(word => (word, 1))
                      .reduceByKey(_ + _).sortBy(_._2, ascending = false)

        val countsN = text.filter(s => s.matches(regexN))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _).sortBy(_._2, ascending = false)
        res.close()
        val topWords = countsW.take(1000)
        val topNumbers = countsN.take(1000)
        ctx.parallelize(topWords).saveAsTextFile(args(1) + "/topWords")
        ctx.parallelize(topNumbers).saveAsTextFile(args(1) + "/topNumbers")
        countsW.saveAsTextFile(args(1) + "/wordsCount") //maybe is better saveAsObject?
        countsN.saveAsTextFile(args(1) + "/numbersCount")
        ctx.stop()


    }
}
