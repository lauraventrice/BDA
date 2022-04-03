import org.apache.hadoop.shaded.com.google.common.base.Predicates.not
import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
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

        val countsW = text.map(s => s.toLowerCase()).filter(s => s.matches(regexW))
                      .map(word => (word, 1))
                      .reduceByKey(_ + _).sortBy(_._2, ascending = false)

        val countsN = text.filter(s => s.matches(regexN))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _).sortBy(_._2, ascending = false)

        countsW.saveAsTextFile(args(1) + "/wordscount")
        countsN.saveAsTextFile(args(1) + "/numberscount")
        ctx.stop()
    }
}
