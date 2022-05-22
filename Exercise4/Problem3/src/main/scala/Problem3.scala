import org.apache.spark.{SparkConf, SparkContext}

object Problem3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    def parse(line: String) = {
      val pokemonBaseInfo = line.split(",", 5)
      ""
    }

    val pathCSV = "dataset/All_Pokemon.csv"

    val pokemon = sc.textFile(pathCSV).map(parse)
    pokemon.repartition(1).saveAsTextFile("a")
  }
}
