import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

class Pokemon(id: Int, name: String, type1: String, type2: String, stats: Array[Int], generation: Char,
              finalEvo: Boolean, legendary: Boolean, mega: Boolean, typeEffectiveness: ArrayBuffer[Double],
              height: Double, weight: Double) extends java.io.Serializable{
  val pokedexNumber: Int = id
  val pokemonName: String = name
  val pokemonType1: String = type1
  val pokemonType2: String = type2
  val pokemonStats: Array[Int] = stats
  val pokemonGeneration: Char = generation
  val isFinalEvo: Boolean = finalEvo
  val isLegendary: Boolean = legendary
  val isMega: Boolean = mega
  val pokemonEffectiveness: ArrayBuffer[Double] = typeEffectiveness
  val pokemonHeight: Double = height
  val pokemonWeight: Double = weight

  override def toString: String = {
    "Number: " + pokedexNumber + ", Name: " + pokemonName + ", Type1: " + pokemonType1 + ", Type2: " + pokemonType2 +
      ", Generation: " + pokemonGeneration + ", FinalEvo: " + isFinalEvo + ", Legendary: " + isLegendary +
      ", Mega: " + isMega + ", Height: " + pokemonHeight + ", Weight: " + pokemonWeight
  }
}

object Problem3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    def parse(line: String) = {
      val baseInfo = line.split(",", 5)
      val others = baseInfo(4)
      var tmp = ("", "")
      if(others.startsWith("\"[")){
        tmp = others.splitAt(others.indexOf("]\",")+2)
      }
      else{
        tmp = others.splitAt(others.indexOf(","))
      }
      val advanceInfo = tmp._2.replaceFirst(",", "").split(",")
      val stats: Array[Int] = Array(advanceInfo(0).toInt, advanceInfo(1).toInt, advanceInfo(2).toInt, advanceInfo(3).toInt, advanceInfo(4).toInt, advanceInfo(5).toInt)
      val isFinalEvo = advanceInfo(12)(0).equals('1')
      val isLegendary = advanceInfo(13)(0).equals('1')
      val isMega = advanceInfo(14)(0).equals('1')
      val typeEffectiveness: ArrayBuffer[Double] = ArrayBuffer.empty
      (0 to 15).foreach(index => {
        typeEffectiveness.append(advanceInfo(16+index).toDouble)
      })

      new Pokemon(baseInfo(0).toInt, baseInfo(1), baseInfo(2), baseInfo(3), stats, advanceInfo(9)(0),
        isFinalEvo, isLegendary, isMega, typeEffectiveness, advanceInfo(32).toDouble, advanceInfo(33).toDouble)
    }

    val pathCSV = "dataset/All_Pokemon.csv"

    val pokemon = sc.textFile(pathCSV).filter(!_.startsWith("Number,")).map(parse).cache()

  }
}
