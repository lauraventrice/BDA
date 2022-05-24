import org.apache.spark.rdd.RDD
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

    //(a)
    def parse(data: String) = {
      val line = data.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      val id = line(0).toInt
      val name = line(1)
      val type1 = line(2)
      val type2 = line(3)
      val stats: Array[Int] = Array(line(5).toInt, line(6).toInt, line(7).toInt, line(8).toInt, line(9).toInt, line(10).toInt)
      val generation = line(14)(0)
      val isFinalEvo = line(17)(0).equals('1')
      val isLegendary = line(19)(0).equals('1')
      val isMega = line(20)(0).equals('1')
      val typeEffectiveness: ArrayBuffer[Double] = ArrayBuffer.empty
      (0 to 17).foreach(index => {
        typeEffectiveness.append(line(23+index).toDouble)
      })
      val height = line(41).toDouble
      val weight = line(42).toDouble

      new Pokemon(id, name, type1, type2, stats, generation, isFinalEvo, isLegendary, isMega,
        typeEffectiveness, height, weight)
    }

    // TODO dove è meglio mettere l'array con i nomi delle stats e dei "typeEffectivness" (in pokemon o nel main?)
    val pathCSV = "dataset/All_Pokemon.csv"
    val pokemonData = sc.textFile(pathCSV)
    val header = pokemonData.first()
    val pokemon = pokemonData.filter(!_.equals(header)).map(parse).cache()

    //(b)
    // TODO dobbiamo trovare altri parametri per fare il kmeans altrimenti le stats danno errore troppo elevato e
    // risultati di merda

    //(c)
    def mostCommonType(rdd: RDD[Pokemon]) = {
      rdd.map(_.pokemonType1).union(rdd.filter(!_.pokemonType2.equals("")).map(_.pokemonType2))
        .groupBy(_.toString).map(x => (x._1, x._2.size))
    }

    val bestThreeTypes = mostCommonType(pokemon).sortBy(_._2, ascending = false).take(3)

    def strongestPokemonForType(rdd: RDD[Pokemon], pokemonType: String) = {
      rdd.map(pokemon => (pokemon.pokemonName, pokemon.pokemonType1, pokemon.pokemonType2, pokemon.pokemonStats))
        .filter(pokemon => pokemon._2.equals(pokemonType) || pokemon._3.equals(pokemonType))
        .map(pokemon => (pokemon._1, pokemon._4.sum))
    }

    strongestPokemonForType(pokemon, bestThreeTypes(0)._1).sortBy(_._2, ascending = false).take(1)
  }
}
