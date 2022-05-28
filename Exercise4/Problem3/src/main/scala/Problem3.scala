import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import java.io.{BufferedWriter, File, FileWriter}
import scala.math._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, File, FileWriter}
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

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

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

    /*
    //(b)
    // TODO scegli se tenere tutti questi metodi oppure fare qualcosa di più piccolo
    // TODO rendi più bello il codice
    def clusterSquareDistance(data: RDD[Vector], meansModel: KMeansModel): Double = {
      val tmp = data.map(vector => {
        val dist = distToCentroid(vector, meansModel)
        dist * dist
      })
      tmp.sum()
    }

    def distance(a: Vector, b: Vector): Double = math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

    // Distance between an element and its centroid
    def distToCentroid(vector: Vector, meansModel: KMeansModel): Double = {
      val cluster = meansModel.predict(vector)
      val centroid = meansModel.clusterCenters(cluster)
      distance(centroid, vector)
    }

    def normalize(value: Double, max: Double, min: Double): Double = (value-min) / (max-min)

    def getBest(array: ArrayBuffer[(Int, Double)]): (Int, Double) = {
      val threshold = 2
      var elem = array(0)
      var flag = true
      var i = 1
      while (flag) {
        if (array(i-1)._2 > array(i)._2 * threshold) elem = array(i)
        else flag = false
        if(i.equals(array.length-1)) flag = false
        i = i+1
      }
      elem
    }

    val maxValues = pokemon.map(pokemon => (pokemon.pokemonWeight, pokemon.pokemonHeight))
    val maxWeight = maxValues.sortBy(_._1, ascending = false).take(1)(0)._1
    val minWeight = maxValues.sortBy(_._1).take(1)(0)._1
    val maxHeight = maxValues.sortBy(_._2, ascending = false).take(1)(0)._2
    val minHeight = maxValues.sortBy(_._2).take(1)(0)._2

    val kMeansData = pokemon.map { pokemon =>
      val weight: Double = normalize(pokemon.pokemonWeight, maxWeight, minWeight)
      val height: Double = normalize(pokemon.pokemonHeight, maxHeight, minHeight)
      val vector = Vectors.dense(Array(weight, height))
      vector
    }.cache()

    val distancesArray: ArrayBuffer[(Int, Double)] = ArrayBuffer()
    val clusterSquareDistanceArray: ArrayBuffer[(Int, Double)] = ArrayBuffer()
    val models: ArrayBuffer[KMeansModel] = ArrayBuffer()

    // Choose the best k for the clustering
    (10 to 50 by 10).foreach(k => {
      val KMeans = new KMeans().setK(k).setEpsilon(1.0e-4)
      val meansModel = KMeans.run(kMeansData)
      distancesArray += ((k, kMeansData.map(d => distToCentroid(d, meansModel)).mean()))
      clusterSquareDistanceArray += ((k, clusterSquareDistance(kMeansData, meansModel)))
      models += meansModel
    })

    val file = new File("./numberK.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    clusterSquareDistanceArray.foreach(x => {
      bw.write(x._1.toString + "," + x._2.toString + "\n")
    })
    bw.close()

    val bestSquareDistance = getBest(clusterSquareDistanceArray)
    println("k=", bestSquareDistance._1)
    val bestDistance = distancesArray.filter(elem => elem._1.equals(bestSquareDistance._1)).take(1)(0)
    val bestModel = models(bestDistance._1/10 - 1)

    val totalExample = kMeansData.map(vector => bestModel.predict(vector) + "," + vector.toArray.mkString(","))
    totalExample.repartition(1).saveAsTextFile("./totalExample")
    
     */

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

    /* TODO commentato per fare i test, questo funziona
    //(e)
    val columns = Array("Normal", "Fire", "Water", "Electric", "Grass", "Ice", "Fighting", "Poison", "Ground", "Flying",
      "Psychic", "Bug", "Rock", "Ghost", "Dragon", "Dark", "Steel", "Fairy")

    var schema = new StructType().add(StructField("Name", StringType, nullable = false))
      .add(StructField("pokedexNumber", IntegerType, nullable = false))
      .add(StructField("Type", StringType, nullable = false))
    columns.foreach(x => schema = schema.add(StructField(x, StringType, nullable = false)))

    // TODO rendilo più umano
    val data = pokemon.map(pokemon => {
      Row(pokemon.pokemonName, pokemon.pokedexNumber, (pokemon.pokemonType1, pokemon.pokemonType2).toString(), pokemon.pokemonEffectiveness(0).toString, pokemon.pokemonEffectiveness(1).toString, pokemon.pokemonEffectiveness(2).toString,
        pokemon.pokemonEffectiveness(3).toString, pokemon.pokemonEffectiveness(4).toString, pokemon.pokemonEffectiveness(5).toString, pokemon.pokemonEffectiveness(6).toString,
        pokemon.pokemonEffectiveness(7).toString, pokemon.pokemonEffectiveness(8).toString, pokemon.pokemonEffectiveness(9).toString, pokemon.pokemonEffectiveness(10).toString,
        pokemon.pokemonEffectiveness(11).toString, pokemon.pokemonEffectiveness(12).toString, pokemon.pokemonEffectiveness(13).toString, pokemon.pokemonEffectiveness(14).toString,
        pokemon.pokemonEffectiveness(15).toString, pokemon.pokemonEffectiveness(16).toString, pokemon.pokemonEffectiveness(17).toString)
    })

    val dataframe = spark.createDataFrame(data, schema)

    dataframe.show(5)

    var index_columns_OHE: Array[String] = Array()

    columns.foreach(elem => {index_columns_OHE = index_columns_OHE.union(Array(elem + "OHE"))})

    val labelIndexer = new StringIndexer()
      .setInputCol("Type")
      .setOutputCol("label")
      .setHandleInvalid("skip")
      .fit(dataframe)

    val stringIndexer = columns.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "_index")
    }
    val oneHotEncoder = columns.map { colName =>
      new OneHotEncoder()
        .setInputCol(colName + "_index")
        .setOutputCol(colName + "OHE")
    }
    val vector = new VectorAssembler()
      .setInputCols(index_columns_OHE)
      .setOutputCol("features")

    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")

    val cl =  new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")

    val stages = Array(labelIndexer) ++ stringIndexer ++ oneHotEncoder ++ Array(vector, scaler, cl)
    val pipeline = new Pipeline().setStages(stages)

    val paramGrid = new ParamGridBuilder()
      .addGrid(cl.maxDepth, Array(5, 10, 15 ))
      .addGrid(cl.impurity, Array("entropy", "gini"))
      .addGrid(cl.maxBins, Array(20, 50, 100))
      .build()

    //Cross-validation
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 5-fold cross-validation
      .setParallelism(2)

    val cvModel = cv.fit(dataframe)

    // We want to print only the wrong prediction
    // TODO sarebbe bello capire qual è il nuovo tipo che predice per poterlo commentare
    cvModel.transform(dataframe)
      .select("Type", "name", "label", "prediction")
      .collect()
      .foreach { case Row(id: String, name:String, label:Double, prediction: Double) =>
        if (prediction != label) println(s"($id, $name) --> label=$label, prediction=$prediction")
      }

    val predictionAndLabels = cvModel.transform(dataframe).select("label", "prediction").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

    val accuracy = predictionAndLabels.filter(pl => pl._1.equals(pl._2)).count().toDouble / dataframe.count().toDouble
    println(accuracy)

     */
  }
}
