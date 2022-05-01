import org.apache.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, QuantileDiscretizer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import spire.compat.ordering
import org.apache.spark.ml.linalg.VectorUDT
import javax.management.ValueExp
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.io.File


object Problem2 {
  def parseNOAA(rawData: RDD[String]): RDD[Row] = {

    rawData
      .filter(line => line.substring(87, 92) != "+9999") // filter out missing temperature labels
      .map { line =>
        val year = line.substring(15, 19).toInt
        val month = line.substring(19, 21).toInt
        val day = line.substring(21, 23).toInt
        val hour = line.substring(23, 25).toInt
        val latitude = line.substring(28, 34).toDouble / 1000
        val longitude = line.substring(34, 41).toDouble / 1000
        val elevationDimension = line.substring(46, 51).toInt
        val directionAngle = line.substring(60, 63).toInt
        val speedRate = line.substring(65, 69).toDouble / 10
        val ceilingHeightDimension = line.substring(70, 75).toInt
        val distanceDimension = line.substring(78, 84).toInt
        val dewPointTemperature = line.substring(93, 98).toDouble / 10
        val label = ((line.substring(87, 92).toDouble / 10)+ 30).toInt 

        Row(year, month, day, hour, latitude, longitude, elevationDimension, directionAngle, speedRate, ceilingHeightDimension, distanceDimension, dewPointTemperature, label)
      }
  }

  def main(args: Array[String]): Unit = {
    //(a)
    val filePath = "./dataFrame"
    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val sc = new SparkContext(sparkConf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    var df : DataFrame = spark.emptyDataFrame
    val schemaString = "year month day hour latitude longitude elevationDimension directionAngle speedRate ceilingHeightDimension distanceDimension dewPointTemperature label"
    var schema = new StructType
    // Generate the schema based on the string of schema
    val intField = "year month day hour elevationDimension directionAngle ceilingHeightDimension distanceDimension label"

    if (new java.io.File(filePath).exists){ //Check if we saved the file yet

      val fields = schemaString.split(" ")
        .map(fieldName => if (intField contains fieldName) StructField(fieldName, dataType = IntegerType, nullable = true)
         else StructField(fieldName, dataType = DoubleType, nullable = true))

      schema = StructType(fields)
      df = spark.read.option("header",value = true).schema(schema)
        .csv(filePath)

      df.cache()
    } else {
      val rawNOAA = sc.textFile("./NOAA-065900/065900*")
      val parsedNOAA = parseNOAA(rawNOAA)

      val fields = schemaString.split(" ")
        .map(fieldName => if (intField contains fieldName) StructField(fieldName, dataType = IntegerType, nullable = true)
         else StructField(fieldName, dataType = DoubleType, nullable = true))
      schema = StructType(fields)

      //Create DataFrame
      df = spark.createDataFrame(parsedNOAA, schema)

      df.cache()

      df.repartition(1).write.option("header",value = true).csv(filePath)
    }
    //(b)
   
    val minTemp = df.select("label").orderBy(asc("label")).first().getInt(0)
    val maxTemp = df.select("label").orderBy(desc("label")).first().getInt(0)

    val trainData= df.filter("year <= 2021")
    val testData = df.filter("year > 2021")

    trainData.cache()
    testData.cache()

    val columns = schemaString.split(" ")
    val features = columns.filterNot(column => column.equals("label"))

    val vector = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    val standardScalar = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

      /*
    val normalizeLabel = new MinMaxScaler()
      .setInputCol("airTemperature")
      .setOutputCol("label")
      .setMin(0)
      .setMax(45)
*/
    val forest = new RandomForestClassifier()
      .setNumTrees(10)
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")
      .setFeatureSubsetStrategy("auto")
/*
    val normalizeLabelTrue = new MinMaxScaler()
      .setInputCol("label")
      .setOutputCol("labelNorm")
      .setMin(minTemp)
      .setMax(maxTemp)

    val normalizePrediction = new MinMaxScaler()
      .setInputCol("prediction")
      .setOutputCol("predictionNorm")
      .setMin(minTemp)
      .setMax(maxTemp)
*/
    val stagesForest = Array(vector, standardScalar, forest)
    val pipelineForest = new Pipeline().setStages(stagesForest)

    val paramForest = new ParamGridBuilder()
      .addGrid(forest.impurity, Array("entropy", "gini"))
      .addGrid(forest.maxDepth, Array(5)) //TODO add 10,15
      .addGrid(forest.maxBins, Array(20)) //TODO add 50,100
      .build()

    val cvForest = new CrossValidator()
      .setEstimator(pipelineForest)
      .setEvaluator(new BinaryClassificationEvaluator())
      .setEstimatorParamMaps(paramForest)
      .setNumFolds(2)  // Use 5-fold cross-validation //TODO è a 2 per renderlo più veloce ma deve essere 5
      .setParallelism(2)

    
//    val cvModelForest = cvForest.fit(trainData)

    val model = pipelineForest.fit(trainData)

    
    val predictionAndLabels = model.transform(testData)
      .select("label", "prediction")
      .rdd.map(row => (row.getInt(0) , row.getDouble(1) ))
    

    println("############################################################################################################################################") 
    predictionAndLabels.foreach(row => println("Label: "+ row._1 + " Prediction: " + row._2))
/*

    //val rddRowData = sc.parallelize(rowData)
    val rawTestData2 = sc.textFile("./testData2") //Documento creato ad hoc da aggiungere 
    val rddRowData = parseNOAA(rawTestData2)

    val anotherTestData = spark.createDataFrame(rddRowData, schema)

    val predictionAndLabelsAnother = cvModelForest
      .transform(anotherTestData)
      .select("airTemperature", "predictionTrue")
      .rdd.map(row => (row.getDouble(0), row.getDouble(0)))

    println(predictionAndLabelsAnother)
*/
/*

    //(c)

    val testMSE = predictionAndLabels.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    println(s"Test Mean Squared Error = $testMSE")


    //(d)

    val seriesX: RDD[Double] = df.select("airTemperature").rdd.map(row => row.getDouble(0))

    println(seriesX)

    val fields = features.map(feature => df.select(feature).rdd.map(row => row.getDouble(0)))

    print(fields)
    fields.map(column => Statistics.corr(seriesX, column, "spearman"))

    println(s"Correlation is: ${fields.mkString("Array(", ", ", ")")}")
*/
  }
}
