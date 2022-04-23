import org.apache.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{QuantileDiscretizer, StandardScaler, VectorAssembler}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import javax.management.ValueExp
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
        val airTemperature = line.substring(87, 92).toDouble / 10

        Row(year, month, day, hour, latitude, longitude, elevationDimension, directionAngle, speedRate, ceilingHeightDimension, distanceDimension, dewPointTemperature, airTemperature)
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
    val schemaString = "year month day hour latitude longitude elevationDimension directionAngle speedRate ceilingHeightDimension distanceDimension dewPointTemperature airTemperature"
    var schema = new StructType
    // Generate the schema based on the string of schema
    val intField = "year month day hour elevationDimension directionAngle ceilingHeightDimension distanceDimension"

    if (new java.io.File(filePath).exists){ //Check if we saved the file yet
      df = spark.read.option("header",value = true)
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
    val trainData= df.filter("year <= 2021")
    val testData = df.filter("year > 2021")

    trainData.cache()
    testData.cache()

    val columns = schemaString.split(" ")
    val features = columns.filterNot(column => column.equals("airTemperature"))

    val vector = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    val standardScalar = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)

    val forest = new RandomForestClassifier()
      .setNumTrees(10)
      .setLabelCol("airTemperature")
      .setFeaturesCol("scaledFeatures")
      .setFeatureSubsetStrategy("auto")

    val stagesForest = Array(vector, standardScalar, forest)
    val pipelineForest = new Pipeline().setStages(stagesForest)

    val model = pipelineForest.fit(trainData)

    val predictionAndLabels = model.transform(testData)
      .select("airTemperature", "prediction")
      .rdd.map(row => (row.getDouble(0), row.getDouble(0)))
    
    println(predictionAndLabels)


    //val rddRowData = sc.parallelize(rowData)
    val rawTestData2 = sc.textFile("./testData2") //Documento creato ad hoc
    val rddRowData = parseNOAA(rawTestData2)

    val anotherTestData = spark.createDataFrame(rddRowData, schema)

    val predictionAndLabelsAnother = model
      .transform(anotherTestData)
      .select("airTemperature", "prediction")
      .rdd.map(row => (row.getDouble(0), row.getDouble(0)))

    println(predictionAndLabelsAnother)


    //(c)

    val testMSE = predictionAndLabels.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    println(s"Test Mean Squared Error = $testMSE")


    //(d)

    //val fields = columns.map(column => if (intField contains column) df.select(column).rdd.map(row => row.getInt(0))
      //else df.select(column).rdd.map(row => row.getDouble(0)))

    // Since standardScalar is a model we use the fit and pass DF as argument in order to normalize data
    println("NON NORMALIZZATO: ", df)

    /*val discretizer = new QuantileDiscretizer()
      .setInputCol("features")
      .setOutputCol("featuresDiscretized")

    val standardScalarFit = standardScalar.fit(df.select("airTemperature", "features"))
    val dfNormalized = standardScalarFit.transform(df.select("airTemperatures", "features"))
    println("NORMALIZZATO ", dfNormalized)*/

    val seriesX: RDD[Double] = df.select("airTemperature").rdd.map(row => row.getDouble(0))

    val fields = features.map(feature => df.select(feature).rdd.map(row => row.getDouble(0)))

    fields.map(column => Statistics.corr(seriesX, column, "spearman"))


    println(s"Correlation is: ${fields.mkString("Array(", ", ", ")")}")

  }
}
