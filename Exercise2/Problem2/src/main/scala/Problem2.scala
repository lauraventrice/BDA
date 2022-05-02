import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}


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
    // Generate the schema based on the string of schema
    val intField = "year month day hour elevationDimension directionAngle ceilingHeightDimension distanceDimension"

    val fields = schemaString.split(" ")
      .map(fieldName => if (intField contains fieldName) StructField(fieldName, dataType = IntegerType, nullable = true)
      else StructField(fieldName, dataType = DoubleType, nullable = true))

    val schema = StructType(fields)

    if (new java.io.File(filePath).exists){ //Check if we saved the file yet
      df = spark.read.option("header",value = true).schema(schema)
        .csv(filePath)

      df.cache()
    } else {
      val rawNOAA = sc.textFile("./NOAA-065900/065900*")
      val parsedNOAA = parseNOAA(rawNOAA)

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

    val forest = new RandomForestRegressor()
      .setNumTrees(10)
      .setLabelCol("airTemperature")
      .setFeaturesCol("scaledFeatures")
      .setFeatureSubsetStrategy("auto")

    val stagesForest = Array(vector, standardScalar, forest)
    val pipelineForest = new Pipeline().setStages(stagesForest)

    val model = pipelineForest.fit(trainData)

    val predictionAndLabels = model.transform(testData)
      .select("airTemperature", "prediction")
      .rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    
    predictionAndLabels.foreach(row => println("Label: "+ row._1 + " Prediction: " + row._2))

    val rawTestData2 = sc.textFile("./testData2") //New test data 
    val rddRowData = parseNOAA(rawTestData2)

    val anotherTestData = spark.createDataFrame(rddRowData, schema)

    val predictionAndLabelsAnother = model
      .transform(anotherTestData)
      .select("day", "prediction")
      .rdd.map(row => (row.getInt(0), row.getDouble(1)))

    predictionAndLabelsAnother.foreach(row => println("Day: "+ row._1 + " Prediction: " + row._2))

    //(c)

    val testMSE = predictionAndLabels.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    println(s"Test Mean Squared Error = $testMSE")


    //(d)

    val seriesX: RDD[Double] = df.select("airTemperature").rdd.map(row => row.getDouble(0))

    println(seriesX)

    var highestCorrelation = (0.0, "")

    for(feature <- features) {
      val field = df.select(feature).rdd.map(row => 
          if(intField contains feature) row.getInt(0).toDouble
          else row.getDouble(0))
      
      val correlation = Statistics.corr(seriesX, field, "spearman")
      if(correlation > highestCorrelation._1) 
        highestCorrelation = (correlation, feature)
      println("#############################################################################################################################à")
      println(s"Correlation is: $correlation with $feature data")
   
    }

    println(s"Highest Correlation is: $highestCorrelation")
  

  }
}
