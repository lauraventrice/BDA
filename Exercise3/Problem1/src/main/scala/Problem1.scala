import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object Problem1 {

  // ------------------------ Parse the Wikipedia Movie Data ----------------------
  def parseLine(line: String): Row = {
    try {
      var s = line.substring(0, line.indexOf(","))
      val title = s.substring(0, s.indexOf(","))
      s = title.substring(title.indexOf(","))
      var i = 0
      while(i < 3) {
        s = s.substring(s.indexOf(","))
        i += 1
      }
      val genre = s.substring(0, s.indexOf(","))
      s = genre.substring(genre.indexOf(",")).substring(s.indexOf(","))
      val plot = s.substring(0)
      Row(title, genre, plot)
    } catch {
      case e: Exception => Row("", "", "")
    }
  }

  def parse(lines: RDD[String]): Array[Row] = {
    val movies = ArrayBuffer.empty[Row]
    var movie = Row.empty
    var content = ""
    for (line <- lines) {
      if(line != "Release Year,Title,Origin/Ethnicity,Director,Cast,Genre,Wiki Page,Plot") {
        try {
          if (line.endsWith("\"")) {
            movie = parseLine(line)
            movies += movie
            content = ""
          } else {
            content += line + "\n"
          }
        } catch {
          case e: Exception => content = ""
        }
      }
    }
    movies.toArray
  }

  def main(args: Array[String]): Unit = {
    //(a)
    val filePath = "./dataFrame"
    val sparkConf = new SparkConf().setAppName("Problem1")
    val sc = new SparkContext(sparkConf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    var df : DataFrame = spark.emptyDataFrame
    val schemaString = "title genre plot"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, dataType = StringType, nullable = true))

    val schema = StructType(fields)

    if (new java.io.File(filePath).exists){ //Check if we saved the file yet
      df = spark.read.option("header",value = true).schema(schema)
        .csv(filePath)

      df.cache()
    } else {
      val rawMovies = sc.textFile("./wiki_movie_plots_deduped*")
      val parsedMovies = sc.parallelize(parse(rawMovies))

      df = spark.createDataFrame(parsedMovies, schema)

      df.cache()

      df.repartition(1).write.option("header",value = true).csv(filePath)
    }
    /*
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

    val paramForest = new ParamGridBuilder()
      .addGrid(forest.maxDepth, Array(5, 10, 15))
      .addGrid(forest.maxBins, Array(20, 50, 100))
      .build()

    val optimizedForest = new CrossValidator()
      .setEstimator(pipelineForest)
      .setEvaluator(new RegressionEvaluator().setLabelCol("airTemperature").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paramForest)
      .setNumFolds(5)
      .setParallelism(2)

    val optimizedModel = optimizedForest.fit(trainData)

    val predictionAndLabels = optimizedModel.transform(testData)
      .select("airTemperature", "prediction")
      .rdd.map(row => (row.getDouble(0), row.getDouble(1)))

    val rawTestData2 = sc.textFile("./testData2") //New test data
    val rddRowData = parseNOAA(rawTestData2)

    val anotherTestData = spark.createDataFrame(rddRowData, schema)

    val predictionAndLabelsAnother = optimizedModel
      .transform(anotherTestData)
      .select("day", "prediction")
      .rdd.map(row => (row.getInt(0), row.getDouble(1)))

    //(c)
    val testMSE = predictionAndLabels.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    println(s"Test Mean Squared Error = $testMSE")

    //(d)

    val seriesX: RDD[Double] = df.select("airTemperature").rdd.map(row => row.getDouble(0))

    var highestCorrelation = (0.0, "")

    for(feature <- features) {
      val field = df.select(feature).rdd.map(row =>
        if(intField contains feature) row.getInt(0).toDouble
        else row.getDouble(0))

      val correlation = Statistics.corr(seriesX, field, "spearman")
      if(correlation > highestCorrelation._1)
        highestCorrelation = (correlation, feature)
      println("#############################################################################################################################")
      println(s"Correlation is: $correlation with $feature data")

    }

    println(s"Highest Correlation is: $highestCorrelation")

*/
  }
}
