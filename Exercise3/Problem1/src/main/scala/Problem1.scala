import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object Problem1 {

  // ------------------------ Parse the Wikipedia Movie Data ----------------------
/*
  def parseLine(line: String): Row = {
    
    var s = line.substring(line.indexOf(",")) //Release Year 
    var title = ""
    if(s.startsWith(",\"")) {
      s = s.substring(2)
      title = s.substring(0, s.indexOf("\","))
      s = s.substring(s.indexOf("\",") + 1)
    } else {
      s = s.substring(1)
      title = s.substring(0, s.indexOf(","))
      s = s.substring(s.indexOf(","))
    }
    println(s"TITLE: $title  ")
    s = s.substring(1) //VIRGOLA
    //println("ORIGIN: " + s.substring(0, s.indexOf(",")))
    s = s.substring(s.indexOf(",")) //Origin/Ethnicity
    //println(s"REST STRING AFTER ORIGIN: $s")
    var i = 0
    while (i < 2) { //Director and Cast
      if(s.startsWith(",\"")) {
        s = s.substring(2)
        //println("DIRECTOR/CAST: " + s.substring(0, s.indexOf("\"")))
        s = s.substring(s.indexOf("\",") + 1)
      } else {
        s = s.substring(1)
        //println("DIRECTOR/CAST: " + s.substring(0, s.indexOf(",")))
        s = s.substring(s.indexOf(","))
      }
      i = i + 1
    }    
     
    //println(s"STRING AFTER DIRECTOR AND CAST: $s")
    var genre = ""
    if(s.startsWith(",\"")) {
      s = s.substring(2)
      genre = s.substring(0, s.indexOf("\""))
      s = s.substring(s.indexOf("\"") + 1)
    } else {
      s = s.substring(1)
      genre = s.substring(0, s.indexOf(","))
      s = s.substring(s.indexOf(","))
    }
      
    println(s"GENRE: $genre")
    //println(s"RESTO DOPO GENRE: $s")
    s = s.substring(s.indexOf(","))

    if(s.startsWith(",\"")) { //Wiki Page
      s = s.substring(2)
      //println("WIKIPAGE: " + s.substring(0, s.indexOf("\"")))
      s = s.substring(s.indexOf("\"") + 1)
    } else {
      s = s.substring(1)
      //println("WIKI PAGE: " + s.substring(0, s.indexOf(",")))
      s = s.substring(s.indexOf(",")) 
    }
    s = s.substring(1)      
    //println(s"PLOT: $s")
    val plot = s
    Row(title, genre, plot)
    
  }

  def parse(lines: RDD[String]): RDD[Row] = {
    lines.map { line =>
        parseLine(line)
      }
  }

  def isToConcat(movie: String): Boolean = {
    try {
      println(s"STRINGA PRIMA DI CONTROLLARE IL PLOT: $movie")
      var s = movie.substring(movie.indexOf(",")) //Release Year
      var title = ""
      if(s.startsWith(",\"")) {
        s = s.substring(2)
        title = s.substring(0, s.indexOf("\","))
        s = s.substring(s.indexOf("\",") + 1)
      } else {
        s = s.substring(1)
        title = s.substring(0, s.indexOf(","))
        s = s.substring(s.indexOf(","))
      }
      //println(s"TITLE: $title")
      s = s.substring(1) //VIRGOLA
      //println("ORIGIN: " + s.substring(0, s.indexOf(",")))
      s = s.substring(s.indexOf(",")) //Origin/Ethnicity
      //println(s"REST STRING AFTER ORIGIN: $s")
      var i = 0
      while (i < 2) { //Director and Cast
        if(s.startsWith(",\"")) {
          s = s.substring(2)
          //println("DIRECTOR/CAST: " + s.substring(0, s.indexOf("\"")))
          s = s.substring(s.indexOf("\",") + 1)
        } else {
          s = s.substring(1)
          //println("DIRECTOR/CAST: " + s.substring(0, s.indexOf(",")))
          s = s.substring(s.indexOf(","))
        }
        i = i + 1
      }

      //println(s"STRING AFTER DIRECTOR AND CAST: $s")
      var genre = ""
      if(s.startsWith(",\"")) {
        s = s.substring(2)
        genre = s.substring(0, s.indexOf("\""))
        s = s.substring(s.indexOf("\"") + 1)
      } else {
        s = s.substring(1)
        genre = s.substring(0, s.indexOf(","))
        s = s.substring(s.indexOf(","))
      }

      //println(s"GENRE: $genre")
      //println(s"RESTO DOPO GENRE: $s")
      s = s.substring(s.indexOf(","))

      if(s.startsWith(",\"")) { //Wiki Page
        s = s.substring(2)
        //println("WIKIPAGE: " + s.substring(0, s.indexOf("\"")))
        s = s.substring(s.indexOf("\"") + 1)
      } else {
        s = s.substring(1)
        //println("WIKI PAGE: " + s.substring(0, s.indexOf(",")))
        s = s.substring(s.indexOf(","))
      }
      s = s.substring(1)
      println(s"PLOT: $s")
      val plot = s
      plot.startsWith("\"") && ((!movie.endsWith("\"") && !movie.endsWith("\"\"\"")) || movie.endsWith("\"\"") || movie.endsWith(",\""))
    } catch {
      case e: Exception => true
    }
   }

  def parseLines(lines: RDD[String]): Array[String] = {
    val result = ArrayBuffer.empty[String]
    var content = ""
    var linesArray = lines.collect()
    //println(linesArray(0))
    linesArray = linesArray.slice(0, linesArray.length)
    var concat = false
    for (line <- linesArray) {
      /* quando bisogna concatenare? due casi condizioni insieme
      * 1) il plot inizia con virgoletta e non finisce con virgoletta
      * */
      if(!concat) {
       if(isToConcat(line)) {
          content = content + line + " "
        } else {
          content = content.concat(line)
          result.append(content)
          //println("RIGA: " + content)
          content = ""
        }
      } else {
        //si concatena
        if(line.endsWith("\"")) {
          if(line.endsWith("\"\"")) {
            if(line.endsWith("\"\"\"")) {
              //la riga è finita
              content = content.concat(line)
              result.append(content)
              println("RIGA: " + content)
              content = ""
              concat = false
            } else {
              content = content + line + " "
              //continuo a concatenare
            }
          } else {
            content = content.concat(line)
            result.append(content)
            println("RIGA: " + content)
            content = ""
            concat = false
            //la riga è finita 
          }
        } else {
          content = content + line + " "
          //continuo a concatenare
        }
      }
    }
    result.toArray
  }*/


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

      df = spark.read.option("header",value = true)
        .option("delimiter", value = ",")
        .option("multiLine", value = true)
        .option("inferSchema", value = true)
        .csv("./wiki_movie_plots_deduped.csv")



      //val rawMovies = sc.textFile("./wiki_movie_plots_deduped.csv", 1)
      //val parsedLines = parseLines(rawMovies)

      //val rawMoviesLines = sc.parallelize(parsedLines)
      //val parsedMovies = parse(rawMoviesLines)

      //df = spark.createDataFrame(parsedMovies, schema)

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
