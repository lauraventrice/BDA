import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{expr}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._
import scala.io.Source._
import java.util.Properties
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._


object Problem1 {

  // ------------------------ Parse the Wikipedia Movie Data ----------------------

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
    s = s.substring(1)
    s = s.substring(s.indexOf(",")) //Origin/Ethnicity
    var i = 0
    while (i < 2) { //Director and Cast
      if(s.startsWith(",\"")) {
        s = s.substring(2)
        s = s.substring(s.indexOf("\",") + 1)
      } else {
        s = s.substring(1)
        s = s.substring(s.indexOf(","))
      }
      i = i + 1
    }    

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
    s = s.substring(s.indexOf(","))

    if(s.startsWith(",\"")) { //Wiki Page
      s = s.substring(2)
      s = s.substring(s.indexOf("\"") + 1)
    } else {
      s = s.substring(1)
      s = s.substring(s.indexOf(",")) 
    }
    s = s.substring(1)
    val plot = s
    Row(title, genre, plot)
    
  }

  def parse(lines: RDD[String]): RDD[Row] = {
    lines.map { line =>
        parseLine(line)
      }
  }

  def cleanCSV(lines: RDD[String]): ArrayBuffer[String] ={
    val a = lines.filter(elem => {
      elem.length >= 4 && elem(0).isDigit && elem(1).isDigit && elem(2).isDigit && elem(3).isDigit && (elem.length > 4 && elem(4).equals(','))
    })
    val b = a.collect()
    val c = lines.collect()
    var i = 0
    val rows: ArrayBuffer[String] = ArrayBuffer.empty
    var kk = true
    while(i < c.length){
      var row = c(i)
      while(kk && !b.contains(c(i+1))){
        row += c(i+1)
        i = i+1
        if(i.equals(c.length-2)) kk = false
      }
      rows.append(row)
      i = i+1
    }
    rows
  }


  def main(args: Array[String]): Unit = {
    //(a)
    val filePath = "./dataFrame"
    val sparkConf = new SparkConf().setAppName("Problem1")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

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
      val rawMovies = sc.textFile("./wiki_movie_plots_deduped.csv", 1)
      sc.parallelize(cleanCSV(rawMovies)).repartition(1).saveAsTextFile("a")
      val parsedLines = cleanCSV(rawMovies)
      val rawMoviesLines = sc.parallelize(parsedLines)
      val parsedMovies = parse(rawMoviesLines)

      df = spark.createDataFrame(parsedMovies, schema)
      df.cache()

      df.repartition(1).write.option("header",value = true).csv(filePath)
    }

    /*
    (b) Next, add an additional column called features to your DataFrame, which contains a list of lemmatized
      text tokens extracted from each of the plot fields using the NLP-based plainTextToLemmas
      function of the given shell script.
     */

    def isOnlyLetters(str: String): Boolean = {
      str.forall(c => Character.isLetter(c))
    }

    val bStopWords = sc.broadcast(fromFile("../Data/stopwords.txt").getLines().toSet)

    def createNLPPipeline(): StanfordCoreNLP = {
      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      new StanfordCoreNLP(props)
    }

    def plainTextToLemmas(text: String, pipeline: StanfordCoreNLP): Seq[String] = {
      val doc = new Annotation(text)
      pipeline.annotate(doc)
      val lemmas = new ArrayBuffer[String]()
      val sentences = doc.get(classOf[SentencesAnnotation])
      var i = 0
      val lengthSent = sentences.size()
      while(i < lengthSent) {
        val sentence = sentences.get(i)
        val tokens = sentence.get(classOf[TokensAnnotation])
        var j = 0
        val lengthTok = tokens.size()
        while(j < lengthTok) {
          val token = tokens.get(j)
          val lemma = token.get(classOf[LemmaAnnotation]).toLowerCase
          if (lemma.length > 2 && !bStopWords.value.contains(lemma) && isOnlyLetters(lemma)) {
            lemmas += lemma
          }
          j += 1
        }
        i += 1
      }
      lemmas
    }

    val pipeline = createNLPPipeline()

    val lemmatized = df.withColumn("features", expr("plainTextToLemmas(plot, pipeline)")) //CONTROLLA SE FUNZIONA!

    lemmatized.repartition(1).write.option("header",value = true).csv("result_lemmatized")
    
    /*val lemmatized =
      df.mapPartitions(it => {
        val pipeline = createNLPPipeline()
        val res = it.map ( row => {
            (row.getString(0), row.getString(1), row.getString(2), plainTextToLemmas(row.getString(2), pipeline))
        })
        res
      })*/


    /*

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
