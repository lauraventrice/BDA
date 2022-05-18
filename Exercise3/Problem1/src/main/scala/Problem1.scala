import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties
import scala.collection.mutable._
import scala.io.Source._


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
    var schemaString = "title genre plot"

    var fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, dataType = StringType, nullable = true))

    var schema = StructType(fields)
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
    val numDocs = df.count()
    print("il numero di articoli è " + numDocs)
    /*
    (b) Next, add an additional column called features to your DataFrame, which contains a list of lemmatized
      text tokens extracted from each of the plot fields using the NLP-based plainTextToLemmas
      function of the given shell script.
     */

    def isOnlyLetters(str: String): Boolean = {
      str.forall(c => Character.isLetter(c))
    }

    val bStopWords = sc.broadcast(fromFile("./stopwords.txt").getLines().toSet)

    def createNLPPipeline(): StanfordCoreNLP = {
      val props = new Properties()
      props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
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

    val lemmatized: RDD[Row]=
      df.rdd.mapPartitions(it => {
        val pipeline = createNLPPipeline()
        val res = it.map ( row => {
          Row(row.getString(0), row.getString(1), row.getString(2), plainTextToLemmas(row.getString(2), pipeline).toArray.mkString(","))
        })
        res
      })

    schemaString = "title genre plot features"

    fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, dataType = StringType, nullable = true))

    schema = StructType(fields)

    df = spark.createDataFrame(lemmatized, schema)

    /*
    (d) Compute an SVD decomposition of the 134,164 movie plots contained in your DataFrame by using the following two basic parameters:
      – numFreq = 5000 for the number of frequent terms extracted from all Wikipedia articles, and
      – k = 25 for the number of latent dimensions used for the SVD.
     */
    val k = 25
    val numFreq = 5000
    val moviesTermFreqs = lemmatized.map (movie => {
      val termFreqs = movie.getString(3).split(',').toSeq.foldLeft(new HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      ((movie.getString(0), movie.getString(1)), termFreqs)
    })
    moviesTermFreqs.cache()
    moviesTermFreqs.count()
    println("MOVIES TERM FREQS: ")
    moviesTermFreqs.foreach(x => println(x._2))


    val moviesIds = moviesTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()
    //In order to reduce the term space we filter out infrequent items
    val termsFreqs = moviesTermFreqs.map(_._2).flatMap(_.keySet).map((_, 1)).
      reduceByKey(_ + _, 25) //less than 25

    val ordering = Ordering.by[((String), Int), Int](_._2)
    val topTermsFreqs = termsFreqs.top(numFreq)(ordering)

    val idfs = topTermsFreqs.map {
      case (term, count) =>
        (term, math.log(numDocs.toDouble / count))
    }.toMap


    val idTerms = idfs.keys.zipWithIndex.toMap

    val bIdfs = sc.broadcast(idfs).value
    val bIdTerms = sc.broadcast(idTerms).value

    val vecs = moviesTermFreqs.map(_._2).map(termFreqs => {
      val plotTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, _) => bIdTerms.contains(term)
      }.map {
        case (term, _) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / plotTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.size, termScores)
    })

    vecs.cache()
    vecs.count()

    val mat = new RowMatrix(vecs)
    val svd = mat.computeSVD(k, computeU = true)

    print(svd.toString)

    /* (e)
      Based on the two topTermsInTopConcepts and topDocsInTopConcepts functions provided in the
      RunLSA-shell.scala shell script, compute the top-25 terms and the top-25 documents, each under
      the k = 25 latent concepts, for the above SVD.
      Additionally modify the topDocsInTopConcepts function, such that it also prints the top-5 most
      frequent genre labels of the Wikipedia articles returned by this function under each of the latent
      concepts.
      Manually inspect the results to determine if the SVD improves the representation of the documents
      */
    // ------------------- Query the Latent Semantic Index ------------------------

    def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                               numConcepts: Int, numTerms: Int): Seq[Seq[(String, Double)]] = {
      val v = svd.V //encodes document-to-concept mappings
      val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
      val arr = v.toArray
      for (i <- 0 until numConcepts) {
        val offs = i * v.numRows
        val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
        val sorted = termWeights.sortBy(-_._1)
        topTerms += sorted.take(numTerms).map {
          case (score, id) =>
            (bIdTerms.find(_._2 == id).getOrElse(("", -1))._1, score)
        }
      }
      topTerms
    }

    def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix],
                              numConcepts: Int, numDocs: Int): Seq[Seq[((String, String), Double)]] = {
      val u = svd.U
      val topDocs = new ArrayBuffer[Seq[((String, String), Double)]]()
      for (i <- 0 until numConcepts) {
        val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
        topDocs += docWeights.top(numDocs).map {
          case (score, id) => (moviesIds(id), score)
        }
      }
      moviesTermFreqs.map(movie => (movie._1._2, 1)).reduceByKey(_ + _).sortBy(_._2).top(5).foreach(println)
      topDocs
    }

    val topConceptTerms = topTermsInTopConcepts(svd, 12, 25)
    val topConceptDocs = topDocsInTopConcepts(svd, 12, 25)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }

    /*
    (f) Modify the provided topDocsForTermQuery function such that it computes the Cosine measure
      (instead of the inner product) between a translated document vector d′ and a similarly translated
      query vector q′ over the latent semantic space.
      Sort the matching documents in descending order of Cosine similarities, and finally return the top-25
      document vectors (and corresponding title entries) with the highest similarities to each such keyword
      query.
      Finally, think of 5–10 interesting keyword queries for movies and report their results.
     */

    def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
      require(x.size == y.size)
      dotProduct(x, y)/(magnitude(x) * magnitude(y))
    }

    def dotProduct(x: Array[Double], y: Array[Double]): Double = {
      var sum = 0.0
      for((a, b) <- x zip y) {
        sum += a*b
      }
      sum
    }

    def magnitude(x: Array[Double]): Double = {
      math.sqrt(x.map(i => i*i).sum)
    }

    def computeCosine(matrix: RowMatrix, matrix1: Matrix): RowMatrix = {
      new RowMatrix(matrix.rows.map(vector => Vectors.dense(cosineSimilarity(vector.toArray, matrix1.toArray))))
    }

    def termsToQueryVector(terms: scala.collection.immutable.Seq[String],
                            idTerms: scala.collection.immutable.Map[String, Int],
                            idfs: scala.collection.immutable.Map[String, Double]): BSparseVector[Double] = {
      val indices = terms.map(idTerms(_)).toArray
      val values = terms.map(idfs(_)).toArray
      new BSparseVector[Double](indices, values, idTerms.size)
    }

    def topDocsForTermQuery(
                             US: RowMatrix,
                             V: Matrix,
                             query: BSparseVector[Double]): Seq[(String, Double, Long)] = {
      val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
      val termRowArr = (breezeV.t * query).toArray
      val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)
      val docScores = computeCosine(US, termRowVec)
      val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId()
      val title = lemmatized.map(movie => movie.getString(0))

      val zippedRDD = (title.zip(allDocWeights)).map{ case (title, (weights, id)) => (title, weights, id) } //zip with title
      zippedRDD.sortBy(_._2, ascending = false).top(25)
    }

    def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
      val sArr = diag.toArray
      new RowMatrix(mat.rows.map { vec =>
        val vecArr = vec.toArray
        val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
        Vectors.dense(newArr)
      })
    }

    val US = multiplyByDiagonalRowMatrix(svd.U, svd.s)

    val terms = List("love", "war", "family", "action", "marriage", "dead")

    val queryVec = termsToQueryVector(terms, idTerms, idfs)
    topDocsForTermQuery(US, svd.V, queryVec)

  }
}
