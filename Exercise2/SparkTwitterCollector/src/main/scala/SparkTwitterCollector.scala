import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IndexToString, StringIndexer, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector


object SparkTwitterCollector {
  def main(args: Array[String]){
    //Non sono sicuro che sia necessario, ma altrimenti non so come richiamare spark
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val csvPath = "heart_2020_cleaned.csv" //Csv's path
    //Load csv into a DataFrame (a)
    val dataFrame = spark.read.option("inferSchema", value = true)
      .option("header", value = true)
      .csv(csvPath)

    //(b)
    val Array(trainData, testData) = dataFrame.randomSplit(Array(0.9, 0.1)) //Split DataFrame in trainData and testData

    trainData.cache() // subset of dataset used for training
    testData.cache() // subset of dataset used for final evaluation ("testing")

    //FIN QUI CREDO ABBIA SENSO; DOPO è tutto oscuro

    val labelIndexer = new StringIndexer()
      .setInputCol("HeartDisease")
      .setOutputCol("label")
      .fit(trainData)
      .setHandleInvalid("skip")

    val categoryConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("category")
      .setLabels(labelIndexer.labels)

    val tokenizer = new Tokenizer()
      .setInputCol("Smoking")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, labelIndexer, lr, categoryConverter))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(trainData)

    // Make predictions on test documents.
    model.transform(testData)
      .select("Smoking", "probability", "prediction")
      .collect()
      .foreach { case Row(smoking: String, prob: Vector, prediction: Double) =>
        println(s"($smoking) --> prob=$prob, prediction=$prediction")
      }
  }
}
