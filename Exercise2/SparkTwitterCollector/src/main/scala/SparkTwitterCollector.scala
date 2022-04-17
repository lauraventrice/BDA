import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IndexToString, OneHotEncoder, StandardScaler, StringIndexer, Tokenizer, VectorAssembler}
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
    //(i)
    val Array(trainData, testData) = dataFrame.randomSplit(Array(0.9, 0.1)) //Split DataFrame in trainData and testData

    trainData.cache() // subset of dataset used for training
    testData.cache() // subset of dataset used for final evaluation ("testing")

    //Dataset's feature
    val index_columns = Array("Smoking", "AlcoholDrinking", "Stroke", "DiffWalking", "Sex", "Race", "Diabetic", "PhysicalActivity", "GenHealth", "Asthma", "KidneyDisease", "SkinCancer", "AgeCategory")
    val numerical_columns = Array("BMI", "PhysicalHealth", "MentalHealth", "SleepTime")

    var index_columns_OHE: Array[String] = Array()
    var index_columns_index: Array[String] = Array()

    //Create two array for input and output
    index_columns.foreach(elem =>
      {index_columns_OHE = index_columns_OHE.union(Array(elem + "OHE"))
      index_columns_index = index_columns_index.union(Array(elem + "_index"))}
    )

    //Array of features' name
    val features = index_columns_OHE ++ numerical_columns

    //Transform HeartDisease from a String in an Integer named label
    val labelIndexer = new StringIndexer()
      .setInputCol("HeartDisease")
      .setOutputCol("label")
      .setHandleInvalid("skip")
      .fit(trainData)

    //Transform the whole set of String features in OneHotEncoder features
    val stringIndexer = new StringIndexer()
      .setInputCols(index_columns)
      .setOutputCols(index_columns_index)
    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(index_columns_index)
      .setOutputCols(index_columns_OHE)

    //Features' union
    val vector = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    //Feature scaling helps in decreasing the convergence time
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures")

    //Regression
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")

    //Pipeline
    val stages = Array(labelIndexer, stringIndexer, oneHotEncoder, vector, scaler, lr)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(trainData)

    model.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }
  }
}
