import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


object SparkTwitterCollector {
  def main(args: Array[String]){
    //Declare sparkSession
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    //Csv's path
    val csvPath = "heart_2020_cleaned.csv"

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

    //Classifier
    val cl =  new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")

    //Pipeline
    val stages = Array(labelIndexer, stringIndexer, oneHotEncoder, vector, scaler, cl)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(trainData)

    //See results
    /*model.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }*/

    //(ii) (iii)
    val paramGrid = new ParamGridBuilder()
      .addGrid(cl.maxDepth, Array(5)) //TODO add 10,15
      .addGrid(cl.impurity, Array("entropy", "gini"))
      .addGrid(cl.maxBins, Array(20)) //TODO add 50,100
      .build()

    //Cross-validation
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator()) //Si può ache mettere semplicemente new BinaryClassificationEvaluator
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 5-fold cross-validation //TODO è a 2 per renderlo più veloce ma deve essere 5
      .setParallelism(2)

    val cvModel = cv.fit(trainData)

    //Model's Average Metrics
    val avgMetrics = cvModel.avgMetrics //TODO io non credo sia l'accuracy però molto simile a quello che vogliamo penso

    cvModel.save("./tree") //Saving Model

    //See results
    /*cvModel.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }*/

    //(iv)

    //Prediction and True Label
    val predictionAndLabels = cvModel.transform(testData).select("label", "prediction").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    //Precision for each label
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    //Recall for each label
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    //Model's accuracy
    val accuracyComputed = predictionAndLabels.filter(pl => pl._1.equals(pl._2)).count().toDouble / testData.count().toDouble
    println("Accuracy: " + accuracyComputed)

    //(c)
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")
      .setFeatureSubsetStrategy("auto")

    val stagesForest = Array(labelIndexer, stringIndexer, oneHotEncoder, vector, scaler, rf)
    val pipelineForest = new Pipeline().setStages(stagesForest)

    val paramForest = new ParamGridBuilder()
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .addGrid(rf.numTrees, Array(5)) //TODO add 10, 20
      .addGrid(rf.maxDepth, Array(5)) //TODO add 10,15
      .addGrid(rf.maxBins, Array(20)) //TODO add 50,100
      .build()

    val cvForest = new CrossValidator()
      .setEstimator(pipelineForest)
      .setEvaluator(new BinaryClassificationEvaluator())
      .setEstimatorParamMaps(paramForest)
      .setNumFolds(2)  // Use 5-fold cross-validation //TODO è a 2 per renderlo più veloce ma deve essere 5
      .setParallelism(2)

    val cvModelForest = cvForest.fit(trainData)

    //See results
    /*cvModelForest.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }*/

    val predictionAndLabelsForest = cvModelForest.transform(testData).select("label", "prediction").rdd.map(row => (row.getDouble(0), row.getDouble(1)))
    val metricsForest = new BinaryClassificationMetrics(predictionAndLabelsForest)

    //Model's accuracy
    val accuracyForest = predictionAndLabels.filter(pl => pl._1.equals(pl._2)).count().toDouble / testData.count().toDouble
    println("Accuracy: " + accuracyForest)

  }
}
