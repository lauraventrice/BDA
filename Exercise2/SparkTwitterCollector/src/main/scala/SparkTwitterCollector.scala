import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IndexToString, LabeledPoint, OneHotEncoder, PCA, StandardScaler, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._


object SparkTwitterCollector {
  def main(args: Array[String]){
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

    //Classifier
    val cl =  new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")

    //Pipeline
    val stages = Array(labelIndexer, stringIndexer, oneHotEncoder, vector, scaler, cl)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(trainData)

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

    //TODO credo che questo sia
    //measure the average accuracy over the two possible labels of heart disease for each of the cross-validation and hyper-parameter iterations.
    //Però non sono sicuro, sopratutto perchè io uso areaUnderROC ma non so se sia quello che vogliono
    // AH si questo è per il (iii)
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    //Cross-validation
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator) //Si può ache mettere semplicemente new BinaryClassificationEvaluator
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 5-fold cross-validation //TODO è a 2 per renderlo più veloce ma deve essere 5
      .setParallelism(2)

    val cvModel = cv.fit(trainData)

    //TODO si fa così?
    cvModel.save("./tree") //Saving Model
    /*cvModel.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }*/

    //(iv)

    val predictionAndLabels = cvModel.transform(testData).select("label", "prediction").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    val precision = metrics.precisionByThreshold //Credo che il treshold sia la label ma non ho certezze, ciao Laura!
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    val pipelinePredictionDf = cvModel.transform(testData)
    val accuracyEv = evaluator.evaluate(pipelinePredictionDf)
    val accuracyComputed = predictionAndLabels.filter(pl => pl._1.equals(pl._2)).count().toDouble / testData.count().toDouble
    println(accuracyComputed + "AAAA")
    println(accuracyEv + "BBBB")
    //TODO non sono sicuro sia l'accuracy ma internet la spaccia in questo modo

    //(c)
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("scaledFeatures")
      .setFeatureSubsetStrategy("auto")

    val stagesForest = Array(labelIndexer, stringIndexer, oneHotEncoder, vector, scaler, rf)
    val pipelineForest = new Pipeline().setStages(stagesForest)*/

    /*val modelForest = pipelineForest.fit(trainData)

    modelForest.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }*/

   /* val paramForest = new ParamGridBuilder()
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .addGrid(rf.numTrees, Array(5)) //TODO add 10, 20
      .addGrid(rf.maxDepth, Array(5)) //TODO add 10,15
      .addGrid(rf.maxBins, Array(20)) //TODO add 50,100
      .build()

    val cvForest = new CrossValidator()
      .setEstimator(pipelineForest)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramForest)
      .setNumFolds(2)  // Use 5-fold cross-validation //TODO è a 2 per renderlo più veloce ma deve essere 5
      .setParallelism(2)*/

    /*val cvModelForest = cvForest.fit(trainData)

    cvModelForest.transform(testData)
      .select("HeartDisease","probability", "prediction")
      .collect()
      .foreach { case Row(id: String, prob: Vector, prediction: Double) =>
        println(s"($id) --> prob=$prob, prediction=$prediction")
      }*/

  }
}
