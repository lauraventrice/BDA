import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.sql.Row

object Problem1 {
  def main(args: Array[String]): Unit = {
    //Declare sparkSession
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .getOrCreate;

    //Csv's path
    val csvPath = "heart_2020_cleaned.csv"

    //Load csv into a DataFrame (a)
    var dataFrame = spark.read.option("inferSchema", value = true)
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
    val stringIndexer = index_columns.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "_index")
    }
    val oneHotEncoder = index_columns.map { colName =>
      new OneHotEncoder()
        .setInputCol(colName + "_index")
        .setOutputCol(colName + "OHE")
    }

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
    val stages = Array(labelIndexer) ++ stringIndexer ++ oneHotEncoder ++ Array(vector, scaler, cl)
    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(trainData)

    //See results
    /*model.transform(testData)
      .select("HeartDisease", "prediction")
      .collect()
      .foreach { case Row(id: String, prediction: Double) =>
        println(s"($id) --> prediction=$prediction")
      }*/

    //(ii) (iii)
    val paramGrid = new ParamGridBuilder()
      .addGrid(cl.maxDepth, Array(5, 10, 15))
      .addGrid(cl.impurity, Array("entropy", "gini"))
      .addGrid(cl.maxBins, Array(20, 50, 100))
      .build()

    //Cross-validation
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 5-fold cross-validation
      .setParallelism(2)

    val cvModel = cv.fit(trainData)

    //Model's Average Metrics
    val avgMetrics = cvModel.avgMetrics

    cvModel.save("./tree") //Saving Model

    //See results
    /*
    cvModel.transform(testData)
      .select("HeartDisease", "prediction")
      .collect()
      .foreach { case Row(id: String, prediction: Double) =>
        println(s"($id) --> prediction=$prediction")
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


    val stagesForest = Array(labelIndexer) ++ stringIndexer ++ oneHotEncoder ++ Array(vector, scaler, rf)
    val pipelineForest = new Pipeline().setStages(stagesForest)

    val paramForest = new ParamGridBuilder()
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .addGrid(rf.numTrees, Array(5, 10, 20))
      .addGrid(rf.maxDepth, Array(5, 10, 15))
      .addGrid(rf.maxBins, Array(20, 50, 100))
      .build()

    val cvForest = new CrossValidator()
      .setEstimator(pipelineForest)
      .setEvaluator(new BinaryClassificationEvaluator())
      .setEstimatorParamMaps(paramForest)
      .setNumFolds(5)  // Use 5-fold cross-validation
      .setParallelism(2)

    val cvModelForest = cvForest.fit(trainData)

    //See results
    /*cvModelForest.transform(testData)
      .select("HeartDisease", "prediction")
      .collect()
      .foreach { case Row(id: String, prediction: Double) =>
        println(s"($id) --> prediction=$prediction")
      }*/

    val predictionAndLabelsForest = cvModelForest.transform(testData).select("label", "prediction").rdd.map(row => (row.getDouble(0), row.getDouble(1)))

    //Model's accuracy
    val accuracyForest = predictionAndLabelsForest.filter(pl => pl._1.equals(pl._2)).count().toDouble / testData.count().toDouble
    println("Accuracy: " + accuracyForest)


    //Professor's Models
    val pipeStages = Array(labelIndexer) ++ stringIndexer ++ oneHotEncoder ++ Array(vector)
    val indexer_model = new Pipeline().setStages(pipeStages).fit(dataFrame)

    val df = indexer_model.transform(dataFrame)

    val data2 = df.rdd.map(row => LabeledPoint(
      row.getAs[Double]("label"),
      org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs("features")))
    )

    def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]):
    MulticlassMetrics = {
      val predictionsAndLabels = data.map(example =>
        (model.predict(example.features), example.label)
      )
      new MulticlassMetrics(predictionsAndLabels)
    }

    val Array(trainData2, valData2, testData2) =
      data2.randomSplit(Array(0.8, 0.1, 0.1))

    val evaluations2 =
      for (impurity <- Array("gini", "entropy");
           depth <- Array(20, 30);
           bins <- Array(200, 300))
      yield {
        val model2 = DecisionTree.trainClassifier(
          trainData2, 2, Map(0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 6, 6 -> 4, 7 -> 2, 8 -> 5, 9 -> 2, 10 -> 2, 11 -> 2, 12 -> 13),
          impurity, depth, bins)
        val trainAccuracy = getMetrics(model2, trainData2).accuracy
        val valAccuracy = getMetrics(model2, valData2).accuracy
        ((impurity, depth, bins), (trainAccuracy, valAccuracy)) }

    evaluations2.sortBy(_._2).reverse.foreach(println)

    //Compute best tree's accuracy
    val model2 = DecisionTree.trainClassifier(
      trainData2, 2, Map(0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 6, 6 -> 4, 7 -> 2, 8 -> 5, 9 -> 2, 10 -> 2, 11 -> 2, 12 -> 13),
      "gini", 30, 300)

    val testAccuracy = getMetrics(model2, testData2).accuracy

    println("OldAccuracy: " + testAccuracy)
  }
}
