import org.apache.spark.sql
import org.apache.spark
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.{SparkConf, SparkContext}

object SparkTwitterCollector {
  def main(args: Array[String]){
    //Non sono sicuro che sia necessario, ma altrimenti non so come richiamare spark
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val csvPath = "heart_2020_cleaned.csv" //Csv's path
    //Load csv into a DataFrame (a)
    val dataFrame = spark.read.option("inferSchema", value = true).option("header", value = true).csv(csvPath)
  }
}
