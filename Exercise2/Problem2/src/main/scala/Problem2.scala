import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.sql.Row

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.reflect.io.File


object Problem2 {
  def parseNOAA(rawData: RDD[String]): RDD[Row] = {

    rawData
      .filter(line => line.substring(87, 92) != "+9999") // filter out missing temperature labels
      .map { line =>
        val year = line.substring(15, 19).toInt
        val month = line.substring(19, 21).toInt
        val day = line.substring(21, 23).toInt
        val hour = line.substring(23, 25).toInt
        val latitude = line.substring(28, 34).toDouble / 1000
        val longitude = line.substring(34, 41).toDouble / 1000
        val elevationDimension = line.substring(46, 51).toInt
        val directionAngle = line.substring(60, 63).toInt
        val speedRate = line.substring(65, 69).toDouble / 10
        val ceilingHeightDimension = line.substring(70, 75).toInt
        val distanceDimension = line.substring(78, 84).toInt
        val dewPointTemperature = line.substring(93, 98).toDouble / 10
        val airTemperature = line.substring(87, 92).toDouble / 10

        Row(year, month, day, hour, latitude, longitude, elevationDimension, directionAngle, speedRate, ceilingHeightDimension, distanceDimension, dewPointTemperature, airTemperature)
      }
  }

  def main(args: Array[String]): Unit = {
    //(a)
    val filePath = "./dataFrame"
    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val sc = new SparkContext(sparkConf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

    if (new java.io.File(filePath).exists){ //Check if we saved the file yet
      val df = spark.read.option("header",value = true)
        .csv(filePath)

      df.cache()
    } else {
      val rawNOAA = sc.textFile("./NOAA-065900/065900*")
      val parsedNOAA = parseNOAA(rawNOAA)

      val schemaString = "year month day hour latitude longitude elevationDimension directionAngle speedRate ceilingHeightDimension distanceDimension dewPointTemperature airTemperature"

      // Generate the schema based on the string of schema
      val intField = "year month day hour elevationDimension directionAngle ceilingHeightDimension distanceDimension"
      val fields = schemaString.split(" ")
        .map(fieldName => if (intField contains fieldName) StructField(fieldName, dataType = IntegerType, nullable = true)
        else StructField(fieldName, dataType = DoubleType, nullable = true))
      val schema = StructType(fields)

      //Create DataFrame
      val df = spark.createDataFrame(parsedNOAA, schema)

      df.cache()

      df.repartition(1).write.option("header",value = true).csv(filePath)
    }
  }
}
