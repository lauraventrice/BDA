import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter

import com.esri.core.geometry.{ GeometryEngine, SpatialReference, Geometry, Point }
import GeoJsonProtocol._
import spray.json._

class Collision(date: LocalDate, time: LocalTime, borough: String, latitude: String, longitude: String, onStreet: String,
                crossStreet: String, offStreet: String, personInjured: Int, personKilled: Int,
                contributingFactor1: String, contributingFactor2: String,
                vehicle1: String, vehicle2: String) extends java.io.Serializable {

  val dateCollision: LocalDate = date
  val timeCollision: LocalTime = time
  var boroughCollision: String = borough
  val latitudeCollision: String = latitude
  val longitudeCollision: String = longitude
  val locationCollision: String = getLocation(latitude, longitude)
  val onStreetNameCollision: String = onStreet
  val crossStreetNameCollision: String = crossStreet
  val offStreetNameCollision: String = offStreet
  val numberOfPersonsInjuredCollision: Int = personInjured
  val numberOfPersonsKilledCollision: Int = personKilled
  val contributingFactorVehicle1: String = contributingFactor1
  val contributingFactorVehicle2: String = contributingFactor2
  val vehicleTypeCode1: String = vehicle1
  val vehicleTypeCode2: String = vehicle2

  // Parse location from latitude and longitude
  def getLocation(latitude: String, longitude: String): String = {
    "(" + latitude + ", " + longitude + ")"
  }

  // Filter wrong collision
  def checkWrongCollision(): Boolean = {
    numberOfPersonsKilledCollision < 0 || numberOfPersonsInjuredCollision < 0 ||
      dateCollision.isBefore(LocalDate.parse("01/01/2013", DateTimeFormatter.ofPattern("MM/dd/yyyy"))) ||
      dateCollision.isAfter(LocalDate.parse("01/31/2013", DateTimeFormatter.ofPattern("MM/dd/yyyy"))) ||
      (longitudeCollision.equals("") && latitudeCollision.equals("") && boroughCollision.equals("")) /*||
      (boroughCollision.equals("") || onStreetNameCollision.equals("") || crossStreetNameCollision.equals("")) ||
      (contributingFactorVehicle2.equals("Unspecified") || contributingFactorVehicle2.equals("Unspecified"))*/
  }

  // Find borough from the coordinates
  def addBorough(features: FeatureCollection) = {
    if(boroughCollision.equals("") && (!longitudeCollision.equals("") || !latitudeCollision.equals(""))) {
      val borough = features.find(f => f.geometry.contains(new Point(longitudeCollision.toDouble, latitudeCollision.toDouble)))
      if (borough != None) boroughCollision = borough.get("borough").toString.toUpperCase().replace("\"", "")
    }
    this
  }
}

object Problem2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //(a)
    // Remove excess spaces from street names
    def cleanString(string: String) = string.split(" ").mkString("", " ", "")

    def getDate(date: String) = LocalDate.parse(date, DateTimeFormatter.ofPattern("MM/dd/yyyy"))

    def getTime(time: String) = LocalTime.parse(time, DateTimeFormatter.ofPattern("HH:mm"))

    def parse(line: String) = {
      if(line.startsWith("DATE,")){
        new Collision(LocalDate.now(), LocalTime.now(), "", "", "", "", "", "",
          -1, -1, "", "", "", "")
      }
      else {
        val fields = line.split(",", 7)
        val date = getDate(fields(0))
        if (fields(1).length == 4) fields(1) = "0" + fields(1)
        val time = getTime(fields(1))
        val features = fields(6)
        var location = ""
        if (!features.indexOf("\"").equals(-1)) {
          location = features.substring(features.indexOf("\""), features.lastIndexOf("\"") + 1)
        }
        val others = features.replace(location, "").substring(1).split(",")
        val onStreet = cleanString(others(0))
        val crossStreet = cleanString(others(1))
        val offStreet = cleanString(others(2))
        var vehicle2 = ""
        if (others.length > 18) vehicle2 = others(18)
        new Collision(date, time, fields(2), fields(4), fields(5), onStreet, crossStreet, offStreet,
          others(3).toInt, others(4).toInt, others(11), others(12), others(17), vehicle2)
      }
    }

    val geojson = scala.io.Source.fromFile("./Data/nyc-borough-boundaries-polygon.geojson")
      .mkString.parseJson.convertTo[FeatureCollection]

    val collisions = sc.textFile("NYPD_Motor_Vehicle_Collisions.csv").map(parse).map(_.addBorough(geojson))
      .filter(!_.checkWrongCollision()).cache()


    //(b)
    def mostDangerousStreet(rdd: RDD[Collision]) = {
      rdd.map(x => ((x.boroughCollision, x.onStreetNameCollision, x.crossStreetNameCollision),
          x.numberOfPersonsInjuredCollision + x.numberOfPersonsKilledCollision))
        .reduceByKey(_ + _)
    }

    println("Most Dangerous Street:")
    mostDangerousStreet(collisions).sortBy(_._2, ascending = false).take(25).foreach(println)

    def mostCommonContributingFactors(rdd: RDD[Collision]) = {
      rdd.map(x => ((x.boroughCollision, x.onStreetNameCollision, x.crossStreetNameCollision),
        x.contributingFactorVehicle1))
        .union(rdd.map(x => ((x.boroughCollision, x.onStreetNameCollision, x.crossStreetNameCollision),
        x.contributingFactorVehicle2)))
        .groupBy(x => (x._1, x._2))
        .map(x => (x._1, x._2.size))
    }

    println("Most Common Contributing Factors:")
    mostCommonContributingFactors(collisions).sortBy(_._2, ascending = false).take(5).foreach(println)


    //(c)
    def mostDangerousTime(rdd: RDD[Collision]) = {
      rdd.map(x => ((x.dateCollision.getDayOfWeek, x.timeCollision.getHour),
        x.numberOfPersonsInjuredCollision + x.numberOfPersonsKilledCollision))
        .reduceByKey(_ + _)
    }

    println("Most Dangerous Time:")
    mostDangerousTime(collisions).sortBy(_._2, ascending = false).take(25).foreach(println)

    //(d)
    def mostVehiclesAccidents(rdd: RDD[Collision]) = {
      rdd.map(x => ((x.vehicleTypeCode1, x.vehicleTypeCode2), 1))
        .reduceByKey(_ + _)
    }

    println("Most Vehicles Accidents:")
    mostVehiclesAccidents(collisions).sortBy(x => x._2, ascending = false).take(5).foreach(println)

    //(e)
    def mostDangerousStreetDifference(rdd: RDD[Collision]): RDD[((String, String, String), Int)] = {
      rdd.map(x => ((x.boroughCollision, x.onStreetNameCollision, x.crossStreetNameCollision),
        (x.numberOfPersonsInjuredCollision - x.numberOfPersonsKilledCollision).abs))
        .reduceByKey(_ + _)
    }

    println("Most Dangerous Street Difference:")
    mostDangerousStreetDifference(collisions).sortBy(_._2, ascending = false).take(5).foreach(println)

    def mostDangerousTimeDifference(rdd: RDD[Collision]) = {
      rdd.map(x => ((x.dateCollision.getDayOfWeek, x.timeCollision.getHour),
        x.numberOfPersonsInjuredCollision - x.numberOfPersonsKilledCollision))
        .reduceByKey(_ + _)
    }

    println("Most Dangerous Time Difference:")
    mostDangerousTimeDifference(collisions).sortBy(x => x._2, ascending = false).take(5).foreach(println)
  }
}
