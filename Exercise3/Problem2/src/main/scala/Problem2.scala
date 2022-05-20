import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// TODO controlla se date può essere tipo Date e time tipo Time
// TODO latitude and longitude String(?)
// TODO togli la prima riga col nome delle features (dal parsing) altrimenti errore

class Collision(date: String, time: String, borough: String, latitude: String, longitude: String, onStreet: String,
                crossStreet: String, offStreet: String, personInjured: Int, personKilled: Int,
                contributingFactor1: String, contributingFactor2: String,
                vehicle1: String, vehicle2: String) extends java.io.Serializable {

  val dateCollision: String = date
  val timeCollision: String = time
  val boroughCollision: String = borough
  val latitudeCollision: String = latitude
  val longitudeCollision: String = longitude
  val locationCollision: String = getLocation(latitude, longitude)
  val onStreetNameCollision: String = onStreet
  val crossStreetNameCollision: String = crossStreet
  val offStreetNameCollision: String = offStreet
  val numberOfPersonInjuredCollision: Int = personInjured
  val numberOfPersonKilledCollision: Int = personKilled
  val contributingFactorVehicle1: String = contributingFactor1
  val contributingFactorVehicle2: String = contributingFactor2
  val vehicleTypeCode1: String = vehicle1
  val vehicleTypeCode2: String = vehicle2

  // Parse location from latitude and longitude
  def getLocation(latitude: String, longitude: String): String = {
    "(" + latitude + ", " + longitude + ")"
  }

  override def toString: String = {
    "Date: " + dateCollision + ", Time: " + timeCollision + ", Borough: " + boroughCollision +
      ", Latitude: " + latitudeCollision + ", Longitude: " + longitudeCollision + ", Location: " + locationCollision +
      ", OnStreet: " + onStreetNameCollision + ", CrossStreet: " + crossStreetNameCollision +
      ", OffStreet: " + offStreetNameCollision + ", PersonInjured: " + numberOfPersonInjuredCollision +
      ", PersonKilled: " + numberOfPersonKilledCollision + ", Contributing1: " + contributingFactorVehicle1 +
      ", Contributing2: " + contributingFactorVehicle2 + ", Vehicle1: " + vehicleTypeCode1 +
      ", Vehicle2: " + vehicleTypeCode2
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

    // TODO controlla se si può migliorare
    def parse(line: String) = {
      val fields = line.split(",", 7)
      val features = fields(6)
      var location = ""
      if(!features.indexOf("\"").equals(-1)) {
        location = features.substring(features.indexOf("\""), features.lastIndexOf("\"") + 1)
      }
      val others = features.replace(location, "").substring(1).split(",")
      val onStreet = cleanString(others(0))
      val crossStreet = cleanString(others(1))
      val offStreet = cleanString(others(2))
      var vehicle2 = ""
      if(others.length > 18) vehicle2 = others(18)
      new Collision(fields(0), fields(1), fields(2), fields(4), fields(5), onStreet, crossStreet, offStreet,
        others(3).toInt, others(4).toInt, others(11), others(12), others(17), vehicle2)
    }

    // TODO Also filter out useless or meaningless lines from your data set
    val collisions = sc.textFile("NYPD_Motor_Vehicle_Collisions.csv").map(parse)

    /*
    Find the most dangerous street crossings according to the number of people that were injured or even killed
    in collisions which are recorded within the data set. Return tuples of (BOROUGH, ON STREET NAME, CROSS STREET NAME)
    together with the number of people involved (either injured or killed) and
    // TODO che vogliono qua?
    a list of the 5 most common contributingfactors (of either one of the two vehicles involved in a collision) for each such crossing.
    Sort all crossings in
    descending order of the total number of people involved in these accidents and report the top-25 most dangerous
    crossings.
     */
    //(b)
    // TODO ci sono anche le righe che non hanno nessuna delle tre informazioni (da eliminare?)
    def mostDangerousStreet(rdd: RDD[Collision]) = {
      rdd.map(x => (x.boroughCollision, x.onStreetNameCollision, x.crossStreetNameCollision,
          x.numberOfPersonInjuredCollision + x.numberOfPersonKilledCollision))
        .map(x => ((x._1, x._2, x._3), x._4))
        .reduceByKey(_ + _)

    }

    mostDangerousStreet(collisions).sortBy(x => x._2, ascending = false).take(5).foreach(println)
  }
}
