import com.esri.core.geometry.{Geometry, GeometryEngine, Point, SpatialReference}
import com.github.nscala_time.time.Imports.{DateTime, Duration}
import GeoJsonProtocol._

import java.text.SimpleDateFormat
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.StatCounter

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import spray.json._

import java.util.Locale

val conf = new SparkConf()
  .setMaster("local")
  .setAppName("RunTaxiTrips")

//val sparkConf = new SparkConf()
//val sc = new SparkContext(sparkConf)
sc.setLogLevel("ERROR")

val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

def point(longitude: String, latitude: String): Point = {
  new Point(longitude.toDouble, latitude.toDouble)
}

case class TaxiTrip (
  pickupTime:  org.joda.time.DateTime,
  dropoffTime: org.joda.time.DateTime,
  pickupLoc:   com.esri.core.geometry.Point,
  dropoffLoc:  com.esri.core.geometry.Point) extends java.io.Serializable

def parse(line: String): (String, TaxiTrip) = {
  val fields = line.split(',')
  val license = fields(1)
  val pickupTime = new org.joda.time.DateTime(formatter.parse(fields(5)))
  val dropoffTime = new org.joda.time.DateTime(formatter.parse(fields(6)))
  val pickupLoc = point(fields(10), fields(11))
  val dropoffLoc = point(fields(12), fields(13))
  val trip = TaxiTrip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
  (license, trip)
}

def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
  new Function[S, Either[T, (S, Exception)]] with Serializable {
    def apply(s: S): Either[T, (S, Exception)] = {
      try {
        Left(f(s))
      } catch {
        case e: Exception => Right((s, e))
      }
    }
  }
}

//----------------- Parse & Filter the Taxi Trips -------------------------

val taxiRaw = sc.textFile("./Data/nyc-taxi-trips").sample(false, 0.01) // use 1 percent sample size for debugging!
val taxiParsed = taxiRaw.map(safe(parse))
taxiParsed.cache()

val taxiBad = taxiParsed.collect({
  case t if t.isRight => t.right.get
})

val taxiGood = taxiParsed.collect({
  case t if t.isLeft => t.left.get
})
taxiGood.cache() // cache good lines for later re-use

println("\n" + taxiGood.count() + " taxi trips parsed.")
println(taxiBad.count() + " taxi trips dropped.")

def getHours(trip: TaxiTrip): Long = {
  val d = new Duration(
    trip.pickupTime,
    trip.dropoffTime)
  d.getStandardHours
}

println("\nDistribution of trip durations in hours:")
taxiGood.values.map(getHours).countByValue().
  toList.sorted.foreach(println)

val taxiClean = taxiGood.filter {
  case (lic, trip) =>
    val hrs = getHours(trip)
    0 <= hrs && hrs < 3
}

val taxiDone = taxiClean.filter {
  case (lic, trip) =>
    val zero = new Point(0.0, 0.0)
    !(zero.equals(trip.pickupLoc) || zero.equals(trip.dropoffLoc))
}
taxiDone.cache()

//----------------- Parse the NYC Boroughs Polygons -----------------------

val geojson = scala.io.Source.
  fromFile("./Data/nyc-borough-boundaries-polygon.geojson").mkString

val features = geojson.parseJson.convertTo[FeatureCollection]

// look up the borough for some test point
val p = new Point(-73.994499, 40.75066)
val b = features.find(f => f.geometry.contains(p))

val areaSortedFeatures = features.sortBy(f => {
  val borough = f("boroughCode").convertTo[Int]
  (borough, -f.geometry.area2D())
})

val bFeatures = sc.broadcast(areaSortedFeatures)

def borough(trip: TaxiTrip): Option[String] = {
  val feature: Option[Feature] = bFeatures.value.find(f => {
    f.geometry.contains(trip.dropoffLoc)
  })
  feature.map(f => {
    f("borough").convertTo[String]
  })
}

println("\nDistribution of trips per borough:")
taxiClean.values.map(borough).countByValue().foreach(println)

//----------------- Helper Classes for "Sessionization" -------------------

class FirstKeyPartitioner[K1, K2](partitions: Int) extends org.apache.spark.Partitioner {
  val delegate = new org.apache.spark.HashPartitioner(partitions)
  override def numPartitions: Int = delegate.numPartitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(K1, K2)]
    delegate.getPartition(k._1)
  }
}

def secondaryKey(trip: TaxiTrip) = trip.pickupTime.getMillis

def split(t1: TaxiTrip, t2: TaxiTrip): Boolean = {
  val p1 = t1.pickupTime
  val p2 = t2.pickupTime
  val d = new Duration(p1, p2)
  d.getStandardHours >= 4
}

def groupSorted[K, V, S](
  it:        Iterator[((K, S), V)],
  splitFunc: (V, V) => Boolean): Iterator[(K, List[V])] = {
  val res = List[(K, ArrayBuffer[V])]()
  it.foldLeft(res)((list, next) => list match {
    case Nil =>
      val ((lic, _), trip) = next
      List((lic, ArrayBuffer(trip)))
    case cur :: rest =>
      val (curLic, trips) = cur
      val ((lic, _), trip) = next
      if (!lic.equals(curLic) || splitFunc(trips.last, trip)) {
        (lic, ArrayBuffer(trip)) :: list
      } else {
        trips.append(trip)
        list
      }
  }).map { case (lic, buf) => (lic, buf.toList) }.iterator
}

def groupByKeyAndSortValues[K: Ordering: ClassTag, V: ClassTag, S: Ordering](
  rdd:              RDD[(K, V)],
  secondaryKeyFunc: V => S,
  splitFunc:        (V, V) => Boolean,
  numPartitions:    Int): RDD[(K, List[V])] = {
  val presess = rdd.map {
    case (lic, trip) => ((lic, secondaryKeyFunc(trip)), trip)
  }
  val partitioner = new FirstKeyPartitioner[K, S](numPartitions)
  presess.repartitionAndSortWithinPartitions(partitioner).mapPartitions(groupSorted(_, splitFunc))
}

val sessions = groupByKeyAndSortValues(taxiDone, secondaryKey, split, 30) // use fixed amount of 30 partitions
sessions.cache()

println("\nSome sample sessions:")
sessions.take(5).foreach(println)

//----------------- Final Analysis of the Trip Durations ------------------

def boroughDuration(t1: TaxiTrip, t2: TaxiTrip) = {
  val b = borough(t1)
  val d = new Duration(t1.dropoffTime, t2.pickupTime)
  (b, d)
}

val boroughDurations: RDD[(Option[String], Duration)] =
  sessions.values.flatMap(trips => {
    val iter: Iterator[Seq[TaxiTrip]] = trips.sliding(2)
    val viter = iter.filter(_.size == 2)
    viter.map(p => boroughDuration(p(0), p(1)))
  }).cache()

println("\nDistribution of wait-times in hours:")
boroughDurations.values.map(_.getStandardHours).countByValue().toList.
  sorted.foreach(println)

println("\nFinal stats of wait-times per borough:")
boroughDurations.filter {
  case (b, d) => d.getMillis >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d.getStandardSeconds)
}).reduceByKey((a, b) => a.merge(b)).collect().foreach(println)

/*
(a)
Compute the count, sum and average duration of all taxi trips which started and ended in the same
NYC borough over the entire period of time recorded in the data set.
 */
 
 def getMinutes(trip: TaxiTrip): Long = {
  val d = new Duration(
    trip.pickupTime,
    trip.dropoffTime)
  d.getStandardMinutes
}

def tripSameBorough(trip: TaxiTrip) = {
  borough(trip) == bFeatures.value.find(f => {
    f.geometry.contains(trip.pickupLoc)
  }).map(f => {
    f("borough").convertTo[String]
  })
}

def tripDifferentBorough(trip: TaxiTrip) = {
  borough(trip) != bFeatures.value.find(f => {
    f.geometry.contains(trip.pickupLoc)
  }).map(f => {
    f("borough").convertTo[String]
  })
}
val sameBoroughDurations = taxiDone.filter(trip => tripSameBorough(trip._2)).map(trip => getMinutes(trip._2))

println("\nStatistics taxi trips started and ended in the same borough:")
val statsSameBoroughDurations = sameBoroughDurations.filter(d => d>=0).map(d => {
  val s = new StatCounter()
  s.merge(d)
}).reduce((a, b) => a.merge(b))

println("SUM: ", statsSameBoroughDurations.sum)


/*
(b)
Compute the count, sum and average duration of all taxi trips which started and ended in a different
NYC borough over the entire period of time recorded in the data set.
 */

val differentBoroughDurations = taxiDone.filter(trip => tripDifferentBorough(trip._2)).map(trip => getMinutes(trip._2))

println("\nStatistics taxi trips started and ended in the different borough:")
val statsDifferentBoroughDurations = differentBoroughDurations.filter(d => d>=0).map(d => {
  val s = new StatCounter()
  s.merge(d)
}).reduce((a, b) => a.merge(b))

println("SUM: ", statsDifferentBoroughDurations.sum)

/*
(c)
Repeat steps (a) and (b), but this time return one such statistic for each borough individually
(Brooklyn, Manhattan, ...) over the entire period of time recorded in the data set. Which is thus the busiest borough in NYC?
 */
/*
println("\nCount taxi trips for each borough:")
val countForBorough = taxiDone.map(trip => (borough(trip._2), 1L)).reduceByKey(_+_)
countForBorough.foreach(println)

println("\nSum duration taxi trips for each borough: ")
val sumForBorough = taxiDone.map(trip => (borough(trip._2), getMinutes(trip._2))).reduceByKey(_ + _)
sumForBorough.foreach(println)

println("\nMean duration taxi trips for each borough:")
countForBorough.join(sumForBorough).map{case(borough, (count, sum)) => (borough, sum/count)}.foreach(println)

*/

val forBoroughDurations = taxiDone.map(trip => (borough(trip._2), getMinutes(trip._2)))

println("\nStatistics taxi trips started and ended in the different borough:")
val statsForBoroughDurations = forBoroughDurations.filter {
  case (_, d) => d >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d)
}).reduceByKey((a, b) => a.merge(b))

statsForBoroughDurations.foreach(elem => println(elem + elem._2.sum.toString))

/*
(d)
Repeat steps (a) and (b), but this time return one such statistic for each day-of-the-week (Monday,
Tuesday, ..., Sunday) over the entire period of time recorded in the data set. Which is thus the
busiest day-of-the-week in NYC?
 */

def getDay(trip: TaxiTrip) = {
  trip.pickupTime.dayOfWeek().getAsText(Locale.getDefault)
}
/*
println("\nCount taxi trips for each day:")
val countForDay = taxiDone.map(trip => (getDay(trip._2), 1L)).reduceByKey(_+_)

println("\nSum duration taxi trips for each day: ")
val sumForDay = taxiDone.map(trip => (getDay(trip._2), getMinutes(trip._2))).reduceByKey(_ + _)
sumForDay.foreach(println)

println("\nMean duration taxi trips for each day:")
countForDay.join(sumForDay).map{case(day, (count, sum)) => (day, sum/count)}.foreach(println)

*/

val forDayDurations = taxiDone.map(trip => (getDay(trip._2), getMinutes(trip._2)))

println("\nStatistics taxi trips started and ended in the different borough:")
val statsForDayDurations = forDayDurations.filter {
  case (_, d) => d >= 0
}.mapValues(d => {
  val s = new StatCounter()
  s.merge(d)
}).reduceByKey((a, b) => a.merge(b))

statsForDayDurations.foreach(elem => println(elem + elem._2.sum.toString))

/*
(e)
Modify the provided script such that it indeed computes the average duration between two subsequent
trips conducted by the same taxi driver per NYC borough and per hour-of-the-day (at which the first
trip starts). 2 Points
Note that the current script computes these statistics only per borough. That is, please add the
hour-of-the-day (at which the first trip starts) as an additional grouping condition to the provided
Moodle script.
 */

def hour_of_the_day(t1: TaxiTrip, t2: TaxiTrip) = {
  val day = t1.pickupTime.hourOfDay().getAsText
  val dur = new Duration(t1.dropoffTime, t2.pickupTime)
  (day, dur)
}

def averageDurationBetween(list: List[TaxiTrip]) = {
  val sum = 0.0
  val count = 0.0
  val it = list.iterator
  it.foldLeft(0.0)()
}

sessions.mapValues(list => average)

/*
(f)
Let us assume we wish to detect typical rush hours in the NYC dataset. To do so, normalize all trip
durations by the direct geo-spatial distance between their start and end points, and then compute
the average normalized trip duration for each hour-of-the-day and across all taxi trips. 2 Points
That is, for each taxi trip, compute its duration (e.g., in seconds) and divide this duration by the
direct distance (e.g., in miles or kilometres) between the start and end point of this trip. Then again
group the taxi trips according to the hour-of-the-day at which they started, compute their averages,
and sort these averages (one for each hour) in descending order
 */





