import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._

object Problem2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val rawData = sc.textFile("./archive/Run_1.csv").sample(false, 0.01)

    val data = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      vector
    }

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    val sample = data.map(vector =>
      model.predict(vector) + "," + vector.toArray.mkString(",")).sample(false, 0.01)
    sample.saveAsTextFile("./kmeans-sample")
  }
}
