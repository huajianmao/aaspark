package cn.hjmao.aaspark.ch08

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import com.esri.core.geometry.Point
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import spray.json._
import GeoJsonProtocol._

/**
 * Created by hjmao on 17-3-20.
 */
class RichRow(row: Row) {
  def getAs[T](field: String): Option[T] =
    if (row.isNullAt(row.fieldIndex(field))) None
    else Some(row.getAs[T](field))
}

case class Trip(
  license: String,
  pickupTime: Long,
  dropoffTime: Long,
  pickupX: Double,
  pickupY: Double,
  dropoffX: Double,
  dropoffY: Double
)

object RunGeoTime {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("spark://cubeheader1:7077")
                                      .appName("GeoTime")
                                      .config("spark.executor.memory", "16g")
                                      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                      .getOrCreate()
    import spark.implicits._

    val taxiRaw = spark.read.option("header", "true").csv("hdfs://cubeheader1:9000/hjmao/data/aaspark/ch08/trip_data_1.csv")
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.map(_.left.get).toDS
    taxiGood.cache()

    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    val hoursDF = udf(hours)
//    taxiGood.groupBy(hoursDF($"pickupTime", $"dropoffTime").as("h")).count().sort("h").show()

    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickupTime, dropoffTime) between 0 AND 3")

    val geojson = scala.io.Source.fromURL(
      this.getClass.getResource("/data/ch08/nyc-boroughs.geojson")
    ).mkString
    val features = geojson.parseJson.convertTo[FeatureCollection]
    val areaSortedFeatures = features.sortBy(f => {
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    })
    val bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

    val bLookup = (x: Double, y: Double) => {
      val feature: Option[Feature] = bFeatures.value.find(_.geometry.contains(new Point(x, y)))
      feature.map(_("borough").convertTo[String]).getOrElse("NA")
    }
    val boroughUDF = udf(bLookup)
//    taxiClean.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

    val taxiDone = taxiClean.where(
      "dropoffX != 0 AND dropoffY != 0 AND pickupX != 0 and pickupY != 0"
    )
//    taxiDone.groupBy(boroughUDF($"dropoffX", $"dropoffY")).count().show()

    taxiGood.unpersist()

    val sessions = taxiDone.repartition($"license")
                           .sortWithinPartitions($"license", $"pickupTime")
                           .cache()

    def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
      val b = bLookup(t1.dropoffX, t1.dropoffY)
      val d = (t2.pickupTime - t1.dropoffTime) / 1000
      (b, d)
    }

    val boroughDurations: DataFrame = {
      sessions.mapPartitions(trips => {
        val iter: Iterator[Seq[Trip]] = trips.sliding(2)
        val viter = iter.filter(_.size == 2).filter(p => p(0).license == p(1).license)
        viter.map(p => boroughDuration(p(0), p(1)))
      }).toDF("borough", "seconds")
    }

    boroughDurations.where("seconds > 0")
                    .groupBy("borough")
                    .agg(avg("seconds"), stddev("seconds"))
                    .show()

    boroughDurations.unpersist()

    spark.close()
  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {
      override def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }
    }
  }

  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val optDt = rr.getAs[String](timeField)
    optDt.map(formatter.parse(_).getTime).getOrElse(0L)
  }

  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }

  def parse(line: Row): Trip = {
    val rr = new RichRow(line)
    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
    )
  }
}
