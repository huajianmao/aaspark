package cn.hjmao.aaspark.ch08

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

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
    val spark = SparkSession.builder().master("local[*]")
                                      .appName("GeoTime in Ch08")
                                      .getOrCreate()
    import spark.implicits._

    val taxiRaw = spark.read.option("header", "true").csv("data/ch08/trip_data*.csv")
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.map(_.left.get).toDS
    taxiGood.cache()

    val hours = (pickup: Long, dropoff: Long) => {
      TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
    }
    val hoursDF = udf(hours)
    taxiGood.groupBy(hoursDF($"pickupTime", $"dropoffTime").as("h")).count().sort("h").show()

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
