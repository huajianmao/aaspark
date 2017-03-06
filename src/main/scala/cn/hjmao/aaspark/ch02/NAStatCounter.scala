package cn.hjmao.aaspark.ch02

import org.apache.spark.util.StatCounter

/**
 * Created by hjmao on 06/03/2017.
 */
class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    missing += other.missing
    stats.merge(other.stats)
    this
  }

  override def toString: String = {
    "stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double): NAStatCounter = new NAStatCounter().add(x)
}
