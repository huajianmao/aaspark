package cn.hjmao.aaspark.ch02

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * Created by hjmao on 06/03/2017.
 */

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

// scalastyle:off println
object Linkage {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "spark://cubeheader1:7077"
    }
    val conf = new SparkConf()
      .setAppName("AASpark")
      .setMaster(master)
      .setExecutorEnv("--driver-memory", "16g")
    val sc = new SparkContext(conf)

    val dataset = "hdfs://cubeheader1:9000/hjmao/data/aaspark/ch02/block_*.csv"
    val rawblocks: RDD[String] = sc.textFile(dataset)

//    small(rawblocks)
    distributed(rawblocks)

    sc.stop
  }

  private def small(rawblocks: RDD[String]) = {
    val head = rawblocks.take(10)
    val mds = head.filter(!isHeader(_)).map(parse)
    val grouped = mds.groupBy(md => md.matched)
    grouped.mapValues(x => x.size).foreach(println)
  }

  private def distributed(rawblocks: RDD[String]) = {
    val noheader = rawblocks.filter(!isHeader(_))
    val parsed = noheader.map(parse)
    parsed.cache()
//    // 2.9
//    val matchCounts = parsed.map(_.matched).countByValue()
//    val matchCountsSeq = matchCounts.toSeq
//    matchCountsSeq.sortBy(_._1).foreach(println)
//    matchCountsSeq.sortBy(_._2).foreach(println)
//    matchCountsSeq.sortBy(_._2).reverse.foreach(println)

//    // 2.10
//    parsed.map(_.scores(0)).filter(!java.lang.Double.isNaN(_)).stats()
//    val stats = (0 until 9).map(i => {
//      parsed.map(_.scores(i)).filter(!java.lang.Double.isNaN(_)).stats()
//    })
//    val nasRDD = parsed.map(md => {
//      md.scores.map(s => NAStatCounter(s))
//    })
//    val reduced = nasRDD.reduce((n1, n2) => {
//      n1.zip(n2).map {case (a, b) => a.merge(b)}
//    })
//    reduced.foreach(println)

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

    statsm.zip(statsn).map {
      case (m, n) => (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })
    ct.filter(_.score >= 4.0).map(_.md.matched).countByValue()
    ct.filter(_.score >= 2.0).map(_.md.matched).countByValue()
  }

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }

  def toDouble(s: String): Double = {
    if ("?".equals(s)) Double.NaN else s.toDouble
  }

  def parse(line: String): MatchData = {
    val pieces = line.split(',')
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(NAStatCounter(_))
      iter.foreach(array => {
        nas.zip(array).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })
  }

  def naz(d: Double): Double = if (Double.NaN.equals(d)) 0.0 else d
  case class Scored(md: MatchData, score: Double)
}
// scalastyle:on println
