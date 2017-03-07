package cn.hjmao.aaspark.ch03

import scala.collection.Map

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by hjmao on 06/03/2017.
 */

// scalastyle:off println
object Recommender {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local[20]"
    }

    val conf = new SparkConf()
      .setAppName("Recommender Engine")
      .setMaster(master)
      .setExecutorEnv("--driver-memory", "32g")
      .setExecutorEnv("--executor-memory", "16g")

    val sc = new SparkContext(conf)

    val userArtistDataset = "data/ch03/user_artist_data.txt"
    val rawUserArtistData = sc.textFile(userArtistDataset)
    // rawUserArtistData.take(10).foreach(println)
    // println(rawUserArtistData.map(_.split(' ')(0).toDouble).stats())
    // println(rawUserArtistData.map(_.split(' ')(1).toDouble).stats())

    val artistDataset = "data/ch03/artist_data.txt"
    val rawArtistData = sc.textFile(artistDataset)
    val artistByID = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

    val artistAliasDataset = ("data/ch03/artist_alias.txt")
    val rawArtistAlias = sc.textFile(artistAliasDataset)
    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()
    // println(artistByID.lookup(6803336).head)
    // println(artistByID.lookup(1000010).head)
    val bArtistAlias = sc.broadcast(artistAlias)
    val trainData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }.cache()

    /*
    // First Model
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).filter {
      case Array(user, _, _) => user.toInt == 2093760
    }
    val existingProducts = rawArtistsForUser.map {
      case Array(_, artist, _) => artist.toInt
    }.collect().toSet
    artistByID.filter {
      case (id, name) => existingProducts.contains(id)
    }.values.collect().foreach(println)
    val recommendations = model.recommendProducts(2093760, 5)
    recommendations.foreach(println)
    val recommendedProductIDs = recommendations.map(_.product).toSet
    artistByID.filter {
      case (id, name) => recommendedProductIDs.contains(id)
    }.values.collect().foreach(println)
    */

    sc.stop
  }
}
// scalastyle:on println
