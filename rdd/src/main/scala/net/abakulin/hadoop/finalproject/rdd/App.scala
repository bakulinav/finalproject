package net.abakulin.hadoop.finalproject.rdd

import java.sql.DriverManager

import net.abakulin.hadoop.finalproject.common.{Product, Purchase}
import org.apache.commons.net.util.SubnetUtils
import org.apache.hadoop.io.Text
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {

  def usage(): String = {
    "Usage: \n" +
      "app.jar [ top10categories | top10products | top10countries ] <path-to-data>"
  }

  def main(args : Array[String]): Unit = {
    if (args.length < 2) {
      println(usage())
      sys.exit(1)
    }

    val mode = args(0)
    val path = args(1)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val purchs = sc.sequenceFile(path, classOf[Text], classOf[Text])
      .map(a => a._2.toString)
      .map(str => str.split(","))
      .map(arr => Purchase(Product(arr(0), arr(3)), arr(1).toDouble, arr(2), arr(4)))

    mode.toLowerCase match {
      case "top10categories" => getTop10Categories(purchs)
      case "top10products" => getTop10ProductsInCategory(purchs)
      case "top10countries" => {
        val networksRdd = sc.textFile("/user/cloudera/Country-Blocks-IPv4.csv")
        val countriesRdd = sc.textFile("/user/cloudera/Country-Locations-en.csv")

        val ipRangeToCountry = getNetworkToCountryMap(networksRdd, countriesRdd)

        val ipRangeToCountryBcst = sc.broadcast(ipRangeToCountry)

        getTop10Countries(purchs, ipRangeToCountryBcst)
      }
    }

    sc.stop()
  }

  def getTop10Categories(purchs : RDD[Purchase]) {

    val conn = getConnection()
    val stmt = conn.prepareStatement("INSERT INTO top10CategoryRdd(category, cnt) VALUES (?, ?)")

    purchs
      .map(p => (p.product.category, 1))
      .reduceByKey(_+_)
      .takeOrdered(10)(Ordering.by(p => -p._2))
      .foreach(x => {
          stmt.setString(1, x._1)
          stmt.setInt(2, x._2)
          stmt.executeUpdate()
        }
      )

    conn.close()
  }

  def getTop10ProductsInCategory(purchs: RDD[Purchase]) = {
    val conn = getConnection()
    val stmt = conn.prepareStatement("INSERT INTO top10ProductRdd(category, product, cnt) VALUES (?, ?, ?)")

    purchs
      .map(p => ((p.product.category, p.product.name), 1))
      .reduceByKey(_+_)
      .map(el => (el._1._1, (el._1._2, el._2)))
      .groupByKey()
      .mapValues(a => a.toList.sortBy(p => -p._2).take(10))
      .flatMap(x => x._2.toArray.map(el => (x._1, el._1, el._2)))
      .collect()
      .foreach(x => {
          stmt.setString(1, x._1)
          stmt.setString(2, x._2)
          stmt.setInt(3, x._3)
          stmt.executeUpdate()
        }
      )

    conn.close()
  }

  def getTop10Countries(purchs: RDD[Purchase], ipRangeToCountryBcst: Broadcast[Array[((Long, Long), String)]]) = {
    val ipRangeToCountry = ipRangeToCountryBcst.value
    val resolveCountry: Long => String = getCountryByIp(ipRangeToCountry)

    val conn = getConnection()
    val stmt = conn.prepareStatement("INSERT INTO top10CountryRdd(country, value) VALUES (?, ?)")

    purchs
      .map(p => (ipToLong(p.clientIp), p.price))
      .map(x => (resolveCountry(x._1), x._2))
      .reduceByKey(_+_)
      .takeOrdered(10)(Ordering.by(p => -p._2))
      .foreach(
        x => {
          stmt.setString(1, x._1)
          stmt.setDouble(2, x._2)
          stmt.executeUpdate()
        }
      )

    conn.close()
  }

  def getCountryByIp(ipRangeToCountry: Array[((Long, Long), String)])(ip:Long) = {
    def binarySearch(ip: Long, start: Int, end: Int): String = {
      if (start > end) return ""

      val mid = start + (end-start+1)/2
      val lhc = ipRangeToCountry(mid) // low-high-country
      inRange(ip, lhc._1) match {
        case 0 => lhc._2
        case -1 => binarySearch(ip, start, mid - 1)
        case 1 => binarySearch(ip, mid + 1, end)
      }
    }

    def inRange(ip: Long, rng: (Long, Long)): Int = {
      ip match {
        case (x:Long) if (rng._1 <= x && x <= rng._2) => 0
        case (x:Long) if (ip < rng._1) => -1
        case (x:Long) if (ip > rng._2) => 1
      }
    }

    binarySearch(ip, 0, ipRangeToCountry.length - 1)
  }

  def getGeoIdToNetwork(networksRdd: RDD[String]): RDD[(String, String)] = {
    // ﻿network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider
    val head = networksRdd.first()

    networksRdd
      .filter(ln => ln != head)
      .map(ln => ln.trim().split(","))
      .filter(arr => arr.length == 6)
      .map(arr => (arr(1), arr(0)))
  }

  def getGeoIdToCountry(countriesRdd: RDD[String]): RDD[(String, String)] = {
    // ﻿geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name
    val head = countriesRdd.first()

    countriesRdd
      .filter(ln => ln != head)
      .map(ln => ln.trim().split(","))
      .filter(arr => arr.length == 6)
      .map(arr => (arr(0), arr.last))
  }

  def getNetworkToCountryMap(networksRdd: RDD[String], countriesRdd: RDD[String]): Array[((Long, Long), String)] = {
    val geoIdToNetwork : RDD[(String, String)] = getGeoIdToNetwork(networksRdd)
    val geoIdToCountry : RDD[(String, String)]= getGeoIdToCountry(countriesRdd)

    geoIdToNetwork
      .join(geoIdToCountry)
      .map(x => x._2)
      .map(x => (networkToIpRange(x._1), x._2))
      .sortBy(x => x._1._1)
      .collect()
  }

  def networkToIpRange(network: String) : (Long, Long) = {
    val info = new SubnetUtils(network).getInfo
    (ipToLong(info.getLowAddress), ipToLong(info.getHighAddress))
  }

  def ipToLong(ip: String): Long = {
    ip.split("\\.")
      .map(_.toLong)
      .zip(Array(24, 16, 8, 0))
      .map(a => a._1 << a._2)
      .sum
  }

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/events", "root", "cloudera")
  }
}
