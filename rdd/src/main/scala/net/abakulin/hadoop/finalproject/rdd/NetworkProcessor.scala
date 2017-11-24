package net.abakulin.hadoop.finalproject.rdd

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.rdd.RDD

object NetworkProcessor {
  def getNetworkToCountryMap(networksRdd: RDD[String], countriesRdd: RDD[String]): Array[((Long, Long), String)] = {
    val geoIdToNetwork : RDD[(String, String)] = getGeoIdToNetwork(networksRdd)
    val geoIdToCountry : RDD[(String, String)] = getGeoIdToCountry(countriesRdd)

    geoIdToNetwork
      .join(geoIdToCountry)
      .map(x => x._2)
      .map(x => (networkToIpRange(x._1), x._2))
      .sortBy(x => x._1._1)
      .collect()
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
}
