package net.abakulin.hadoop.finalproject.hive

import java.util
import java.util.Comparator

import org.apache.hadoop.hive.ql.exec.UDF

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class IpToGeoIdResolver extends UDF with Serializable {

  // network,low,high,low_num,high_num,geoname_id
  val dataPath = "/ips.csv"

//  val ipTree: util.TreeMap[(Long, Long), Int] = getIpData
  val ipResolver: Long => Int = getCountryByIp(getIpData)

  def getIpData = {
//    val tree = new util.TreeMap[(Long, Long), Int](new IpTupleComparator)
    var rows = ArrayBuffer[Array[String]]()

    val bufferedSource = Source.fromInputStream(getClass.getResourceAsStream(dataPath))
    for (line <- bufferedSource.getLines().drop(1)) {
      rows += line.split(",").map(_.trim)
    }
    bufferedSource.close()

    rows
      .map(arr => ((arr(3).toLong, arr(4).toLong), arr(5).toInt))
        .toArray

//    tree
  }

  def evaluate(ip: String): Int = {

    val ipNum = ipToNum(ip)
//    val search = ipNum, ipNum)

//    ipTree.get(search)
    ipResolver(ipNum)
  }

  def ipToNum(ip: String): Long = {
    val arr = ip.split("\\.")

    (arr(0).toLong << 24) +
      (arr(1).toLong << 16) +
      (arr(2).toLong << 8) +
      arr(3).toLong
  }

  class IpTupleComparator extends Comparator[(Long, Long)] {
    override def compare(o1: (Long, Long), o2: (Long, Long)): Int = {
      val (leftLow, leftHigh) = (o1._1, o1._2)
      val (rightLow, rightHigh) = (o2._1, o2._2)

      if (leftHigh <= rightLow) return -1
      if (leftLow >= rightHigh) return 1
      if(rightLow <= leftLow && leftLow <= rightHigh && leftHigh <= rightHigh && leftHigh >= rightLow) return 0

      0
    }
  }

  def getCountryByIp(ipRangeToCountry: Array[((Long, Long), Int)])(ip:Long) = {
    def binarySearch(ip: Long, start: Int, end: Int): Int = {
      if (start > end) return 0

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
}
