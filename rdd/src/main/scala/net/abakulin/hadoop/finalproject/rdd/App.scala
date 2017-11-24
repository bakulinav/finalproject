package net.abakulin.hadoop.finalproject.rdd

import org.apache.hadoop.io.Text
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

    val purchasePreprocessor = new PurchasePreprocessor
    val purchs = purchasePreprocessor.process(sc.sequenceFile(path, classOf[Text], classOf[Text]))

    mode.toLowerCase match {
      case "top10categories" => {
        val processor = new Top10Categories
        processor.process(purchs)
      }
      case "top10products" => {
        val processor = new Top10Products
        processor.process(purchs)
      }
      case "top10countries" => {
        val networksRdd = sc.textFile("/user/cloudera/Country-Blocks-IPv4.csv")
        val countriesRdd = sc.textFile("/user/cloudera/Country-Locations-en.csv")

        val ipRangeToCountry = NetworkProcessor.getNetworkToCountryMap(networksRdd, countriesRdd)

        val ipRangeToCountryBcst = sc.broadcast(ipRangeToCountry)

        val processor = new Top10Countries
        processor.process(purchs, ipRangeToCountryBcst)
      }
    }
  }
}
