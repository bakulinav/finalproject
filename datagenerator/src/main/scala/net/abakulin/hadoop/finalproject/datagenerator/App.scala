package net.abakulin.hadoop.finalproject.datagenerator

import java.io.{IOException, PrintStream}
import java.net.{InetAddress, Socket}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import net.abakulin.hadoop.finalproject.common.{Product, Purchase}
import org.apache.commons.net.util.SubnetUtils

import scala.io.Source
import scala.util.Random

/**
 * @author ${user.name}
 */
object App {

  val df: DateTimeFormatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss")

  def main(args : Array[String]) {

    if (args.length < 3) {
      println("Usage: app.jar <eventsNum> <flumeHost> <flumePort>")
      sys.exit(1)
    }

    val num = args(0).toInt
    val flumeHost = args(1)
    val flumePort = args(2).toInt

    println( "Generate data and send it to Flume endpoint" )

    val fromDay = LocalDateTime.of(2017, 10, 1, 0, 0, 0)
    val toDay = LocalDateTime.of(2017, 10, 7, 23, 59, 59)
    val ipRandom = new IpRandom

    try {
      val soc = new Socket(InetAddress.getByName(flumeHost), flumePort)
      val out = new PrintStream(soc.getOutputStream)

      var x = 0
      for (x <- 1 to num) {
        val purch = Purchase(randomProduct(), randomPrice(), randomDateTime(fromDay, toDay), ipRandom.get())

        println(s"$x sending : $purch")

        out.println(purch.toString)
        out.flush()

        if (x % 1000 == 0) {
          Thread.sleep(1000)
        }
      }

      soc.close()
    } catch {
      case ex : IOException => println(s"Fail to connect to Flume on $flumeHost:$flumePort : ${ex.getMessage}")
      case ex : InterruptedException => println("Sleep was interrupted")
    }
  }

  def randomPrice(): Double = {
    val random = new Random(System.nanoTime)
    1000 * random.nextDouble().abs
  }

  def randomDateTime(from: LocalDateTime, to: LocalDateTime): String = {
    val diff = ChronoUnit.SECONDS.between(from, to)
    val random = new Random(System.nanoTime)
    from.plusSeconds(random.nextInt(diff.toInt)).format(df)
  }

  def randomProduct(): Product = {
    val random = new Random(System.nanoTime)
    Product(s"prod_${random.nextInt(20)}", s"cat_${random.nextInt(20)}")
  }

  class IpRandom() {
    val ips : Array[String] = Source.fromInputStream(getClass.getResource("/ipv4.txt").openStream())
      .mkString
      .split("\n")

    def get(): String = {
      val random = new Random(System.nanoTime())
      val network = ips(random.nextInt(ips.length))

      new SubnetUtils(network).getInfo.getLowAddress
    }
  }
}
