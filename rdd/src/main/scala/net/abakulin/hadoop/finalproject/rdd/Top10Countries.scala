package net.abakulin.hadoop.finalproject.rdd

import java.sql.DriverManager

import net.abakulin.hadoop.finalproject.common.Purchase
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class Top10Countries {
  def process(purchs: RDD[Purchase], ipRangeToCountryBcst: Broadcast[Array[((Long, Long), String)]]) : Unit = {
    val ipRangeToCountry = ipRangeToCountryBcst.value

    val resolveCountry: Long => String = NetworkProcessor.getCountryByIp(ipRangeToCountry)

    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/events", "root", "cloudera")
    val stmt = conn.prepareStatement("INSERT INTO top10CountryRdd(country, value) VALUES (?, ?)")

    purchs
      .map(p => (NetworkProcessor.ipToLong(p.clientIp), p.price))
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

    stmt.close()
    conn.close()
  }
}
