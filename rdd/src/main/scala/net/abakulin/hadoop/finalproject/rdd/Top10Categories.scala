package net.abakulin.hadoop.finalproject.rdd

import java.sql.DriverManager

import net.abakulin.hadoop.finalproject.common.Purchase
import org.apache.spark.rdd.RDD

class Top10Categories {
  def process(purchs : RDD[Purchase]) : Unit = {
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/events", "root", "cloudera")
    val stmt = conn.prepareStatement("INSERT INTO top10CategoryRdd(category, cnt) VALUES (?, ?)")

    purchs
      .map(p => (p.product.category, 1))
      .reduceByKey(_+_)
      .takeOrdered(10)(Ordering.by(p => -p._2))
      .foreach(x => {
        stmt.setString(1, x._1)
        stmt.setInt(2, x._2)
        stmt.executeUpdate()
      })

    stmt.close()
    conn.close()
  }
}
