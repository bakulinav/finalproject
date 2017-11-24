package net.abakulin.hadoop.finalproject.rdd

import java.sql.DriverManager

import net.abakulin.hadoop.finalproject.common.Purchase
import org.apache.spark.rdd.RDD

class Top10Products {
  def process(purchs : RDD[Purchase]) : Unit = {
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/events", "root", "cloudera")
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
      })

    stmt.close()
    conn.close()
  }
}
