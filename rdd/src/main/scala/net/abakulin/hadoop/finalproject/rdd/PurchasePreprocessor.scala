package net.abakulin.hadoop.finalproject.rdd

import net.abakulin.hadoop.finalproject.common.{Product, Purchase}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

class PurchasePreprocessor {
  def process(purchs: RDD[(Text, Text)]) = {
    purchs
    .map(a => a._2.toString)
      .map(str => str.split(","))
      .map(arr => Purchase(Product(arr(0), arr(3)), arr(1).toDouble, arr(2), arr(4)))
  }
}
