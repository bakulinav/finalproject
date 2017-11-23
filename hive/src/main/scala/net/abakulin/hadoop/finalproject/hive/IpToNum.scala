package net.abakulin.hadoop.finalproject.hive

import org.apache.hadoop.hive.ql.exec.UDF

class IpToNum extends UDF {
  def evaluate(ip: String): Long = {
    val arr = ip.split("\\.")

    (arr(0).toLong << 24) +
      (arr(1).toLong << 16) +
        (arr(2).toLong << 8) +
          arr(3).toLong
  }
}
