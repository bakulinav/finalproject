package net.abakulin.hadoop.finalproject.hive

import java.util.regex.Pattern

import org.apache.commons.net.util.SubnetUtils
import org.apache.hadoop.hive.ql.exec.UDF

class HighIPFromNetwork extends UDF {
  private val cidrPattern = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})/(\\d{1,3})")

  def evaluate(mask: String): String = {
    val matcher = cidrPattern.matcher(mask)
    if (!matcher.matches()) return ""

    val subnet = new SubnetUtils(mask)
      subnet.setInclusiveHostCount(true)

    subnet.getInfo.getHighAddress
  }
}
