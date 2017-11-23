package net.abakulin.hadoop.finalproject.udf

import net.abakulin.hadoop.finalproject.hive.IpToGeoIdResolver
import org.scalatest.FunSpec
import org.scalatest._

class IpToGeoIdResolverTest extends FunSpec with Matchers {
  describe("IP Geo resolver") {
    it ("should return valid geo id by IP") {
      val resolver = new IpToGeoIdResolver()

      resolver.evaluate("1.0.0.5") should be(2077456)
      resolver.evaluate("1.0.0.0") should be(2077456)
      resolver.evaluate("1.0.0.255") should be(2077456)
      resolver.evaluate("0.0.0.0") should be(0)
    }
  }
}
