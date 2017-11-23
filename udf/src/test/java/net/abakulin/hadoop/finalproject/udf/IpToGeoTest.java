package net.abakulin.hadoop.finalproject.udf;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class IpToGeoTest {
    @Test
    public void evaluate() throws Exception {
        IpToGeo ipToGeo = new IpToGeo();

        Assert.assertThat(ipToGeo.evaluate("1.0.0.5"), Is.is(2077456));
        Assert.assertThat(ipToGeo.evaluate("1.0.0.0"), Is.is(2077456));
        Assert.assertThat(ipToGeo.evaluate("1.0.0.255"), Is.is(2077456));
        Assert.assertThat(ipToGeo.evaluate("0.0.0.0"), Is.is(0));
    }
}