CREATE DATABASE IF NOT EXISTS finalproject;

CREATE EXTERNAL TABLE IF NOT EXISTS finalproject.purchases (
  product STRING
  ,price DOUBLE
  ,datetime STRING
  ,category STRING
  ,clientIp STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS SEQUENCEFILE;

ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-01") LOCATION '/user/cloudera/events/2017/10/01';
ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-02") LOCATION '/user/cloudera/events/2017/10/02';
ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-03") LOCATION '/user/cloudera/events/2017/10/03';
ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-04") LOCATION '/user/cloudera/events/2017/10/04';
ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-05") LOCATION '/user/cloudera/events/2017/10/05';
ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-06") LOCATION '/user/cloudera/events/2017/10/06';
ALTER TABLE finalproject.purchases ADD IF NOT EXISTS PARTITION (dt="2017-10-07") LOCATION '/user/cloudera/events/2017/10/07';

CREATE EXTERNAL TABLE networks(
  network STRING
  ,geoname_id INT
  ,registered_country_geoname_id INT
  ,represented_country_geoname_id INT
  ,is_anonymous_proxy INT
  ,is_satellite_provider INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/cloudera/Country-Blocks-IPv4.csv' INTO TABLE networks;

CREATE EXTERNAL TABLE countries (
  geoname_id INT
  ,locale_code STRING
  ,continent_code STRING
  ,continent_name STRING
  ,country_iso_code STRING
  ,country_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/cloudera/Country-Locations-en.csv' INTO TABLE countries;

ADD JAR 'hdfs:///user/cloudera/udf-1.0.jar';
ADD JAR 'hdfs:///user/cloudera/hive-1.0.jar';

CREATE FUNCTION lowIp as 'net.abakulin.hadoop.finalproject.hive.LowIPFromNetwork' using jar 'hdfs:///user/cloudera/hive-1.0.jar';
CREATE FUNCTION highIp as 'net.abakulin.hadoop.finalproject.hive.HighIPFromNetwork' using jar 'hdfs:///user/cloudera/hive-1.0.jar';
CREATE FUNCTION ipToLong as 'net.abakulin.hadoop.finalproject.hive.IPToLong' using jar 'hdfs:///user/cloudera/hive-1.0.jar';
CREATE FUNCTION ipToGeo as 'net.abakulin.hadoop.finalproject.udf.IpToGeo' using jar 'hdfs:///user/cloudera/udf-1.0.jar';

DROP TABLE ips;

CREATE TABLE ips
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
    STORED AS TEXTFILE
AS
  SELECT x.network, x.low, x.high, x.low_num, x.high_num, x.geo_id as geoname_id FROM (
    SELECT
        network,
        lowIp(network) AS low,
        highIp(network) AS high,
        ipToNum(lowIp(network)) AS low_num,
        ipToNum(highIp(network)) AS high_num,
        COALESCE(geoname_id, registered_country_geoname_id, represented_country_geoname_id) AS geo_id
      FROM networks
    ) x
  WHERE x.geo_id IS NOT NULL SORT BY x.low_num, x.high_num
;

-- TOP tables
DROP TABLE top10Categories;
CREATE TABLE IF NOT EXISTS top10Categories (
  category STRING,
  cnt INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION 'hdfs:///user/cloudera/hive/top10Categories';

DROP TABLE top10Products;
CREATE TABLE IF NOT EXISTS top10Products (
  category STRING,
  product STRING,
  cnt INT,
  pos INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION 'hdfs:///user/cloudera/hive/top10Products';

DROP TABLE top10Countries;
CREATE TABLE IF NOT EXISTS top10Countries (
  country_name STRING,
  value DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION 'hdfs:///user/cloudera/hive/top10countries';