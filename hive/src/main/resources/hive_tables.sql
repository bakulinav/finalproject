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

