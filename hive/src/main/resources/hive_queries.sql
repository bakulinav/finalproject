# top 10 categories
INSERT OVERWRITE TABLE top10Categories
  SELECT category, count(category) as cnt
  FROM purchases GROUP BY category
  ORDER BY cnt DESC
  LIMIT 10;

# top 10 products
INSERT OVERWRITE TABLE top10Products
  SELECT category, product, cnt, pos FROM (
    SELECT category, product, cnt, ROW_NUMBER() OVER(PARTITION BY category ORDER BY cnt DESC) as pos
      FROM (
        SELECT category, product, count(product) as cnt
        FROM purchases
        GROUP BY category, product
    ) catprod
  ) top
  WHERE pos <= 10;

set hive.auto.convert.join=false;

# top 10 countries
INSERT OVERWRITE TABLE top10Countries
  SELECT country_name, value FROM (
    SELECT purch2.geo_id, sum(purch2.price) as value
        FROM (
          SELECT ipToGeo(clientIp) as geo_id, price FROM purchases WHERE clientIp != '0.0.0.0'
        ) purch2
        GROUP BY purch2.geo_id
        ORDER BY value DESC
        LIMIT 10
  ) t
  join countries c ON (t.geo_id = c.geoname_id)
;
