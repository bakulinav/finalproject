# top 10 categories
SELECT category, count(category) as cnt
FROM purchases GROUP BY category
ORDER BY cnt DESC;

SELECT category, product, cnt, pos FROM (
  SELECT category, product, cnt, ROW_NUMBER() OVER(PARTITION BY category ORDER BY cnt DESC) as pos
    FROM (
      SELECT category, product, count(product) as cnt
      FROM purchases
      GROUP BY category, product
  ) catprod
) top
WHERE pos <= 10

# top 10 countries
# TODO: implement UDF to resolve country by IP
SELECT clientIp, sum(price) as value
FROM purchases
GROUP BY clientIp
ORDER BY value DESC
LIMIT 10;
