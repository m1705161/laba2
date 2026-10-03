SELECT c_count, count(*) AS custdist
FROM (
  SELECT c_custkey, count(o_orderkey) AS c_count
  FROM customer c LEFT OUTER JOIN orders o ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
  GROUP BY c_custkey
) c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
