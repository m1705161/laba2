SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem l, part p
WHERE p_partkey = l_partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < (
    SELECT 0.2 * avg(l_quantity)
    FROM lineitem l2
    WHERE l2.l_partkey = p_partkey
  );
