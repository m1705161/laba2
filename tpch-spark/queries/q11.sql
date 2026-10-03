SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
FROM partsupp ps, supplier s, nation n
WHERE ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
  SELECT sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
  FROM partsupp ps2, supplier s2, nation n2
  WHERE ps2.ps_suppkey = s2.s_suppkey
    AND s2.s_nationkey = n2.n_nationkey
    AND n2.n_name = 'GERMANY'
)
ORDER BY value DESC;
