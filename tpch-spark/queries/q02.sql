SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM part p, supplier s, partsupp ps, nation n, region r
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
    SELECT min(ps2.ps_supplycost)
    FROM partsupp ps2, supplier s2, nation n2, region r2
    WHERE p_partkey = ps2.ps_partkey
      AND s2.s_suppkey = ps2.ps_suppkey
      AND s2.s_nationkey = n2.n_nationkey
      AND n2.n_regionkey = r2.r_regionkey
      AND r2.r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;
