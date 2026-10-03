-- Материализация 8 таблиц TPC-H (sf5) в Iceberg через Trino.
--
-- Зачем: коннектор `tpch` генерит данные на лету (in-memory), а у Spark
-- такого коннектора нет. Поэтому сначала «сохраняем» данные из Trino
-- в настоящие Iceberg-таблицы (в MinIO через Lakekeeper), а уже по ним
-- гоняем оригинальные 22 запроса TPC-H на Spark.
--
-- Колонки переименовываются в каноничные имена l_/o_/c_/p_/s_/ps_/n_/r_,
-- чтобы 22 запроса остались «оригинальными» (как в спецификации TPC-H).
-- У коннектора `tpch` имена БЕЗ префикса: orderkey -> l_orderkey и т.д.
--
-- Скрипт можно запускать повторно: каждая таблица сначала дропается
-- (DROP TABLE IF EXISTS), поэтому ошибки "already exists" не будет.
--
-- Каталог: bench (Iceberg REST -> Lakekeeper), схема: tpch.
-- Масштаб: sf5 (lineitem ~ 30 млн строк, orders ~ 7.5 млн).
--
-- Запуск (Trino уже поднят):
--   trino --execute "$(cat materialize_sf5.sql)"
-- или в контейнере:
--   docker exec -i trino trino < materialize_sf5.sql

CREATE SCHEMA IF NOT EXISTS bench.tpch;

DROP TABLE IF EXISTS bench.tpch.lineitem;
CREATE TABLE bench.tpch.lineitem AS
SELECT orderkey AS l_orderkey, partkey AS l_partkey, suppkey AS l_suppkey,
       linenumber AS l_linenumber, quantity AS l_quantity,
       extendedprice AS l_extendedprice, discount AS l_discount, tax AS l_tax,
       returnflag AS l_returnflag, linestatus AS l_linestatus,
       shipdate AS l_shipdate, commitdate AS l_commitdate,
       receiptdate AS l_receiptdate, shipinstruct AS l_shipinstruct,
       shipmode AS l_shipmode, comment AS l_comment
FROM tpch.sf5.lineitem;

DROP TABLE IF EXISTS bench.tpch.orders;
CREATE TABLE bench.tpch.orders AS
SELECT orderkey AS o_orderkey, custkey AS o_custkey,
       orderstatus AS o_orderstatus, totalprice AS o_totalprice,
       orderdate AS o_orderdate, orderpriority AS o_orderpriority,
       clerk AS o_clerk, shippriority AS o_shippriority, comment AS o_comment
FROM tpch.sf5.orders;

DROP TABLE IF EXISTS bench.tpch.customer;
CREATE TABLE bench.tpch.customer AS
SELECT custkey AS c_custkey, name AS c_name, address AS c_address,
       nationkey AS c_nationkey, phone AS c_phone, acctbal AS c_acctbal,
       mktsegment AS c_mktsegment, comment AS c_comment
FROM tpch.sf5.customer;

DROP TABLE IF EXISTS bench.tpch.part;
CREATE TABLE bench.tpch.part AS
SELECT partkey AS p_partkey, name AS p_name, mfgr AS p_mfgr,
       brand AS p_brand, type AS p_type, size AS p_size,
       container AS p_container, retailprice AS p_retailprice, comment AS p_comment
FROM tpch.sf5.part;

DROP TABLE IF EXISTS bench.tpch.supplier;
CREATE TABLE bench.tpch.supplier AS
SELECT suppkey AS s_suppkey, name AS s_name, address AS s_address,
       nationkey AS s_nationkey, phone AS s_phone, acctbal AS s_acctbal,
       comment AS s_comment
FROM tpch.sf5.supplier;

DROP TABLE IF EXISTS bench.tpch.partsupp;
CREATE TABLE bench.tpch.partsupp AS
SELECT partkey AS ps_partkey, suppkey AS ps_suppkey,
       availqty AS ps_availqty, supplycost AS ps_supplycost, comment AS ps_comment
FROM tpch.sf5.partsupp;

DROP TABLE IF EXISTS bench.tpch.nation;
CREATE TABLE bench.tpch.nation AS
SELECT nationkey AS n_nationkey, name AS n_name,
       regionkey AS n_regionkey, comment AS n_comment
FROM tpch.sf5.nation;

DROP TABLE IF EXISTS bench.tpch.region;
CREATE TABLE bench.tpch.region AS
SELECT regionkey AS r_regionkey, name AS r_name, comment AS r_comment
FROM tpch.sf5.region;

-- Проверка после материализации:
--   SHOW TABLES IN bench.tpch;
--   SELECT count(*) FROM bench.tpch.lineitem;  -- ~ 30 000 000
