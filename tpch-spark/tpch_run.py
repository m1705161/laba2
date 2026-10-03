#!/usr/bin/env python3
"""
TPC-H: 22 запроса на Spark с N конкурентными потоками (streams).

Один SparkSession делится между всеми потоками. Каждый поток прогоняет все
22 запроса последовательно, со сдвигом на свой номер (по кругу) — так потоки
не стартуют одновременно на одном и том же запросе (как в официальной модели
TPC-H throughput-теста). Итого streams * 22 исполнений.

Замеряется время каждого исполнения; в конце — общее wall-time и пропускная
способность (запросов/сек). Результат пишется в CSV.

Данные: каталог `bench` (Iceberg REST -> Lakekeeper), схема `tpch`, таблицы
с каноничными именами l_/o_/... (см. materialize_sf5.sql).

Запуск (в контейнере apache/spark:3.5.3):
    spark-submit --master 'local[*]' tpch_run.py \
        --streams 10 --queries /path/to/queries --out results.csv
"""
import argparse
import csv
import glob
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession

spark = None  # один общий SparkSession на все потоки


def load_queries(path):
    """Читает q01.sql..q22.sql в dict {имя: sql} (сортировка по имени)."""
    queries = {}
    for f in sorted(glob.glob(os.path.join(path, "*.sql"))):
        name = os.path.splitext(os.path.basename(f))[0]
        with open(f, encoding="utf-8") as fh:
            queries[name] = fh.read().strip()
    return queries


def run_stream(stream_id, queries, results, lock):
    """Один поток: прогоняет все запросы по кругу со сдвигом stream_id."""
    names = list(queries)
    n = len(names)
    order = names[stream_id % n:] + names[:stream_id % n]
    for qname in order:
        sql = queries[qname]
        print(f"[stream {stream_id}] {qname}: старт", flush=True)
        t0 = time.perf_counter()
        try:
            spark.sql(sql).collect()  # collect() — полное выполнение запроса
            status = "ok"
        except Exception as e:
            status = "error: " + type(e).__name__
        dt = time.perf_counter() - t0
        print(f"[stream {stream_id}] {qname}: {dt:.2f} с ({status})", flush=True)
        with lock:
            results.append((stream_id, qname, round(dt, 3), status))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default="queries", help="папка с q01.sql..q22.sql")
    ap.add_argument("--streams", type=int, default=10, help="число конкурентных потоков")
    ap.add_argument("--out", default="results.csv", help="куда писать CSV")
    args = ap.parse_args()

    queries = load_queries(args.queries)
    print(f"Запросов: {len(queries)}, потоков: {args.streams}", flush=True)

    global spark
    spark = SparkSession.builder.appName("tpch-22-spark").getOrCreate()
    # Таблицы лежат в каталоге bench (Iceberg REST), схема tpch.
    spark.catalog.setCurrentCatalog("bench")
    spark.catalog.setCurrentDatabase("tpch")
    print("SparkSession готов (каталог bench.tpch), начинаю прогон...", flush=True)

    results = []
    lock = threading.Lock()
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.streams) as ex:
        futs = [ex.submit(run_stream, i, queries, results, lock)
                for i in range(args.streams)]
        for f in as_completed(futs):
            f.result()
    wall = time.perf_counter() - t0

    total = len(results)
    ok = sum(1 for r in results if r[3] == "ok")
    errs = [r for r in results if r[3] != "ok"]
    print(f"Исполнений: {total}, ok: {ok}, ошибок: {len(errs)}")
    print(f"Общее время: {wall:.2f} с, пропускная способность: {total / wall:.2f} запросов/с")
    for r in errs:
        print(f"  ! {r[1]} (stream {r[0]}): {r[3]}")

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["stream", "query", "seconds", "status"])
        w.writerows(results)
    print(f"Результат: {args.out}")


if __name__ == "__main__":
    main()
