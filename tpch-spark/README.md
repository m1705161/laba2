# Лаба 2, часть Spark: полный тест TPC-H (22 запроса) на Spark

Продолжение гайда 3. Там ты гонял на Spark **один** запрос (Q1) и сравнивал его с Trino. Здесь — **весь тест TPC-H целиком**: все 22 запроса на Spark, по данным масштаба `sf5`, в **10 конкурентных потоков**, с замером времени и пропускной способности.

---

## 0. Что это и зачем (прочитай, прежде чем запускать)

### Что такое TPC-H

**TPC-H** — эталонный бенчмарк баз данных. Это модель «оптовой торговли»: клиенты, заказы, поставщики, детали, страны — 8 таблиц (`lineitem`, `orders`, `customer`, `part`, `supplier`, `partsupp`, `nation`, `region`). На этих данных принято сравнивать движки между собой, потому что:

- объём задаётся одной цифрой — **scale factor** (`sf5` = «в 5 раз больше базовой базы»), так что тест можно прогнать на слабом ноутбуке и на кластере;
- **22 стандартных запроса** (`Q1`…`Q22`) покрывают разные аналитические паттерны — агрегации, джойны нескольких таблиц, подзапросы, `EXISTS`/`NOT EXISTS`, `IN`-фильтры. Один движок быстрее на одних запросах, другой — на других.

Масштаб `sf5` — это ~30 млн строк в `lineitem`, ~7,5 млн заказов. Достаточно большой, чтобы разница между движками и режимами стала заметна, но ещё влезает в ноутбук.

### Что именно мы делаем

1. **Берём данные из Trino и сохраняем в Iceberg.** У Trino есть встроенный коннектор `tpch`, который «придумывает» эти 8 таблиц на лету. У Spark такого коннектора **нет** — поэтому данные сначала «материализуем» (записываем) в настоящие Iceberg-таблицы в MinIO, а уже их читает Spark.
2. **Гоняем все 22 запроса на Spark.** Один скрипт запускает 22 запроса.
3. **Запускаем их в 10 потоков.** Это и есть режим `throughput` из настоящего TPC-H: не «как быстро один запрос», а «сколько запросов в секунду система выдерживает под нагрузкой, когда 10 пользователей бьют одновременно». Итого `10 × 22 = 220` исполнений.
4. **Меряем и сравниваем** — время каждого запроса и суммарную пропускную способность.

> **Почему не один поток, а десять.** Одиночный запрос показывает «максимальную скорость». Параллельный — «сколько реально выдержит система». В продакшене важна вторая цифра: это и проверяет TPC-H throughput-тест.

---

## 1. Что должно уже работать (пререквизиты)

Из гайда 1 у тебя должен быть поднят стек **MinIO → Lakekeeper → Trino**. Проверь, что всё на месте:

```bash
docker ps   # должны быть контейнеры: lakehouse-minio, lakekeeper, trino (имена могут отличаться)
curl -s http://localhost:8181/health   # должен вернуть 200
curl -s http://localhost:8080/v1/info  # Trino отвечает JSON
```

Если `docker ps` падает с «daemon is not running» — запусти Docker Desktop и подожди, пока он поднимется.

### Каталоги `bench` и `tpch` (обязательно!)

`materialize_sf5.sql` пишет в каталог `bench` и читает данные из каталога `tpch`. Оба должны существовать в Trino — иначе всё упадёт с `Catalog ... not found`. Если ты проходил гайд 3 (§2–3), они уже есть — пропусти этот блок и проверь командой ниже.

**Проверка:**

```bash
docker exec trino trino --execute "SHOW CATALOGS"
```

Должны быть: `bench`, `tpch` (плюс служебные `system`, `lakekeeper`).

Если их нет — создай (всё из гайда 3):

**а) бакет `bench` в MinIO** (консоль http://localhost:9001, логин `minioadmin`/`minioadmin` → Buckets → Create Bucket → `bench`) **и warehouse `bench` в Lakekeeper**:

```bash
curl -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" \
  -d '{
    "warehouse-name": "bench",
    "project-id": "00000000-0000-0000-0000-000000000000",
    "storage-profile": {
      "type": "s3",
      "bucket": "bench",
      "key-prefix": "",
      "endpoint": "http://host.docker.internal:9000",
      "region": "us-east-1",
      "path-style-access": true,
      "flavor": "s3-compat",
      "sts-enabled": false
    },
    "storage-credential": {
      "type": "s3",
      "credential-type": "access-key",
      "access-key-id": "minioadmin",
      "secret-access-key": "minioadmin"
    }
  }'
```

Ответ `201 Created` со `"status":"active"` — готово (если `overlaps with existing warehouse` — warehouse уже есть, это нормально).

**б) два файла каталогов в папку `catalog/`** (та самая папка, что смонтирована в Trino из гайда 1, обычно `~/lakehouse/catalog`):

```bash
cd ~/lakehouse
mkdir -p catalog

cat > catalog/bench.properties << 'EOF'
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.docker.internal:8181/catalog
iceberg.rest-catalog.warehouse=bench
iceberg.rest-catalog.nested-namespace-enabled=true
iceberg.unique-table-location=true
fs.native-s3.enabled=true
s3.endpoint=http://host.docker.internal:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
EOF

cat > catalog/tpch.properties << 'EOF'
connector.name=tpch
EOF
```

**в) перезапусти Trino** (файлы читаются только при старте):

```bash
docker restart trino
```

Подожди ~30 секунд и снова проверь `SHOW CATALOGS` — должны появиться `bench` и `tpch`.

---

## 2. Что в папке

| Файл | Что делает |
|---|---|
| `materialize_sf5.sql` | Trino: создаёт 8 таблиц `bench.tpch.*` из `tpch.sf5`, переименовывая колонки в каноничные `l_/o_/c_/...` |
| `queries/q01.sql`…`q22.sql` | 22 каноничных запроса TPC-H (Spark SQL, стандартные имена колонок) |
| `tpch_run.py` | PySpark-драйвер: `N` потоков × 22 запроса, замер времени, CSV-результат |
| `spark-defaults.conf` | конфиг Spark: каталог `bench` → Lakekeeper, S3FileIO, FAIR-планировщик |

---

## 3. Шаг 1 — материализовать `sf5` из Trino в Iceberg

Открой терминал **в этой папке** (`tpch-spark/`) и выполни:

```bash
docker exec -i trino trino < materialize_sf5.sql
```

Что произошло: Trino сгенерировал 8 таблиц `tpch.sf5` (~30 млн строк в `lineitem`), записал их в Iceberg-таблицы в бакет `bench` (через Lakekeeper) и по пути переименовал колонки в каноничные имена `l_/o_/c_/...`. Это займёт несколько минут — дождись, пока команда вернётся.

> **Почему переименование.** У коннектора `tpch` колонки называются без префикса (`orderkey`, `extendedprice`), а в оригинальных 22 запросах TPC-H — с префиксом (`l_orderkey`, `l_extendedprice`). Чтобы запросы из `queries/` были 1-в-1 из спецификации, при материализации переименовываем.

**Проверка** (должно быть 8 таблиц и ~30 млн строк):

```bash
docker exec trino trino --execute "SHOW TABLES IN bench.tpch"
docker exec trino trino --execute "SELECT count(*) FROM bench.tpch.lineitem"
```

> **Мало RAM / слабый ноутбук?** Замени `sf5` на `sf1` во всех местах `materialize_sf5.sql` (это ~6 млн строк, считается за секунды). Остальное не меняется.

---

## 4. Шаг 2 — скачать jar'ы Iceberg для Spark (один раз)

Из этой же папки `tpch-spark/`:

```bash
mkdir -p jars
curl -fsSL -o jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
curl -fsSL -o jars/iceberg-aws-bundle-1.7.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
```

Это два jar'а, которые учат Spark читать Iceberg-таблицы и ходить в MinIO.

---

## 5. Шаг 3 — прогнать тест (Spark, 10 потоков)

По-прежнему из папки `tpch-spark/`:

```bash
docker run --rm \
  --add-host host.docker.internal:host-gateway \
  -v "$(pwd)/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf" \
  -v "$(pwd)/jars:/opt/spark/iceberg-jars" \
  -v "$(pwd):/opt/spark/tpch" \
  apache/spark:3.5.3 \
  /opt/spark/bin/spark-submit --master 'local[*]' \
    /opt/spark/tpch/tpch_run.py \
    --streams 10 \
    --queries /opt/spark/tpch/queries \
    --out /opt/spark/tpch/results.csv
```

Первый запуск скачает образ `apache/spark:3.5.3` (~1.5 ГБ) — это нормально. Дальше Spark стартует JVM (~15–30 с) и прогоняет 220 исполнений.

Что делают флаги:

- `--streams 10` — число конкурентных потоков (каждый прогоняет все 22 запроса);
- `--queries ...` — папка с `q01.sql`…`q22.sql`;
- `--out ...` — куда писать CSV (пишется в смонтированную папку, т.е. появится здесь же, в `tpch-spark/results.csv`).

---

## 6. Как читать результат и что сдать

После прогона в папке появится `results.csv` (`stream,query,seconds,status` — 220 строк), а в выводе драйвера — сводка:

```
Исполнений: 220, ok: 220, ошибок: 0
Общее время: 142.37 с, пропускная способность: 1.55 запросов/с
```

Ответь в отчёте на вопросы:

1. **Сколько исполнений, сколько `ok`, сколько `error`, какая пропускная способность?** Если есть `error` — какие запросы и почему (см. §7).
2. **Какой запрос самый медленный, какой самый быстрый?** Посчитай в CSV (среднее/медиану по каждому `query`). Почему одни запросы тяжелее других (какие таблицы джойнят, есть ли подзапросы)?
3. **Базовая точка для сравнения.** Прогони тот же драйвер с `--streams 1` (один поток) и сравни: во сколько раз выросла пропускная способность при 10 потоках? Стал ли каждый отдельный запрос медленнее? Почему 10 потоков не дают ровно `10×` (подсказка: все делят одни ядра CPU)?
4. **Trino vs Spark.** В гайде 3 (§11.4) ты гонял Q1 на Trino. Сравни время Q1 на Spark из `results.csv` — где быстрее и почему (Spark платит «налог на старт» JVM).

> **Зачем всё это.** Ты научился не просто «запустить запрос», а прогнать стандартный бенчмарк в режиме нагрузки и снять пропускную способность — это то, чем меряются движки в реальных сравнениях.

---

## 7. Если что-то пошло не так

| Симптом | Причина | Что сделать |
|---|---|---|
| `docker: daemon is not running` | Docker Desktop выключен | запусти Docker Desktop |
| `Catalog 'bench' does not exist` / `Catalog 'tpch' does not exist` | нет файлов `catalog/*.properties` или Trino не перезапущен | пройди §1, потом `docker restart trino` |
| `Schema tpch.sf5 does not exist` | не создан `catalog/tpch.properties` | §1, пункт «б» |
| `Table bench.tpch.lineitem does not exist` | не прогнан шаг 1 (материализация) | `docker exec -i trino trino < materialize_sf5.sql` |
| `Query exceeded memory limit` на материализации | `sf5` не хватает памяти Trino | начни с `sf1` (см. §3) |
| `No FileSystem for scheme "s3"` | в конфиге нет `io-impl=S3FileIO` | проверь `spark-defaults.conf` |
| `Class not found: ...IcebergSparkSessionExtensions` | jar'ы не на classpath | проверь, что оба jar'а лежат в `jars/` и `spark.jars` совпадает с путём `/opt/spark/iceberg-jars/...` |
| `ClassNotFoundException: org.apache.spark.launcher.Main` | jar'ы смонтированы поверх `/opt/spark/jars` | монтируй в `/opt/spark/iceberg-jars`, как в команде §5 |
| `Cannot connect to host.docker.internal:8181/:9000` | нет флага `--add-host host.docker.internal:host-gateway` | добавь его в `docker run` |
| запросы `q20`/`q21` упали с `error` | ограничение Spark на глубокие коррелированные подзапросы | это известный предел Spark, а не ошибка данных — отметь в отчёте |

---

## 8. Как почистить и поднять заново

После прогона на машине остаются три вещи: Spark-контейнер, материализованные таблицы `bench.tpch.*` в MinIO (это гигабайты) и результаты в папке. Чтобы прогнать лабу заново «с чистого листа», чистим по шагам — **что удалить, а что оставить**.

### Что НЕ трогаем

- Стек **MinIO → Lakekeeper → Trino** (контейнеры `lakehouse-minio`, `lakekeeper`, `trino`) — он общий для всех лаб, останавливать не нужно.
- Файлы в `tpch-spark/`: `queries/`, `tpch_run.py`, `spark-defaults.conf`, `materialize_sf5.sql`, `jars/` — всё переиспользуется при каждом прогоне.
- Каталог `tpch` (генератор) — он «придумывает» данные на лету, там нечего чистить.

### Шаг 1 — Spark-контейнер и образ

Контейнер запускался с `--rm`, поэтому после завершения он **удаляется сам**. Проверь, что ничего не осталось:

```bash
docker ps -a | grep spark    # должно быть пусто
```

Если вдруг остался (запускал без `--rm`) — удали принудительно:

```bash
docker rm -f <имя-контейнера-spark>
```

Образ `apache/spark:3.5.3` (~1.5 ГБ) остаётся в кэше Docker — это нормально, он пригодится в следующий раз. Освободить место, если совсем нужно:

```bash
docker rmi apache/spark:3.5.3    # тогда следующий прогон скачает его заново
```

### Шаг 2 — материализованные таблицы (главное!)

`materialize_sf5.sql` создаёт таблицы через `CREATE TABLE`, поэтому повторный прогон упадёт с `table ... already exists`. Дропни всю схему одной командой — она удалит все 8 таблиц:

```bash
docker exec trino trino --execute "DROP SCHEMA bench.tpch CASCADE"
```

Проверка, что чисто (схемы `tpch` под `bench` больше нет):

```bash
docker exec trino trino --execute "SHOW SCHEMAS IN bench"
```

> **Про файлы в MinIO.** Iceberg при `DROP TABLE` не стирает файлы данных сразу — оставляет их «осиротевшими». Новому прогону они не мешают: каждая таблица пишется в уникальную папку (`iceberg.unique-table-location=true`). Если нужно вернуть гигабайты диска — почисти бакет `bench` в консоли MinIO (http://localhost:9001 → Buckets → `bench` → удалить файлы) либо пересоздай бакет.

### Шаг 3 — результаты и логи в папке

Удали старые CSV и логи, чтобы не спутать с новым прогоном (или переименуй, если сдаёшь их как артефакт):

```bash
cd tpch-spark
rm -f results*.csv run*.log
```

### Готово — поднимаем заново

Полный цикл «с нуля» теперь такой:

1. `docker exec -i trino trino < materialize_sf5.sql` — материализовать данные (§3);
2. `docker run --rm ... apache/spark:3.5.3 ...` — прогнать тест (§5).

---

## Бонус (для рубрики p1 «звёздочка»)

Тот же `tpch_run.py` можно переиспользовать в практической p1: прогнать свой Gold-запрос в 10 потоков → «Параллельный запуск запроса (+1)». Сравнение Trino vs Spark на одном запросе → ещё +1.
