# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics - Final Project.
Author: **Gisele UWIBEREYEHO** | Reg No: **101366** | Adventist University of Central Africa (AUCA), Kigali, Rwanda

Repository: https://github.com/GISELE-UWIBEREYEHO/Big-data-analytics_final_project

A multi-model analytics system for a synthetic e-commerce platform:

| System | Role in this project |
|---|---|
| **MongoDB 7** (document model) | Orders, users with purchase summaries, product catalog, session subset; aggregation pipelines |
| **HBase 2.4.17** (wide-column model) | All sessions keyed by user and reverse time; daily product metrics |
| **Apache Spark 3.5.9** (PySpark) | Cleaning to Parquet, batch analytics, Spark SQL, and the query that joins HBase and MongoDB data |

The full write-up (design decisions, results, limitations) is in the technical report (`report/Technical_Report.pdf`).

## Architecture

```
dataset_generator.py  ->  JSON files (users, products, categories, sessions_*, transactions)
        |
        +--> MongoDB  (load_mongo.py, aggregations.py)
        +--> HBase    (load_hbase.py, queries.py)
        +--> Spark    (clean_normalize.py -> Parquet -> batch_analysis.py, spark_sql_analytics.py)
                          |
        HBase sessions + MongoDB orders --> integrated_funnel.py (Spark join)
                          |
                    make_charts.py --> results/figures/*.png
```

## Repository contents

| File | Part | What it does |
|---|---|---|
| `dataset_generator.py` | Data | Generates the synthetic dataset (configurable size, writes JSON to `--out`) |
| `docker-compose.yml` | Setup | Starts MongoDB and HBase containers |
| `load_mongo.py` | Part 1 | Builds the MongoDB collections and indexes |
| `aggregations.py` | Part 1 | Five MongoDB aggregation pipelines (results to `results/mongo_*.csv`) |
| `create_tables.hbase` | Part 1 | HBase shell commands that create the two tables |
| `hbase_common.py` | Part 1 | Shared helpers: connection, row-key design, table creation |
| `load_hbase.py` | Part 1 | Loads sessions into `user_sessions` and daily counters into `product_metrics` |
| `queries.py` | Part 1 | HBase queries (latest sessions of a user, date range, session detail, counts, product metrics) |
| `clean_normalize.py` | Part 2 | Spark cleaning and flattening to Parquet, with a data-quality report |
| `batch_analysis.py` | Part 2 | Co-purchase, co-view recommendation indicators and cohort analysis |
| `spark_sql_analytics.py` | Part 2 | Six Spark SQL queries (CTEs, window functions, cross-system join) |
| `integrated_funnel.py` | Part 3 | Funnel analysis combining HBase sessions, MongoDB orders and Spark |
| `make_charts.py` | Part 4 | Creates the report charts from the CSV results |
| `results/` | Output | CSV results, `quality_report.json` and `figures/` produced by the scripts |

## Prerequisites

- **Python 3.9+** and `pip install -r requirements.txt`
- **Java 8, 11 or 17** (needed by Spark 3.5)
- **Docker Desktop** (for MongoDB and HBase)
- **Spark 3.5.x**: either the `pyspark` package from `requirements.txt`, or an unzipped Spark download with `SPARK_HOME` pointing at the folder that contains `bin` and `python`. The Spark scripts call `findspark`, which uses `SPARK_HOME` if it is set.

Tested on: Windows, Anaconda, Python 3.11, Java 1.8, Spark 3.5.9 (local mode), Docker Desktop, MongoDB 7, HBase 2.4.17.

## How to reproduce the results

Run every command from the repository folder, in this order.

**1. Start the databases**
```
docker compose up -d
docker compose ps          # auca-mongo and auca-hbase should be "Up"
```
Wait about a minute for HBase, then open http://localhost:16010 (HBase Master page).

**2. Generate the data** (defaults: 10,000 users, 5,000 products, 100,000 sessions, 25,000 transactions, about 200 MB)
```
python dataset_generator.py --out data
```
Options: `--users`, `--products`, `--sessions`, `--transactions`, `--chunk-size`, `--out`.

**3. Spark: clean and normalize**
```
python clean_normalize.py --data data --out clean
```
Prints a data-quality report and writes ten Parquet tables to `clean/`.
Windows: if Python cannot find Spark, run `set SPARK_HOME=C:\spark` first (use your own Spark folder).

**4. MongoDB: load and analyse**
```
python load_mongo.py --data data
python aggregations.py
```

**5. HBase: create tables, load and query**
```
python load_hbase.py --data data
python queries.py demo
```
The tables are created automatically. To create them with the shell instead:
```
docker exec -i auca-hbase /root/hbase-2.4.17/bin/hbase shell < create_tables.hbase
```
(The path `/root/hbase-2.4.17` is specific to the Docker image used in `docker-compose.yml`.)

**6. Spark: batch analysis and Spark SQL**
```
python batch_analysis.py --clean clean --out results
python spark_sql_analytics.py --clean clean --out results
```

**7. Integrated query (HBase + MongoDB + Spark)**
```
python integrated_funnel.py
```
Use `--max-sessions 20000` for a quicker test run.

**8. Charts**
```
python make_charts.py
```
Writes the PNG charts to `results/figures/`.

## Sample queries

HBase shell (`docker exec -it auca-hbase /root/hbase-2.4.17/bin/hbase shell`):
```
scan 'user_sessions', {ROWPREFIXFILTER => 'user_000042#', LIMIT => 5, COLUMNS => ['info']}
scan 'product_metrics', {STARTROW => 'prod_00313#20261001', STOPROW => 'prod_00313#20261101'}
```

HBase from Python (`queries.py`):
```
python queries.py user user_000042 --limit 5
python queries.py user-range user_000042 2026-08-01 2026-09-01
python queries.py session user_000042
python queries.py count user_000042
python queries.py product prod_00313 2026-08-01 2026-10-31
```

MongoDB shell:
```
docker exec -it auca-mongo mongosh auca_ecommerce --eval "db.transactions.countDocuments({})"
docker exec -it auca-mongo mongosh auca_ecommerce --eval "db.users.findOne()"
```

The same pipelines are in `aggregations.py`; each prints a table and saves a CSV in `results/`.

## Design summary

**MongoDB** - embed what is read together: subcategories inside categories, category name and price history inside products, line items inside transactions, a pre-computed `purchase_summary` inside each user, page views inside session documents (the cart dictionary is stored as an array).

**HBase** - `user_sessions` row key `user_id#reverse_timestamp#session_id` (newest session first, so "latest N sessions of a user" is a short prefix scan); column families `info`, `events` (one cell per page view) and `cart`. `product_metrics` row key `product_id#yyyymmdd`, family `m`.

**Spark** - explicit schemas (the cart is a map keyed by product id), cleaning rules, flattened Parquet tables, a pair self-join that first removes single-item baskets, and window functions in Spark SQL.

## Key results (from the committed CSV files)

- Funnel (100,000 sessions): 98,750 viewed a product, 67,759 reached the cart, 43,643 reached checkout, 12,564 have an order (12.6% of sessions).
- Net revenue: $7.33M (Aug), $7.59M (Sep), $7.93M (Oct); +3.5% and +4.4% month over month.
- Regular and loyal buyers are 18.0% of users and 37.3% of revenue.
- 12,436 transactions (49.7%) have no session_id; device and traffic-source conversion differ by less than one percentage point.

## Troubleshooting

| Problem | Fix |
|---|---|
| `No module named pyspark` | Set `SPARK_HOME` to the folder containing `bin` and `python`, or `pip install pyspark==3.5.9`. Check with `python -c "import findspark; findspark.init(); import pyspark; print(pyspark.__version__)"` |
| `Unable to find py4j ... SPARK_HOME may not be configured correctly` | `SPARK_HOME` points at the wrong folder level; it must directly contain `python\lib\py4j-*.zip` |
| HBase connection refused or timed out | `docker compose ps`; wait a minute after starting; `docker compose restart hbase`. `integrated_funnel.py` retries and resumes interrupted scans automatically |
| `docker pull` fails midway (slow connection) | Run `docker compose up -d` again; finished image layers are kept |
| Spark writes fail on Windows (`winutils` / `NativeIO`) | Set `HADOOP_HOME` to a folder containing `bin\winutils.exe` for your Hadoop version |

## Limitations

The data is synthetic and uniform (devices, channels and cohorts show almost no differences); half of the orders carry no session link, so session-based conversion is a lower bound; HBase runs from a community Docker image (version 2.4.17) because no official image exists; everything runs on a single machine, so scalability is discussed in the report rather than measured.
