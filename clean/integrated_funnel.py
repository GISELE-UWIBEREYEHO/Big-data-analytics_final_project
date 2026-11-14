"""
integrated_funnel.py -- Part 3: ONE analytical query that spans HBase, MongoDB and Spark.

BUSINESS QUESTION
    Where do shoppers drop out between browsing a product and paying, and does the
    drop-off differ by device and by traffic source (referrer)?

DATA SOURCES (why each system is used)
    HBase   : the browsing events of every session (page views, device, referrer).
              Wide, sparse, time-ordered rows keyed by user+time -> one scan streams them out.
    MongoDB : the orders. Each transaction document carries its session_id and total,
              so it is the system of record for "did the session end in a purchase".
    Spark   : joins the two extracts and computes the funnel and revenue per segment
              in parallel; neither HBase nor MongoDB can join across systems.

PROCESSING STEPS
    1. Scan HBase `user_sessions` (info + events families) -> one funnel record per session.
    2. Query MongoDB `transactions` that are linked to a session -> (session_id, total).
    3. Spark: LEFT JOIN sessions to transactions on session_id.
    4. Spark: aggregate funnel stages, conversion rates and revenue overall / by device / by referrer.
    5. Reconcile: sessions that reached the confirmation page vs sessions with a real order.

Usage (HBase and MongoDB containers running, both already loaded):
    python integrated_funnel.py
    python integrated_funnel.py --max-sessions 20000      # quicker test
"""

import argparse
import os
import sys
import time

try:
    import findspark
    findspark.init()
except Exception:
    pass

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

SESSION_SCHEMA = StructType([
    StructField("session_id", StringType()), StructField("user_id", StringType()),
    StructField("device_type", StringType()), StructField("referrer", StringType()),
    StructField("viewed_product", BooleanType()), StructField("reached_cart", BooleanType()),
    StructField("reached_checkout", BooleanType()), StructField("reached_confirmation", BooleanType()),
    StructField("n_events", IntegerType()),
])
TXN_SCHEMA = StructType([
    StructField("session_id", StringType()), StructField("txn_total", DoubleType()),
    StructField("txn_status", StringType()),
])


def extract_from_hbase(host, port, max_sessions):
    """Step 1 -- full scan of user_sessions, reduced to one funnel record per session."""
    import happybase
    conn = happybase.Connection(host, port, timeout=120000)
    table = conn.table("user_sessions")
    rows, t0 = [], time.time()
    for _key, d in table.scan(columns=[b"info", b"events"], batch_size=2000,
                              limit=max_sessions or None):
        pages = {v.split(b"|")[1].decode() for c, v in d.items() if c.startswith(b"events:")}
        rows.append((
            d.get(b"info:session_id", b"").decode(), d.get(b"info:user_id", b"").decode(),
            d.get(b"info:device_type", b"unknown").decode(),
            d.get(b"info:referrer", b"unknown").decode(),
            "product_detail" in pages, "cart" in pages,
            "checkout" in pages, "confirmation" in pages,
            sum(1 for c in d if c.startswith(b"events:"))))
        if len(rows) % 20000 == 0:
            print(f"  ...{len(rows):,} sessions read from HBase ({time.time() - t0:.0f}s)")
    conn.close()
    print(f"HBase  : {len(rows):,} sessions extracted in {time.time() - t0:.0f}s")
    return rows


def extract_from_mongo(uri):
    """Step 2 -- orders that are linked to a session."""
    from pymongo import MongoClient
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client["auca_ecommerce"]
    cur = db.transactions.find({"session_id": {"$ne": None}},
                               {"session_id": 1, "total": 1, "status": 1})
    rows = [(t["session_id"], float(t.get("total") or 0.0), t.get("status")) for t in cur]
    client.close()
    print(f"MongoDB: {len(rows):,} session-linked transactions extracted")
    return rows


def pct(num, den):
    return F.round(100 * F.col(num) / F.col(den), 1)


def funnel(df, by=None):
    """Aggregate funnel stages (+ conversion rates and revenue) for the given grouping column."""
    g = df.groupBy(by) if by else df.groupBy(F.lit("all_sessions").alias("segment"))
    out = g.agg(
        F.count("*").alias("sessions"),
        F.sum(F.col("viewed_product").cast("int")).alias("viewed_product"),
        F.sum(F.col("reached_cart").cast("int")).alias("reached_cart"),
        F.sum(F.col("reached_checkout").cast("int")).alias("reached_checkout"),
        F.sum(F.col("purchased").cast("int")).alias("purchased"),
        F.round(F.sum("revenue"), 2).alias("revenue"))
    return (out.withColumn("view_to_cart_pct", pct("reached_cart", "viewed_product"))
               .withColumn("cart_to_checkout_pct", pct("reached_checkout", "reached_cart"))
               .withColumn("checkout_to_purchase_pct", pct("purchased", "reached_checkout"))
               .withColumn("session_conversion_pct", pct("purchased", "sessions"))
               .withColumn("revenue_per_session", F.round(F.col("revenue") / F.col("sessions"), 2)))


def main():
    ap = argparse.ArgumentParser(description="Integrated funnel analysis (HBase + MongoDB + Spark)")
    ap.add_argument("--hbase-host", default="localhost")
    ap.add_argument("--hbase-port", type=int, default=9090)
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--out", default="results")
    ap.add_argument("--max-sessions", type=int, default=0, help="0 = all sessions in HBase")
    ap.add_argument("--driver-memory", default="4g")
    args, _ = ap.parse_known_args()

    try:
        sess_rows = extract_from_hbase(args.hbase_host, args.hbase_port, args.max_sessions)
        txn_rows = extract_from_mongo(args.mongo_uri)
    except Exception as e:
        sys.exit(f"ERROR while reading the source systems: {e}\n"
                 "Check: docker compose ps  (both containers Up), and that load_hbase.py and "
                 "load_mongo.py have been run.")
    if not sess_rows:
        sys.exit("HBase returned no sessions. Run: python load_hbase.py --data data")

    spark = (SparkSession.builder.master("local[*]").appName("integrated_funnel")
             .config("spark.driver.memory", args.driver_memory)
             .config("spark.sql.shuffle.partitions", "16").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    sessions = spark.createDataFrame(sess_rows, SESSION_SCHEMA)       # from HBase
    txns = spark.createDataFrame(txn_rows, TXN_SCHEMA)                # from MongoDB

    # Step 3 -- cross-system join. A session has at most one transaction (dataset rule).
    joined = (sessions.join(txns, "session_id", "left")
              .withColumn("purchased", F.col("txn_total").isNotNull())
              .withColumn("revenue", F.coalesce(F.col("txn_total"), F.lit(0.0)))).cache()

    # Step 4 -- funnel overall / by device / by referrer
    os.makedirs(args.out, exist_ok=True)
    for name, by in [("overall", None), ("by_device", "device_type"), ("by_referrer", "referrer")]:
        res = funnel(joined, by)
        res = res.orderBy("sessions", ascending=False) if by else res
        print(f"\n=== integrated funnel: {name} ===")
        res.show(truncate=False)
        path = os.path.join(args.out, f"integrated_funnel_{name}.csv")
        res.toPandas().to_csv(path, index=False)
        print(f"  saved -> {path}")

    # Step 5 -- reconciliation between the two systems
    rec = joined.agg(
        F.sum(((F.col("reached_confirmation")) & (~F.col("purchased"))).cast("int"))
         .alias("confirmation_page_but_no_order"),
        F.sum(((~F.col("reached_confirmation")) & (F.col("purchased"))).cast("int"))
         .alias("order_but_no_confirmation_page"),
        F.sum((F.col("purchased") & ~F.col("reached_checkout")).cast("int"))
         .alias("order_without_checkout_page"))
    print("\n=== reconciliation HBase vs MongoDB ===")
    rec.show(truncate=False)
    rec.toPandas().to_csv(os.path.join(args.out, "integrated_funnel_reconciliation.csv"), index=False)
    spark.stop()


if __name__ == "__main__":
    main()
