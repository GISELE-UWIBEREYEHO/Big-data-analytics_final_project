"""
batch_analysis.py -- Part 2 (Spark batch job 2): product recommendation indicators + cohort analysis.

Reads the Parquet tables written by clean_normalize.py and computes:

  copurchase : "users who bought X also bought Y"   (transaction_items, basket = transaction)
  coview     : "products frequently viewed together" (page_views,        basket = session)
  cohort     : users grouped by registration month, spend over the following months

Association metrics for a product pair (A, B), with N = total baskets:
  support      = baskets containing both / N
  confidence   = baskets containing both / baskets containing A     (P(B | A))
  lift         = confidence / (baskets containing B / N)             (> 1 => positively associated)

Usage:
    python batch_analysis.py --clean clean --out results
    python batch_analysis.py --clean clean --out results --task cohort
    python batch_analysis.py --min-pair 3 --top 30
"""

import argparse
import os
import sys

try:
    import findspark
    findspark.init()
except Exception:
    pass

from pyspark.sql import SparkSession, Window, functions as F

REVENUE_STATUSES = ["completed", "shipped", "delivered"]


def spark_session(driver_memory):
    spark = (SparkSession.builder.master("local[*]").appName("batch_analysis")
             .config("spark.driver.memory", driver_memory)
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_csv(df, out_dir, name):
    """Small result tables -> one CSV file via pandas (avoids Hadoop file-system quirks on Windows)."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"spark_{name}.csv")
    df.toPandas().to_csv(path, index=False)
    print(f"  saved -> {path}")


def pair_stats(items, basket, item, min_pair):
    """Market-basket pair statistics from a (basket, item) DataFrame."""
    items = items.select(basket, item).distinct()          # one row per (basket, item)
    n_baskets = items.select(basket).distinct().count()

    # how many baskets contain each item (needs ALL baskets, including single-item ones)
    item_cnt = items.groupBy(item).agg(F.count("*").alias("baskets"))

    # Optimisation: baskets with a single item can never form a pair, so drop them BEFORE the
    # self-join. This shrinks the shuffle a lot; the item counts above are unaffected.
    multi = (items.groupBy(basket).agg(F.count("*").alias("n"))
             .filter("n >= 2").select(basket))
    multi_items = items.join(multi, basket).cache()

    a, b = multi_items.alias("a"), multi_items.alias("b")
    pairs = (a.join(b, (F.col(f"a.{basket}") == F.col(f"b.{basket}")) &
                       (F.col(f"a.{item}") < F.col(f"b.{item}")))   # < keeps each pair once
             .groupBy(F.col(f"a.{item}").alias("item_a"), F.col(f"b.{item}").alias("item_b"))
             .agg(F.count("*").alias("pair_baskets"))
             .filter(F.col("pair_baskets") >= min_pair))

    ca = item_cnt.select(F.col(item).alias("item_a"), F.col("baskets").alias("baskets_a"))
    cb = item_cnt.select(F.col(item).alias("item_b"), F.col("baskets").alias("baskets_b"))
    res = (pairs.join(ca, "item_a").join(cb, "item_b")
           .withColumn("support", F.round(F.col("pair_baskets") / F.lit(n_baskets), 6))
           .withColumn("confidence_a_to_b", F.round(F.col("pair_baskets") / F.col("baskets_a"), 4))
           .withColumn("confidence_b_to_a", F.round(F.col("pair_baskets") / F.col("baskets_b"), 4))
           .withColumn("lift", F.round(F.col("pair_baskets") * F.lit(n_baskets) /
                                       (F.col("baskets_a") * F.col("baskets_b")), 2)))
    return res, n_baskets


def name_pairs(res, products):
    """Attach product names to both sides of each pair."""
    pa = products.select(F.col("product_id").alias("item_a"), F.col("name").alias("name_a"))
    pb = products.select(F.col("product_id").alias("item_b"), F.col("name").alias("name_b"))
    return (res.join(pa, "item_a", "left").join(pb, "item_b", "left")
            .select("item_a", "name_a", "item_b", "name_b", "pair_baskets", "support",
                    "confidence_a_to_b", "confidence_b_to_a", "lift")
            .orderBy(F.desc("pair_baskets"), F.desc("lift")))


def run_pairs(spark, clean, out, products, items, basket, item, name, min_pair, top):
    print(f"\n=== {name} ===")
    res, n = pair_stats(items, basket, item, min_pair)
    named = name_pairs(res, products).cache()
    total = named.count()
    print(f"baskets: {n:,} | product pairs seen together >= {min_pair} times: {total:,}")
    named.limit(top).show(10, truncate=40)
    save_csv(named.limit(1000), out, name)


def run_cohort(spark, clean, out):
    print("\n=== cohort: registration month vs spending ===")
    users = spark.read.parquet(f"{clean}/users")
    txns = (spark.read.parquet(f"{clean}/transactions")
            .filter(F.col("status").isin(REVENUE_STATUSES)))

    cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_users"))

    joined = (txns.join(users.select("user_id", "registration_month", "registration_date"), "user_id")
              .withColumn("month_offset", F.floor(F.months_between(
                  F.trunc("txn_date", "month"), F.trunc(F.to_date("registration_date"), "month")))
                  .cast("int")))
    before = joined.filter("month_offset < 0").count()
    if before:
        print(f"note: {before:,} purchases happened before the registration month "
              f"(synthetic data); excluded from the cohort table")
    joined = joined.filter("month_offset >= 0")

    agg = (joined.groupBy("registration_month", "month_offset")
           .agg(F.countDistinct("user_id").alias("buyers"),
                F.count("*").alias("orders"),
                F.round(F.sum("total"), 2).alias("revenue")))
    res = (agg.join(cohort_size, "registration_month")
           .withColumn("revenue_per_cohort_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
           .withColumn("buyer_rate_pct", F.round(100 * F.col("buyers") / F.col("cohort_users"), 2))
           .orderBy("registration_month", "month_offset"))
    res.show(15, truncate=False)
    save_csv(res, out, "cohort")


def main():
    ap = argparse.ArgumentParser(description="Spark batch analysis")
    ap.add_argument("--clean", default="clean", help="folder with the Parquet tables")
    ap.add_argument("--out", default="results")
    ap.add_argument("--task", default="all", choices=["all", "copurchase", "coview", "cohort"])
    ap.add_argument("--min-pair", type=int, default=2, help="minimum baskets containing the pair")
    ap.add_argument("--top", type=int, default=15)
    ap.add_argument("--driver-memory", default="4g")
    args, _ = ap.parse_known_args()
    if not os.path.isdir(os.path.join(args.clean, "transactions")):
        sys.exit(f"ERROR: '{args.clean}' has no Parquet tables. Run clean_normalize.py first.")

    spark = spark_session(args.driver_memory)
    products = spark.read.parquet(f"{args.clean}/products")

    if args.task in ("all", "copurchase"):
        items = (spark.read.parquet(f"{args.clean}/transaction_items")
                 .select("transaction_id", "product_id"))
        run_pairs(spark, args.clean, args.out, products, items, "transaction_id", "product_id",
                  "copurchase", args.min_pair, args.top)

    if args.task in ("all", "coview"):
        views = (spark.read.parquet(f"{args.clean}/page_views")
                 .filter((F.col("page_type") == "product_detail") & F.col("product_id").isNotNull())
                 .select("session_id", "product_id"))
        run_pairs(spark, args.clean, args.out, products, views, "session_id", "product_id",
                  "coview", args.min_pair, args.top)

    if args.task in ("all", "cohort"):
        run_cohort(spark, args.clean, args.out)

    spark.stop()


if __name__ == "__main__":
    main()
