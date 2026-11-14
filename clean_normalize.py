"""
clean_normalize.py  --  Part 2 (Spark batch job 1): clean and normalize the raw dataset.

Reads the raw JSON files produced by dataset_generator.py, cleans them, flattens the
nested structures into analysis-ready tables, and writes them as Parquet.

Input  (--data):  users.json, products.json, categories.json, transactions.json,
                  sessions_*.json
Output (--out):   users, products, categories, subcategories, sessions, page_views,
                  cart_items, transactions, transaction_items   (Parquet folders)
                  + quality_report.json (row counts before/after cleaning)

Run (Anaconda Prompt, from the project folder):
    python clean_normalize.py --data data --out clean
"""

import argparse
import json
import os
import sys

# Make PySpark importable when Spark is unzipped manually (SPARK_HOME) instead of pip-installed.
try:
    import findspark
    findspark.init()
except Exception:
    pass  # pip-installed pyspark needs no help

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType, LongType, MapType,
    StringType, StructField, StructType,
)

# --------------------------------------------------------------------------------------
# Explicit schemas
# Why: (1) faster than inferring, (2) cart_contents is a dict keyed by product_id, and
# schema inference would turn every product id into its own column. A MapType avoids that.
# --------------------------------------------------------------------------------------
GEO = StructType([
    StructField("city", StringType()), StructField("state", StringType()),
    StructField("country", StringType()), StructField("ip_address", StringType()),
])

USERS_SCHEMA = StructType([
    StructField("user_id", StringType()), StructField("geo_data", GEO),
    StructField("registration_date", StringType()), StructField("last_active", StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType()), StructField("name", StringType()),
    StructField("category_id", StringType()), StructField("subcategory_id", StringType()),
    StructField("base_price", DoubleType()), StructField("current_stock", LongType()),
    StructField("is_active", BooleanType()),
    StructField("price_history", ArrayType(StructType([
        StructField("price", DoubleType()), StructField("date", StringType())]))),
    StructField("creation_date", StringType()),
])

CATEGORIES_SCHEMA = StructType([
    StructField("category_id", StringType()), StructField("name", StringType()),
    StructField("subcategories", ArrayType(StructType([
        StructField("subcategory_id", StringType()), StructField("name", StringType()),
        StructField("profit_margin", DoubleType())]))),
])

PAGE_VIEW = StructType([
    StructField("timestamp", StringType()), StructField("page_type", StringType()),
    StructField("product_id", StringType()), StructField("category_id", StringType()),
    StructField("view_duration", LongType()),
])

SESSIONS_SCHEMA = StructType([
    StructField("session_id", StringType()), StructField("user_id", StringType()),
    StructField("start_time", StringType()), StructField("end_time", StringType()),
    StructField("duration_seconds", LongType()), StructField("geo_data", GEO),
    StructField("device_profile", StructType([
        StructField("type", StringType()), StructField("os", StringType()),
        StructField("browser", StringType())])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(PAGE_VIEW)),
    StructField("cart_contents", MapType(StringType(), StructType([
        StructField("quantity", LongType()), StructField("price", DoubleType())]))),
    StructField("conversion_status", StringType()), StructField("referrer", StringType()),
])

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType()), StructField("session_id", StringType()),
    StructField("user_id", StringType()), StructField("timestamp", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType()), StructField("quantity", LongType()),
        StructField("unit_price", DoubleType()), StructField("subtotal", DoubleType())]))),
    StructField("subtotal", DoubleType()), StructField("discount", DoubleType()),
    StructField("total", DoubleType()), StructField("payment_method", StringType()),
    StructField("status", StringType()),
])


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
def read_json(spark, pattern, schema):
    """Files are JSON arrays (not one object per line), so multiLine is required."""
    return spark.read.schema(schema).option("multiLine", True).json(pattern)


def clean_str(col, lower=False, upper=False, default=None):
    """Trim, turn empty strings into NULL, normalize case, optionally fill a default."""
    c = F.trim(F.col(col)) if isinstance(col, str) else F.trim(col)
    c = F.when(c == "", None).otherwise(c)
    if lower:
        c = F.lower(c)
    if upper:
        c = F.upper(c)
    if default is not None:
        c = F.coalesce(c, F.lit(default))
    return c


def ts(col):
    """ISO-8601 string -> timestamp (NULL if unparsable)."""
    return F.to_timestamp(F.col(col))


def write(df, out_dir, name):
    path = os.path.join(out_dir, name)
    df.write.mode("overwrite").parquet(path)
    print(f"  wrote {name:<18} -> {path}")


# --------------------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Clean and normalize the e-commerce JSON data.")
    ap.add_argument("--data", default="data", help="Folder with the raw JSON files")
    ap.add_argument("--out", default="clean", help="Output folder for Parquet tables")
    ap.add_argument("--driver-memory", default="4g")
    args, _ = ap.parse_known_args()   # tolerant of extra args (e.g. when run from Jupyter)

    if not os.path.isdir(args.data):
        sys.exit(f"ERROR: data folder '{args.data}' not found. Run dataset_generator.py first.")
    os.makedirs(args.out, exist_ok=True)

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        print("NOTE: Windows detected and HADOOP_HOME is not set. If writing Parquet fails "
              "with a winutils/NativeIO error, tell me and we will add winutils (small fix).")

    spark = (SparkSession.builder
             .master("local[*]").appName("clean_normalize")
             .config("spark.driver.memory", args.driver_memory)
             .config("spark.sql.shuffle.partitions", "8")      # small local data: fewer tasks
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    p = lambda f: os.path.join(args.data, f)
    report = {}   # table -> {"raw": n, "clean": n}

    # ----------------------------------------------------------------- categories
    print("Cleaning categories ...")
    cat_raw = read_json(spark, p("categories.json"), CATEGORIES_SCHEMA)
    categories = (cat_raw.select(
        clean_str("category_id").alias("category_id"),
        clean_str("name").alias("category_name"))
        .dropna(subset=["category_id"]).dropDuplicates(["category_id"]))
    subcategories = (cat_raw
        .select(clean_str("category_id").alias("category_id"),
                F.explode_outer("subcategories").alias("s"))
        .select("category_id",
                clean_str(F.col("s.subcategory_id")).alias("subcategory_id"),
                clean_str(F.col("s.name")).alias("subcategory_name"),
                F.col("s.profit_margin").alias("profit_margin"))
        .dropna(subset=["category_id", "subcategory_id"])
        .filter(F.col("profit_margin").between(0, 1))          # margin must be a fraction
        .dropDuplicates(["subcategory_id"]))
    report["categories"] = {"raw": cat_raw.count(), "clean": categories.count()}
    report["subcategories"] = {"raw": cat_raw.select(F.explode("subcategories")).count(),
                               "clean": subcategories.count()}

    # ----------------------------------------------------------------- users
    print("Cleaning users ...")
    users_raw = read_json(spark, p("users.json"), USERS_SCHEMA)
    users = (users_raw.select(
        clean_str("user_id").alias("user_id"),
        clean_str(F.col("geo_data.city")).alias("city"),
        clean_str(F.col("geo_data.state"), upper=True).alias("state"),
        clean_str(F.col("geo_data.country"), upper=True).alias("country"),
        ts("registration_date").alias("registration_date"),
        ts("last_active").alias("last_active"))
        .dropna(subset=["user_id"])
        .dropDuplicates(["user_id"])
        # last_active can never be before registration
        .filter(F.col("last_active").isNull() | F.col("registration_date").isNull()
                | (F.col("last_active") >= F.col("registration_date")))
        .withColumn("registration_month", F.date_format("registration_date", "yyyy-MM")))
    report["users"] = {"raw": users_raw.count(), "clean": users.count()}

    # ----------------------------------------------------------------- products
    print("Cleaning products ...")
    prod_raw = read_json(spark, p("products.json"), PRODUCTS_SCHEMA)
    products = (prod_raw.select(
        clean_str("product_id").alias("product_id"),
        clean_str("name").alias("name"),
        clean_str("category_id").alias("category_id"),
        clean_str("subcategory_id").alias("subcategory_id"),   # NULL: generator omits it
        F.col("base_price"),
        F.col("current_stock"),
        F.col("is_active"),
        ts("creation_date").alias("creation_date"),
        F.size("price_history").alias("n_price_changes"),
        F.element_at("price_history", -1).getField("price").alias("latest_price"))
        .dropna(subset=["product_id"])
        .dropDuplicates(["product_id"])
        .filter(F.col("base_price").isNotNull() & (F.col("base_price") >= 0))
        # negative stock is impossible: clip to 0
        .withColumn("current_stock", F.greatest(F.coalesce(F.col("current_stock"), F.lit(0)),
                                                F.lit(0)))
        .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(False))))
    report["products"] = {"raw": prod_raw.count(), "clean": products.count()}

    # product price history as its own table (one row per price change)
    price_history = (prod_raw
        .select(clean_str("product_id").alias("product_id"),
                F.explode_outer("price_history").alias("h"))
        .select("product_id", F.col("h.price").alias("price"), ts("h.date").alias("changed_at"))
        .dropna(subset=["product_id", "price", "changed_at"]))

    # ----------------------------------------------------------------- sessions
    print("Cleaning sessions ...")
    sess_raw = read_json(spark, p("sessions_*.json"), SESSIONS_SCHEMA).cache()
    sess_base = (sess_raw
        .withColumn("session_id", clean_str("session_id"))
        .withColumn("user_id", clean_str("user_id"))
        .dropna(subset=["session_id", "user_id"])
        .dropDuplicates(["session_id"])
        .withColumn("start_time", ts("start_time"))
        .withColumn("end_time", ts("end_time"))
        .dropna(subset=["start_time"])
        # duration: recompute from timestamps when missing/negative
        .withColumn("duration_seconds", F.when(
            F.col("duration_seconds").isNull() | (F.col("duration_seconds") < 0),
            F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))
            .otherwise(F.col("duration_seconds")))
        .filter(F.col("duration_seconds") >= 0)
        .cache())

    sessions = sess_base.select(
        "session_id", "user_id", "start_time", "end_time", "duration_seconds",
        F.to_date("start_time").alias("session_date"),
        clean_str(F.col("geo_data.city")).alias("city"),
        clean_str(F.col("geo_data.state"), upper=True).alias("state"),
        clean_str(F.col("geo_data.country"), upper=True).alias("country"),
        clean_str(F.col("device_profile.type"), lower=True, default="unknown").alias("device_type"),
        clean_str(F.col("device_profile.os"), default="unknown").alias("os"),
        clean_str(F.col("device_profile.browser"), default="unknown").alias("browser"),
        clean_str("referrer", lower=True, default="unknown").alias("referrer"),
        clean_str("conversion_status", lower=True, default="unknown").alias("conversion_status"),
        F.size("page_views").alias("n_page_views"),
        F.size(F.array_distinct("viewed_products")).alias("n_products_viewed"),
        F.size(F.map_keys("cart_contents")).alias("n_cart_items"))

    # one row per page view, with its position inside the session
    page_views = (sess_base
        .select("session_id", "user_id", F.posexplode("page_views").alias("seq", "pv"))
        .select("session_id", "user_id", (F.col("seq") + 1).alias("seq"),
                ts("pv.timestamp").alias("event_time"),
                clean_str(F.col("pv.page_type"), lower=True).alias("page_type"),
                clean_str(F.col("pv.product_id")).alias("product_id"),
                clean_str(F.col("pv.category_id")).alias("category_id"),
                F.col("pv.view_duration").alias("view_duration"))
        .dropna(subset=["event_time", "page_type"])
        .filter(F.col("view_duration").isNull() | (F.col("view_duration") >= 0))
        .withColumn("event_date", F.to_date("event_time")))

    # cart_contents is a map {product_id: {quantity, price}} -> one row per cart line
    cart_items = (sess_base
        .select("session_id", "user_id", F.explode_outer("cart_contents").alias("product_id", "c"))
        .select("session_id", "user_id", "product_id",
                F.col("c.quantity").alias("quantity"), F.col("c.price").alias("price"))
        .dropna(subset=["product_id", "quantity", "price"])
        .filter((F.col("quantity") > 0) & (F.col("price") >= 0)))

    report["sessions"] = {"raw": sess_raw.count(), "clean": sessions.count()}
    report["page_views"] = {"raw": sess_raw.select(F.explode("page_views")).count(),
                            "clean": page_views.count()}
    report["cart_items"] = {"raw": sess_raw.select(F.explode("cart_contents")).count(),
                            "clean": cart_items.count()}

    # ----------------------------------------------------------------- transactions
    print("Cleaning transactions ...")
    txn_raw = read_json(spark, p("transactions.json"), TRANSACTIONS_SCHEMA)
    txn_base = (txn_raw
        .withColumn("transaction_id", clean_str("transaction_id"))
        .withColumn("user_id", clean_str("user_id"))
        .dropna(subset=["transaction_id", "user_id"])
        .dropDuplicates(["transaction_id"])
        .withColumn("timestamp", ts("timestamp"))
        .dropna(subset=["timestamp"])
        .filter((F.col("total") >= 0) & (F.col("subtotal") >= 0))
        .cache())

    transactions = txn_base.select(
        "transaction_id",
        clean_str("session_id").alias("session_id"),           # NULL = not linked to a session
        "user_id",
        F.col("timestamp").alias("txn_time"),
        F.to_date("timestamp").alias("txn_date"),
        "subtotal",
        F.coalesce(F.col("discount"), F.lit(0.0)).alias("discount"),
        "total",
        clean_str("payment_method", lower=True, default="unknown").alias("payment_method"),
        clean_str("status", lower=True, default="unknown").alias("status"),
        F.size("items").alias("n_items"),
        clean_str("session_id").isNotNull().alias("has_session"))

    transaction_items = (txn_base
        .select("transaction_id", "user_id", F.col("timestamp").alias("txn_time"),
                F.explode("items").alias("i"))
        .select("transaction_id", "user_id", "txn_time",
                clean_str(F.col("i.product_id")).alias("product_id"),
                F.col("i.quantity").alias("quantity"),
                F.col("i.unit_price").alias("unit_price"),
                F.col("i.subtotal").alias("line_total"))
        .dropna(subset=["product_id"])
        .filter((F.col("quantity") > 0) & (F.col("unit_price") >= 0)))

    report["transactions"] = {"raw": txn_raw.count(), "clean": transactions.count()}
    report["transaction_items"] = {"raw": txn_raw.select(F.explode("items")).count(),
                                   "clean": transaction_items.count()}

    # ----------------------------------------------------------------- integrity checks
    # Report (do not delete) rows whose foreign keys do not resolve.
    print("Checking referential integrity ...")
    checks = {
        "sessions_with_unknown_user":
            sessions.join(users, "user_id", "left_anti").count(),
        "transactions_with_unknown_user":
            transactions.join(users, "user_id", "left_anti").count(),
        "transactions_with_session_id_not_found":
            transactions.filter("has_session")
            .join(sessions.select("session_id"), "session_id", "left_anti").count(),
        "transactions_without_session":
            transactions.filter(~F.col("has_session")).count(),
        "txn_items_with_unknown_product":
            transaction_items.join(products.select("product_id"), "product_id", "left_anti").count(),
        "page_views_with_unknown_product":
            page_views.filter(F.col("product_id").isNotNull())
            .join(products.select("product_id"), "product_id", "left_anti").count(),
    }
    report["integrity_checks"] = checks

    # ----------------------------------------------------------------- write
    print(f"Writing Parquet tables to '{args.out}' ...")
    write(users, args.out, "users")
    write(products, args.out, "products")
    write(price_history, args.out, "price_history")
    write(categories, args.out, "categories")
    write(subcategories, args.out, "subcategories")
    write(sessions, args.out, "sessions")
    write(page_views, args.out, "page_views")
    write(cart_items, args.out, "cart_items")
    write(transactions, args.out, "transactions")
    write(transaction_items, args.out, "transaction_items")

    with open(os.path.join(args.out, "quality_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n=== DATA QUALITY REPORT ===")
    print(f"{'table':<20}{'raw':>12}{'clean':>12}{'dropped':>10}")
    for t, v in report.items():
        if t != "integrity_checks":
            print(f"{t:<20}{v['raw']:>12,}{v['clean']:>12,}{v['raw'] - v['clean']:>10,}")
    print("\nIntegrity checks:")
    for k, v in checks.items():
        print(f"  {k:<42}{v:>10,}")

    spark.stop()


if __name__ == "__main__":
    main()
