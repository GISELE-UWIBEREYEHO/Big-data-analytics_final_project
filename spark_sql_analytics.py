"""
spark_sql_analytics.py -- Part 2 (Spark SQL): complex analytical queries on the cleaned tables.

Every query is plain Spark SQL over temporary views built from the Parquet tables written by
clean_normalize.py. Results are printed and saved to results/spark_sql_<name>.csv.

Query 6 shows how Spark SQL could join data that LIVES in different systems: the views
`mongo_users`, `mongo_transactions` stand in for MongoDB collections and `hbase_user_sessions`
stands in for the HBase table. With the connectors (mongo-spark-connector, hbase-spark) these
views would be created from the live databases instead of Parquet; the SQL stays identical.

Usage:
    python spark_sql_analytics.py --clean clean --out results
    python spark_sql_analytics.py --only 3 6        # run selected queries
"""

import argparse
import os
import sys

try:
    import findspark
    findspark.init()
except Exception:
    pass

from pyspark.sql import SparkSession

REV = "('completed','shipped','delivered')"

QUERIES = {}

# 1 ---------------------------------------------------------------------------------------
# Top customers. Orders and sessions are aggregated in SEPARATE CTEs before joining:
# joining transactions x sessions directly would multiply rows and inflate every sum.
QUERIES["1_top_customers"] = f"""
WITH txn_agg AS (
    SELECT user_id, COUNT(*) AS orders, ROUND(SUM(total), 2) AS total_spent
    FROM transactions WHERE status IN {REV}
    GROUP BY user_id
),
sess_agg AS (
    SELECT user_id, COUNT(*) AS sessions, ROUND(AVG(duration_seconds), 0) AS avg_session_s
    FROM sessions GROUP BY user_id
)
SELECT u.user_id, u.city, u.state, t.orders, t.total_spent,
       s.sessions, s.avg_session_s,
       ROUND(t.orders / s.sessions * 100, 1) AS orders_per_100_sessions
FROM users u
JOIN txn_agg t  ON u.user_id = t.user_id
JOIN sess_agg s ON u.user_id = s.user_id
ORDER BY t.total_spent DESC
LIMIT 15
"""

# 2 ---------------------------------------------------------------------------------------
# Revenue by device type: transactions -> sessions (only transactions linked to a session).
QUERIES["2_revenue_by_device"] = f"""
SELECT s.device_type,
       COUNT(*)                         AS orders,
       ROUND(SUM(t.total), 2)           AS revenue,
       ROUND(AVG(t.total), 2)           AS avg_order_value,
       ROUND(AVG(t.discount), 2)        AS avg_discount
FROM transactions t
JOIN sessions s ON t.session_id = s.session_id
WHERE t.status IN {REV}
GROUP BY s.device_type
ORDER BY revenue DESC
"""

# 3 ---------------------------------------------------------------------------------------
# Conversion funnel by device. One row per session flags which funnel stages it reached.
QUERIES["3_funnel_by_device"] = """
WITH stages AS (
    SELECT session_id,
           MAX(CASE WHEN page_type = 'product_detail' THEN 1 ELSE 0 END) AS viewed_product,
           MAX(CASE WHEN page_type = 'cart'           THEN 1 ELSE 0 END) AS reached_cart,
           MAX(CASE WHEN page_type = 'checkout'       THEN 1 ELSE 0 END) AS reached_checkout,
           MAX(CASE WHEN page_type = 'confirmation'   THEN 1 ELSE 0 END) AS purchased
    FROM page_views
    GROUP BY session_id
)
SELECT s.device_type,
       COUNT(*)                                  AS sessions,
       SUM(st.viewed_product)                    AS viewed_product,
       SUM(st.reached_cart)                      AS reached_cart,
       SUM(st.reached_checkout)                  AS reached_checkout,
       SUM(st.purchased)                         AS purchased,
       ROUND(100 * SUM(st.reached_cart)     / SUM(st.viewed_product), 1) AS view_to_cart_pct,
       ROUND(100 * SUM(st.reached_checkout) / SUM(st.reached_cart), 1)   AS cart_to_checkout_pct,
       ROUND(100 * SUM(st.purchased)        / SUM(st.reached_checkout), 1) AS checkout_to_purchase_pct
FROM sessions s
JOIN stages st ON s.session_id = st.session_id
GROUP BY s.device_type
ORDER BY sessions DESC
"""

# 4 ---------------------------------------------------------------------------------------
# Window function: top 3 products by revenue inside every category.
QUERIES["4_top_products_per_category"] = f"""
WITH rev AS (
    SELECT p.category_id, c.category_name, ti.product_id, p.name,
           ROUND(SUM(ti.line_total), 2) AS revenue, SUM(ti.quantity) AS units
    FROM transaction_items ti
    JOIN transactions t ON ti.transaction_id = t.transaction_id
    JOIN products p     ON ti.product_id = p.product_id
    JOIN categories c   ON p.category_id = c.category_id
    WHERE t.status IN {REV}
    GROUP BY p.category_id, c.category_name, ti.product_id, p.name
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rnk
    FROM rev
)
SELECT category_name, rnk, product_id, name, revenue, units
FROM ranked WHERE rnk <= 3
ORDER BY category_name, rnk
"""

# 5 ---------------------------------------------------------------------------------------
# Window function LAG: month-over-month revenue growth.
QUERIES["5_monthly_growth"] = f"""
WITH m AS (
    SELECT date_format(txn_date, 'yyyy-MM') AS month,
           COUNT(*) AS orders, ROUND(SUM(total), 2) AS revenue
    FROM transactions WHERE status IN {REV}
    GROUP BY date_format(txn_date, 'yyyy-MM')
)
SELECT month, orders, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev_month_revenue,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY month))
                  / LAG(revenue) OVER (ORDER BY month), 1) AS growth_pct
FROM m ORDER BY month
"""

# 6 ---------------------------------------------------------------------------------------
# Cross-system join (see module docstring): MongoDB users + transactions with HBase sessions.
# Business question: does more browsing engagement go with higher customer value?
QUERIES["6_engagement_vs_value"] = f"""
WITH engagement AS (                       -- from the HBase-style table
    SELECT user_id, COUNT(*) AS sessions, AVG(duration_seconds) AS avg_session_s
    FROM hbase_user_sessions GROUP BY user_id
),
spend AS (                                 -- from the MongoDB-style collections
    SELECT user_id, COUNT(*) AS orders, SUM(total) AS total_spent
    FROM mongo_transactions WHERE status IN {REV} GROUP BY user_id
),
joined AS (
    SELECT u.user_id, COALESCE(e.sessions, 0) AS sessions,
           COALESCE(s.orders, 0) AS orders, COALESCE(s.total_spent, 0) AS total_spent
    FROM mongo_users u
    LEFT JOIN engagement e ON u.user_id = e.user_id
    LEFT JOIN spend s      ON u.user_id = s.user_id
)
SELECT CASE WHEN sessions <= 5  THEN '1_low (<=5 sessions)'
            WHEN sessions <= 10 THEN '2_medium (6-10)'
            WHEN sessions <= 15 THEN '3_high (11-15)'
            ELSE '4_very_high (16+)' END                AS engagement_tier,
       COUNT(*)                                          AS users,
       ROUND(AVG(orders), 2)                             AS avg_orders,
       ROUND(AVG(total_spent), 2)                        AS avg_spend,
       ROUND(100 * AVG(CASE WHEN orders > 0 THEN 1 ELSE 0 END), 1) AS buyer_pct
FROM joined
GROUP BY 1
ORDER BY 1
"""


def main():
    ap = argparse.ArgumentParser(description="Spark SQL analytics")
    ap.add_argument("--clean", default="clean")
    ap.add_argument("--out", default="results")
    ap.add_argument("--only", nargs="*", default=[], help="query numbers to run, e.g. --only 3 6")
    ap.add_argument("--driver-memory", default="4g")
    args, _ = ap.parse_known_args()
    if not os.path.isdir(os.path.join(args.clean, "transactions")):
        sys.exit(f"ERROR: '{args.clean}' has no Parquet tables. Run clean_normalize.py first.")

    spark = (SparkSession.builder.master("local[*]").appName("spark_sql_analytics")
             .config("spark.driver.memory", args.driver_memory)
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    for t in ["users", "products", "categories", "sessions", "page_views",
              "transactions", "transaction_items"]:
        spark.read.parquet(f"{args.clean}/{t}").createOrReplaceTempView(t)
    # stand-ins for tables that live in MongoDB / HBase (replace with connector reads in production)
    spark.table("users").createOrReplaceTempView("mongo_users")
    spark.table("transactions").createOrReplaceTempView("mongo_transactions")
    spark.table("sessions").createOrReplaceTempView("hbase_user_sessions")

    os.makedirs(args.out, exist_ok=True)
    for name, sql in QUERIES.items():
        if args.only and name.split("_")[0] not in args.only:
            continue
        print(f"\n=== {name} ===")
        df = spark.sql(sql)
        df.show(15, truncate=45)
        path = os.path.join(args.out, f"spark_sql_{name}.csv")
        df.toPandas().to_csv(path, index=False)
        print(f"  saved -> {path}")
    spark.stop()


if __name__ == "__main__":
    main()
