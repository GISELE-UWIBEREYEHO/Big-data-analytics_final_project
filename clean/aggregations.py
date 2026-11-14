"""
aggregations.py -- MongoDB aggregation pipelines for the project (database: auca_ecommerce).

Pipelines
  1. revenue_by_category   : $match, $unwind, $lookup, $group, $project   (transactions + products)
  2. top_products          : $unwind, $group, $sort, $limit, $lookup      (transactions + products)
  3. user_segmentation     : $addFields + $switch, $group                 (users, by purchase frequency)
  4. monthly_revenue       : $match, $group by month                      (transactions)
  5. conversion_by_referrer: $group with $cond                            (sessions, nested documents)

Each result is printed and saved to results/mongo_<name>.csv (used later for the charts).

Usage:
    python aggregations.py              # run all pipelines
    python aggregations.py 1 3          # run only pipelines 1 and 3
"""

import csv
import os
import sys
import time

from pymongo import MongoClient

DB_NAME = "auca_ecommerce"
REVENUE_STATUSES = ["completed", "shipped", "delivered"]

# ---------------------------------------------------------------------------------------
# 1. Revenue by category: join each sold line item to its product to find the category.
# ---------------------------------------------------------------------------------------
REVENUE_BY_CATEGORY = [
    {"$match": {"status": {"$in": REVENUE_STATUSES}}},
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id",
                 "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$group": {"_id": "$p.category.category_id",
                "category_name": {"$first": "$p.category.name"},
                "revenue": {"$sum": "$items.subtotal"},
                "units_sold": {"$sum": "$items.quantity"},
                "order_lines": {"$sum": 1}}},
    {"$project": {"_id": 0, "category_id": "$_id", "category_name": 1,
                  "revenue": {"$round": ["$revenue", 2]}, "units_sold": 1, "order_lines": 1,
                  "avg_line_value": {"$round": [{"$divide": ["$revenue", "$order_lines"]}, 2]}}},
    {"$sort": {"revenue": -1}},
]

# ---------------------------------------------------------------------------------------
# 2. Top 10 products by revenue, enriched with product name/category/stock status.
# ---------------------------------------------------------------------------------------
TOP_PRODUCTS = [
    {"$match": {"status": {"$in": REVENUE_STATUSES}}},
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id",
                "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"},
                "orders": {"$sum": 1}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 10},
    {"$lookup": {"from": "products", "localField": "_id",
                 "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$project": {"_id": 0, "product_id": "$_id", "name": "$p.name",
                  "category": "$p.category.name", "is_active": "$p.is_active",
                  "units_sold": 1, "orders": 1, "revenue": {"$round": ["$revenue", 2]}}},
]

# ---------------------------------------------------------------------------------------
# 3. User segmentation by purchase frequency (uses the embedded purchase_summary).
# ---------------------------------------------------------------------------------------
USER_SEGMENTATION = [
    {"$addFields": {"segment": {"$switch": {"branches": [
        {"case": {"$eq": ["$purchase_summary.orders", 0]}, "then": "1_never_purchased"},
        {"case": {"$eq": ["$purchase_summary.orders", 1]}, "then": "2_one_time"},
        {"case": {"$lt": ["$purchase_summary.orders", 4]}, "then": "3_occasional_2_3"},
        {"case": {"$lt": ["$purchase_summary.orders", 7]}, "then": "4_regular_4_6"}],
        "default": "5_loyal_7_plus"}}}},
    {"$group": {"_id": "$segment",
                "users": {"$sum": 1},
                "total_spend": {"$sum": "$purchase_summary.total_spent"},
                "avg_orders": {"$avg": "$purchase_summary.orders"}}},
    {"$project": {"_id": 0, "segment": "$_id", "users": 1,
                  "total_spend": {"$round": ["$total_spend", 2]},
                  "avg_spend_per_user": {"$round": [{"$divide": ["$total_spend", "$users"]}, 2]},
                  "avg_orders": {"$round": ["$avg_orders", 2]}}},
    {"$sort": {"segment": 1}},
]

# ---------------------------------------------------------------------------------------
# 4. Revenue per month (time-series analysis on real Date values).
# ---------------------------------------------------------------------------------------
MONTHLY_REVENUE = [
    {"$match": {"status": {"$in": REVENUE_STATUSES}}},
    {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                "revenue": {"$sum": "$total"},
                "orders": {"$sum": 1},
                "discounts": {"$sum": "$discount"}}},
    {"$project": {"_id": 0, "month": "$_id", "orders": 1,
                  "revenue": {"$round": ["$revenue", 2]},
                  "discounts": {"$round": ["$discounts", 2]},
                  "avg_order_value": {"$round": [{"$divide": ["$revenue", "$orders"]}, 2]}}},
    {"$sort": {"month": 1}},
]

# ---------------------------------------------------------------------------------------
# 5. Session conversion by traffic source (whole journey lives in ONE session document).
# ---------------------------------------------------------------------------------------
CONVERSION_BY_REFERRER = [
    {"$group": {"_id": "$referrer",
                "sessions": {"$sum": 1},
                "converted": {"$sum": {"$cond": [{"$eq": ["$conversion_status", "converted"]}, 1, 0]}},
                "avg_page_views": {"$avg": {"$size": "$page_views"}},
                "avg_duration_s": {"$avg": "$duration_seconds"}}},
    {"$project": {"_id": 0, "referrer": "$_id", "sessions": 1, "converted": 1,
                  "conversion_rate_pct": {"$round": [{"$multiply": [{"$divide": ["$converted", "$sessions"]}, 100]}, 2]},
                  "avg_page_views": {"$round": ["$avg_page_views", 1]},
                  "avg_duration_s": {"$round": ["$avg_duration_s", 0]}}},
    {"$sort": {"conversion_rate_pct": -1}},
]

PIPELINES = {
    1: ("revenue_by_category", "transactions", REVENUE_BY_CATEGORY),
    2: ("top_products", "transactions", TOP_PRODUCTS),
    3: ("user_segmentation", "users", USER_SEGMENTATION),
    4: ("monthly_revenue", "transactions", MONTHLY_REVENUE),
    5: ("conversion_by_referrer", "sessions", CONVERSION_BY_REFERRER),
}


def show(rows, limit=12):
    if not rows:
        print("  (no rows)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows[:limit])) for c in cols}
    print("  " + "  ".join(c.ljust(widths[c]) for c in cols))
    for r in rows[:limit]:
        print("  " + "  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))
    if len(rows) > limit:
        print(f"  ... {len(rows) - limit} more rows (all saved to CSV)")


def save_csv(name, rows):
    os.makedirs("results", exist_ok=True)
    path = os.path.join("results", f"mongo_{name}.csv")
    if rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    return path


def main():
    chosen = [int(a) for a in sys.argv[1:] if a.isdigit()] or list(PIPELINES)
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as e:
        sys.exit(f"ERROR: cannot reach MongoDB -> {e}\nCheck: docker compose ps")
    db = client[DB_NAME]

    for n in chosen:
        name, coll, pipeline = PIPELINES[n]
        t0 = time.time()
        rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
        ms = (time.time() - t0) * 1000
        print(f"\n[{n}] {name}  (collection: {coll}, {len(rows)} rows, {ms:.0f} ms)")
        show(rows)
        print(f"  saved -> {save_csv(name, rows)}")
    client.close()


if __name__ == "__main__":
    main()
