"""
load_mongo.py -- load the e-commerce data into MongoDB (database: auca_ecommerce).

Collections created
  categories    : one doc per category with its subcategories EMBEDDED (the hierarchy)
  products      : one doc per product, category name EMBEDDED, price_history EMBEDDED
  users         : one doc per user, geo EMBEDDED + a pre-computed purchase_summary EMBEDDED
  transactions  : one doc per transaction, line items EMBEDDED
  sessions      : (subset) one doc per session, page_views EMBEDDED -- full user journey

Usage (Anaconda Prompt, project folder, MongoDB container running):
    pip install pymongo
    python load_mongo.py --data data
    python load_mongo.py --data data --sessions 50000     # bigger session subset
Re-running is safe: every collection is dropped and rebuilt.
"""

import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime

from pymongo import ASCENDING, DESCENDING, MongoClient

DB_NAME = "auca_ecommerce"
# Transactions that count as real revenue ('processing' orders are not fulfilled yet)
REVENUE_STATUSES = {"completed", "shipped", "delivered"}


def dt(value):
    """ISO string -> datetime (MongoDB stores real dates, so we can do date maths)."""
    return datetime.fromisoformat(value) if value else None


def insert_in_batches(col, docs, size=5000):
    batch, total = [], 0
    for d in docs:
        batch.append(d)
        if len(batch) >= size:
            col.insert_many(batch, ordered=False)
            total += len(batch)
            batch = []
    if batch:
        col.insert_many(batch, ordered=False)
        total += len(batch)
    return total


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser(description="Load data into MongoDB.")
    ap.add_argument("--data", default="data")
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--sessions", type=int, default=20000,
                    help="How many sessions to load (sessions are large; 0 = skip)")
    args, _ = ap.parse_known_args()

    client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception as e:
        sys.exit(f"ERROR: cannot reach MongoDB at {args.uri} -> {e}\n"
                 "Check:  docker compose ps   (auca-mongo must be 'Up')")
    db = client[DB_NAME]
    t0 = time.time()

    # ------------------------------------------------------------ categories
    categories = load_json(os.path.join(args.data, "categories.json"))
    db.categories.drop()
    cat_docs = [{"_id": c["category_id"], "name": c["name"],
                 "subcategories": c.get("subcategories", [])} for c in categories]
    db.categories.insert_many(cat_docs)
    cat_name = {c["_id"]: c["name"] for c in cat_docs}
    print(f"categories   : {len(cat_docs):,}")

    # ------------------------------------------------------------ products
    products = load_json(os.path.join(args.data, "products.json"))
    db.products.drop()

    def product_docs():
        for p in products:
            doc = {
                "_id": p["product_id"],
                "name": p.get("name"),
                # denormalised: analytics never needs a second lookup for the category name
                "category": {"category_id": p.get("category_id"),
                             "name": cat_name.get(p.get("category_id"))},
                "base_price": p.get("base_price"),
                "current_stock": max(p.get("current_stock") or 0, 0),
                "is_active": bool(p.get("is_active")),
                "price_history": [{"price": h["price"], "date": dt(h["date"])}
                                  for h in p.get("price_history") or []],
                "creation_date": dt(p.get("creation_date")),
            }
            if p.get("subcategory_id"):
                doc["subcategory_id"] = p["subcategory_id"]
            yield doc
    print(f"products     : {insert_in_batches(db.products, product_docs()):,}")

    # ------------------------------------------------------------ transactions (+ user summaries)
    transactions = load_json(os.path.join(args.data, "transactions.json"))
    db.transactions.drop()
    summary = {}      # user_id -> purchase summary, embedded into the user document below

    def txn_docs():
        for t in transactions:
            ts = dt(t.get("timestamp"))
            yield {
                "_id": t["transaction_id"],
                "session_id": t.get("session_id"),            # may be null (no linked session)
                "user_id": t["user_id"],
                "timestamp": ts,
                "items": t.get("items") or [],
                "subtotal": t.get("subtotal"),
                "discount": t.get("discount") or 0.0,
                "total": t.get("total"),
                "payment_method": (t.get("payment_method") or "unknown").lower(),
                "status": (t.get("status") or "unknown").lower(),
            }
            if (t.get("status") or "").lower() in REVENUE_STATUSES:
                s = summary.setdefault(t["user_id"], {"orders": 0, "items_bought": 0,
                                                      "total_spent": 0.0,
                                                      "first_purchase": ts, "last_purchase": ts})
                s["orders"] += 1
                s["items_bought"] += sum(i.get("quantity", 0) for i in t.get("items") or [])
                s["total_spent"] += t.get("total") or 0.0
                s["first_purchase"] = min(s["first_purchase"], ts)
                s["last_purchase"] = max(s["last_purchase"], ts)
    print(f"transactions : {insert_in_batches(db.transactions, txn_docs()):,}")

    # ------------------------------------------------------------ users
    users = load_json(os.path.join(args.data, "users.json"))
    db.users.drop()
    empty = {"orders": 0, "items_bought": 0, "total_spent": 0.0,
             "first_purchase": None, "last_purchase": None}

    def user_docs():
        for u in users:
            ps = dict(summary.get(u["user_id"], empty))
            ps["total_spent"] = round(ps["total_spent"], 2)
            yield {"_id": u["user_id"], "geo_data": u.get("geo_data"),
                   "registration_date": dt(u.get("registration_date")),
                   "last_active": dt(u.get("last_active")),
                   "purchase_summary": ps}
    print(f"users        : {insert_in_batches(db.users, user_docs()):,}")

    # ------------------------------------------------------------ sessions (subset)
    db.sessions.drop()
    if args.sessions > 0:
        loaded = 0
        for path in sorted(glob.glob(os.path.join(args.data, "sessions_*.json"))):
            if loaded >= args.sessions:
                break
            sess = load_json(path)

            def session_docs():
                for s in sess:
                    yield {
                        "_id": s["session_id"], "user_id": s["user_id"],
                        "start_time": dt(s["start_time"]), "end_time": dt(s.get("end_time")),
                        "duration_seconds": s.get("duration_seconds"),
                        "geo_data": s.get("geo_data"),
                        "device_profile": s.get("device_profile"),
                        "viewed_products": s.get("viewed_products") or [],
                        "page_views": [{**pv, "timestamp": dt(pv.get("timestamp"))}
                                       for pv in s.get("page_views") or []],
                        # dict keyed by product_id -> array of {product_id, quantity, price}
                        # (dynamic keys cannot be indexed or aggregated well in MongoDB)
                        "cart": [{"product_id": k, **v}
                                 for k, v in (s.get("cart_contents") or {}).items()],
                        "conversion_status": (s.get("conversion_status") or "unknown").lower(),
                        "referrer": (s.get("referrer") or "unknown").lower(),
                    }
            take = args.sessions - loaded
            if len(sess) > take:
                sess = sess[:take]
            loaded += insert_in_batches(db.sessions, session_docs())
            del sess
        print(f"sessions     : {loaded:,}")

    # ------------------------------------------------------------ indexes (match the query patterns)
    db.products.create_index([("category.category_id", ASCENDING)])
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    db.users.create_index([("purchase_summary.orders", DESCENDING)])
    db.users.create_index([("geo_data.state", ASCENDING)])
    db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
    db.sessions.create_index([("referrer", ASCENDING)])
    print(f"indexes created. done in {time.time() - t0:.0f}s")
    client.close()


if __name__ == "__main__":
    main()
