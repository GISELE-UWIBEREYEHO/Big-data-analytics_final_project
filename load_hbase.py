"""
load_hbase.py -- load session data into HBase and build daily product metrics.

Reads the RAW JSON files (no Spark/Parquet needed) and writes:
  user_sessions   : one row per session  (key = user_id#reverse_ts#session_id)
  product_metrics : one row per product per day (views, cart_adds, purchases)

Usage (Anaconda Prompt, from the project folder, HBase container running):
    python load_hbase.py --data data
    python load_hbase.py --data data --max-sessions 20000     # smaller subset
    python load_hbase.py --data data --recreate                # drop + reload tables
"""

import argparse
import glob
import json
import os
import sys
import time
from collections import defaultdict

from hbase_common import (USER_SESSIONS, PRODUCT_METRICS, connect, create_tables,
                          metrics_row_key, session_row_key)


def b(value):
    """Everything in HBase is bytes; store None as empty string."""
    return ("" if value is None else str(value)).encode("utf-8")


def session_to_cells(s):
    """Turn one session document into HBase cells (column family:qualifier -> bytes)."""
    geo = s.get("geo_data") or {}
    dev = s.get("device_profile") or {}
    pvs = s.get("page_views") or []
    cells = {
        b"info:session_id":        b(s["session_id"]),
        b"info:user_id":           b(s["user_id"]),
        b"info:start_time":        b(s["start_time"]),
        b"info:end_time":          b(s.get("end_time")),
        b"info:duration_seconds":  b(s.get("duration_seconds")),
        b"info:device_type":       b((dev.get("type") or "unknown").lower()),
        b"info:os":                b(dev.get("os") or "unknown"),
        b"info:browser":           b(dev.get("browser") or "unknown"),
        b"info:referrer":          b((s.get("referrer") or "unknown").lower()),
        b"info:city":              b(geo.get("city")),
        b"info:state":             b(geo.get("state")),
        b"info:country":           b(geo.get("country")),
        b"info:conversion_status": b((s.get("conversion_status") or "unknown").lower()),
        b"info:n_page_views":      b(len(pvs)),
    }
    # events: sparse wide row. One qualifier per page view (pv_001, pv_002, ...).
    for i, pv in enumerate(pvs, start=1):
        cells[f"events:pv_{i:03d}".encode()] = (
            f"{pv.get('timestamp') or ''}|{pv.get('page_type') or ''}|"
            f"{pv.get('product_id') or ''}|{pv.get('category_id') or ''}|"
            f"{pv.get('view_duration') if pv.get('view_duration') is not None else ''}"
        ).encode("utf-8")
    # cart: one qualifier per product in the cart ("qty|price")
    for pid, line in (s.get("cart_contents") or {}).items():
        if line and (line.get("quantity") or 0) > 0:
            cells[f"cart:{pid}".encode()] = f"{line['quantity']}|{line.get('price')}".encode()
    return cells


def main():
    ap = argparse.ArgumentParser(description="Load sessions into HBase.")
    ap.add_argument("--data", default="data")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--max-sessions", type=int, default=0, help="0 = load everything")
    ap.add_argument("--recreate", action="store_true", help="drop and recreate the tables")
    args, _ = ap.parse_known_args()

    files = sorted(glob.glob(os.path.join(args.data, "sessions_*.json")))
    if not files:
        sys.exit(f"ERROR: no sessions_*.json found in '{args.data}'. Run dataset_generator.py.")

    try:
        conn = connect(args.host, args.port)
        create_tables(conn, recreate=args.recreate)
    except Exception as e:                                   # connection problems are common
        sys.exit(f"ERROR: cannot reach HBase Thrift at {args.host}:{args.port} -> {e}\n"
                 "Check:  docker compose ps   and   docker compose logs hbase --tail 30")

    # daily counters, built while we stream the sessions
    views = defaultdict(int)       # (product_id, date) -> product_detail page views
    cart_adds = defaultdict(int)   # (product_id, date) -> sessions that put it in a cart
    purchases = defaultdict(int)   # (product_id, date) -> units sold

    t0, loaded, skipped = time.time(), 0, 0
    table = conn.table(USER_SESSIONS)
    batch = table.batch(batch_size=args.batch_size)

    for path in files:
        print(f"reading {os.path.basename(path)} ...")
        with open(path, encoding="utf-8") as f:
            sessions = json.load(f)
        for s in sessions:
            if args.max_sessions and loaded >= args.max_sessions:
                break
            if not (s.get("session_id") and s.get("user_id") and s.get("start_time")):
                skipped += 1
                continue
            batch.put(session_row_key(s["user_id"], s["start_time"], s["session_id"]),
                      session_to_cells(s))
            loaded += 1

            day = s["start_time"][:10]
            for pv in s.get("page_views") or []:
                if pv.get("page_type") == "product_detail" and pv.get("product_id"):
                    views[(pv["product_id"], day)] += 1
            for pid, line in (s.get("cart_contents") or {}).items():
                if line and (line.get("quantity") or 0) > 0:
                    cart_adds[(pid, day)] += 1
            if loaded % 10000 == 0:
                print(f"  {loaded:,} sessions loaded ({time.time() - t0:.0f}s)")
        del sessions
        if args.max_sessions and loaded >= args.max_sessions:
            break
    batch.send()
    print(f"user_sessions: {loaded:,} rows loaded, {skipped} skipped, {time.time() - t0:.0f}s")

    # purchases come from the transactions file (units sold per product per day)
    tpath = os.path.join(args.data, "transactions.json")
    if os.path.exists(tpath):
        with open(tpath, encoding="utf-8") as f:
            for t in json.load(f):
                if not t.get("timestamp"):
                    continue
                for it in t.get("items") or []:
                    if it.get("product_id") and (it.get("quantity") or 0) > 0:
                        purchases[(it["product_id"], t["timestamp"][:10])] += it["quantity"]

    # write product_metrics
    mtable = conn.table(PRODUCT_METRICS)
    keys = set(views) | set(cart_adds) | set(purchases)
    with mtable.batch(batch_size=1000) as mb:
        for (pid, day) in keys:
            mb.put(metrics_row_key(pid, day), {
                b"m:views": b(views.get((pid, day), 0)),
                b"m:cart_adds": b(cart_adds.get((pid, day), 0)),
                b"m:purchases": b(purchases.get((pid, day), 0)),
            })
    print(f"product_metrics: {len(keys):,} rows loaded")
    print(f"done in {time.time() - t0:.0f}s")
    conn.close()


if __name__ == "__main__":
    main()
