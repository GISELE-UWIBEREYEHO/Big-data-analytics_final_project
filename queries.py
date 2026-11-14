"""
queries.py -- HBase queries for the project (happybase client).

Examples (Anaconda Prompt, from the project folder):
    python queries.py demo                               # picks a real user and shows all queries
    python queries.py user user_000042 --limit 5         # latest 5 sessions of a user
    python queries.py user-range user_000042 2026-08-01 2026-09-01
    python queries.py session user_000042                # latest session with its page views
    python queries.py product prod_00123 2026-08-01 2026-10-31
    python queries.py count user_000042                  # number of sessions of a user

Equivalent HBase shell commands are shown in the comments of each function.
"""

import argparse
import sys
import time
from datetime import datetime, timedelta

from hbase_common import (USER_SESSIONS, PRODUCT_METRICS, connect, reverse_ts,
                          to_epoch_ms, user_prefix, MAX_LONG)


def _decode(row_key, data):
    out = {"row_key": row_key.decode()}
    for col, val in data.items():
        out[col.decode()] = val.decode("utf-8")
    return out


def latest_sessions(conn, user_id, limit=10):
    """Latest N sessions of one user.
    Shell:  scan 'user_sessions', {ROWPREFIXFILTER => 'user_000042#', LIMIT => 5, COLUMNS => ['info']}
    Newest first because of the reverse timestamp in the row key; the scan stops after
    `limit` rows, so it reads only that user's first rows, not the table."""
    t = conn.table(USER_SESSIONS)
    return [_decode(k, d) for k, d in
            t.scan(row_prefix=user_prefix(user_id), columns=[b"info"], limit=limit)]


def sessions_in_range(conn, user_id, start_date, end_date):
    """Sessions of one user between two dates (inclusive start, exclusive end).
    Newer = smaller reverse_ts, so the scan starts at the END date and stops at the START date."""
    start_ms = to_epoch_ms(start_date + "T00:00:00")
    end_ms = to_epoch_ms(end_date + "T00:00:00")
    row_start = user_prefix(user_id) + reverse_ts(end_ms - 1).encode()
    row_stop = user_prefix(user_id) + f"{MAX_LONG - start_ms + 1:019d}".encode()
    t = conn.table(USER_SESSIONS)
    return [_decode(k, d) for k, d in
            t.scan(row_start=row_start, row_stop=row_stop, columns=[b"info"])]


def latest_session_detail(conn, user_id):
    """Latest session of a user WITH its page-view events and cart (all column families)."""
    t = conn.table(USER_SESSIONS)
    for k, d in t.scan(row_prefix=user_prefix(user_id), limit=1):
        return _decode(k, d)
    return None


def count_sessions(conn, user_id):
    """Number of sessions of a user (key-only scan: no cell data is transferred)."""
    t = conn.table(USER_SESSIONS)
    return sum(1 for _ in t.scan(row_prefix=user_prefix(user_id), filter=b"KeyOnlyFilter()"))


def product_metrics(conn, product_id, start_date, end_date):
    """Daily views / cart adds / purchases of a product between two dates (inclusive)."""
    t = conn.table(PRODUCT_METRICS)
    row_start = f"{product_id}#{start_date.replace('-', '')}".encode()
    row_stop = f"{product_id}#{(datetime.fromisoformat(end_date) + timedelta(days=1)).strftime('%Y%m%d')}".encode()
    return [_decode(k, d) for k, d in t.scan(row_start=row_start, row_stop=row_stop)]


def _show(rows, cols):
    if not rows:
        print("  (no rows)")
        return
    for r in rows:
        print("  " + " | ".join(f"{c.split(':')[-1]}={r.get(c, '')}" for c in cols))


SESSION_COLS = ["info:start_time", "info:device_type", "info:referrer",
                "info:conversion_status", "info:n_page_views"]


def main():
    ap = argparse.ArgumentParser(description="Query HBase tables.")
    ap.add_argument("command", choices=["demo", "user", "user-range", "session", "count", "product"])
    ap.add_argument("args", nargs="*")
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9090)
    a, _ = ap.parse_known_args()

    conn = connect(a.host, a.port)
    try:
        if a.command == "demo":
            t = conn.table(USER_SESSIONS)
            first = next(t.scan(limit=1, columns=[b"info:user_id"]), None)
            if not first:
                sys.exit("user_sessions is empty. Run: python load_hbase.py --data data")
            uid = first[1][b"info:user_id"].decode()
            print(f"Demo user: {uid}\n")
            t0 = time.time(); rows = latest_sessions(conn, uid, 5)
            print(f"1) Latest 5 sessions ({(time.time() - t0) * 1000:.0f} ms)"); _show(rows, SESSION_COLS)
            t0 = time.time(); n = count_sessions(conn, uid)
            print(f"\n2) Total sessions: {n} ({(time.time() - t0) * 1000:.0f} ms)")
            d = latest_session_detail(conn, uid)
            print("\n3) Latest session detail (page views and cart):")
            for k in sorted(d):
                if k.startswith(("events:", "cart:")):
                    print(f"  {k} = {d[k]}")
        elif a.command == "user":
            _show(latest_sessions(conn, a.args[0], a.limit), SESSION_COLS)
        elif a.command == "user-range":
            _show(sessions_in_range(conn, a.args[0], a.args[1], a.args[2]), SESSION_COLS)
        elif a.command == "session":
            d = latest_session_detail(conn, a.args[0])
            for k in sorted(d or {}):
                print(f"{k} = {d[k]}")
        elif a.command == "count":
            print(count_sessions(conn, a.args[0]))
        elif a.command == "product":
            _show(product_metrics(conn, a.args[0], a.args[1], a.args[2]),
                  ["row_key", "m:views", "m:cart_adds", "m:purchases"])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
