"""
hbase_common.py -- shared helpers for the HBase part of the project.

Table design (see schema_design notes in the report):

  user_sessions   row key = <user_id>#<reverse_ts>#<session_id>
      info   : one cell per session attribute (device, referrer, geo, status, ...)
      events : one cell per page view      (events:pv_001, events:pv_002, ...)
      cart   : one cell per cart line      (cart:<product_id> = "qty|price")

  product_metrics row key = <product_id>#<yyyymmdd>
      m      : views, cart_adds, purchases  (daily counters per product)

Why reverse timestamps?  HBase stores rows sorted by row key (ascending, byte order).
reverse_ts = Long.MAX - epoch_ms, zero-padded to 19 digits, so the NEWEST session of a
user sorts FIRST. "Latest N sessions of user X" is then a short prefix scan that stops
after N rows, with no sorting and no full-table scan.
"""

from datetime import datetime, timezone

import happybase

USER_SESSIONS = "user_sessions"
PRODUCT_METRICS = "product_metrics"
MAX_LONG = 9223372036854775807          # Java Long.MAX_VALUE


def connect(host="localhost", port=9090):
    """Open a Thrift connection to HBase (port 9090 is mapped by docker-compose)."""
    return happybase.Connection(host=host, port=port, timeout=60000)


def to_epoch_ms(iso_str):
    """'2026-10-12T14:37:22.123456' -> epoch milliseconds (naive times treated as UTC)."""
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def reverse_ts(epoch_ms):
    return f"{MAX_LONG - epoch_ms:019d}"


def session_row_key(user_id, start_iso, session_id):
    return f"{user_id}#{reverse_ts(to_epoch_ms(start_iso))}#{session_id}".encode()


def user_prefix(user_id):
    # The '#' delimiter stops 'user_00004' from also matching 'user_000042'.
    return f"{user_id}#".encode()


def metrics_row_key(product_id, date_str):
    """date_str = 'YYYY-MM-DD' -> 'prod_00123#20261012'"""
    return f"{product_id}#{date_str.replace('-', '')}".encode()


def create_tables(conn, recreate=False):
    """Create both tables (idempotent). recreate=True drops them first."""
    existing = {t.decode() for t in conn.tables()}
    specs = {
        USER_SESSIONS: {
            "info":   dict(max_versions=1),
            "events": dict(max_versions=1),
            "cart":   dict(max_versions=1),
        },
        PRODUCT_METRICS: {"m": dict(max_versions=1)},
    }
    for name, families in specs.items():
        if name in existing and recreate:
            conn.disable_table(name)
            conn.delete_table(name)
            existing.discard(name)
        if name not in existing:
            conn.create_table(name, families)
            print(f"created table {name}")
        else:
            print(f"table {name} already exists")
