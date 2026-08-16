#!/usr/bin/env python3
"""Small read-only status view for v5_raw_events.db."""

import json
import os
import sqlite3
import time
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
DB = Path(os.environ.get("MEMECOIN_V5_DB", ROOT / "v5_raw_events.db"))


def age(x):
    if x is None:
        return "—"
    s = max(0.0, time.time() - float(x))
    if s < 60: return f"{s:.1f}s"
    if s < 3600: return f"{s/60:.1f}m"
    return f"{s/3600:.1f}h"


def main():
    if not DB.exists():
        raise SystemExit(f"V5 DB not found yet: {DB}")
    db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    total = db.execute("SELECT COUNT(*) FROM v5_raw_transactions").fetchone()[0]
    latest = db.execute("SELECT MAX(observed_at) FROM v5_raw_transactions").fetchone()[0]
    by_source = db.execute("SELECT source_program,COUNT(*) n FROM v5_raw_transactions GROUP BY source_program ORDER BY n DESC").fetchall()
    by_event = db.execute("SELECT event_hint,COUNT(*) n FROM v5_raw_transactions GROUP BY event_hint ORDER BY n DESC").fetchall()
    tokens = db.execute("SELECT COUNT(DISTINCT token_hint) FROM v5_raw_transactions WHERE token_hint IS NOT NULL").fetchone()[0]
    creates = db.execute("SELECT COUNT(*) FROM v5_raw_transactions WHERE event_hint='CREATE'").fetchone()[0]
    migrations = db.execute("SELECT COUNT(*) FROM v5_raw_transactions WHERE event_hint='MIGRATE'").fetchone()[0]
    state = {}
    for r in db.execute("SELECT key,value,updated_at FROM v5_collector_state"):
        try: value = json.loads(r["value"])
        except Exception: value = r["value"]
        state[r["key"]] = value
    session = db.execute("SELECT * FROM v5_sessions ORDER BY started_at DESC LIMIT 1").fetchone()
    db.close()

    size = 0
    for p in (DB, Path(str(DB)+"-wal"), Path(str(DB)+"-shm")):
        try: size += p.stat().st_size
        except OSError: pass

    print("=" * 100)
    print("MEMECOIN LAB — V5 HELIUS DATA FACTORY STATUS")
    print("=" * 100)
    print(f"DB            : {DB}")
    print(f"CONNECTION    : {state.get('connection','UNKNOWN')}")
    print(f"RAW TX        : {total:,}")
    print(f"TOKEN HINTS   : {tokens:,}")
    print(f"CREATE HINTS  : {creates:,}")
    print(f"MIGRATE HINTS : {migrations:,}")
    print(f"NEWEST EVENT  : {age(latest)} ago")
    print(f"DB SIZE       : {size/1024**2:,.1f} MB")
    print()
    print("BY SOURCE")
    for r in by_source: print(f"  {r['source_program']:<12} {r['n']:>10,}")
    print("BY EVENT HINT")
    for r in by_event: print(f"  {r['event_hint']:<12} {r['n']:>10,}")
    if session:
        print()
        print("CURRENT/LAST SESSION")
        print(f"  received    {session['received']:,}")
        print(f"  inserted    {session['inserted']:,}")
        print(f"  duplicates  {session['duplicates']:,}")
        print(f"  reconnects  {session['reconnects']:,}")
        if session['last_error']:
            print(f"  last error  {session['last_error'][:300]}")
    print()
    print("NOTE: event/token fields are conservative HINTS. Raw compressed transaction payload is the source of truth.")


if __name__ == "__main__":
    main()
