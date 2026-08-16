#!/usr/bin/env python3

from pathlib import Path
import py_compile

ROOT = Path.home() / "memecoin_lab"

CODE = r'''
#!/usr/bin/env python3

import os
import signal
import sqlite3
import time
from pathlib import Path


ROOT = Path.home() / "memecoin_lab"

MARKET_DB = ROOT / "validation_v090.db"
RESEARCH_DB = ROOT / "research_v4.db"

REFRESH = 5

shutdown_requested = False


def stop(sig, frame):
    global shutdown_requested
    shutdown_requested = True


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def open_research():

    db = sqlite3.connect(
        RESEARCH_DB,
        timeout=30,
    )

    db.row_factory = sqlite3.Row

    db.execute(
        "PRAGMA journal_mode=WAL"
    )

    db.execute(
        "PRAGMA synchronous=NORMAL"
    )

    db.execute(
        "PRAGMA busy_timeout=30000"
    )

    return db


def open_market():

    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=30,
    )

    db.row_factory = sqlite3.Row

    db.execute(
        "PRAGMA busy_timeout=30000"
    )

    return db


def initialize():

    db = open_research()

    db.executescript("""
    CREATE TABLE IF NOT EXISTS v4_meta (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v4_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL NOT NULL,
        component TEXT NOT NULL,
        event_type TEXT NOT NULL,
        detail TEXT
    );

    CREATE TABLE IF NOT EXISTS v4_hypotheses (
        hypothesis_id TEXT PRIMARY KEY,
        branch TEXT NOT NULL,
        scientific_fingerprint TEXT NOT NULL UNIQUE,
        spec_json TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v4_experiments (
        experiment_id TEXT PRIMARY KEY,
        hypothesis_id TEXT NOT NULL,
        stage TEXT NOT NULL,
        status TEXT NOT NULL,
        result_json TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v4_memory (
        memory_id TEXT PRIMARY KEY,
        branch TEXT,
        verdict TEXT NOT NULL,
        lesson TEXT NOT NULL,
        evidence_json TEXT,
        created_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v4_runtime (
        component TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        detail TEXT,
        updated_at REAL NOT NULL
    );
    """)

    now = time.time()

    db.execute("""
    INSERT INTO v4_meta (
        key,
        value,
        updated_at
    )
    VALUES (
        'architecture',
        'V4_SINGLE_ORCHESTRATOR_NO_SOCKET',
        ?
    )
    ON CONFLICT(key)
    DO UPDATE SET
        value=excluded.value,
        updated_at=excluded.updated_at
    """, (now,))

    db.commit()
    db.close()


def table_exists(db, name):

    return db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE type='table'
      AND name=?
    """, (name,)).fetchone() is not None


def safe_count(db, table):

    if not table_exists(db, table):
        return None

    try:

        return db.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

    except Exception:
        return None


def safe_newest(db, table, candidates):

    if not table_exists(db, table):
        return None

    columns = {
        row[1]
        for row in db.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()
    }

    for column in candidates:

        if column not in columns:
            continue

        try:

            value = db.execute(
                f"SELECT MAX({column}) FROM {table}"
            ).fetchone()[0]

            if value is not None:
                return float(value)

        except Exception:
            pass

    return None


def market_health():

    result = {}

    try:
        db = open_market()

    except Exception as exc:

        return {
            "error": repr(exc)
        }

    now = time.time()

    checks = {
        "t101_migrations": [
            "block_time",
            "detected_at",
            "migration_time",
        ],

        "t116_pump_swaps": [
            "timestamp",
            "created_at",
        ],

        "t116_pump_events": [
            "timestamp",
            "event_time",
            "created_at",
        ],

        "t116_premigration_dump_events": [
            "timestamp",
            "event_time",
            "created_at",
        ],
    }

    for table, candidates in checks.items():

        count = safe_count(
            db,
            table
        )

        newest = safe_newest(
            db,
            table,
            candidates
        )

        age = (
            now - newest
            if newest is not None
            else None
        )

        result[table] = {
            "count": count,
            "age": age,
        }

    db.close()

    return result


def research_health():

    db = open_research()

    result = {}

    for table in (
        "v4_hypotheses",
        "v4_experiments",
        "v4_memory",
        "v4_events",
    ):

        result[table] = db.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

    db.close()

    return result


def age_text(age):

    if age is None:
        return "NA"

    return f"{age:.0f}s"


def display():

    market = market_health()
    research = research_health()

    os.system("clear")

    print("=" * 120)
    print("MEMECOIN LAB — AUTONOMOUS RESEARCH LAB V4")
    print("=" * 120)

    print(
        time.strftime(
            "LOCAL TIME : %Y-%m-%d %H:%M:%S"
        )
    )

    print()

    print("ARCHITECTURE : SINGLE ORCHESTRATOR")
    print("RESEARCH IPC : NONE")
    print("SOCKET       : NONE")
    print("MARKET DB    : validation_v090.db READ ONLY")
    print("RESEARCH DB  : research_v4.db")
    print("LIVE TRADING : DISABLED")

    print()
    print("=" * 120)
    print("FOUNDATION HEALTH")
    print("=" * 120)

    print("🟢 ORCHESTRATOR       RUNNING")
    print("🟢 RESEARCH DB        ACCESSIBLE")

    if "error" in market:

        print(
            "🔴 MARKET DB          "
            + market["error"]
        )

    else:

        print("🟢 MARKET DB          ACCESSIBLE")

    print()
    print("=" * 120)
    print("HISTORICAL DATA")
    print("=" * 120)

    if "error" not in market:

        for table, info in market.items():

            n = info["count"]

            print(
                f"{table:<35}"
                f" N={str(n):>10}"
                f" | NEWEST AGE={age_text(info['age'])}"
            )

    print()
    print("=" * 120)
    print("V4 RESEARCH STATE")
    print("=" * 120)

    print(
        f"HYPOTHESES  : "
        f"{research['v4_hypotheses']}"
    )

    print(
        f"EXPERIMENTS : "
        f"{research['v4_experiments']}"
    )

    print(
        f"MEMORIES    : "
        f"{research['v4_memory']}"
    )

    print(
        f"EVENTS      : "
        f"{research['v4_events']}"
    )

    print()
    print("=" * 120)
    print("V4 BUILD STATUS")
    print("=" * 120)

    print("PHASE 0  FOUNDATION / DB OWNERSHIP       🟢")
    print("PHASE 1  COLLECTOR SUPERVISOR            ⏳")
    print("PHASE 2  RESEARCH WORKER POOL            ⏳")
    print("PHASE 3  HYPOTHESIS DIRECTOR             ⏳")
    print("PHASE 4  ROBUSTNESS / HOLDOUT            ⏳")
    print("PHASE 5  SHADOW / PAPER                  ⏳")

    print()
    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops V4 cleanly"
    )


def main():

    initialize()

    while not shutdown_requested:

        display()

        for _ in range(REFRESH):

            if shutdown_requested:
                break

            time.sleep(1)

    print()
    print("V4 stopped cleanly.")


if __name__ == "__main__":
    main()
'''

path = ROOT / "autonomous_lab_v4.py"

path.write_text(
    CODE.lstrip()
)

py_compile.compile(
    str(path),
    doraise=True
)

print("OK: autonomous_lab_v4.py created")
