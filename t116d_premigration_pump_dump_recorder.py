#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

STATE = "t116_token_state"
CLEAN = "t116_clean_swaps"

PUMP_TABLE = "t116_pump_events"
DUMP_TABLE = "t116_premigration_dump_events"

REFRESH = 5

PUMP_LEVELS = [20, 50, 100, 200]
DUMP_LEVELS = [10, 20, 30, 40, 50]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def fmt(x, n=1):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLES
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {PUMP_TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    token_mint TEXT NOT NULL,

    pump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    first_seen REAL,
    first_price REAL,

    trigger_price REAL,
    run_from_first_pct REAL,

    peak_price REAL,
    peak_at REAL,

    buys_total INTEGER,
    sells_total INTEGER,

    buy_sol_total REAL,
    sell_sol_total REAL,
    net_sol_total REAL,

    swaps_30s INTEGER,
    buys_30s INTEGER,
    sells_30s INTEGER,
    net_sol_30s REAL,

    swaps_60s INTEGER,
    buys_60s INTEGER,
    sells_60s INTEGER,
    net_sol_60s REAL,

    migrated INTEGER,
    migration_timestamp REAL,

    created_at REAL NOT NULL,

    UNIQUE(token_mint, pump_level)
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {DUMP_TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    peak_price REAL,
    peak_at REAL,

    trigger_price REAL,
    drawdown_pct REAL,

    run_from_first_pct REAL,

    buys_total INTEGER,
    sells_total INTEGER,

    buy_sol_total REAL,
    sell_sol_total REAL,
    net_sol_total REAL,

    swaps_30s INTEGER,
    buys_30s INTEGER,
    sells_30s INTEGER,
    net_sol_30s REAL,

    swaps_60s INTEGER,
    buys_60s INTEGER,
    sells_60s INTEGER,
    net_sol_60s REAL,

    migrated INTEGER,
    migration_timestamp REAL,

    created_at REAL NOT NULL,

    UNIQUE(token_mint, dump_level)
)
""")

db.commit()


# ============================================================
# FIRST CLEAN PRICE
# ============================================================

def first_clean_price(mint):

    r = db.execute(f"""
    SELECT
        timestamp,
        clean_price_sol
    FROM {CLEAN}
    WHERE
        token_mint=?
        AND price_valid=1
        AND clean_price_sol IS NOT NULL
        AND clean_price_sol > 0
    ORDER BY timestamp ASC
    LIMIT 1
    """, (mint,)).fetchone()

    return r


# ============================================================
# EVENT INSERT
# ============================================================

def maybe_pump_event(s):

    run = s["run_from_first_pct"]

    if run is None:
        return

    first = first_clean_price(
        s["token_mint"]
    )

    if not first:
        return

    for level in PUMP_LEVELS:

        if run < level:
            continue

        db.execute(f"""
        INSERT OR IGNORE INTO {PUMP_TABLE} (

            token_mint,
            pump_level,

            trigger_timestamp,

            first_seen,
            first_price,

            trigger_price,
            run_from_first_pct,

            peak_price,
            peak_at,

            buys_total,
            sells_total,

            buy_sol_total,
            sell_sol_total,
            net_sol_total,

            swaps_30s,
            buys_30s,
            sells_30s,
            net_sol_30s,

            swaps_60s,
            buys_60s,
            sells_60s,
            net_sol_60s,

            migrated,
            migration_timestamp,

            created_at
        )

        VALUES (
            ?,?,
            ?,
            ?,?,
            ?,?,
            ?,?,
            ?,?,
            ?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,
            ?
        )
        """, (

            s["token_mint"],
            level,

            s["current_price_at"],

            s["first_seen"],
            first["clean_price_sol"],

            s["current_price"],
            s["run_from_first_pct"],

            s["peak_price"],
            s["peak_at"],

            s["buys_total"],
            s["sells_total"],

            s["buy_sol_total"],
            s["sell_sol_total"],
            s["net_sol_total"],

            s["swaps_30s"],
            s["buys_30s"],
            s["sells_30s"],
            s["net_sol_30s"],

            s["swaps_60s"],
            s["buys_60s"],
            s["sells_60s"],
            s["net_sol_60s"],

            s["migrated"],
            s["migration_timestamp"],

            time.time(),
        ))


def maybe_dump_event(s):

    dd = s["drawdown_from_peak_pct"]

    if dd is None:
        return

    for level in DUMP_LEVELS:

        if dd > -level:
            continue

        db.execute(f"""
        INSERT OR IGNORE INTO {DUMP_TABLE} (

            token_mint,
            dump_level,

            trigger_timestamp,

            peak_price,
            peak_at,

            trigger_price,
            drawdown_pct,

            run_from_first_pct,

            buys_total,
            sells_total,

            buy_sol_total,
            sell_sol_total,
            net_sol_total,

            swaps_30s,
            buys_30s,
            sells_30s,
            net_sol_30s,

            swaps_60s,
            buys_60s,
            sells_60s,
            net_sol_60s,

            migrated,
            migration_timestamp,

            created_at
        )

        VALUES (
            ?,?,
            ?,
            ?,?,
            ?,?,
            ?,
            ?,?,
            ?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,
            ?
        )
        """, (

            s["token_mint"],
            level,

            s["current_price_at"],

            s["peak_price"],
            s["peak_at"],

            s["current_price"],
            s["drawdown_from_peak_pct"],

            s["run_from_first_pct"],

            s["buys_total"],
            s["sells_total"],

            s["buy_sol_total"],
            s["sell_sol_total"],
            s["net_sol_total"],

            s["swaps_30s"],
            s["buys_30s"],
            s["sells_30s"],
            s["net_sol_30s"],

            s["swaps_60s"],
            s["buys_60s"],
            s["sells_60s"],
            s["net_sol_60s"],

            s["migrated"],
            s["migration_timestamp"],

            time.time(),
        ))


# ============================================================
# UPDATE EVENTS WITH MIGRATION STATUS
# ============================================================

def sync_migration():

    for table in (
        PUMP_TABLE,
        DUMP_TABLE,
    ):

        rows = db.execute(f"""
        SELECT DISTINCT token_mint
        FROM {table}
        """).fetchall()

        for r in rows:

            state = db.execute(f"""
            SELECT
                migrated,
                migration_timestamp
            FROM {STATE}
            WHERE token_mint=?
            """, (
                r["token_mint"],
            )).fetchone()

            if not state:
                continue

            db.execute(f"""
            UPDATE {table}
            SET
                migrated=?,
                migration_timestamp=?
            WHERE token_mint=?
            """, (
                state["migrated"],
                state["migration_timestamp"],
                r["token_mint"],
            ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")

    states = db.execute(f"""
    SELECT *
    FROM {STATE}
    WHERE
        migrated=0
        AND current_price IS NOT NULL
    ORDER BY last_seen DESC
    """).fetchall()

    pumps = sorted(
        [
            r for r in states
            if r["run_from_first_pct"] is not None
        ],
        key=lambda r:
            r["run_from_first_pct"],
        reverse=True
    )

    dumps = sorted(
        [
            r for r in states
            if r["drawdown_from_peak_pct"] is not None
        ],
        key=lambda r:
            r["drawdown_from_peak_pct"]
    )

    pump_events = db.execute(f"""
    SELECT
        COUNT(*) AS events,
        COUNT(DISTINCT token_mint) AS tokens
    FROM {PUMP_TABLE}
    """).fetchone()

    dump_events = db.execute(f"""
    SELECT
        COUNT(*) AS events,
        COUNT(DISTINCT token_mint) AS tokens
    FROM {DUMP_TABLE}
    """).fetchone()

    print("=" * 190)
    print(
        "MEMECOIN LAB — T116D PRE-MIGRATION PUMP / DUMP RECORDER"
    )
    print("=" * 190)

    print(
        f"ACTIVE TOKENS      : {len(states)}"
    )

    print(
        f"PUMP EVENTS        : "
        f"{pump_events['events'] or 0} "
        f"| TOKENS={pump_events['tokens'] or 0}"
    )

    print(
        f"DUMP EVENTS        : "
        f"{dump_events['events'] or 0} "
        f"| TOKENS={dump_events['tokens'] or 0}"
    )

    print()

    print(
        "PUMP LEVELS        : "
        + " / ".join(
            f"+{x}%"
            for x in PUMP_LEVELS
        )
    )

    print(
        "DUMP LEVELS        : "
        + " / ".join(
            f"-{x}%"
            for x in DUMP_LEVELS
        )
    )

    print(
        "MODE               : OBSERVATION ONLY"
    )

    print(
        "MODEL FITTING      : NONE"
    )

    print()
    print("=" * 190)
    print("🔥 STRONGEST CURRENT PUMPS")
    print("=" * 190)

    for r in pumps[:25]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| RUN={fmt(r['run_from_first_pct'],1):>8}% "
            f"| DD={fmt(r['drawdown_from_peak_pct'],1):>7}% "
            f"| N={r['clean_swaps']:3d} "
            f"| B/S={r['buys_total']:3d}/{r['sells_total']:<3d} "
            f"| NET={r['net_sol_total']:+8.3f} "
            f"| 30s={r['net_sol_30s']:+7.3f} "
            f"| 60s={r['net_sol_60s']:+7.3f} "
            f"| MIG={r['migrated']}"
        )

    print()
    print("=" * 190)
    print("🚀 FAST ACTIVE PUMPS — LAST 60s")
    print("=" * 190)

    fast = sorted(
        [
            r for r in states
            if (
                r["net_sol_60s"] is not None
                and r["swaps_60s"] is not None
            )
        ],
        key=lambda r:
            (
                r["net_sol_60s"],
                r["swaps_60s"]
            ),
        reverse=True
    )

    for r in fast[:25]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| RUN={fmt(r['run_from_first_pct'],1):>8}% "
            f"| 60s SW={r['swaps_60s']:3d} "
            f"| B/S={r['buys_60s']:2d}/{r['sells_60s']:<2d} "
            f"| NET60={r['net_sol_60s']:+8.3f} "
            f"| NETTOT={r['net_sol_total']:+8.3f}"
        )

    print()
    print("=" * 190)
    print("🔻 DEEPEST CURRENT DUMPS")
    print("=" * 190)

    for r in dumps[:25]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| DD={fmt(r['drawdown_from_peak_pct'],1):>8}% "
            f"| RUN={fmt(r['run_from_first_pct'],1):>8}% "
            f"| N={r['clean_swaps']:3d} "
            f"| NET={r['net_sol_total']:+8.3f} "
            f"| 60s={r['net_sol_60s']:+7.3f}"
        )

    print()
    print("=" * 190)
    print("🏁 RECENT MIGRATIONS FROM T116")
    print("=" * 190)

    mig = db.execute(f"""
    SELECT *
    FROM {STATE}
    WHERE migrated=1
    ORDER BY migration_timestamp DESC
    LIMIT 20
    """).fetchall()

    if not mig:

        print(
            "No T116 token matched to migration yet."
        )

    else:

        for r in mig:

            print(
                f"{r['token_mint'][:18]:18} "
                f"| RUN={fmt(r['run_from_first_pct'],1):>8}% "
                f"| DD={fmt(r['drawdown_from_peak_pct'],1):>7}% "
                f"| N={r['clean_swaps']:3d} "
                f"| NET={r['net_sol_total']:+8.3f}"
            )

    print()
    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T116D only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        states = db.execute(f"""
        SELECT *
        FROM {STATE}
        WHERE current_price IS NOT NULL
        """).fetchall()

        for s in states:

            maybe_pump_event(s)
            maybe_dump_event(s)

        db.commit()

        sync_migration()

        show()

        time.sleep(
            REFRESH
        )

except KeyboardInterrupt:

    print()
    print(
        "T116D stopped safely."
    )

finally:

    db.close()
