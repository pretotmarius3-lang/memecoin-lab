#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

TABLE = "t114_frozen_holdout"
META = "t114_frozen_meta"

REFRESH = 10

MAX_ENTRY_DELAY = 35
MAX_PATH_GAP = 60

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return sum(xs) / len(xs) if xs else None


def std(xs):
    xs = [x for x in xs if valid(x)]

    if len(xs) < 2:
        return None

    m = mean(xs)

    return math.sqrt(
        sum(
            (x - m) ** 2
            for x in xs
        ) / len(xs)
    )


def pct(a, b):
    if (
        not valid(a)
        or not valid(b)
        or a <= 0
    ):
        return None

    return 100.0 * (
        b / a - 1.0
    )


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# FREEZE META
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {META} (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""")

row = db.execute(f"""
SELECT value
FROM {META}
WHERE key='started_at'
""").fetchone()

if row:
    STARTED_AT = float(
        row["value"]
    )

else:
    STARTED_AT = time.time()

    meta = {
        "started_at":
            str(STARTED_AT),

        "candidate_A":
            "STAGE30_FULL_CONFIRMATION",

        "candidate_B":
            "STAGE60_STRUCTURE",

        "status":
            "FROZEN",

        "threshold_search":
            "NONE",

        "entry_rule":
            "NONE",
    }

    for k, v in meta.items():

        db.execute(f"""
        INSERT OR REPLACE INTO {META}
        (key,value)
        VALUES (?,?)
        """, (
            k,
            str(v)
        ))

    db.commit()


# ============================================================
# FREEZE DISCOVERY SCALERS
# ============================================================

A_FEATURES = [
    "market_cap_at_dump",
    "liquidity_at_dump",
    "volume_m5_at_dump",
    "pre60_swaps",
    "pre60_buys",
    "pre60_buy_sol",
    "post_stage_buys",
    "post_stage_buy_sol",
    "pre_stage_mae",
]

B_FEATURES = [
    "market_cap_at_dump",
    "liquidity_at_dump",
]


def discovery_rows(stage):

    return db.execute("""
    SELECT *
    FROM t110c_strict_stage_forward

    WHERE
        requested_stage_seconds=?
        AND trigger_timestamp < ?
        AND actual_stage_delay_s <= ?
        AND mature_300=1
        AND max_gap_after_entry_300 <= ?
    """, (
        stage,
        STARTED_AT,
        MAX_ENTRY_DELAY,
        MAX_PATH_GAP
    )).fetchall()


def fit_scaler(rows, features):

    out = {}

    for f in features:

        vals = [
            r[f]
            for r in rows
            if valid(r[f])
        ]

        out[f] = {
            "mean":
                mean(vals),

            "std":
                std(vals),
        }

    return out


A_DISCOVERY = discovery_rows(30)
B_DISCOVERY = discovery_rows(60)

A_SCALER = fit_scaler(
    A_DISCOVERY,
    A_FEATURES
)

B_SCALER = fit_scaler(
    B_DISCOVERY,
    B_FEATURES
)


def z(v, stat):

    if not valid(v):
        return None

    m = stat["mean"]
    s = stat["std"]

    if (
        m is None
        or s is None
        or s == 0
    ):
        return 0.0

    return (
        v - m
    ) / s


def score(row, features, scaler):

    vals = []

    for f in features:

        v = z(
            row[f],
            scaler[f]
        )

        if v is not None:
            vals.append(v)

    return mean(vals)


# ============================================================
# OUTPUT TABLE
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    t108_event_id INTEGER NOT NULL,
    token_mint TEXT NOT NULL,
    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    candidate TEXT NOT NULL,

    requested_stage_seconds INTEGER NOT NULL,

    actual_stage_timestamp REAL,
    actual_stage_delay_s REAL,

    stage_price REAL,

    frozen_score REAL,

    -- outcomes
    end_return_300 REAL,
    max_return_300 REAL,
    min_return_300 REAL,

    end_return_900 REAL,
    max_return_900 REAL,
    min_return_900 REAL,

    reclaim_peak_300 INTEGER,
    reclaim_peak_900 INTEGER,

    mature_300 INTEGER NOT NULL DEFAULT 0,
    mature_900 INTEGER NOT NULL DEFAULT 0,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL,

    UNIQUE(
        t108_event_id,
        candidate
    )
)
""")

db.commit()


# ============================================================
# PROSPECTIVE SOURCE ONLY
# ============================================================

def prospective_rows():

    return db.execute("""
    SELECT *
    FROM t110c_strict_stage_forward

    WHERE
        trigger_timestamp >= ?
        AND actual_stage_delay_s <= ?

    ORDER BY trigger_timestamp
    """, (
        STARTED_AT,
        MAX_ENTRY_DELAY
    )).fetchall()


# ============================================================
# INSERT / UPDATE
# ============================================================

def process_row(r):

    if r["requested_stage_seconds"] == 30:

        candidate = "A_STAGE30_FULL_CONFIRMATION"
        features = A_FEATURES
        scaler = A_SCALER

    elif r["requested_stage_seconds"] == 60:

        candidate = "B_STAGE60_STRUCTURE"
        features = B_FEATURES
        scaler = B_SCALER

    else:
        return


    frozen_score = score(
        r,
        features,
        scaler
    )


    db.execute(f"""
    INSERT INTO {TABLE} (

        t108_event_id,
        token_mint,
        dump_level,

        trigger_timestamp,

        candidate,

        requested_stage_seconds,

        actual_stage_timestamp,
        actual_stage_delay_s,

        stage_price,

        frozen_score,

        end_return_300,
        max_return_300,
        min_return_300,

        end_return_900,
        max_return_900,
        min_return_900,

        reclaim_peak_300,
        reclaim_peak_900,

        mature_300,
        mature_900,

        created_at,
        last_update_at
    )

    VALUES (
        ?,?,?,
        ?,
        ?,
        ?,
        ?,?,
        ?,
        ?,
        ?,?,?,
        ?,?,?,
        ?,?,
        ?,?,
        ?,?
    )

    ON CONFLICT(
        t108_event_id,
        candidate
    )

    DO UPDATE SET

        actual_stage_timestamp=
            excluded.actual_stage_timestamp,

        actual_stage_delay_s=
            excluded.actual_stage_delay_s,

        stage_price=
            excluded.stage_price,

        frozen_score=
            excluded.frozen_score,

        end_return_300=
            excluded.end_return_300,

        max_return_300=
            excluded.max_return_300,

        min_return_300=
            excluded.min_return_300,

        end_return_900=
            excluded.end_return_900,

        max_return_900=
            excluded.max_return_900,

        min_return_900=
            excluded.min_return_900,

        reclaim_peak_300=
            excluded.reclaim_peak_300,

        reclaim_peak_900=
            excluded.reclaim_peak_900,

        mature_300=
            excluded.mature_300,

        mature_900=
            excluded.mature_900,

        last_update_at=
            excluded.last_update_at
    """, (

        r["t108_event_id"],
        r["token_mint"],
        r["dump_level"],

        r["trigger_timestamp"],

        candidate,

        r["requested_stage_seconds"],

        r["actual_stage_timestamp"],
        r["actual_stage_delay_s"],

        r["stage_price"],

        frozen_score,

        r["end_return_300"],
        r["max_return_300"],
        r["min_return_300"],

        r["end_return_900"],
        r["max_return_900"],
        r["min_return_900"],

        r["reclaim_old_peak_after_entry_300"],
        r["reclaim_old_peak_after_entry_900"],

        r["mature_300"],
        r["mature_900"],

        time.time(),
        time.time(),
    ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    ORDER BY
        trigger_timestamp DESC,
        candidate
    """).fetchall()

    print("=" * 185)
    print(
        "MEMECOIN LAB — T114 FROZEN PROSPECTIVE HOLDOUT"
    )
    print("=" * 185)

    print(
        f"FROZEN START      : "
        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(STARTED_AT))}"
    )

    print(
        f"DISCOVERY A ROWS  : {len(A_DISCOVERY)}"
    )

    print(
        f"DISCOVERY B ROWS  : {len(B_DISCOVERY)}"
    )

    print(
        f"HOLDOUT ROWS      : {len(rows)}"
    )

    print(
        f"HOLDOUT TOKENS    : "
        f"{len(set(r['token_mint'] for r in rows))}"
    )

    print()
    print(
        "A = 30s FULL_CONFIRMATION"
    )

    print(
        "B = 60s STRUCTURE"
    )

    print(
        "THRESHOLD SEARCH  : NONE"
    )

    print(
        "ENTRY RULE        : NONE"
    )

    print(
        "STATUS            : 🔒 FROZEN"
    )


    print()
    print("=" * 185)
    print("PROSPECTIVE HOLDOUT")
    print("=" * 185)


    for r in rows[:40]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| {r['candidate']:<28} "
            f"| D=-{r['dump_level']:2d}% "
            f"| SCORE={fmt(r['frozen_score']):>7} "
            f"| MAX300={fmt(r['max_return_300'],1):>7}% "
            f"| MIN300={fmt(r['min_return_300'],1):>7}% "
            f"| END300={fmt(r['end_return_300'],1):>7}%"
        )


    print()
    print("=" * 185)
    print("READINESS")
    print("=" * 185)

    mature = [
        r
        for r in rows
        if r["mature_300"]
    ]

    mature_tokens = {
        r["token_mint"]
        for r in mature
    }

    print(
        f"MATURE 300 ROWS   : {len(mature)}"
    )

    print(
        f"MATURE TOKENS     : {len(mature_tokens)}"
    )


    if len(mature_tokens) >= 50:

        print(
            "🟢 HOLDOUT LARGE ENOUGH FOR SERIOUS VALIDATION."
        )

    elif len(mature_tokens) >= 20:

        print(
            "🟡 HOLDOUT DESCRIPTIVE VALIDATION AVAILABLE."
        )

    else:

        print(
            "🔵 COLLECTING UNSEEN PROSPECTIVE TOKENS."
        )


    print()
    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T114 only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for r in prospective_rows():

            process_row(r)

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T114 stopped safely."
    )


finally:

    db.close()
