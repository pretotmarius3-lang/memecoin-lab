#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

SOURCE = "t108_dump_events"
TABLE = "t109c_dump_path"

REFRESH = 10

HORIZONS = [
    30,
    60,
    300,
    900,
]

REBOUND_LEVELS = [
    10,
    20,
    30,
    50,
]


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
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


def fmt(x, n=1):

    if x is None:
        return "NA"

    return f"{x:.{n}f}"


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute(
    "PRAGMA busy_timeout=5000"
)


# ============================================================
# TABLE
# ============================================================

horizon_cols = []

for h in HORIZONS:

    threshold_cols = []

    for level in REBOUND_LEVELS:

        threshold_cols.append(
            f"ever_up{level}_{h}s INTEGER"
        )

    horizon_cols.append(f"""
        done_{h}s INTEGER NOT NULL DEFAULT 0,

        snapshots_{h}s INTEGER,
        first_snapshot_delay_{h}s REAL,
        last_snapshot_delay_{h}s REAL,
        max_gap_{h}s REAL,

        end_price_{h}s REAL,
        end_return_{h}s REAL,

        high_price_{h}s REAL,
        high_return_{h}s REAL,
        high_time_{h}s REAL,

        low_price_{h}s REAL,
        low_return_{h}s REAL,
        low_time_{h}s REAL,

        mfe_{h}s REAL,
        mae_{h}s REAL,

        ever_new_low_{h}s INTEGER,
        ever_reclaim_peak_{h}s INTEGER,

        first_reclaim_trigger_time_{h}s REAL,
        first_reclaim_peak_time_{h}s REAL,

        {",".join(threshold_cols)}
    """)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    t108_event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    trigger_price REAL NOT NULL,

    peak_price REAL NOT NULL,
    peak_at REAL,

    drawdown_pct REAL,

    {",".join(horizon_cols)},

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# SOURCE
# ============================================================

def dump_events():

    return db.execute(f"""
    SELECT *
    FROM {SOURCE}

    ORDER BY trigger_timestamp
    """).fetchall()


# ============================================================
# BASE ROW
# ============================================================

def ensure_row(e):

    db.execute(f"""
    INSERT OR IGNORE INTO {TABLE} (

        t108_event_id,

        token_mint,

        dump_level,

        trigger_timestamp,

        trigger_price,

        peak_price,
        peak_at,

        drawdown_pct,

        created_at,
        last_update_at
    )

    VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """, (

        e["id"],

        e["token_mint"],

        e["dump_level"],

        e["trigger_timestamp"],

        e["trigger_price"],

        e["peak_price"],
        e["peak_at"],

        e["drawdown_pct"],

        time.time(),
        time.time(),
    ))

    db.commit()


# ============================================================
# PATH
# ============================================================

def path_rows(
    mint,
    trigger_ts,
    end_ts
):

    return db.execute("""
    SELECT
        timestamp,
        price_usd

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp <= ?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp ASC
    """, (
        mint,
        trigger_ts,
        end_ts
    )).fetchall()


# ============================================================
# COMPUTE HORIZON
# ============================================================

def compute_horizon(
    e,
    h
):

    target = (
        e["trigger_timestamp"]
        + h
    )

    if time.time() < target:
        return None


    rows = path_rows(
        e["token_mint"],
        e["trigger_timestamp"],
        target
    )


    if not rows:
        return {
            "done": 1,
            "snapshots": 0,
        }


    trigger_price = e[
        "trigger_price"
    ]

    peak_price = e[
        "peak_price"
    ]


    prices = [
        (
            r["timestamp"],
            r["price_usd"]
        )
        for r in rows
    ]


    # --------------------------------------------------------
    # COVERAGE
    # --------------------------------------------------------

    first_ts = prices[0][0]
    last_ts = prices[-1][0]

    first_delay = (
        first_ts
        - e["trigger_timestamp"]
    )

    last_delay = (
        target
        - last_ts
    )


    gaps = []

    prev = (
        e["trigger_timestamp"]
    )

    for ts, _ in prices:

        gaps.append(
            ts - prev
        )

        prev = ts

    gaps.append(
        target - prev
    )

    max_gap = (
        max(gaps)
        if gaps
        else None
    )


    # --------------------------------------------------------
    # HIGH / LOW / END
    # --------------------------------------------------------

    high_ts, high_price = max(
        prices,
        key=lambda x: x[1]
    )

    low_ts, low_price = min(
        prices,
        key=lambda x: x[1]
    )

    end_ts, end_price = prices[-1]


    high_ret = pct(
        trigger_price,
        high_price
    )

    low_ret = pct(
        trigger_price,
        low_price
    )

    end_ret = pct(
        trigger_price,
        end_price
    )


    # MFE / MAE relative to trigger.
    mfe = high_ret
    mae = low_ret


    # --------------------------------------------------------
    # TRUE PATH CONDITIONS
    # --------------------------------------------------------

    ever_new_low = int(
        any(
            price < trigger_price
            for _, price in prices
        )
    )

    ever_reclaim_peak = int(
        any(
            price >= peak_price
            for _, price in prices
        )
    )


    # --------------------------------------------------------
    # FIRST RECLAIM OF TRIGGER
    # --------------------------------------------------------

    trigger_reclaim_time = None

    for ts, price in prices:

        if price >= trigger_price:

            # Don't count the trigger observation itself
            # as a "recovery".
            if ts > e["trigger_timestamp"]:

                trigger_reclaim_time = (
                    ts
                    - e["trigger_timestamp"]
                )

                break


    # --------------------------------------------------------
    # FIRST RECLAIM OF OLD PEAK
    # --------------------------------------------------------

    peak_reclaim_time = None

    for ts, price in prices:

        if price >= peak_price:

            peak_reclaim_time = (
                ts
                - e["trigger_timestamp"]
            )

            break


    # --------------------------------------------------------
    # REBOUND THRESHOLDS
    # --------------------------------------------------------

    ever_thresholds = {}

    for level in REBOUND_LEVELS:

        ever_thresholds[
            level
        ] = int(
            any(
                (
                    pct(
                        trigger_price,
                        price
                    )
                    or 0
                ) >= level

                for _, price in prices
            )
        )


    return {

        "done":
            1,

        "snapshots":
            len(prices),

        "first_snapshot_delay":
            first_delay,

        "last_snapshot_delay":
            last_delay,

        "max_gap":
            max_gap,

        "end_price":
            end_price,

        "end_return":
            end_ret,

        "high_price":
            high_price,

        "high_return":
            high_ret,

        "high_time":
            high_ts
            - e["trigger_timestamp"],

        "low_price":
            low_price,

        "low_return":
            low_ret,

        "low_time":
            low_ts
            - e["trigger_timestamp"],

        "mfe":
            mfe,

        "mae":
            mae,

        "ever_new_low":
            ever_new_low,

        "ever_reclaim_peak":
            ever_reclaim_peak,

        "first_reclaim_trigger_time":
            trigger_reclaim_time,

        "first_reclaim_peak_time":
            peak_reclaim_time,

        "ever_thresholds":
            ever_thresholds,
    }


# ============================================================
# SAVE
# ============================================================

def fill_event(e):

    state = db.execute(f"""
    SELECT *
    FROM {TABLE}

    WHERE t108_event_id=?
    """, (
        e["id"],
    )).fetchone()


    for h in HORIZONS:

        if state[
            f"done_{h}s"
        ] == 1:
            continue


        r = compute_horizon(
            e,
            h
        )

        if r is None:
            continue


        if r[
            "snapshots"
        ] == 0:

            db.execute(f"""
            UPDATE {TABLE}

            SET
                done_{h}s=1,
                snapshots_{h}s=0,
                last_update_at=?

            WHERE t108_event_id=?
            """, (
                time.time(),
                e["id"],
            ))

            db.commit()

            continue


        threshold_sql = []

        threshold_values = []

        for level in REBOUND_LEVELS:

            threshold_sql.append(
                f"ever_up{level}_{h}s=?"
            )

            threshold_values.append(
                r[
                    "ever_thresholds"
                ][level]
            )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            done_{h}s=1,

            snapshots_{h}s=?,

            first_snapshot_delay_{h}s=?,
            last_snapshot_delay_{h}s=?,
            max_gap_{h}s=?,

            end_price_{h}s=?,
            end_return_{h}s=?,

            high_price_{h}s=?,
            high_return_{h}s=?,
            high_time_{h}s=?,

            low_price_{h}s=?,
            low_return_{h}s=?,
            low_time_{h}s=?,

            mfe_{h}s=?,
            mae_{h}s=?,

            ever_new_low_{h}s=?,
            ever_reclaim_peak_{h}s=?,

            first_reclaim_trigger_time_{h}s=?,
            first_reclaim_peak_time_{h}s=?,

            {",".join(threshold_sql)},

            last_update_at=?

        WHERE t108_event_id=?
        """, (

            r["snapshots"],

            r["first_snapshot_delay"],
            r["last_snapshot_delay"],
            r["max_gap"],

            r["end_price"],
            r["end_return"],

            r["high_price"],
            r["high_return"],
            r["high_time"],

            r["low_price"],
            r["low_return"],
            r["low_time"],

            r["mfe"],
            r["mae"],

            r["ever_new_low"],
            r["ever_reclaim_peak"],

            r["first_reclaim_trigger_time"],
            r["first_reclaim_peak_time"],

            *threshold_values,

            time.time(),

            e["id"],
        ))

        db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system(
        "clear"
    )


    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    ORDER BY trigger_timestamp DESC
    """).fetchall()


    print("=" * 190)

    print(
        "MEMECOIN LAB — T109C DUMP PATH / EXCURSION AUDIT"
    )

    print("=" * 190)


    print(
        f"EVENTS            : {len(rows)}"
    )

    for h in HORIZONS:

        ready = sum(
            r[
                f"done_{h}s"
            ]
            for r in rows
        )

        print(
            f"PATH {h:4d}s READY  : {ready}"
        )


    print()

    print(
        "MODE              : PATH RECONSTRUCTION ONLY"
    )

    print(
        "MODEL FITTING     : NONE"
    )

    print(
        "THRESHOLD SEARCH  : NONE"
    )

    print(
        "IMPORTANT         : PATH QUALITY SHOWN VIA SNAPSHOTS/MAX GAP"
    )


    # ========================================================
    # 300 SECOND PATH
    # ========================================================

    print()
    print("=" * 190)
    print("300s PATH")
    print("=" * 190)


    for r in rows[:30]:

        if not r[
            "done_300s"
        ]:

            continue


        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| N={str(r['snapshots_300s']):>3} "
            f"| GAP={fmt(r['max_gap_300s'],0):>4}s "
            f"| END={fmt(r['end_return_300s']):>7}% "
            f"| MFE={fmt(r['mfe_300s']):>7}% "
            f"| MAE={fmt(r['mae_300s']):>7}% "
            f"| HIGH@={fmt(r['high_time_300s'],0):>4}s "
            f"| LOW@={fmt(r['low_time_300s'],0):>4}s "
            f"| NEWLOW={str(r['ever_new_low_300s']):>4} "
            f"| PEAK={str(r['ever_reclaim_peak_300s']):>4}"
        )


    # ========================================================
    # REBOUND LEVELS
    # ========================================================

    print()
    print("=" * 190)
    print("REBOUND PATH — WITHIN 300s")
    print("=" * 190)


    for r in rows[:30]:

        if not r[
            "done_300s"
        ]:

            continue


        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| +10={r['ever_up10_300s']} "
            f"| +20={r['ever_up20_300s']} "
            f"| +30={r['ever_up30_300s']} "
            f"| +50={r['ever_up50_300s']} "
            f"| TRIGGER_RECLAIM@="
            f"{fmt(r['first_reclaim_trigger_time_300s'],0):>5}s "
            f"| OLD_PEAK@="
            f"{fmt(r['first_reclaim_peak_time_300s'],0):>5}s"
        )


    # ========================================================
    # COVERAGE
    # ========================================================

    print()
    print("=" * 190)
    print("PATH COVERAGE QUALITY")
    print("=" * 190)


    usable = [
        r
        for r in rows
        if (
            r["done_300s"]
            and r["snapshots_300s"]
            and r["snapshots_300s"] > 0
        )
    ]


    good = [
        r
        for r in usable
        if (
            r["max_gap_300s"] is not None
            and r["max_gap_300s"] <= 60
        )
    ]


    excellent = [
        r
        for r in usable
        if (
            r["max_gap_300s"] is not None
            and r["max_gap_300s"] <= 30
        )
    ]


    print(
        f"300s USABLE       : {len(usable)}/{len(rows)}"
    )

    print(
        f"MAX GAP <=60s     : {len(good)}/{len(usable) if usable else 0}"
    )

    print(
        f"MAX GAP <=30s     : {len(excellent)}/{len(usable) if usable else 0}"
    )


    print()

    if usable and not good:

        print(
            "🟠 PRICE PATH IS TOO SPARSE FOR PRECISE INTRA-HORIZON TIMING."
        )

    elif usable and len(good) < len(usable):

        print(
            "🟡 MIXED PATH COVERAGE — USE TIMING FEATURES CAUTIOUSLY."
        )

    elif good:

        print(
            "🟢 300s PATH COVERAGE CURRENTLY SUFFICIENT FOR COARSE EXCURSION ANALYSIS."
        )

    else:

        print(
            "🔵 WAITING FOR MATURE 300s PATHS."
        )


    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T109C only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for e in dump_events():

            ensure_row(
                e
            )

            fill_event(
                e
            )


        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T109C stopped safely."
    )


finally:

    db.close()
