#!/usr/bin/env python3

import sqlite3
import time
import math
import os

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

EXP_ID = "EXP_0121"

DUMPS = "t116_premigration_dump_events"
CLEAN = "t116_clean_swaps"
RAW = "t116_pump_swaps"

TABLE = "lab_exp0121_stage_features"

STAGES = [5, 10, 20, 30, 60]
FORWARD_HORIZON = 300

REFRESH = 10


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA busy_timeout=30000")
db.execute("PRAGMA wal_autocheckpoint=1000")


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def safe_return(a, b):

    if (
        not valid(a)
        or not valid(b)
        or a <= 0
    ):
        return None

    return 100.0 * (
        b / a - 1.0
    )


def table_exists(name):

    return db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE type='table'
      AND name=?
    """, (
        name,
    )).fetchone() is not None


def columns(name):

    return {
        r[1]
        for r in db.execute(
            f"PRAGMA table_info({name})"
        ).fetchall()
    }


# ============================================================
# VALIDATE SOURCES
# ============================================================

for name in (
    DUMPS,
    CLEAN,
    RAW,
    "lab_experiments",
):

    if not table_exists(name):

        raise RuntimeError(
            f"Required table missing: {name}"
        )


dump_cols = columns(DUMPS)
clean_cols = columns(CLEAN)
raw_cols = columns(RAW)

required_dump = {
    "id",
    "token_mint",
    "trigger_timestamp",
}

required_clean = {
    "token_mint",
    "timestamp",
    "clean_price_sol",
    "price_valid",
}

required_raw = {
    "token_mint",
    "timestamp",
    "side",
    "sol_delta",
}

if not required_dump.issubset(
    dump_cols
):
    raise RuntimeError(
        "Dump schema incompatible"
    )

if not required_clean.issubset(
    clean_cols
):
    raise RuntimeError(
        "Clean price schema incompatible"
    )

if not required_raw.issubset(
    raw_cols
):
    raise RuntimeError(
        "Raw Pump swap schema incompatible"
    )


# ============================================================
# OUTPUT TABLE
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    token_mint TEXT NOT NULL,
    dump_event_id INTEGER NOT NULL,
    stage_s INTEGER NOT NULL,

    trigger_ts REAL NOT NULL,

    entry_ts REAL,
    entry_price REAL,
    entry_delay_s REAL,

    requested_stage_ts REAL,

    decision_ts REAL,
    decision_price REAL,
    decision_delay_s REAL,

    stage_ready INTEGER NOT NULL DEFAULT 0,

    price_samples_stage INTEGER,

    return_since_entry REAL,

    mfe_so_far REAL,
    mae_so_far REAL,

    new_low INTEGER,
    reclaim_entry INTEGER,

    swaps INTEGER,
    buys INTEGER,
    sells INTEGER,

    buy_ratio REAL,

    buy_sol REAL,
    sell_sol REAL,
    net_sol REAL,

    future_ready INTEGER NOT NULL DEFAULT 0,

    future_samples INTEGER,

    future_max300 REAL,
    future_min300 REAL,
    future_end300 REAL,

    future_hit10 INTEGER,
    future_hit20 INTEGER,
    future_hit30 INTEGER,
    future_hit50 INTEGER,

    time_to_10 REAL,
    time_to_20 REAL,
    time_to_30 REAL,
    time_to_50 REAL,

    max_gap_s REAL,

    coverage_status TEXT,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL,

    PRIMARY KEY (
        token_mint,
        stage_s
    )
)
""")

db.commit()


# ============================================================
# EXPERIMENT STATE
# ============================================================

def experiment():

    return db.execute("""
    SELECT *
    FROM lab_experiments
    WHERE experiment_id=?
    """, (
        EXP_ID,
    )).fetchone()


def set_status(status):

    db.execute("""
    UPDATE lab_experiments
    SET
        status=?,
        last_update_at=?
    WHERE experiment_id=?
    """, (
        status,
        time.time(),
        EXP_ID,
    ))

    db.commit()


# ============================================================
# FIRST DUMP — EXACTLY ONE PER TOKEN
# ============================================================

def first_dumps():

    return db.execute(f"""
    WITH ranked AS (

        SELECT
            d.*,

            ROW_NUMBER() OVER (
                PARTITION BY token_mint

                ORDER BY
                    trigger_timestamp ASC,
                    dump_level ASC,
                    id ASC
            ) AS rn

        FROM {DUMPS} d
    )

    SELECT *
    FROM ranked
    WHERE rn=1

    ORDER BY trigger_timestamp ASC
    """).fetchall()


# ============================================================
# CLEAN PRICE HELPERS
# ============================================================

def first_clean_at_or_after(
    mint,
    ts
):

    return db.execute(f"""
    SELECT
        timestamp,
        clean_price_sol

    FROM {CLEAN}

    WHERE
        token_mint=?
        AND price_valid=1
        AND clean_price_sol IS NOT NULL
        AND clean_price_sol > 0
        AND timestamp >= ?

    ORDER BY timestamp ASC

    LIMIT 1
    """, (
        mint,
        ts,
    )).fetchone()


def clean_between(
    mint,
    start_ts,
    end_ts,
    strictly_after=False
):

    op = ">" if strictly_after else ">="

    return db.execute(f"""
    SELECT
        timestamp,
        clean_price_sol

    FROM {CLEAN}

    WHERE
        token_mint=?
        AND price_valid=1
        AND clean_price_sol IS NOT NULL
        AND clean_price_sol > 0

        AND timestamp {op} ?
        AND timestamp <= ?

    ORDER BY timestamp ASC
    """, (
        mint,
        start_ts,
        end_ts,
    )).fetchall()


# ============================================================
# RAW FLOW HELPERS
# ============================================================

def flow_between(
    mint,
    start_ts,
    end_ts
):

    rows = db.execute(f"""
    SELECT
        side,
        sol_delta

    FROM {RAW}

    WHERE
        token_mint=?
        AND timestamp > ?
        AND timestamp <= ?
    """, (
        mint,
        start_ts,
        end_ts,
    )).fetchall()

    swaps = len(rows)

    buys = sum(
        r["side"] == "BUY"
        for r in rows
    )

    sells = sum(
        r["side"] == "SELL"
        for r in rows
    )

    buy_sol = sum(
        abs(r["sol_delta"])
        for r in rows
        if (
            r["side"] == "BUY"
            and valid(
                r["sol_delta"]
            )
        )
    )

    sell_sol = sum(
        abs(r["sol_delta"])
        for r in rows
        if (
            r["side"] == "SELL"
            and valid(
                r["sol_delta"]
            )
        )
    )

    ratio = (
        buys / swaps
        if swaps > 0
        else 0.0
    )

    return {
        "swaps": swaps,
        "buys": buys,
        "sells": sells,

        "buy_ratio": ratio,

        "buy_sol": buy_sol,
        "sell_sol": sell_sol,
        "net_sol": (
            buy_sol
            - sell_sol
        ),
    }


# ============================================================
# MAX GAP
# ============================================================

def max_gap(
    start_ts,
    rows
):

    if not rows:
        return None

    times = [
        start_ts
    ] + [
        r["timestamp"]
        for r in rows
    ]

    gaps = [
        times[i]
        - times[i - 1]

        for i in range(
            1,
            len(times)
        )
    ]

    return (
        max(gaps)
        if gaps
        else None
    )


# ============================================================
# CREATE CASES
# ============================================================

def ensure_cases():

    now = time.time()

    for d in first_dumps():

        for stage in STAGES:

            db.execute(f"""
            INSERT OR IGNORE INTO {TABLE} (

                token_mint,
                dump_event_id,
                stage_s,

                trigger_ts,

                created_at,
                last_update_at
            )

            VALUES (
                ?,?,?,?,?,?
            )
            """, (

                d["token_mint"],
                d["id"],
                stage,

                d[
                    "trigger_timestamp"
                ],

                now,
                now,
            ))

    db.commit()


# ============================================================
# STAGE FEATURES
# ============================================================

def build_stage_features():

    now = time.time()

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE stage_ready=0
    """).fetchall()

    for r in rows:

        mint = r[
            "token_mint"
        ]

        trigger = r[
            "trigger_ts"
        ]

        stage = r[
            "stage_s"
        ]


        # --------------------------------------------
        # STRICT EXECUTABLE ENTRY
        # --------------------------------------------

        entry = first_clean_at_or_after(
            mint,
            trigger
        )

        if not entry:
            continue


        entry_ts = entry[
            "timestamp"
        ]

        entry_price = entry[
            "clean_price_sol"
        ]


        requested_stage = (
            entry_ts
            + stage
        )


        # --------------------------------------------
        # ACTUAL DECISION PRICE
        # --------------------------------------------

        decision = first_clean_at_or_after(
            mint,
            requested_stage
        )

        if not decision:
            continue


        decision_ts = decision[
            "timestamp"
        ]

        decision_price = decision[
            "clean_price_sol"
        ]


        # We cannot know stage data before the requested
        # stage has actually passed.
        if now < decision_ts:
            continue


        path = clean_between(
            mint,
            entry_ts,
            decision_ts
        )


        prices = [
            x[
                "clean_price_sol"
            ]
            for x in path
        ]


        if not prices:
            continue


        returns = [
            safe_return(
                entry_price,
                p
            )
            for p in prices
        ]

        returns = [
            x
            for x in returns
            if valid(x)
        ]

        if not returns:
            continue


        current_return = safe_return(
            entry_price,
            decision_price
        )

        mfe = max(
            returns
        )

        mae = min(
            returns
        )


        new_low = int(
            min(prices)
            < entry_price
        )

        reclaim_entry = int(
            decision_price
            >= entry_price
        )


        flow = flow_between(
            mint,
            entry_ts,
            decision_ts
        )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            entry_ts=?,
            entry_price=?,
            entry_delay_s=?,

            requested_stage_ts=?,

            decision_ts=?,
            decision_price=?,
            decision_delay_s=?,

            stage_ready=1,

            price_samples_stage=?,

            return_since_entry=?,

            mfe_so_far=?,
            mae_so_far=?,

            new_low=?,
            reclaim_entry=?,

            swaps=?,
            buys=?,
            sells=?,

            buy_ratio=?,

            buy_sol=?,
            sell_sol=?,
            net_sol=?,

            last_update_at=?

        WHERE
            token_mint=?
            AND stage_s=?
        """, (

            entry_ts,
            entry_price,
            (
                entry_ts
                - trigger
            ),

            requested_stage,

            decision_ts,
            decision_price,
            (
                decision_ts
                - requested_stage
            ),

            len(path),

            current_return,

            mfe,
            mae,

            new_low,
            reclaim_entry,

            flow[
                "swaps"
            ],

            flow[
                "buys"
            ],

            flow[
                "sells"
            ],

            flow[
                "buy_ratio"
            ],

            flow[
                "buy_sol"
            ],

            flow[
                "sell_sol"
            ],

            flow[
                "net_sol"
            ],

            time.time(),

            mint,
            stage,
        ))

    db.commit()


# ============================================================
# STRICT FUTURE OUTCOMES
# ============================================================

def build_future_outcomes():

    now = time.time()

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    WHERE
        stage_ready=1
        AND future_ready=0
    """).fetchall()


    for r in rows:

        decision_ts = r[
            "decision_ts"
        ]

        decision_price = r[
            "decision_price"
        ]


        if (
            decision_ts is None
            or decision_price is None
        ):
            continue


        horizon_end = (
            decision_ts
            + FORWARD_HORIZON
        )


        if now < horizon_end:
            continue


        future = clean_between(
            r["token_mint"],
            decision_ts,
            horizon_end,
            strictly_after=True
        )


        if not future:

            db.execute(f"""
            UPDATE {TABLE}

            SET
                future_ready=1,
                future_samples=0,

                coverage_status='NO_FORWARD',

                last_update_at=?

            WHERE
                token_mint=?
                AND stage_s=?
            """, (

                time.time(),

                r["token_mint"],
                r["stage_s"],
            ))

            continue


        path = []

        for p in future:

            ret = safe_return(
                decision_price,
                p[
                    "clean_price_sol"
                ]
            )

            if valid(ret):

                path.append(
                    (
                        p["timestamp"],
                        ret
                    )
                )


        if not path:
            continue


        max_row = max(
            path,
            key=lambda x:
                x[1]
        )

        min_row = min(
            path,
            key=lambda x:
                x[1]
        )


        end_ret = path[-1][1]


        def hit(level):

            for ts, ret in path:

                if ret >= level:

                    return (
                        1,
                        ts - decision_ts
                    )

            return (
                0,
                None
            )


        h10, t10 = hit(10)
        h20, t20 = hit(20)
        h30, t30 = hit(30)
        h50, t50 = hit(50)


        gap = max_gap(
            decision_ts,
            future
        )


        if (
            len(path) >= 3
            and gap is not None
            and gap <= 120
        ):

            coverage = "GOOD"

        else:

            coverage = "SPARSE"


        db.execute(f"""
        UPDATE {TABLE}

        SET
            future_ready=1,

            future_samples=?,

            future_max300=?,
            future_min300=?,
            future_end300=?,

            future_hit10=?,
            future_hit20=?,
            future_hit30=?,
            future_hit50=?,

            time_to_10=?,
            time_to_20=?,
            time_to_30=?,
            time_to_50=?,

            max_gap_s=?,

            coverage_status=?,

            last_update_at=?

        WHERE
            token_mint=?
            AND stage_s=?
        """, (

            len(path),

            max_row[1],
            min_row[1],
            end_ret,

            h10,
            h20,
            h30,
            h50,

            t10,
            t20,
            t30,
            t50,

            gap,

            coverage,

            time.time(),

            r[
                "token_mint"
            ],

            r[
                "stage_s"
            ],
        ))

    db.commit()


# ============================================================
# EXPERIMENT PROGRESS
# ============================================================

def update_experiment():

    total_tokens = db.execute(f"""
    SELECT
        COUNT(
            DISTINCT token_mint
        )
    FROM {TABLE}
    """).fetchone()[0]


    good_tokens = db.execute(f"""
    SELECT
        COUNT(
            DISTINCT token_mint
        )
    FROM {TABLE}
    WHERE coverage_status='GOOD'
    """).fetchone()[0]


    mature_rows = db.execute(f"""
    SELECT COUNT(*)
    FROM {TABLE}
    WHERE future_ready=1
    """).fetchone()[0]


    if good_tokens >= 300:

        status = "DISCOVERY_READY"

    elif mature_rows > 0:

        status = "DISCOVERY"

    else:

        status = "COLLECTING"


    db.execute("""
    UPDATE lab_experiments

    SET
        status=?,
        discovery_n=?,
        last_update_at=?

    WHERE experiment_id=?
    """, (

        status,

        good_tokens,

        time.time(),

        EXP_ID,
    ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system(
        "clear"
    )


    exp = experiment()


    print("=" * 160)

    print(
        "MEMECOIN LAB — EXP_0121 "
        "POST-ENTRY FEATURE ENGINE"
    )

    print("=" * 160)


    print(
        f"STATUS       : "
        f"{exp['status'] if exp else 'UNKNOWN'}"
    )

    print(
        "ENTRY        : STRICT FIRST CLEAN SNAPSHOT AFTER FIRST DUMP"
    )

    print(
        "STAGES       : "
        + ", ".join(
            f"{x}s"
            for x in STAGES
        )
    )

    print(
        "FUTURE       : STRICTLY AFTER ACTUAL DECISION SNAPSHOT"
    )

    print(
        "HORIZON      : 300s"
    )

    print(
        "MODEL FIT    : NONE"
    )

    print(
        "THRESHOLD    : NONE"
    )


    print()
    print("=" * 160)
    print("STAGE COVERAGE")
    print("=" * 160)


    print(
        f"{'STAGE':<8}"
        f"{'CASES':>8}"
        f"{'READY':>10}"
        f"{'FUTURE':>10}"
        f"{'GOOD':>10}"
        f"{'SPARSE':>10}"
        f"{'NO DATA':>10}"
        f"{'+20':>10}"
    )


    for stage in STAGES:

        r = db.execute(f"""
        SELECT

            COUNT(*) AS cases,

            SUM(
                stage_ready=1
            ) AS ready,

            SUM(
                future_ready=1
            ) AS future,

            SUM(
                coverage_status='GOOD'
            ) AS good,

            SUM(
                coverage_status='SPARSE'
            ) AS sparse,

            SUM(
                coverage_status='NO_FORWARD'
            ) AS nodata,

            SUM(
                future_hit20=1
                AND coverage_status='GOOD'
            ) AS hit20

        FROM {TABLE}

        WHERE stage_s=?
        """, (
            stage,
        )).fetchone()


        print(
            f"{stage:<8}"
            f"{(r['cases'] or 0):>8}"
            f"{(r['ready'] or 0):>10}"
            f"{(r['future'] or 0):>10}"
            f"{(r['good'] or 0):>10}"
            f"{(r['sparse'] or 0):>10}"
            f"{(r['nodata'] or 0):>10}"
            f"{(r['hit20'] or 0):>10}"
        )


    print()
    print("=" * 160)
    print("LATEST GOOD CASES")
    print("=" * 160)


    rows = db.execute(f"""
    SELECT *

    FROM {TABLE}

    WHERE coverage_status='GOOD'

    ORDER BY decision_ts DESC

    LIMIT 20
    """).fetchall()


    for r in rows:

        print(
            f"{r['token_mint'][:16]:16} "
            f"| T={r['stage_s']:>2}s "
            f"| RET={r['return_since_entry']:>7.1f}% "
            f"| B/S={r['buys']:>2}/{r['sells']:<2} "
            f"| NET={r['net_sol']:>8.3f} "
            f"| MAX={r['future_max300']:>7.1f}% "
            f"| MIN={r['future_min300']:>7.1f}% "
            f"| +20={r['future_hit20']}"
        )


    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops EXP_0121 engine only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    set_status(
        "COLLECTING"
    )

    while True:

        ensure_cases()

        build_stage_features()

        build_future_outcomes()

        update_experiment()

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "EXP_0121 feature engine stopped safely."
    )


finally:

    db.close()
