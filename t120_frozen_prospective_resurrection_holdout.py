#!/usr/bin/env python3

import sqlite3
import os
import time
import math
import statistics

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

DUMP_EVENTS = "t116_premigration_dump_events"
DUMP_OUT = "t117_dump_outcomes"
MIGRATIONS = "t101_migrations"

META = "t120_meta"
DISCOVERY = "t120_discovery_spec"
HOLDOUT = "t120_holdout"

REFRESH = 10

PATH_MAX_RETURN_LIMIT = 1000.0
PATH_MIN_RETURN_LIMIT = -99.99

FEATURES = [
    "run_from_first_pct",
    "drawdown_pct",
    "swaps_30s",
    "swaps_60s",
    "buys_30s",
    "buys_60s",
    "buys_total",
]


# ============================================================
# DB
# ============================================================

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


def stdev(xs):
    xs = [x for x in xs if valid(x)]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def table_exists(name):
    return db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE type='table'
      AND name=?
    """, (
        name,
    )).fetchone() is not None


# ============================================================
# TABLES
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {META} (

    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {DISCOVERY} (

    feature TEXT PRIMARY KEY,

    mean_value REAL NOT NULL,
    std_value REAL NOT NULL,

    frozen_at REAL NOT NULL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {HOLDOUT} (

    dump_event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    scored_at REAL NOT NULL,

    frozen_score REAL NOT NULL,

    run_from_first_pct REAL,
    drawdown_pct REAL,

    swaps_30s INTEGER,
    swaps_60s INTEGER,

    buys_30s INTEGER,
    buys_60s INTEGER,
    buys_total INTEGER,

    path_done_300s INTEGER NOT NULL DEFAULT 0,
    path_snapshots_300s INTEGER,

    max300 REAL,
    min300 REAL,
    end300 REAL,

    hit10 INTEGER,
    hit20 INTEGER,
    hit30 INTEGER,
    hit50 INTEGER,

    time_to_10s REAL,
    time_to_20s REAL,
    time_to_30s REAL,
    time_to_50s REAL,

    time_to_max_s REAL,

    migrated_after_event INTEGER NOT NULL DEFAULT 0,
    migration_timestamp REAL,
    time_to_migration_s REAL,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# DISCOVERY DATASET
# ============================================================

def good_first_dump_rows(
    before_ts=None
):

    extra = ""

    args = []

    if before_ts is not None:
        extra = """
        AND e.trigger_timestamp < ?
        """
        args.append(
            before_ts
        )

    rows = db.execute(f"""
    WITH ranked AS (

        SELECT
            e.*,

            ROW_NUMBER() OVER (
                PARTITION BY e.token_mint

                ORDER BY
                    e.trigger_timestamp ASC,
                    e.dump_level ASC,
                    e.id ASC
            ) AS rn

        FROM {DUMP_EVENTS} e
    )

    SELECT
        e.*,

        o.path_done_300s,
        o.path_snapshots_300s,

        o.path_max_return_300s,
        o.path_min_return_300s,
        o.path_end_return_300s

    FROM ranked e

    JOIN {DUMP_OUT} o
      ON o.t116_dump_event_id=e.id

    WHERE
        e.rn=1
        {extra}

    ORDER BY e.trigger_timestamp ASC
    """, args).fetchall()

    out = []

    for r in rows:

        if (
            r["path_done_300s"] != 1
            or r["path_snapshots_300s"] is None
            or r["path_snapshots_300s"] < 1
        ):
            continue

        mx = r[
            "path_max_return_300s"
        ]

        mn = r[
            "path_min_return_300s"
        ]

        end = r[
            "path_end_return_300s"
        ]

        if not (
            valid(mx)
            and valid(mn)
            and valid(end)
        ):
            continue

        if (
            mx > PATH_MAX_RETURN_LIMIT
            or mn < PATH_MIN_RETURN_LIMIT
            or end < PATH_MIN_RETURN_LIMIT
        ):
            continue

        if not all(
            valid(r[f])
            for f in FEATURES
        ):
            continue

        out.append(r)

    return out


# ============================================================
# FREEZE
# ============================================================

def get_meta(key):

    r = db.execute(f"""
    SELECT value
    FROM {META}
    WHERE key=?
    """, (
        key,
    )).fetchone()

    return (
        r["value"]
        if r
        else None
    )


def set_meta(
    key,
    value
):

    db.execute(f"""
    INSERT INTO {META} (
        key,
        value
    )

    VALUES (?,?)

    ON CONFLICT(key)
    DO UPDATE SET
        value=excluded.value
    """, (
        key,
        str(value)
    ))

    db.commit()


def freeze_if_needed():

    frozen = get_meta(
        "frozen_at"
    )

    if frozen is not None:
        return float(frozen)

    frozen_at = time.time()

    rows = good_first_dump_rows(
        before_ts=frozen_at
    )

    if len(rows) < 300:

        raise RuntimeError(
            f"Need >=300 discovery rows before freeze. "
            f"Current={len(rows)}"
        )

    db.execute(
        f"DELETE FROM {DISCOVERY}"
    )

    for feature in FEATURES:

        vals = [
            r[feature]
            for r in rows
        ]

        m = mean(vals)
        sd = stdev(vals)

        if (
            m is None
            or sd is None
            or sd == 0
        ):
            raise RuntimeError(
                f"Invalid discovery scale for {feature}"
            )

        db.execute(f"""
        INSERT INTO {DISCOVERY} (
            feature,
            mean_value,
            std_value,
            frozen_at
        )

        VALUES (?,?,?,?)
        """, (
            feature,
            m,
            sd,
            frozen_at,
        ))

    set_meta(
        "frozen_at",
        frozen_at
    )

    set_meta(
        "discovery_rows",
        len(rows)
    )

    set_meta(
        "family",
        "STRUCTURE+ACTIVITY+BUY"
    )

    set_meta(
        "target",
        "+20% within 300s after first dump"
    )

    db.commit()

    return frozen_at


# ============================================================
# LOAD FROZEN SPEC
# ============================================================

def frozen_spec():

    rows = db.execute(f"""
    SELECT *
    FROM {DISCOVERY}
    """).fetchall()

    return {
        r["feature"]: (
            r["mean_value"],
            r["std_value"]
        )
        for r in rows
    }


def frozen_score(
    r,
    spec
):

    zs = []

    for feature in FEATURES:

        value = r[feature]

        if not valid(value):
            return None

        m, sd = spec[
            feature
        ]

        if sd == 0:
            return None

        zs.append(
            (value - m)
            / sd
        )

    return mean(zs)


# ============================================================
# NEW PROSPECTIVE FIRST-DUMPS
# ============================================================

def prospective_candidates(
    frozen_at
):

    return db.execute(f"""
    WITH ranked AS (

        SELECT
            e.*,

            ROW_NUMBER() OVER (
                PARTITION BY e.token_mint

                ORDER BY
                    e.trigger_timestamp ASC,
                    e.dump_level ASC,
                    e.id ASC
            ) AS rn

        FROM {DUMP_EVENTS} e
    )

    SELECT e.*

    FROM ranked e

    WHERE
        e.rn=1
        AND e.trigger_timestamp >= ?

    ORDER BY e.trigger_timestamp ASC
    """, (
        frozen_at,
    )).fetchall()


def ensure_holdout_rows(
    frozen_at,
    spec
):

    rows = prospective_candidates(
        frozen_at
    )

    for r in rows:

        exists = db.execute(f"""
        SELECT 1
        FROM {HOLDOUT}
        WHERE dump_event_id=?
        """, (
            r["id"],
        )).fetchone()

        if exists:
            continue

        score = frozen_score(
            r,
            spec
        )

        if score is None:
            continue

        db.execute(f"""
        INSERT INTO {HOLDOUT} (

            dump_event_id,

            token_mint,
            dump_level,

            trigger_timestamp,

            scored_at,

            frozen_score,

            run_from_first_pct,
            drawdown_pct,

            swaps_30s,
            swaps_60s,

            buys_30s,
            buys_60s,
            buys_total,

            created_at,
            last_update_at
        )

        VALUES (
            ?,?,?,?,?,?,
            ?,?,
            ?,?,
            ?,?,?,
            ?,?
        )
        """, (

            r["id"],

            r["token_mint"],
            r["dump_level"],

            r["trigger_timestamp"],

            time.time(),

            score,

            r["run_from_first_pct"],
            r["drawdown_pct"],

            r["swaps_30s"],
            r["swaps_60s"],

            r["buys_30s"],
            r["buys_60s"],
            r["buys_total"],

            time.time(),
            time.time(),
        ))

    db.commit()


# ============================================================
# PRICE PATH FROM CLEAN T116 DATA
# ============================================================

def clean_table():

    if table_exists(
        "t116_clean_swaps"
    ):
        return "t116_clean_swaps"

    raise RuntimeError(
        "t116_clean_swaps missing"
    )


CLEAN = clean_table()


def price_rows(
    mint,
    start,
    end
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
        AND timestamp <= ?

    ORDER BY timestamp ASC
    """, (
        mint,
        start,
        end
    )).fetchall()


# ============================================================
# UPDATE 300s OUTCOMES
# ============================================================

def update_outcomes():

    rows = db.execute(f"""
    SELECT *
    FROM {HOLDOUT}

    WHERE path_done_300s=0
    """).fetchall()

    now = time.time()

    for r in rows:

        trigger = r[
            "trigger_timestamp"
        ]

        if now < (
            trigger + 300
        ):
            continue

        source = db.execute(f"""
        SELECT trigger_price
        FROM {DUMP_EVENTS}
        WHERE id=?
        """, (
            r["dump_event_id"],
        )).fetchone()

        if (
            not source
            or not valid(
                source["trigger_price"]
            )
            or source["trigger_price"] <= 0
        ):
            continue

        entry = source[
            "trigger_price"
        ]

        path = price_rows(
            r["token_mint"],
            trigger,
            trigger + 300
        )

        if not path:

            db.execute(f"""
            UPDATE {HOLDOUT}

            SET
                path_done_300s=1,
                path_snapshots_300s=0,
                last_update_at=?

            WHERE dump_event_id=?
            """, (
                time.time(),
                r["dump_event_id"]
            ))

            continue

        returns = []

        for p in path:

            ret = (
                100.0
                * (
                    p["clean_price_sol"]
                    / entry
                    - 1.0
                )
            )

            returns.append(
                (
                    p["timestamp"],
                    ret
                )
            )

        max_row = max(
            returns,
            key=lambda x: x[1]
        )

        min_row = min(
            returns,
            key=lambda x: x[1]
        )

        max300 = max_row[1]
        min300 = min_row[1]
        end300 = returns[-1][1]

        def first_hit(level):

            for ts, ret in returns:

                if ret >= level:

                    return (
                        1,
                        ts - trigger
                    )

            return (
                0,
                None
            )

        hit10, t10 = first_hit(10)
        hit20, t20 = first_hit(20)
        hit30, t30 = first_hit(30)
        hit50, t50 = first_hit(50)

        db.execute(f"""
        UPDATE {HOLDOUT}

        SET
            path_done_300s=1,

            path_snapshots_300s=?,

            max300=?,
            min300=?,
            end300=?,

            hit10=?,
            hit20=?,
            hit30=?,
            hit50=?,

            time_to_10s=?,
            time_to_20s=?,
            time_to_30s=?,
            time_to_50s=?,

            time_to_max_s=?,

            last_update_at=?

        WHERE dump_event_id=?
        """, (

            len(path),

            max300,
            min300,
            end300,

            hit10,
            hit20,
            hit30,
            hit50,

            t10,
            t20,
            t30,
            t50,

            max_row[0]
            - trigger,

            time.time(),

            r["dump_event_id"],
        ))

    db.commit()


# ============================================================
# MIGRATION LINK
# ============================================================

def update_migrations():

    rows = db.execute(f"""
    SELECT *
    FROM {HOLDOUT}
    WHERE migrated_after_event=0
    """).fetchall()

    for r in rows:

        m = db.execute(f"""
        SELECT
            COALESCE(
                block_time,
                detected_at
            ) AS migration_ts

        FROM {MIGRATIONS}

        WHERE
            token_mint=?
            AND status='OK'

        ORDER BY
            COALESCE(
                block_time,
                detected_at
            ) ASC

        LIMIT 1
        """, (
            r["token_mint"],
        )).fetchone()

        if not m:
            continue

        mts = m[
            "migration_ts"
        ]

        if (
            mts is None
            or mts < r[
                "trigger_timestamp"
            ]
        ):
            continue

        db.execute(f"""
        UPDATE {HOLDOUT}

        SET
            migrated_after_event=1,

            migration_timestamp=?,

            time_to_migration_s=?,

            last_update_at=?

        WHERE dump_event_id=?
        """, (

            mts,

            mts
            - r[
                "trigger_timestamp"
            ],

            time.time(),

            r["dump_event_id"],
        ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show(
    frozen_at
):

    os.system("clear")

    discovery_rows = int(
        get_meta(
            "discovery_rows"
        )
    )

    rows = db.execute(f"""
    SELECT *
    FROM {HOLDOUT}
    ORDER BY trigger_timestamp DESC
    """).fetchall()

    mature = [
        r
        for r in rows
        if (
            r[
                "path_done_300s"
            ] == 1
            and r[
                "path_snapshots_300s"
            ] is not None
            and r[
                "path_snapshots_300s"
            ] >= 1
        )
    ]

    hits20 = sum(
        r["hit20"] == 1
        for r in mature
    )

    print("=" * 190)

    print(
        "MEMECOIN LAB — T120 FROZEN PROSPECTIVE "
        "PRE-MIGRATION RESURRECTION HOLDOUT"
    )

    print("=" * 190)

    print(
        "STATUS            : 🔒 FROZEN"
    )

    print(
        "FAMILY            : STRUCTURE+ACTIVITY+BUY"
    )

    print(
        "FEATURES          : "
        + ", ".join(
            FEATURES
        )
    )

    print(
        "TARGET            : +20% within 300s"
    )

    print(
        "REFIT             : NONE"
    )

    print(
        "THRESHOLD SEARCH  : NONE"
    )

    print(
        "ENTRY RULE        : NONE"
    )

    print()

    print(
        "FROZEN AT         : "
        + time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(
                frozen_at
            )
        )
    )

    print(
        f"DISCOVERY ROWS     : "
        f"{discovery_rows}"
    )

    print(
        f"HOLDOUT ROWS       : "
        f"{len(rows)}"
    )

    print(
        f"MATURE 300         : "
        f"{len(mature)}"
    )

    print(
        f"HIT +20            : "
        f"{hits20}/{len(mature)}"
        if mature
        else "HIT +20            : 0/0"
    )

    print(
        f"MIGRATED AFTER     : "
        f"{sum(r['migrated_after_event']==1 for r in rows)}"
    )


    # ========================================================
    # RECENT HOLDOUT
    # ========================================================

    print()
    print("=" * 190)
    print("PROSPECTIVE HOLDOUT")
    print("=" * 190)

    for r in rows[:35]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| SCORE={r['frozen_score']:7.3f} "
            f"| MAX300={fmt(r['max300'],1):>7}% "
            f"| MIN300={fmt(r['min300'],1):>7}% "
            f"| END300={fmt(r['end300'],1):>7}% "
            f"| +20={str(r['hit20']):>4} "
            f"| T20={fmt(r['time_to_20s'],0):>5}s "
            f"| MIG={r['migrated_after_event']}"
        )


    # ========================================================
    # SCORE QUARTILES
    # ========================================================

    print()
    print("=" * 190)
    print("FROZEN SCORE QUARTILES — PROSPECTIVE ONLY")
    print("=" * 190)

    if len(mature) >= 20:

        ordered = sorted(
            mature,
            key=lambda r:
                r[
                    "frozen_score"
                ]
        )

        n = len(ordered)

        print(
            f"{'Q':<5}"
            f"{'N':>8}"
            f"{'+20':>12}"
            f"{'MAX AVG':>12}"
            f"{'MIN AVG':>12}"
            f"{'END AVG':>12}"
        )

        for i in range(4):

            a = int(
                n * i / 4
            )

            b = int(
                n * (
                    i + 1
                ) / 4
            )

            part = ordered[
                a:b
            ]

            if not part:
                continue

            rate = (
                sum(
                    r["hit20"] == 1
                    for r in part
                )
                / len(part)
            )

            print(
                f"Q{i+1:<4}"
                f"{len(part):>8}"
                f"{100*rate:>11.1f}%"
                f"{fmt(mean([r['max300'] for r in part]),1):>12}"
                f"{fmt(mean([r['min300'] for r in part]),1):>12}"
                f"{fmt(mean([r['end300'] for r in part]),1):>12}"
            )

    else:

        print(
            "Need >=20 mature holdout rows "
            "for quartile display."
        )


    # ========================================================
    # READINESS
    # ========================================================

    print()
    print("=" * 190)
    print("READINESS")
    print("=" * 190)

    print(
        f"MATURE HOLDOUT : "
        f"{len(mature)}"
    )

    if len(mature) < 50:

        print(
            f"🔵 CHECKPOINT 1: "
            f"{50-len(mature)} MORE TO 50"
        )

    elif len(mature) < 100:

        print(
            "🟢 CHECKPOINT 1 REACHED"
        )

        print(
            f"🔵 CHECKPOINT 2: "
            f"{100-len(mature)} MORE TO 100"
        )

    else:

        print(
            "🟢 100+ PROSPECTIVE CASES AVAILABLE"
        )

        print(
            "Ready for frozen holdout evaluation."
        )

    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T120 only"
    )


# ============================================================
# MAIN LOOP
# ============================================================

try:

    frozen_at = freeze_if_needed()

    spec = frozen_spec()

    if len(spec) != len(
        FEATURES
    ):

        raise RuntimeError(
            "Frozen feature spec incomplete"
        )

    while True:

        ensure_holdout_rows(
            frozen_at,
            spec
        )

        update_outcomes()

        update_migrations()

        show(
            frozen_at
        )

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T120 stopped safely."
    )


finally:

    db.close()
