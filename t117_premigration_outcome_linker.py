#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

CLEAN = "t116_clean_swaps"
STATE = "t116_token_state"

PUMP_SOURCE = "t116_pump_events"
DUMP_SOURCE = "t116_premigration_dump_events"

PUMP_OUT = "t117_pump_outcomes"
DUMP_OUT = "t117_dump_outcomes"

REFRESH = 10

HORIZONS = [30, 60, 120, 300, 900]


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
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLES
# ============================================================

def horizon_cols(prefix):

    cols = []

    for h in HORIZONS:

        cols.append(f"""
            {prefix}_done_{h}s INTEGER NOT NULL DEFAULT 0,
            {prefix}_end_return_{h}s REAL,
            {prefix}_max_return_{h}s REAL,
            {prefix}_min_return_{h}s REAL,
            {prefix}_new_high_{h}s INTEGER,
            {prefix}_new_low_{h}s INTEGER,
            {prefix}_snapshots_{h}s INTEGER
        """)

    return ",".join(cols)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {PUMP_OUT} (

    t116_pump_event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,
    pump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,
    trigger_price REAL NOT NULL,

    peak_price_at_trigger REAL,

    {horizon_cols("path")},

    migrated INTEGER NOT NULL DEFAULT 0,
    migration_timestamp REAL,
    seconds_to_migration REAL,

    migrated_after_event INTEGER,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {DUMP_OUT} (

    t116_dump_event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,
    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,
    trigger_price REAL NOT NULL,

    peak_price_at_trigger REAL,

    {horizon_cols("path")},

    rebound20_300 INTEGER,
    rebound50_300 INTEGER,
    reclaim_old_peak_300 INTEGER,

    rebound20_900 INTEGER,
    rebound50_900 INTEGER,
    reclaim_old_peak_900 INTEGER,

    migrated INTEGER NOT NULL DEFAULT 0,
    migration_timestamp REAL,
    seconds_to_migration REAL,

    migrated_after_event INTEGER,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# CLEAN PATH
# ============================================================

def path_rows(
    mint,
    start_ts,
    end_ts
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
        start_ts,
        end_ts
    )).fetchall()


def compute_path(
    mint,
    trigger_ts,
    trigger_price,
    old_peak,
    horizon
):

    target = (
        trigger_ts
        + horizon
    )

    if time.time() < target:
        return None


    rows = path_rows(
        mint,
        trigger_ts,
        target
    )


    if not rows:

        return {
            "done": 1,
            "snapshots": 0,
        }


    prices = [
        (
            r["timestamp"],
            r["clean_price_sol"]
        )
        for r in rows
    ]


    high = max(
        p
        for _, p in prices
    )

    low = min(
        p
        for _, p in prices
    )

    end = prices[-1][1]


    max_ret = pct(
        trigger_price,
        high
    )

    min_ret = pct(
        trigger_price,
        low
    )

    end_ret = pct(
        trigger_price,
        end
    )


    new_high = int(
        old_peak is not None
        and high > old_peak
    )


    new_low = int(
        low < trigger_price
    )


    return {
        "done":
            1,

        "snapshots":
            len(rows),

        "end_return":
            end_ret,

        "max_return":
            max_ret,

        "min_return":
            min_ret,

        "new_high":
            new_high,

        "new_low":
            new_low,
    }


# ============================================================
# MIGRATION
# ============================================================

def migration_for(
    mint
):

    return db.execute("""
    SELECT
        COALESCE(
            block_time,
            detected_at
        ) AS migration_ts

    FROM t101_migrations

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
        mint,
    )).fetchone()


# ============================================================
# ENSURE BASE ROWS
# ============================================================

def ensure_pump_rows():

    rows = db.execute(f"""
    SELECT *
    FROM {PUMP_SOURCE}
    """).fetchall()


    for e in rows:

        db.execute(f"""
        INSERT OR IGNORE INTO {PUMP_OUT} (

            t116_pump_event_id,

            token_mint,
            pump_level,

            trigger_timestamp,
            trigger_price,

            peak_price_at_trigger,

            created_at,
            last_update_at
        )

        VALUES (
            ?,?,?,?,?,?,?,?
        )
        """, (

            e["id"],

            e["token_mint"],
            e["pump_level"],

            e["trigger_timestamp"],
            e["trigger_price"],

            e["peak_price"],

            time.time(),
            time.time(),
        ))


    db.commit()


def ensure_dump_rows():

    rows = db.execute(f"""
    SELECT *
    FROM {DUMP_SOURCE}
    """).fetchall()


    for e in rows:

        db.execute(f"""
        INSERT OR IGNORE INTO {DUMP_OUT} (

            t116_dump_event_id,

            token_mint,
            dump_level,

            trigger_timestamp,
            trigger_price,

            peak_price_at_trigger,

            created_at,
            last_update_at
        )

        VALUES (
            ?,?,?,?,?,?,?,?
        )
        """, (

            e["id"],

            e["token_mint"],
            e["dump_level"],

            e["trigger_timestamp"],
            e["trigger_price"],

            e["peak_price"],

            time.time(),
            time.time(),
        ))


    db.commit()


# ============================================================
# UPDATE PATHS
# ============================================================

def update_pump_paths():

    rows = db.execute(f"""
    SELECT *
    FROM {PUMP_OUT}
    """).fetchall()


    for r in rows:

        for h in HORIZONS:

            if r[
                f"path_done_{h}s"
            ]:
                continue


            p = compute_path(

                r["token_mint"],

                r["trigger_timestamp"],

                r["trigger_price"],

                r["peak_price_at_trigger"],

                h
            )


            if p is None:
                continue


            db.execute(f"""
            UPDATE {PUMP_OUT}

            SET
                path_done_{h}s=?,
                path_end_return_{h}s=?,
                path_max_return_{h}s=?,
                path_min_return_{h}s=?,
                path_new_high_{h}s=?,
                path_new_low_{h}s=?,
                path_snapshots_{h}s=?,

                last_update_at=?

            WHERE t116_pump_event_id=?
            """, (

                p["done"],

                p.get(
                    "end_return"
                ),

                p.get(
                    "max_return"
                ),

                p.get(
                    "min_return"
                ),

                p.get(
                    "new_high"
                ),

                p.get(
                    "new_low"
                ),

                p.get(
                    "snapshots"
                ),

                time.time(),

                r[
                    "t116_pump_event_id"
                ],
            ))


    db.commit()


def update_dump_paths():

    rows = db.execute(f"""
    SELECT *
    FROM {DUMP_OUT}
    """).fetchall()


    for r in rows:

        for h in HORIZONS:

            if r[
                f"path_done_{h}s"
            ]:
                continue


            p = compute_path(

                r["token_mint"],

                r["trigger_timestamp"],

                r["trigger_price"],

                r["peak_price_at_trigger"],

                h
            )


            if p is None:
                continue


            db.execute(f"""
            UPDATE {DUMP_OUT}

            SET
                path_done_{h}s=?,
                path_end_return_{h}s=?,
                path_max_return_{h}s=?,
                path_min_return_{h}s=?,
                path_new_high_{h}s=?,
                path_new_low_{h}s=?,
                path_snapshots_{h}s=?,

                last_update_at=?

            WHERE t116_dump_event_id=?
            """, (

                p["done"],

                p.get(
                    "end_return"
                ),

                p.get(
                    "max_return"
                ),

                p.get(
                    "min_return"
                ),

                p.get(
                    "new_high"
                ),

                p.get(
                    "new_low"
                ),

                p.get(
                    "snapshots"
                ),

                time.time(),

                r[
                    "t116_dump_event_id"
                ],
            ))


        latest = db.execute(f"""
        SELECT *
        FROM {DUMP_OUT}
        WHERE t116_dump_event_id=?
        """, (
            r[
                "t116_dump_event_id"
            ],
        )).fetchone()


        updates = {}


        for h in (
            300,
            900,
        ):

            mx = latest[
                f"path_max_return_{h}s"
            ]

            nh = latest[
                f"path_new_high_{h}s"
            ]


            if mx is not None:

                updates[
                    f"rebound20_{h}"
                ] = int(
                    mx >= 20
                )

                updates[
                    f"rebound50_{h}"
                ] = int(
                    mx >= 50
                )


            if nh is not None:

                updates[
                    f"reclaim_old_peak_{h}"
                ] = int(
                    nh == 1
                )


        if updates:

            sql = ", ".join(
                f"{k}=?"
                for k in updates
            )


            db.execute(
                f"""
                UPDATE {DUMP_OUT}

                SET
                    {sql},
                    last_update_at=?

                WHERE t116_dump_event_id=?
                """,
                (
                    *updates.values(),

                    time.time(),

                    r[
                        "t116_dump_event_id"
                    ],
                )
            )


    db.commit()


# ============================================================
# UPDATE MIGRATION
# ============================================================

def update_migrations(
    table,
    id_col
):

    rows = db.execute(f"""
    SELECT *
    FROM {table}
    """).fetchall()


    for r in rows:

        m = migration_for(
            r["token_mint"]
        )


        if not m:
            continue


        migration_ts = m[
            "migration_ts"
        ]


        delay = (
            migration_ts
            - r[
                "trigger_timestamp"
            ]
        )


        db.execute(f"""
        UPDATE {table}

        SET
            migrated=1,

            migration_timestamp=?,

            seconds_to_migration=?,

            migrated_after_event=?,

            last_update_at=?

        WHERE {id_col}=?
        """, (

            migration_ts,

            delay,

            int(
                migration_ts
                >= r[
                    "trigger_timestamp"
                ]
            ),

            time.time(),

            r[
                id_col
            ],
        ))


    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system(
        "clear"
    )


    pumps = db.execute(f"""
    SELECT *

    FROM {PUMP_OUT}

    ORDER BY trigger_timestamp DESC
    """).fetchall()


    dumps = db.execute(f"""
    SELECT *

    FROM {DUMP_OUT}

    ORDER BY trigger_timestamp DESC
    """).fetchall()


    print("=" * 190)

    print(
        "MEMECOIN LAB — T117 PRE-MIGRATION OUTCOME LINKER"
    )

    print("=" * 190)


    print(
        f"PUMP EVENTS       : {len(pumps)}"
    )

    print(
        f"DUMP EVENTS       : {len(dumps)}"
    )

    print(
        f"PUMP→MIGRATED     : "
        f"{sum(r['migrated_after_event'] == 1 for r in pumps)}"
    )

    print(
        f"DUMP→MIGRATED     : "
        f"{sum(r['migrated_after_event'] == 1 for r in dumps)}"
    )


    print()

    print(
        "MODE              : OUTCOME LINKING ONLY"
    )

    print(
        "MODEL FITTING     : NONE"
    )

    print(
        "THRESHOLD SEARCH  : NONE"
    )


    # ========================================================
    # PUMPS
    # ========================================================

    print()
    print("=" * 190)
    print("🔥 PUMP FOLLOW-THROUGH")
    print("=" * 190)


    for r in pumps[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| P=+{r['pump_level']:3d}% "
            f"| MAX300="
            f"{fmt(r['path_max_return_300s']):>7}% "
            f"| MIN300="
            f"{fmt(r['path_min_return_300s']):>7}% "
            f"| END300="
            f"{fmt(r['path_end_return_300s']):>7}% "
            f"| NH300="
            f"{str(r['path_new_high_300s']):>4} "
            f"| MIG="
            f"{r['migrated']} "
            f"| TTM="
            f"{fmt(r['seconds_to_migration'],0):>6}s"
        )


    # ========================================================
    # DUMPS
    # ========================================================

    print()
    print("=" * 190)
    print("🔻 DUMP RESURRECTION")
    print("=" * 190)


    for r in dumps[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| MAX300="
            f"{fmt(r['path_max_return_300s']):>7}% "
            f"| MIN300="
            f"{fmt(r['path_min_return_300s']):>7}% "
            f"| +20="
            f"{str(r['rebound20_300']):>4} "
            f"| +50="
            f"{str(r['rebound50_300']):>4} "
            f"| PEAK="
            f"{str(r['reclaim_old_peak_300']):>4} "
            f"| MIG="
            f"{r['migrated']} "
            f"| TTM="
            f"{fmt(r['seconds_to_migration'],0):>6}s"
        )


    # ========================================================
    # SUMMARY
    # ========================================================

    print()
    print("=" * 190)
    print("300s SUMMARY")
    print("=" * 190)


    mature_pumps = [
        r
        for r in pumps
        if r[
            "path_done_300s"
        ]
    ]


    mature_dumps = [
        r
        for r in dumps
        if r[
            "path_done_300s"
        ]
    ]


    print(
        f"MATURE PUMPS      : {len(mature_pumps)}"
    )

    print(
        f"MATURE DUMPS      : {len(mature_dumps)}"
    )


    if mature_dumps:

        rebound20 = sum(
            r[
                "rebound20_300"
            ] == 1
            for r in mature_dumps
        )

        rebound50 = sum(
            r[
                "rebound50_300"
            ] == 1
            for r in mature_dumps
        )

        peak = sum(
            r[
                "reclaim_old_peak_300"
            ] == 1
            for r in mature_dumps
        )


        print(
            f"DUMP +20/300      : "
            f"{rebound20}/{len(mature_dumps)}"
        )

        print(
            f"DUMP +50/300      : "
            f"{rebound50}/{len(mature_dumps)}"
        )

        print(
            f"DUMP RECLAIM PEAK : "
            f"{peak}/{len(mature_dumps)}"
        )


    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T117 only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        ensure_pump_rows()

        ensure_dump_rows()

        update_pump_paths()

        update_dump_paths()

        update_migrations(
            PUMP_OUT,
            "t116_pump_event_id"
        )

        update_migrations(
            DUMP_OUT,
            "t116_dump_event_id"
        )

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()

    print(
        "T117 stopped safely."
    )


finally:

    db.close()
