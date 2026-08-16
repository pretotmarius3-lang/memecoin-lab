#!/usr/bin/env python3

import sqlite3
import time
import math
import hashlib
import json
from pathlib import Path

DB = "validation_v090.db"

TABLE = "t100_dex_market_structure_prospective"
META = "t100_dex_market_structure_meta"

REFRESH = 5

OFFSETS = [
    0,
    30,
    60,
    300,
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
# CREATE META
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {META} (

    id INTEGER PRIMARY KEY CHECK (id=1),

    created_at REAL NOT NULL,

    boundary_id INTEGER NOT NULL,

    freeze_hash TEXT NOT NULL,

    offsets_json TEXT NOT NULL,

    matching_rule TEXT NOT NULL,

    mode TEXT NOT NULL
)
""")


existing_meta = db.execute(f"""
SELECT *
FROM {META}
WHERE id=1
""").fetchone()


if existing_meta is None:

    boundary = db.execute("""
    SELECT COALESCE(MAX(id),0)
    FROM events
    """).fetchone()[0]


    freeze_material = {
        "experiment":
            "T100_DEX_MARKET_STRUCTURE_PROSPECTIVE",

        "boundary_id":
            int(boundary),

        "offsets":
            OFFSETS,

        "matching_rule":
            "token_mint + latest dex_prices snapshot at or before target timestamp",

        "future_snapshots":
            "forbidden",

        "historical_backfill":
            False,

        "model_fitting":
            False,

        "threshold_search":
            False,
    }


    canonical = json.dumps(
        freeze_material,
        sort_keys=True,
        separators=(",", ":")
    ).encode()


    freeze_hash = hashlib.sha256(
        canonical
    ).hexdigest()


    db.execute(f"""
    INSERT INTO {META} (
        id,
        created_at,
        boundary_id,
        freeze_hash,
        offsets_json,
        matching_rule,
        mode
    )
    VALUES (
        1,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    )
    """, (
        time.time(),
        int(boundary),
        freeze_hash,
        json.dumps(OFFSETS),
        freeze_material["matching_rule"],
        "PROSPECTIVE_ONLY",
    ))


    db.commit()


meta = db.execute(f"""
SELECT *
FROM {META}
WHERE id=1
""").fetchone()


BOUNDARY = int(
    meta["boundary_id"]
)

FREEZE_HASH = meta[
    "freeze_hash"
]


# ============================================================
# CREATE STORAGE
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    event_id INTEGER NOT NULL,

    token_mint TEXT NOT NULL,

    event_timestamp REAL NOT NULL,

    target_offset INTEGER NOT NULL,

    target_timestamp REAL NOT NULL,

    captured_at REAL NOT NULL,

    boundary_id INTEGER NOT NULL,

    freeze_hash TEXT NOT NULL,

    matched INTEGER NOT NULL DEFAULT 0,

    dex_price_row_id INTEGER,

    dex_timestamp REAL,

    snapshot_age REAL,

    price_usd REAL,
    price_native REAL,

    liquidity_usd REAL,
    market_cap REAL,
    fdv REAL,

    volume_m5 REAL,
    buys_m5 INTEGER,
    sells_m5 INTEGER,

    pair_address TEXT,
    dex_id TEXT,

    status TEXT,

    PRIMARY KEY (
        event_id,
        target_offset
    )
)
""")


db.commit()


# ============================================================
# MATCHER
# ============================================================

def find_snapshot(
    token,
    target_ts
):

    row = db.execute("""
    SELECT
        id,
        timestamp,

        price_usd,
        price_native,

        liquidity_usd,
        market_cap,
        fdv,

        volume_m5,
        buys_m5,
        sells_m5,

        pair_address,
        dex_id

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp <= ?

    ORDER BY
        timestamp DESC,
        id DESC

    LIMIT 1
    """, (
        token,
        target_ts
    )).fetchone()


    if row is None:
        return None


    return row


# ============================================================
# ENSURE EVENT/OFFSET ROWS
# ============================================================

def capture_new_events():

    events = db.execute("""
    SELECT
        id,
        timestamp,
        token_mint

    FROM events

    WHERE
        id > ?
        AND timestamp IS NOT NULL
        AND token_mint IS NOT NULL

    ORDER BY id
    """, (
        BOUNDARY,
    )).fetchall()


    inserted = 0


    for e in events:

        for offset in OFFSETS:

            target_ts = (
                e["timestamp"]
                + offset
            )


            cur = db.execute(f"""
            INSERT OR IGNORE INTO {TABLE} (

                event_id,
                token_mint,
                event_timestamp,

                target_offset,
                target_timestamp,

                captured_at,

                boundary_id,
                freeze_hash,

                matched,
                status
            )

            VALUES (
                ?, ?, ?,
                ?, ?,
                ?,
                ?, ?,
                0,
                'WAIT'
            )
            """, (

                e["id"],
                e["token_mint"],
                e["timestamp"],

                offset,
                target_ts,

                time.time(),

                BOUNDARY,
                FREEZE_HASH,
            ))


            inserted += (
                cur.rowcount
                if cur.rowcount is not None
                else 0
            )


    db.commit()

    return inserted


# ============================================================
# RESOLVE WAITING SNAPSHOTS
# ============================================================

def resolve_waiting():

    now = time.time()


    rows = db.execute(f"""
    SELECT
        event_id,
        token_mint,
        target_offset,
        target_timestamp

    FROM {TABLE}

    WHERE
        matched=0
        AND status='WAIT'
        AND target_timestamp <= ?

    ORDER BY
        target_timestamp,
        event_id,
        target_offset
    """, (
        now,
    )).fetchall()


    updated = 0


    for r in rows:

        snap = find_snapshot(
            r["token_mint"],
            r["target_timestamp"]
        )


        if snap is None:

            db.execute(f"""
            UPDATE {TABLE}

            SET
                captured_at=?,
                status='NO_SNAPSHOT'

            WHERE
                event_id=?
                AND target_offset=?
            """, (
                time.time(),
                r["event_id"],
                r["target_offset"],
            ))

            updated += 1

            continue


        age = (
            r["target_timestamp"]
            - snap["timestamp"]
        )


        if age < -1e-9:

            raise RuntimeError(
                "Future snapshot leak detected."
            )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            captured_at=?,

            matched=1,

            dex_price_row_id=?,
            dex_timestamp=?,
            snapshot_age=?,

            price_usd=?,
            price_native=?,

            liquidity_usd=?,
            market_cap=?,
            fdv=?,

            volume_m5=?,
            buys_m5=?,
            sells_m5=?,

            pair_address=?,
            dex_id=?,

            status='MATCHED'

        WHERE
            event_id=?
            AND target_offset=?
        """, (

            time.time(),

            snap["id"],
            snap["timestamp"],
            age,

            snap["price_usd"],
            snap["price_native"],

            snap["liquidity_usd"],
            snap["market_cap"],
            snap["fdv"],

            snap["volume_m5"],
            snap["buys_m5"],
            snap["sells_m5"],

            snap["pair_address"],
            snap["dex_id"],

            r["event_id"],
            r["target_offset"],
        ))


        updated += 1


    db.commit()

    return updated


# ============================================================
# DISPLAY
# ============================================================

def show():

    total = db.execute(f"""
    SELECT
        COUNT(*) AS n
    FROM {TABLE}
    """).fetchone()["n"]


    events = db.execute(f"""
    SELECT
        COUNT(
            DISTINCT event_id
        ) AS n
    FROM {TABLE}
    """).fetchone()["n"]


    tokens = db.execute(f"""
    SELECT
        COUNT(
            DISTINCT token_mint
        ) AS n
    FROM {TABLE}
    """).fetchone()["n"]


    matched = db.execute(f"""
    SELECT
        COUNT(*) AS n
    FROM {TABLE}
    WHERE matched=1
    """).fetchone()["n"]


    wait = db.execute(f"""
    SELECT
        COUNT(*) AS n
    FROM {TABLE}
    WHERE status='WAIT'
    """).fetchone()["n"]


    no_snapshot = db.execute(f"""
    SELECT
        COUNT(*) AS n
    FROM {TABLE}
    WHERE status='NO_SNAPSHOT'
    """).fetchone()["n"]


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 150)

    print(
        "MEMECOIN LAB — T100A DEX MARKET-STRUCTURE PROSPECTIVE RECORDER"
    )

    print("=" * 150)

    print(
        f"BOUNDARY ID      : {BOUNDARY}"
    )

    print(
        f"FREEZE HASH      : {FREEZE_HASH}"
    )

    print()

    print(
        f"ROWS             : {total}"
    )

    print(
        f"EVENTS           : {events}"
    )

    print(
        f"TOKENS           : {tokens}"
    )

    print()

    print(
        f"MATCHED          : {matched}"
    )

    print(
        f"WAIT             : {wait}"
    )

    print(
        f"NO SNAPSHOT      : {no_snapshot}"
    )

    print()

    print(
        "OFFSETS          : 0 / +30 / +60 / +300s"
    )

    print(
        "MATCHING         : token + latest snapshot <= target time"
    )

    print(
        "FUTURE SNAPSHOT  : FORBIDDEN"
    )

    print(
        "HIST BACKFILL    : FORBIDDEN"
    )

    print(
        "MODEL FITTING    : NONE"
    )

    print(
        "T59/T78/T82/T86  : UNTOUCHED"
    )

    print()

    print(
        f"Refresh every {REFRESH}s."
    )

    print(
        "CTRL+C stops T100A only."
    )


# ============================================================
# INTEGRITY
# ============================================================

def integrity():

    bad = db.execute(f"""
    SELECT COUNT(*)

    FROM {TABLE}

    WHERE
        event_id <= ?
        OR boundary_id != ?
        OR freeze_hash != ?
        OR (
            matched=1
            AND dex_timestamp > target_timestamp
        )
    """, (
        BOUNDARY,
        BOUNDARY,
        FREEZE_HASH,
    )).fetchone()[0]


    if bad:

        raise RuntimeError(
            f"T100 integrity violation: {bad} rows"
        )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        capture_new_events()

        resolve_waiting()

        integrity()

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()

    print(
        "T100A stopped safely."
    )


finally:

    db.close()
