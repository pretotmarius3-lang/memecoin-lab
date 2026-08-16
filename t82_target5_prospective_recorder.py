#!/usr/bin/env python3

import sqlite3
import json
import math
import time
import hashlib
from pathlib import Path

DB = "validation_v090.db"

FREEZE_FILE = Path(
    "t82_target5_frozen.json"
)

TABLE = "t82_target5_prospective"

REFRESH_SEC = 10


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int,float))
        and math.isfinite(x)
    )


# ============================================================
# LOAD / VERIFY FREEZE
# ============================================================

if not FREEZE_FILE.exists():

    raise RuntimeError(
        "Missing t82_target5_frozen.json"
    )


freeze = json.loads(
    FREEZE_FILE.read_text()
)


stored_hash = freeze[
    "freeze_sha256"
]


copy = dict(
    freeze
)

copy.pop(
    "freeze_sha256",
    None
)


canonical = json.dumps(
    copy,
    sort_keys=True,
    separators=(",", ":")
).encode()


computed_hash = hashlib.sha256(
    canonical
).hexdigest()


if stored_hash != computed_hash:

    raise RuntimeError(
        "T82 FREEZE HASH MISMATCH"
    )


boundary_id = int(
    freeze["boundary_id"]
)


run_threshold = float(
    freeze[
        "target"
    ][
        "runner_threshold"
    ]
)


dump_threshold = float(
    freeze[
        "target"
    ][
        "dump_threshold"
    ]
)


# ============================================================
# CLASSIFICATION
# ============================================================

def classify(r60):

    if not valid(r60):

        return (
            "WAIT",
            None,
            0
        )

    if r60 >= run_threshold:

        return (
            "RUN",
            1,
            1
        )

    if r60 <= dump_threshold:

        return (
            "DUMP",
            0,
            1
        )

    return (
        "NEUTRAL",
        None,
        1
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


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,

    event_timestamp REAL NOT NULL,
    captured_at REAL NOT NULL,

    boundary_id INTEGER NOT NULL,
    freeze_sha256 TEXT NOT NULL,

    dex_return_60s REAL,

    status TEXT,
    binary_label INTEGER,
    labeled_60 INTEGER DEFAULT 0
)
""")


db.commit()


# ============================================================
# CAPTURE
# ============================================================

def capture_new():

    rows = db.execute(f"""
    SELECT
        e.id,
        e.timestamp,
        e.token_mint,
        e.dex_return_60s

    FROM events e

    LEFT JOIN {TABLE} t
        ON t.event_id=e.id

    WHERE
        e.id > ?
        AND t.event_id IS NULL
        AND e.token_mint IS NOT NULL
        AND e.timestamp IS NOT NULL

    ORDER BY e.id
    """, (
        boundary_id,
    )).fetchall()


    added = 0


    for r in rows:

        if r["id"] <= boundary_id:

            raise RuntimeError(
                "T82 boundary violation"
            )


        status, binary, labeled = classify(
            r["dex_return_60s"]
        )


        db.execute(f"""
        INSERT OR IGNORE INTO {TABLE} (

            event_id,
            token_mint,

            event_timestamp,
            captured_at,

            boundary_id,
            freeze_sha256,

            dex_return_60s,

            status,
            binary_label,
            labeled_60
        )

        VALUES (
            ?, ?,
            ?, ?,
            ?, ?,
            ?,
            ?, ?, ?
        )
        """, (

            r["id"],
            r["token_mint"],

            r["timestamp"],
            time.time(),

            boundary_id,
            stored_hash,

            r["dex_return_60s"],

            status,
            binary,
            labeled
        ))


        added += 1


    db.commit()

    return added


# ============================================================
# UPDATE WAITING LABELS
# ============================================================

def refresh_labels():

    waiting = db.execute(f"""
    SELECT
        event_id

    FROM {TABLE}

    WHERE
        labeled_60=0
    """).fetchall()


    updated = 0


    for r in waiting:

        source = db.execute("""
        SELECT dex_return_60s
        FROM events
        WHERE id=?
        """, (
            r["event_id"],
        )).fetchone()


        if source is None:
            continue


        status, binary, labeled = classify(
            source["dex_return_60s"]
        )


        if labeled:

            db.execute(f"""
            UPDATE {TABLE}

            SET
                dex_return_60s=?,
                status=?,
                binary_label=?,
                labeled_60=?

            WHERE event_id=?
            """, (

                source["dex_return_60s"],
                status,
                binary,
                labeled,

                r["event_id"]
            ))


            updated += 1


    db.commit()

    return updated


# ============================================================
# DISPLAY
# ============================================================

def show():

    r = db.execute(f"""
    SELECT
        COUNT(*) AS rows,

        COUNT(
            DISTINCT token_mint
        ) AS tokens,

        SUM(
            CASE WHEN status='RUN'
            THEN 1 ELSE 0 END
        ) AS run,

        SUM(
            CASE WHEN status='DUMP'
            THEN 1 ELSE 0 END
        ) AS dump,

        SUM(
            CASE WHEN status='NEUTRAL'
            THEN 1 ELSE 0 END
        ) AS neutral,

        SUM(
            CASE WHEN status='WAIT'
            THEN 1 ELSE 0 END
        ) AS wait

    FROM {TABLE}
    """).fetchone()


    latest = db.execute(f"""
    SELECT
        event_id,
        token_mint,
        dex_return_60s,
        status

    FROM {TABLE}

    ORDER BY event_id DESC

    LIMIT 15
    """).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 140)

    print(
        "MEMECOIN LAB — T82 ±5% PROSPECTIVE TARGET RECORDER"
    )

    print("=" * 140)

    print(
        f"BOUNDARY ID      : {boundary_id}"
    )

    print(
        f"FREEZE HASH      : {stored_hash}"
    )

    print()

    print(
        f"ROWS             : {r['rows'] or 0}"
    )

    print(
        f"TOKENS           : {r['tokens'] or 0}"
    )

    print()

    print(
        f"RUN >= +5%       : {r['run'] or 0}"
    )

    print(
        f"DUMP <= -5%      : {r['dump'] or 0}"
    )

    print(
        f"NEUTRAL          : {r['neutral'] or 0}"
    )

    print(
        f"WAIT             : {r['wait'] or 0}"
    )

    print()

    print(
        "T59              : UNTOUCHED"
    )

    print(
        "T78              : UNTOUCHED"
    )

    print(
        "MODEL FITTING    : NONE"
    )

    print(
        "THRESHOLD SEARCH : NONE"
    )


    print()
    print("=" * 140)
    print("LATEST")
    print("=" * 140)


    for x in latest:

        r60 = (
            "NA"
            if not valid(
                x["dex_return_60s"]
            )
            else f"{x['dex_return_60s']:+.2f}%"
        )

        print(
            f"ID={x['event_id']:5d} "
            f"| R60={r60:>8} "
            f"| {str(x['status']):7} "
            f"| {x['token_mint'][:28]}"
        )


    print()
    print(
        f"Refresh every {REFRESH_SEC}s."
    )

    print(
        "CTRL+C stops T82 only."
    )


# ============================================================
# PREFLIGHT
# ============================================================

bad = db.execute(f"""
SELECT COUNT(*)

FROM {TABLE}

WHERE
    event_id <= ?
    OR boundary_id != ?
    OR freeze_sha256 != ?
""", (
    boundary_id,
    boundary_id,
    stored_hash,
)).fetchone()[0]


if bad:

    raise RuntimeError(
        f"T82 integrity violation: {bad} rows"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        capture_new()

        refresh_labels()

        show()

        time.sleep(
            REFRESH_SEC
        )


except KeyboardInterrupt:

    print()
    print(
        "T82 stopped safely."
    )


finally:

    db.close()
