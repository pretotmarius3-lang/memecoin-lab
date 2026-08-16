#!/usr/bin/env python3

import sqlite3
import json
import math
import time
import hashlib
from pathlib import Path

DB = "validation_v090.db"

FREEZE_FILE = Path(
    "t78_capv2_buyervel10_frozen.json"
)

TABLE = "t78_capv2_buyervel10_prospective"

REFRESH_SEC = 10


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def sigmoid(z):

    z = max(
        min(z, 35.0),
        -35.0
    )

    return (
        1.0
        / (
            1.0
            + math.exp(-z)
        )
    )


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# LOAD / VERIFY FREEZE
# ============================================================

if not FREEZE_FILE.exists():

    raise RuntimeError(
        "Missing t78_capv2_buyervel10_frozen.json"
    )


freeze = json.loads(
    FREEZE_FILE.read_text()
)


stored_hash = freeze.get(
    "freeze_sha256"
)


hash_copy = dict(
    freeze
)

hash_copy.pop(
    "freeze_sha256",
    None
)


canonical = json.dumps(
    hash_copy,
    sort_keys=True,
    separators=(",", ":"),
).encode()


computed_hash = hashlib.sha256(
    canonical
).hexdigest()


if stored_hash != computed_hash:

    raise RuntimeError(
        "T78 FREEZE HASH MISMATCH. "
        "Frozen specification appears modified."
    )


boundary_id = int(
    freeze[
        "boundary_id"
    ]
)


model = freeze[
    "model"
]


means = freeze[
    "standardization"
][
    "means"
]


stds = freeze[
    "standardization"
][
    "stds"
]


WINDOW = float(
    freeze[
        "definitions"
    ][
        "buyer_window_seconds"
    ]
)


# ============================================================
# SCORE
# ============================================================

def score_model(values):

    z = model[
        "intercept"
    ]

    for feature in model[
        "features"
    ]:

        x = values.get(
            feature
        )

        if not valid(x):
            return None

        mu = means[
            feature
        ]

        sd = stds[
            feature
        ]

        if (
            not valid(sd)
            or abs(sd) < 1e-12
        ):
            sd = 1.0

        zx = (
            x - mu
        ) / sd

        z += (
            model[
                "coefficients"
            ][
                feature
            ]
            * zx
        )

    return sigmoid(z)


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

    fa REAL,
    new_wallets30 REAL,
    recent_buy_share REAL,
    recent_net_share REAL,
    breadth_score REAL,
    late_chase_score REAL,

    early_price_return REAL,
    early_net_sol REAL,
    early_div REAL,

    buyer_velocity_10 REAL,
    buyer_unique_30 INTEGER,

    t78_score REAL,

    dex_return_60s REAL,

    status TEXT,
    binary_label INTEGER,
    labeled_60 INTEGER DEFAULT 0
)
""")


db.commit()


# ============================================================
# OUTCOME
# ============================================================

def classify(r60):

    if not valid(r60):

        return (
            "WAIT",
            None,
            0
        )

    if r60 >= 10.0:

        return (
            "RUN",
            1,
            1
        )

    if r60 <= -10.0:

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
# BUYER VELOCITY 10
# ============================================================

def buyer_velocity_10(
    token_mint,
    event_ts
):

    buys = db.execute("""
    SELECT
        timestamp,
        wallet

    FROM swaps

    WHERE
        token_mint=?
        AND side='BUY'
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL

    ORDER BY timestamp
    """, (
        token_mint,
        event_ts-WINDOW,
        event_ts
    )).fetchall()


    first_by_wallet = {}


    for r in buys:

        first_by_wallet.setdefault(
            r["wallet"],
            r["timestamp"]
        )


    arrivals = sorted(
        first_by_wallet.values()
    )


    buyer_unique_30 = len(
        arrivals
    )


    if len(arrivals) < 2:

        return (
            None,
            buyer_unique_30
        )


    n10 = sum(
        t >= event_ts-10.0
        for t in arrivals
    )


    velocity = (
        n10 / 10.0
    )


    return (
        velocity,
        buyer_unique_30
    )


# ============================================================
# BUILD EVENT
# ============================================================

def build_event(event_id):

    r = db.execute("""
    SELECT
        e.id,
        e.timestamp,
        e.token_mint,

        e.fa,
        e.new_wallets30,
        e.dex_return_60s,

        s.recent_buy_share,
        s.recent_net_share,
        s.breadth_score,
        s.late_chase_score,

        s.early_price_return,
        s.early_net_sol

    FROM events e

    JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

    WHERE
        e.id=?
    """, (
        event_id,
    )).fetchone()


    if r is None:
        return None


    if r["id"] <= boundary_id:

        raise RuntimeError(
            f"T78 boundary violation: event {r['id']} "
            f"<= {boundary_id}"
        )


    early_div = None


    if (
        valid(
            r["early_price_return"]
        )
        and valid(
            r["early_net_sol"]
        )
    ):

        early_div = (
            r["early_price_return"]
            - r["early_net_sol"]
        )


    vel10, unique30 = buyer_velocity_10(
        r["token_mint"],
        r["timestamp"]
    )


    values = {
        "fa":
            r["fa"],

        "new_wallets30":
            r["new_wallets30"],

        "recent_buy_share":
            r["recent_buy_share"],

        "recent_net_share":
            r["recent_net_share"],

        "breadth_score":
            r["breadth_score"],

        "late_chase_score":
            r["late_chase_score"],

        "early_div":
            early_div,

        "buyer_velocity_10":
            vel10,
    }


    score = score_model(
        values
    )


    status, binary, labeled = classify(
        r["dex_return_60s"]
    )


    return {
        "event_id":
            r["id"],

        "token_mint":
            r["token_mint"],

        "event_timestamp":
            r["timestamp"],

        "fa":
            r["fa"],

        "new_wallets30":
            r["new_wallets30"],

        "recent_buy_share":
            r["recent_buy_share"],

        "recent_net_share":
            r["recent_net_share"],

        "breadth_score":
            r["breadth_score"],

        "late_chase_score":
            r["late_chase_score"],

        "early_price_return":
            r["early_price_return"],

        "early_net_sol":
            r["early_net_sol"],

        "early_div":
            early_div,

        "buyer_velocity_10":
            vel10,

        "buyer_unique_30":
            unique30,

        "t78_score":
            score,

        "dex_return_60s":
            r["dex_return_60s"],

        "status":
            status,

        "binary_label":
            binary,

        "labeled_60":
            labeled,
    }


# ============================================================
# CAPTURE
# ============================================================

def capture_new():

    ids = db.execute(f"""
    SELECT e.id

    FROM events e

    LEFT JOIN {TABLE} t
        ON t.event_id=e.id

    WHERE
        e.id > ?
        AND t.event_id IS NULL

    ORDER BY e.id
    """, (
        boundary_id,
    )).fetchall()


    added = 0


    for row in ids:

        item = build_event(
            row["id"]
        )

        if item is None:
            continue


        db.execute(f"""
        INSERT OR IGNORE INTO {TABLE} (

            event_id,
            token_mint,

            event_timestamp,
            captured_at,

            boundary_id,
            freeze_sha256,

            fa,
            new_wallets30,
            recent_buy_share,
            recent_net_share,
            breadth_score,
            late_chase_score,

            early_price_return,
            early_net_sol,
            early_div,

            buyer_velocity_10,
            buyer_unique_30,

            t78_score,

            dex_return_60s,

            status,
            binary_label,
            labeled_60
        )

        VALUES (
            ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?,
            ?,
            ?, ?, ?
        )
        """, (

            item["event_id"],
            item["token_mint"],

            item["event_timestamp"],
            time.time(),

            boundary_id,
            stored_hash,

            item["fa"],
            item["new_wallets30"],
            item["recent_buy_share"],
            item["recent_net_share"],
            item["breadth_score"],
            item["late_chase_score"],

            item["early_price_return"],
            item["early_net_sol"],
            item["early_div"],

            item["buyer_velocity_10"],
            item["buyer_unique_30"],

            item["t78_score"],

            item["dex_return_60s"],

            item["status"],
            item["binary_label"],
            item["labeled_60"],
        ))


        added += 1


    db.commit()

    return added


# ============================================================
# REFRESH OUTCOMES
# ============================================================

def refresh_labels():

    rows = db.execute(f"""
    SELECT
        event_id

    FROM {TABLE}

    WHERE
        labeled_60=0
    """).fetchall()


    updated = 0


    for row in rows:

        e = db.execute("""
        SELECT
            dex_return_60s

        FROM events

        WHERE id=?
        """, (
            row["event_id"],
        )).fetchone()


        if e is None:
            continue


        status, binary, labeled = classify(
            e["dex_return_60s"]
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

                e["dex_return_60s"],
                status,
                binary,
                labeled,
                row["event_id"],
            ))


            updated += 1


    db.commit()

    return updated


# ============================================================
# DISPLAY
# ============================================================

def show():

    row = db.execute(f"""
    SELECT
        COUNT(*) AS rows,

        COUNT(
            DISTINCT token_mint
        ) AS tokens,

        SUM(
            CASE
            WHEN status='RUN'
            THEN 1 ELSE 0
            END
        ) AS run,

        SUM(
            CASE
            WHEN status='DUMP'
            THEN 1 ELSE 0
            END
        ) AS dump,

        SUM(
            CASE
            WHEN status='NEUTRAL'
            THEN 1 ELSE 0
            END
        ) AS neutral,

        SUM(
            CASE
            WHEN status='WAIT'
            THEN 1 ELSE 0
            END
        ) AS wait,

        SUM(
            CASE
            WHEN t78_score IS NOT NULL
            THEN 1 ELSE 0
            END
        ) AS scored

    FROM {TABLE}
    """).fetchone()


    latest = db.execute(f"""
    SELECT
        event_id,
        token_mint,

        buyer_velocity_10,
        early_div,
        t78_score,

        status

    FROM {TABLE}

    ORDER BY event_id DESC

    LIMIT 15
    """).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 150)

    print(
        "MEMECOIN LAB — T78 CAP-v2 + BUYER VELOCITY 10 PROSPECTIVE SHADOW"
    )

    print("=" * 150)


    print(
        f"BOUNDARY ID      : {boundary_id}"
    )

    print(
        f"FREEZE HASH      : {stored_hash}"
    )

    print(
        f"ROWS             : {row['rows'] or 0}"
    )

    print(
        f"TOKENS           : {row['tokens'] or 0}"
    )

    print(
        f"SCORED           : {row['scored'] or 0}"
    )

    print()

    print(
        f"RUN              : {row['run'] or 0}"
    )

    print(
        f"DUMP             : {row['dump'] or 0}"
    )

    print(
        f"NEUTRAL          : {row['neutral'] or 0}"
    )

    print(
        f"WAIT             : {row['wait'] or 0}"
    )

    print()

    print(
        "MODEL            : M3_CAPV2_BUYERVEL10"
    )

    print(
        "REFIT            : FORBIDDEN"
    )

    print(
        "THRESHOLD SEARCH : FORBIDDEN"
    )

    print(
        "T59              : UNTOUCHED"
    )


    print()
    print("=" * 150)
    print("LATEST")
    print("=" * 150)


    for r in latest:

        print(
            f"ID={r['event_id']:5d} "
            f"| V10={fmt(r['buyer_velocity_10'],3):>7} "
            f"| EDIV={fmt(r['early_div'],3):>9} "
            f"| SCORE={fmt(r['t78_score'],4):>7} "
            f"| {str(r['status']):7} "
            f"| {r['token_mint'][:24]}"
        )


    print()
    print(
        f"Refresh every {REFRESH_SEC}s."
    )

    print(
        "CTRL+C stops T78 only."
    )


# ============================================================
# PRE-FLIGHT INTEGRITY
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
        f"T78 integrity violation: {bad} bad stored rows."
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
        "T78 stopped safely."
    )


finally:

    db.close()
