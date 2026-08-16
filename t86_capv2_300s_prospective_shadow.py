#!/usr/bin/env python3

import sqlite3
import json
import math
import time
import hashlib
from pathlib import Path

DB = "validation_v090.db"
FREEZE_FILE = Path("t86_capv2_300s_frozen.json")
TABLE = "t86_capv2_300s_prospective"
REFRESH = 10


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def sigmoid(z):
    z = max(min(z, 35.0), -35.0)
    return 1.0 / (1.0 + math.exp(-z))


# ============================================================
# LOAD + VERIFY FREEZE
# ============================================================

freeze = json.loads(
    FREEZE_FILE.read_text()
)

stored_hash = freeze["freeze_sha256"]

check = dict(freeze)
check.pop("freeze_sha256")

canonical = json.dumps(
    check,
    sort_keys=True,
    separators=(",", ":")
).encode()

computed_hash = hashlib.sha256(
    canonical
).hexdigest()

if computed_hash != stored_hash:
    raise RuntimeError(
        "T86 FREEZE HASH MISMATCH"
    )


boundary = int(
    freeze["boundary_id"]
)

model = freeze["model"]

features = model["features"]
means = model["means"]
stds = model["stds"]
betas = model["betas"]

intercept = float(
    model["intercept"]
)

run_threshold = float(
    freeze["target"]["run_threshold"]
)

dump_threshold = float(
    freeze["target"]["dump_threshold"]
)


# ============================================================
# MODEL
# ============================================================

def score(row):

    vals = {}

    for f in features:

        x = row[f]

        if not valid(x):
            return None

        vals[f] = x

    z = intercept

    for f in features:

        standardized = (
            vals[f] - means[f]
        ) / stds[f]

        z += (
            betas[f]
            * standardized
        )

    return sigmoid(z)


def classify(r300):

    if not valid(r300):
        return "WAIT", None, 0

    if r300 >= run_threshold:
        return "RUN", 1, 1

    if r300 <= dump_threshold:
        return "DUMP", 0, 1

    return "NEUTRAL", None, 1


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


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

    capv2_300_score REAL,

    dex_return_300s REAL,

    status TEXT,
    binary_label INTEGER,
    labeled_300 INTEGER DEFAULT 0
)
""")

db.commit()


# ============================================================
# SOURCE
# ============================================================

def source_rows():

    return db.execute(f"""
    SELECT
        e.id,
        e.timestamp,
        e.token_mint,

        e.fa,
        e.new_wallets30,

        e.dex_return_300s,

        s.recent_buy_share,
        s.recent_net_share,
        s.breadth_score,
        s.late_chase_score,

        s.early_price_return,
        s.early_net_sol

    FROM events e

    JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

    LEFT JOIN {TABLE} t
        ON t.event_id=e.id

    WHERE
        e.id > ?
        AND t.event_id IS NULL
        AND e.timestamp IS NOT NULL
        AND e.token_mint IS NOT NULL

    ORDER BY e.id
    """, (
        boundary,
    )).fetchall()


# ============================================================
# CAPTURE
# ============================================================

def capture():

    rows = source_rows()

    added = 0

    for r in rows:

        if r["id"] <= boundary:
            raise RuntimeError(
                "T86 boundary violation"
            )

        early_div = None

        if (
            valid(r["early_price_return"])
            and valid(r["early_net_sol"])
        ):
            early_div = (
                r["early_price_return"]
                - r["early_net_sol"]
            )

        model_row = {
            "fa": r["fa"],
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
        }

        probability = score(
            model_row
        )

        status, label, done = classify(
            r["dex_return_300s"]
        )

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

            capv2_300_score,

            dex_return_300s,

            status,
            binary_label,
            labeled_300
        )

        VALUES (
            ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?,
            ?,
            ?, ?, ?
        )
        """, (

            r["id"],
            r["token_mint"],
            r["timestamp"],
            time.time(),

            boundary,
            stored_hash,

            r["fa"],
            r["new_wallets30"],
            r["recent_buy_share"],
            r["recent_net_share"],
            r["breadth_score"],
            r["late_chase_score"],

            r["early_price_return"],
            r["early_net_sol"],
            early_div,

            probability,

            r["dex_return_300s"],

            status,
            label,
            done,
        ))

        added += 1

    db.commit()

    return added


# ============================================================
# REFRESH 300s OUTCOMES
# ============================================================

def refresh_labels():

    waiting = db.execute(f"""
    SELECT event_id
    FROM {TABLE}
    WHERE labeled_300=0
    """).fetchall()

    updated = 0

    for row in waiting:

        src = db.execute("""
        SELECT
            dex_return_300s,
            dex_done_300s

        FROM events

        WHERE id=?
        """, (
            row["event_id"],
        )).fetchone()

        if src is None:
            continue

        if (
            src["dex_done_300s"] != 1
            or not valid(
                src["dex_return_300s"]
            )
        ):
            continue

        status, label, done = classify(
            src["dex_return_300s"]
        )

        db.execute(f"""
        UPDATE {TABLE}

        SET
            dex_return_300s=?,
            status=?,
            binary_label=?,
            labeled_300=?

        WHERE event_id=?
        """, (

            src["dex_return_300s"],
            status,
            label,
            done,

            row["event_id"],
        ))

        updated += 1

    db.commit()

    return updated


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
        OR freeze_sha256 != ?
    """, (
        boundary,
        boundary,
        stored_hash,
    )).fetchone()[0]

    if bad:
        raise RuntimeError(
            f"T86 INTEGRITY VIOLATION: {bad}"
        )


# ============================================================
# DISPLAY
# ============================================================

def show():

    s = db.execute(f"""
    SELECT
        COUNT(*) rows,
        COUNT(DISTINCT token_mint) tokens,

        SUM(
            CASE
            WHEN capv2_300_score IS NOT NULL
            THEN 1 ELSE 0 END
        ) scored,

        SUM(
            CASE WHEN status='RUN'
            THEN 1 ELSE 0 END
        ) run,

        SUM(
            CASE WHEN status='DUMP'
            THEN 1 ELSE 0 END
        ) dump,

        SUM(
            CASE WHEN status='NEUTRAL'
            THEN 1 ELSE 0 END
        ) neutral,

        SUM(
            CASE WHEN status='WAIT'
            THEN 1 ELSE 0 END
        ) wait

    FROM {TABLE}
    """).fetchone()


    latest = db.execute(f"""
    SELECT
        event_id,
        token_mint,
        early_div,
        capv2_300_score,
        dex_return_300s,
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
        "MEMECOIN LAB — T86 CAP-v2 @300s PROSPECTIVE SHADOW"
    )
    print("=" * 150)

    print(
        f"BOUNDARY ID      : {boundary}"
    )

    print(
        f"FREEZE HASH      : {stored_hash}"
    )

    print(
        f"ROWS             : {s['rows'] or 0}"
    )

    print(
        f"TOKENS           : {s['tokens'] or 0}"
    )

    print(
        f"SCORED           : {s['scored'] or 0}"
    )

    print()

    print(
        f"RUN >= +10%      : {s['run'] or 0}"
    )

    print(
        f"DUMP <= -10%     : {s['dump'] or 0}"
    )

    print(
        f"NEUTRAL          : {s['neutral'] or 0}"
    )

    print(
        f"WAIT             : {s['wait'] or 0}"
    )

    print()

    print(
        "MODEL            : T85 M1 CAP-v2"
    )

    print(
        "TARGET           : dex_return_300s ±10%"
    )

    print(
        "REFIT            : FORBIDDEN"
    )

    print(
        "THRESHOLD SEARCH : FORBIDDEN"
    )

    print(
        "T59/T78/T82      : UNTOUCHED"
    )

    print()
    print("=" * 150)
    print("LATEST")
    print("=" * 150)

    for r in latest:

        ediv = (
            "NA"
            if not valid(r["early_div"])
            else f"{r['early_div']:.3f}"
        )

        score_v = (
            "NA"
            if not valid(
                r["capv2_300_score"]
            )
            else f"{r['capv2_300_score']:.4f}"
        )

        r300 = (
            "NA"
            if not valid(
                r["dex_return_300s"]
            )
            else f"{r['dex_return_300s']:+.2f}%"
        )

        print(
            f"ID={r['event_id']:5d} "
            f"| EDIV={ediv:>9} "
            f"| SCORE={score_v:>7} "
            f"| R300={r300:>8} "
            f"| {str(r['status']):7} "
            f"| {r['token_mint'][:25]}"
        )

    print()
    print(
        f"Refresh every {REFRESH}s."
    )

    print(
        "CTRL+C stops T86 only."
    )


# ============================================================
# LOOP
# ============================================================

integrity()

try:

    while True:

        capture()
        refresh_labels()
        integrity()
        show()

        time.sleep(
            REFRESH
        )

except KeyboardInterrupt:

    print()
    print(
        "T86 stopped safely."
    )

finally:

    db.close()
