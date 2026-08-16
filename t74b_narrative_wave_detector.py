#!/usr/bin/env python3

import sqlite3
import re
import unicodedata
import math
from collections import defaultdict

DB = "validation_v090.db"
META = "t74_token_metadata"
OUT = "t74_narrative_wave"

# ============================================================
# NORMALIZATION
# ============================================================

STOPWORDS = {
    "the","a","an","of","and","or","for","to","in","on","at",
    "coin","token","meme","memecoin","crypto","official",
    "sol","solana","pump","cto","community"
}

def clean(s):
    if not s:
        return ""

    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(
        c for c in s
        if not unicodedata.combining(c)
    )

    s = s.lower()
    s = s.replace("$", " ")

    s = re.sub(
        r"[^a-z0-9]+",
        " ",
        s
    )

    return " ".join(
        s.split()
    )


def words(s):
    return [
        w for w in clean(s).split()
        if len(w) >= 2
        and w not in STOPWORDS
    ]


def narrative_key(name, symbol):
    """
    Conservative v1.

    Prefer meaningful name words.
    Symbol is fallback / supporting information.

    No fuzzy matching yet.
    No semantic AI clustering yet.
    """

    nw = words(name)
    sw = words(symbol)

    # Remove symbol duplication from name words
    unique = []

    for w in nw:
        if w not in unique:
            unique.append(w)

    # If name has meaningful content, use max first 3 words
    if unique:
        return "_".join(
            unique[:3]
        )

    # fallback symbol
    if sw:
        return "_".join(
            sw[:2]
        )

    return None


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
CREATE TABLE IF NOT EXISTS {OUT} (

    token_mint TEXT PRIMARY KEY,

    first_event_id INTEGER,
    first_seen_at REAL,

    name TEXT,
    symbol TEXT,

    narrative_key TEXT,

    narrative_rank INTEGER,

    narrative_age_sec REAL,

    prior_15m INTEGER,
    prior_60m INTEGER,
    prior_6h INTEGER,
    prior_24h INTEGER,

    velocity_15m REAL,
    velocity_60m REAL,

    acceleration REAL,

    late_clone_score REAL,

    computed_at REAL
)
""")

db.commit()


# ============================================================
# LOAD FIRST APPEARANCE
# ============================================================

rows = db.execute(f"""
SELECT
    m.token_mint,
    m.name,
    m.symbol,

    MIN(e.id) AS first_event_id,
    MIN(e.timestamp) AS first_seen_at

FROM {META} m

JOIN events e
    ON e.token_mint=m.token_mint

WHERE
    m.status='OK'
    AND e.timestamp IS NOT NULL

GROUP BY
    m.token_mint

ORDER BY
    first_seen_at,
    first_event_id
""").fetchall()


tokens = []

for r in rows:

    key = narrative_key(
        r["name"],
        r["symbol"]
    )

    tokens.append({
        "token_mint":
            r["token_mint"],

        "name":
            r["name"],

        "symbol":
            r["symbol"],

        "first_event_id":
            r["first_event_id"],

        "first_seen_at":
            float(r["first_seen_at"]),

        "key":
            key
    })


# ============================================================
# STRICT HISTORICAL WAVE CONSTRUCTION
# ============================================================

history = defaultdict(list)

out = []

for x in tokens:

    key = x["key"]
    ts = x["first_seen_at"]

    if not key:

        continue

    # IMPORTANT:
    # history contains only tokens chronologically BEFORE x.
    prior = history[key]

    ages = [
        ts - p["first_seen_at"]
        for p in prior
        if p["first_seen_at"] <= ts
    ]

    prior_15m = sum(
        1 for a in ages
        if 0 <= a <= 15*60
    )

    prior_60m = sum(
        1 for a in ages
        if 0 <= a <= 60*60
    )

    prior_6h = sum(
        1 for a in ages
        if 0 <= a <= 6*3600
    )

    prior_24h = sum(
        1 for a in ages
        if 0 <= a <= 24*3600
    )

    rank = len(prior) + 1

    if prior:
        narrative_age = (
            ts
            - prior[0]["first_seen_at"]
        )
    else:
        narrative_age = 0.0

    # tokens/hour
    velocity_15m = (
        prior_15m / 0.25
    )

    velocity_60m = float(
        prior_60m
    )

    # Recent creation speed relative to the broader hour.
    # Positive = wave becoming faster.
    acceleration = (
        velocity_15m
        - velocity_60m
    )

    # Simple descriptive saturation variable.
    # NOT a trading threshold.
    late_clone_score = math.log1p(
        prior_6h
    )

    out.append({
        **x,

        "rank":
            rank,

        "narrative_age":
            narrative_age,

        "prior_15m":
            prior_15m,

        "prior_60m":
            prior_60m,

        "prior_6h":
            prior_6h,

        "prior_24h":
            prior_24h,

        "velocity_15m":
            velocity_15m,

        "velocity_60m":
            velocity_60m,

        "acceleration":
            acceleration,

        "late_clone_score":
            late_clone_score
    })

    # Add current token only AFTER computing its features.
    history[key].append(
        x
    )


# ============================================================
# WRITE DERIVED TABLE
# ============================================================

db.execute(
    f"DELETE FROM {OUT}"
)

for x in out:

    db.execute(f"""
    INSERT INTO {OUT} (

        token_mint,

        first_event_id,
        first_seen_at,

        name,
        symbol,

        narrative_key,

        narrative_rank,

        narrative_age_sec,

        prior_15m,
        prior_60m,
        prior_6h,
        prior_24h,

        velocity_15m,
        velocity_60m,

        acceleration,

        late_clone_score,

        computed_at
    )

    VALUES (
        ?, ?, ?,
        ?, ?,
        ?,
        ?,
        ?,
        ?, ?, ?, ?,
        ?, ?,
        ?,
        ?,
        strftime('%s','now')
    )
    """, (

        x["token_mint"],

        x["first_event_id"],
        x["first_seen_at"],

        x["name"],
        x["symbol"],

        x["key"],

        x["rank"],

        x["narrative_age"],

        x["prior_15m"],
        x["prior_60m"],
        x["prior_6h"],
        x["prior_24h"],

        x["velocity_15m"],
        x["velocity_60m"],

        x["acceleration"],

        x["late_clone_score"]
    ))

db.commit()


# ============================================================
# AUDIT
# ============================================================

total = db.execute(
    f"SELECT COUNT(*) FROM {OUT}"
).fetchone()[0]

keys = db.execute(
    f"""
    SELECT COUNT(DISTINCT narrative_key)
    FROM {OUT}
    """
).fetchone()[0]

multi = db.execute(f"""
SELECT COUNT(*)
FROM (
    SELECT narrative_key
    FROM {OUT}
    GROUP BY narrative_key
    HAVING COUNT(*) >= 2
)
""").fetchone()[0]


print("=" * 150)
print(
    "MEMECOIN LAB — T74B NARRATIVE NORMALIZER + WAVE DETECTOR"
)
print("=" * 150)

print(
    f"TOKENS             : {total}"
)

print(
    f"NARRATIVE KEYS     : {keys}"
)

print(
    f"MULTI-TOKEN WAVES  : {multi}"
)

print(
    "LABELS USED        : NO"
)

print(
    "MODEL FITTING      : NO"
)

print(
    "THRESHOLD SEARCH   : NO"
)

print(
    "T59                : UNTOUCHED"
)


# ============================================================
# TOP REPEATED NARRATIVES
# ============================================================

print()
print("=" * 150)
print("A) TOP REPEATED NARRATIVE KEYS")
print("=" * 150)

top = db.execute(f"""
SELECT
    narrative_key,
    COUNT(*) AS n,
    MAX(narrative_rank) AS max_rank,
    MAX(prior_15m) AS max_prior15,
    MAX(prior_60m) AS max_prior60,
    MAX(acceleration) AS max_accel

FROM {OUT}

GROUP BY
    narrative_key

HAVING COUNT(*) >= 2

ORDER BY
    n DESC,
    narrative_key

LIMIT 40
""").fetchall()


if not top:
    print(
        "No repeated exact narrative keys yet."
    )

for r in top:

    print(
        f"{r['narrative_key'][:40]:40} "
        f"| N={r['n']:3d} "
        f"| RANK={r['max_rank']:3d} "
        f"| P15={r['max_prior15']:3d} "
        f"| P60={r['max_prior60']:3d} "
        f"| ACC={r['max_accel']:+7.2f}"
    )


# ============================================================
# CONTENT OF WAVES
# ============================================================

print()
print("=" * 150)
print("B) CONTENT OF LARGEST WAVES")
print("=" * 150)

wave_keys = [
    r["narrative_key"]
    for r in top[:15]
]

for key in wave_keys:

    rr = db.execute(f"""
    SELECT
        name,
        symbol,
        narrative_rank,
        prior_15m,
        prior_60m,
        acceleration,
        token_mint

    FROM {OUT}

    WHERE
        narrative_key=?

    ORDER BY
        first_seen_at,
        first_event_id
    """, (
        key,
    )).fetchall()

    print()
    print(
        f"[{key}] — {len(rr)} tokens"
    )

    for r in rr:

        print(
            f"  #{r['narrative_rank']:02d} "
            f"| {str(r['symbol'] or '-')[:12]:12} "
            f"| {str(r['name'] or '-')[:34]:34} "
            f"| P15={r['prior_15m']:2d} "
            f"| P60={r['prior_60m']:2d} "
            f"| ACC={r['acceleration']:+6.2f} "
            f"| {r['token_mint'][:16]}"
        )


# ============================================================
# FASTEST WAVES
# ============================================================

print()
print("=" * 150)
print("C) HIGHEST OBSERVED NARRATIVE ACCELERATION")
print("=" * 150)

fast = db.execute(f"""
SELECT
    narrative_key,
    name,
    symbol,
    narrative_rank,
    prior_15m,
    prior_60m,
    prior_6h,
    acceleration

FROM {OUT}

WHERE
    narrative_rank > 1

ORDER BY
    acceleration DESC,
    prior_15m DESC

LIMIT 25
""").fetchall()

for r in fast:

    print(
        f"{r['narrative_key'][:34]:34} "
        f"| #{r['narrative_rank']:02d} "
        f"| P15={r['prior_15m']:2d} "
        f"| P60={r['prior_60m']:2d} "
        f"| P6H={r['prior_6h']:2d} "
        f"| ACC={r['acceleration']:+6.2f} "
        f"| {str(r['name'] or '-')[:30]}"
    )


print()
print("=" * 150)
print("D) IMPORTANT")
print("=" * 150)

print(
    "T74B is descriptive only."
)

print(
    "Exact normalized keys only — no fuzzy/semantic matching yet."
)

print(
    "Every wave feature is computed before adding the current token."
)

print(
    "No outcome labels were queried."
)

print(
    "Do NOT interpret narrative_rank/acceleration as trading signals yet."
)

print(
    "Next step: visually audit cluster quality before T74C."
)

db.close()
