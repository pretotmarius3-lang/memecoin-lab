#!/usr/bin/env python3

import sqlite3
import re
import unicodedata
from collections import defaultdict, Counter

DB = "validation_v090.db"

META = "t74_token_metadata"
EXACT = "t74_narrative_wave"
OUT = "t74_narrative_concepts"


# ============================================================
# NORMALIZATION
# ============================================================

STOPWORDS = {
    "the","a","an","of","and","or","for","to","in","on","at",
    "coin","token","meme","memecoin","crypto","official",
    "sol","solana","pump","cto","community",
    "new","real","original"
}


# Conservative alias families.
#
# This is NOT learned from outcomes.
# It is semantic normalization only.
#
# Keep this list small and explicit.
ALIASES = {
    "elon": "musk",
    "elonmusk": "musk",

    "tesla": "tesla",
    "roadster": "tesla",
    "cybertruck": "tesla",

    "dogwifhat": "wif",
    "wif": "wif",

    "luigi": "luigi_mangione",
    "mangione": "luigi_mangione",

    "pusheen": "pusheen",

    "fartcoin": "fartcoin",
    "fart": "fartcoin",

    "cursor": "cursor_ai",

    "ebola": "ebola",
    "ebov": "ebola",

    "yeezy": "yeezy",
    "kanye": "yeezy",

    "stonks": "stonks",

    "pippin": "pippin",

    "claudette": "claudette",

    "phoebe": "cookie_monster",

    "cookie": "cookie_monster",

    "chatjipiti": "chatjipiti",
}


def clean(s):

    if not s:
        return ""

    s = unicodedata.normalize(
        "NFKD",
        str(s)
    )

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


def tokenize(s):

    return [
        w
        for w in clean(s).split()
        if len(w) >= 2
        and w not in STOPWORDS
    ]


def alias_token(w):

    return ALIASES.get(
        w,
        w
    )


def concept_set(name, symbol):

    tokens = []

    for source in [
        name,
        symbol
    ]:

        for w in tokenize(source):

            c = alias_token(w)

            if c not in tokens:
                tokens.append(c)


    # ========================================================
    # Multi-token semantic rules
    # ========================================================

    raw = set(
        tokenize(name)
        + tokenize(symbol)
    )


    # Tesla / Musk family
    if (
        "tesla" in raw
        or "roadster" in raw
        or "cybertruck" in raw
    ):
        if "tesla" not in tokens:
            tokens.append("tesla")


    if (
        "elon" in raw
        or "musk" in raw
    ):
        if "musk" not in tokens:
            tokens.append("musk")


    # Luigi Mangione
    if (
        "luigi" in raw
        or "mangione" in raw
    ):
        if "luigi_mangione" not in tokens:
            tokens.append(
                "luigi_mangione"
            )


    # Cookie Monster
    if (
        "cookie" in raw
        and "monster" in raw
    ):
        if "cookie_monster" not in tokens:
            tokens.append(
                "cookie_monster"
            )


    # Cursor AI
    if (
        "cursor" in raw
        and "ai" in raw
    ):
        if "cursor_ai" not in tokens:
            tokens.append(
                "cursor_ai"
            )


    # Flying Tesla Roadster specific concept
    if (
        "flying" in raw
        and (
            "tesla" in raw
            or "roadster" in raw
        )
    ):
        if "flying_tesla" not in tokens:
            tokens.append(
                "flying_tesla"
            )


    return sorted(
        set(tokens)
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
CREATE TABLE IF NOT EXISTS {OUT} (

    token_mint TEXT NOT NULL,

    first_event_id INTEGER,
    first_seen_at REAL,

    name TEXT,
    symbol TEXT,

    exact_key TEXT,

    concept TEXT NOT NULL,

    concept_rank INTEGER,

    concept_age_sec REAL,

    prior_15m INTEGER,
    prior_60m INTEGER,
    prior_6h INTEGER,
    prior_24h INTEGER,

    velocity_15m REAL,
    velocity_60m REAL,

    acceleration REAL,

    concept_token_count_before INTEGER,

    PRIMARY KEY (
        token_mint,
        concept
    )
)
""")


db.commit()


# ============================================================
# LOAD TOKENS
# ============================================================

rows = db.execute(f"""
SELECT
    m.token_mint,
    m.name,
    m.symbol,

    w.narrative_key AS exact_key,

    MIN(e.id) AS first_event_id,
    MIN(e.timestamp) AS first_seen_at

FROM {META} m

JOIN events e
    ON e.token_mint=m.token_mint

LEFT JOIN {EXACT} w
    ON w.token_mint=m.token_mint

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

    concepts = concept_set(
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

        "exact_key":
            r["exact_key"],

        "first_event_id":
            r["first_event_id"],

        "first_seen_at":
            float(
                r["first_seen_at"]
            ),

        "concepts":
            concepts,
    })


# ============================================================
# STRICT CHRONOLOGICAL CONCEPT HISTORY
# ============================================================

history = defaultdict(list)

derived = []


for x in tokens:

    ts = x[
        "first_seen_at"
    ]


    for concept in x[
        "concepts"
    ]:

        prior = history[
            concept
        ]


        ages = [
            ts - p["first_seen_at"]
            for p in prior
            if p["first_seen_at"] <= ts
        ]


        prior_15m = sum(
            1
            for a in ages
            if 0 <= a <= 15*60
        )

        prior_60m = sum(
            1
            for a in ages
            if 0 <= a <= 60*60
        )

        prior_6h = sum(
            1
            for a in ages
            if 0 <= a <= 6*3600
        )

        prior_24h = sum(
            1
            for a in ages
            if 0 <= a <= 24*3600
        )


        rank = (
            len(prior)
            + 1
        )


        if prior:

            age = (
                ts
                - prior[0][
                    "first_seen_at"
                ]
            )

        else:

            age = 0.0


        velocity_15m = (
            prior_15m
            / 0.25
        )

        velocity_60m = float(
            prior_60m
        )


        acceleration = (
            velocity_15m
            - velocity_60m
        )


        derived.append({
            **x,

            "concept":
                concept,

            "rank":
                rank,

            "age":
                age,

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

            "prior_total":
                len(prior),
        })


    # IMPORTANT:
    # Current token is added only AFTER its
    # features have been calculated.
    for concept in x[
        "concepts"
    ]:

        history[
            concept
        ].append({
            "token_mint":
                x["token_mint"],

            "first_seen_at":
                ts,

            "name":
                x["name"],

            "symbol":
                x["symbol"],
        })


# ============================================================
# REBUILD OUTPUT
# ============================================================

db.execute(
    f"DELETE FROM {OUT}"
)


for x in derived:

    db.execute(f"""
    INSERT INTO {OUT} (

        token_mint,

        first_event_id,
        first_seen_at,

        name,
        symbol,

        exact_key,

        concept,

        concept_rank,

        concept_age_sec,

        prior_15m,
        prior_60m,
        prior_6h,
        prior_24h,

        velocity_15m,
        velocity_60m,

        acceleration,

        concept_token_count_before
    )

    VALUES (
        ?, ?, ?,
        ?, ?,
        ?,
        ?,
        ?,
        ?,
        ?, ?, ?, ?,
        ?, ?,
        ?,
        ?
    )
    """, (

        x["token_mint"],

        x["first_event_id"],
        x["first_seen_at"],

        x["name"],
        x["symbol"],

        x["exact_key"],

        x["concept"],

        x["rank"],

        x["age"],

        x["prior_15m"],
        x["prior_60m"],
        x["prior_6h"],
        x["prior_24h"],

        x["velocity_15m"],
        x["velocity_60m"],

        x["acceleration"],

        x["prior_total"]
    ))


db.commit()


# ============================================================
# AUDIT
# ============================================================

print("=" * 150)
print(
    "MEMECOIN LAB — T74B.1 NARRATIVE ENTITY / CONCEPT CLUSTERING"
)
print("=" * 150)


token_count = db.execute(
    f"""
    SELECT COUNT(
        DISTINCT token_mint
    )
    FROM {OUT}
    """
).fetchone()[0]


concept_count = db.execute(
    f"""
    SELECT COUNT(
        DISTINCT concept
    )
    FROM {OUT}
    """
).fetchone()[0]


rows_count = db.execute(
    f"""
    SELECT COUNT(*)
    FROM {OUT}
    """
).fetchone()[0]


multi_concepts = db.execute(f"""
SELECT COUNT(*)

FROM (

    SELECT concept

    FROM {OUT}

    GROUP BY concept

    HAVING COUNT(
        DISTINCT token_mint
    ) >= 2
)
""").fetchone()[0]


print(
    f"TOKENS WITH CONCEPTS   : {token_count}"
)

print(
    f"CONCEPT ROWS           : {rows_count}"
)

print(
    f"UNIQUE CONCEPTS        : {concept_count}"
)

print(
    f"MULTI-TOKEN CONCEPTS   : {multi_concepts}"
)

print(
    "LABELS USED            : NO"
)

print(
    "MODEL FITTING          : NO"
)

print(
    "T59                    : UNTOUCHED"
)


# ============================================================
# A) TOP CONCEPTS
# ============================================================

print()
print("=" * 150)
print("A) TOP MULTI-TOKEN CONCEPTS")
print("=" * 150)


top = db.execute(f"""
SELECT
    concept,

    COUNT(
        DISTINCT token_mint
    ) AS n,

    MAX(
        concept_rank
    ) AS max_rank,

    MAX(
        prior_15m
    ) AS max_prior15,

    MAX(
        prior_60m
    ) AS max_prior60,

    MAX(
        acceleration
    ) AS max_accel

FROM {OUT}

GROUP BY concept

HAVING n >= 2

ORDER BY
    n DESC,
    max_accel DESC,
    concept

LIMIT 50
""").fetchall()


for r in top:

    print(
        f"{r['concept'][:35]:35} "
        f"| N={r['n']:3d} "
        f"| RANK={r['max_rank']:3d} "
        f"| P15={r['max_prior15']:3d} "
        f"| P60={r['max_prior60']:3d} "
        f"| ACC={r['max_accel']:+7.2f}"
    )


# ============================================================
# B) CONTENT
# ============================================================

print()
print("=" * 150)
print("B) CONTENT OF LARGEST CONCEPT CLUSTERS")
print("=" * 150)


concepts = [
    r["concept"]
    for r in top[:20]
]


for concept in concepts:

    rr = db.execute(f"""
    SELECT
        token_mint,
        name,
        symbol,
        exact_key,

        concept_rank,
        prior_15m,
        prior_60m,
        acceleration

    FROM {OUT}

    WHERE
        concept=?

    ORDER BY
        first_seen_at,
        first_event_id
    """, (
        concept,
    )).fetchall()


    print()
    print(
        f"[{concept}] — "
        f"{len(rr)} tokens"
    )


    for r in rr:

        print(
            f"  #{r['concept_rank']:02d} "
            f"| {str(r['symbol'] or '-')[:12]:12} "
            f"| {str(r['name'] or '-')[:34]:34} "
            f"| exact={str(r['exact_key'] or '-')[:25]:25} "
            f"| P15={r['prior_15m']:2d} "
            f"| P60={r['prior_60m']:2d} "
            f"| ACC={r['acceleration']:+6.2f}"
        )


# ============================================================
# C) TOKENS WITH MULTIPLE CONCEPTS
# ============================================================

print()
print("=" * 150)
print("C) TOKENS WITH MULTIPLE CONCEPTS")
print("=" * 150)


multi = db.execute(f"""
SELECT
    token_mint,
    name,
    symbol,
    COUNT(*) AS concept_count,

    GROUP_CONCAT(
        concept,
        ', '
    ) AS concepts

FROM {OUT}

GROUP BY
    token_mint

HAVING
    COUNT(*) >= 2

ORDER BY
    concept_count DESC,
    name

LIMIT 40
""").fetchall()


for r in multi:

    print(
        f"{str(r['symbol'] or '-')[:12]:12} "
        f"| {str(r['name'] or '-')[:35]:35} "
        f"| N={r['concept_count']:2d} "
        f"| {r['concepts']}"
    )


# ============================================================
# D) ACCELERATION
# ============================================================

print()
print("=" * 150)
print("D) HIGHEST CONCEPT ACCELERATION")
print("=" * 150)


fast = db.execute(f"""
SELECT
    concept,
    name,
    symbol,

    concept_rank,
    prior_15m,
    prior_60m,
    prior_6h,

    acceleration

FROM {OUT}

WHERE
    concept_rank > 1

ORDER BY
    acceleration DESC,
    prior_15m DESC,
    concept

LIMIT 30
""").fetchall()


for r in fast:

    print(
        f"{r['concept'][:32]:32} "
        f"| #{r['concept_rank']:02d} "
        f"| P15={r['prior_15m']:2d} "
        f"| P60={r['prior_60m']:2d} "
        f"| P6H={r['prior_6h']:2d} "
        f"| ACC={r['acceleration']:+6.2f} "
        f"| {str(r['name'] or '-')[:32]}"
    )


# ============================================================
# E) EXACT VS CONCEPT
# ============================================================

print()
print("=" * 150)
print("E) EXACT CLONE VS BROADER CONCEPT")
print("=" * 150)


rows_cmp = db.execute(f"""
SELECT
    concept,

    COUNT(
        DISTINCT token_mint
    ) AS concept_tokens,

    COUNT(
        DISTINCT exact_key
    ) AS exact_keys

FROM {OUT}

GROUP BY
    concept

HAVING
    concept_tokens >= 2

ORDER BY
    concept_tokens DESC,
    exact_keys DESC

LIMIT 40
""").fetchall()


for r in rows_cmp:

    print(
        f"{r['concept'][:35]:35} "
        f"| TOKENS={r['concept_tokens']:3d} "
        f"| EXACT_KEYS={r['exact_keys']:3d}"
    )


print()
print("=" * 150)
print("F) IMPORTANT")
print("=" * 150)

print(
    "T74B.1 uses NO outcome labels."
)

print(
    "Concept aliases are explicit and frozen inside this script."
)

print(
    "No fuzzy model / embedding clustering is used."
)

print(
    "A token may belong to multiple concepts."
)

print(
    "Concept wave features are strictly chronological."
)

print(
    "Current token is added only after its own wave metrics are computed."
)

print(
    "Do NOT evaluate trading performance yet."
)

print(
    "Next step = manually inspect cluster quality."
)

db.close()
