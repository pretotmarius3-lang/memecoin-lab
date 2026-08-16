#!/usr/bin/env python3

import sqlite3
import re
import unicodedata
from collections import defaultdict

DB = "validation_v090.db"

META = "t74_token_metadata"
EXACT = "t74_narrative_wave"
OUT = "t74_narrative_canonical"

# ============================================================
# BLIND VOCABULARY
# ============================================================

STOPWORDS = {
    "the","a","an","of","and","or","for","to","in","on","at",
    "coin","token","meme","memecoin","crypto","official",
    "sol","solana","pump","pumpfun","cto","community",
    "new","real","original",

    "all","is","my","this","that","you","your","we","our",
    "had","have","has","do","did","was","were","be","been",
    "call","buy","sell","ape","aped","lower","lol","know",
    "if","me","from","sent","over","out","into","with",
    "most","more","very","just","not","no"
}

BROAD_THEMES = {
    "cat": "cat",
    "dog": "dog",
    "ai": "ai",
    "robot": "ai",
    "bot": "ai",

    "frog": "frog",
    "monkey": "monkey",
    "ape": "monkey",

    "wolf": "wolf",
    "dolphin": "dolphin",
    "whale": "whale",

    "politics": "politics",
    "president": "politics",
    "election": "politics",

    "celebrity": "celebrity",
}

ENTITY_ALIASES = {
    "elon": "musk",
    "musk": "musk",
    "elonmusk": "musk",

    "tesla": "tesla",
    "roadster": "tesla",
    "cybertruck": "tesla",

    "luigi": "luigi_mangione",
    "mangione": "luigi_mangione",

    "yeezy": "yeezy",
    "yzy": "yeezy",
    "kanye": "yeezy",

    "ebola": "ebola",
    "ebov": "ebola",
    "ebolavirus": "ebola",

    "cursor": "cursor_ai",

    "fartcoin": "fartcoin",
    "fart": "fartcoin",

    "pippin": "pippin",

    "claudette": "claudette",

    "pusheen": "pusheen",

    "stonks": "stonks",

    "chatjipiti": "chatjipiti",

    "chud": "chud",
}

# Explicit narratives.
# Blind semantic rules only.
NARRATIVE_RULES = [
    (
        "flying_tesla",
        {"flying"},
        {"tesla","roadster","cybertruck"}
    ),

    (
        "luigi_mangione",
        set(),
        {"luigi","mangione"}
    ),

    (
        "cookie_monster",
        {"cookie","monster"},
        set()
    ),

    (
        "cursor_ai",
        {"cursor"},
        {"ai"}
    ),

    (
        "nice_musk",
        {"nice"},
        {"musk","elon"}
    ),

    (
        "pusheen_cat",
        {"pusheen"},
        {"cat"}
    ),

    (
        "ebola",
        set(),
        {"ebola","ebov","ebolavirus"}
    ),

    (
        "yeezy",
        set(),
        {"yeezy","yzy","kanye"}
    ),

    (
        "fartcoin",
        set(),
        {"fartcoin"}
    ),

    (
        "claudette",
        set(),
        {"claudette"}
    ),

    (
        "chatjipiti",
        set(),
        {"chatjipiti"}
    ),
]


# ============================================================
# NORMALIZATION
# ============================================================

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


def tokens(s):
    return [
        w
        for w in clean(s).split()
        if len(w) >= 2
        and w not in STOPWORDS
    ]


def raw_token_set(name, symbol):
    return set(
        tokens(name)
        + tokens(symbol)
    )


# ============================================================
# CLASSIFICATION
# ============================================================

def classify(name, symbol):

    raw = raw_token_set(
        name,
        symbol
    )

    broad = set()
    entities = set()
    narratives = set()

    # ------------------------
    # BROAD THEME
    # ------------------------

    for w in raw:
        if w in BROAD_THEMES:
            broad.add(
                BROAD_THEMES[w]
            )

    # ------------------------
    # ENTITY
    # ------------------------

    for w in raw:
        if w in ENTITY_ALIASES:
            entities.add(
                ENTITY_ALIASES[w]
            )

    # semantic implication
    if (
        "roadster" in raw
        or "cybertruck" in raw
    ):
        entities.add(
            "tesla"
        )

    if (
        "elon" in raw
        or "musk" in raw
    ):
        entities.add(
            "musk"
        )

    # ------------------------
    # NARRATIVE
    # ------------------------

    for (
        narrative,
        require_all,
        require_any
    ) in NARRATIVE_RULES:

        all_ok = (
            not require_all
            or require_all.issubset(raw)
        )

        any_ok = (
            not require_any
            or bool(
                raw & require_any
            )
        )

        if all_ok and any_ok:
            narratives.add(
                narrative
            )

    # Preserve repeated exact names as
    # fallback narrative, but only when
    # name contains meaningful content.
    meaningful_name = tokens(
        name
    )

    exact_fallback = None

    if meaningful_name:
        exact_fallback = "_".join(
            meaningful_name[:4]
        )

    return {
        "broad":
            sorted(broad),

        "entities":
            sorted(entities),

        "narratives":
            sorted(narratives),

        "exact_fallback":
            exact_fallback,

        "raw":
            sorted(raw),
    }


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

    level TEXT NOT NULL,
    canonical_key TEXT NOT NULL,

    rank INTEGER,

    age_sec REAL,

    prior_15m INTEGER,
    prior_60m INTEGER,
    prior_6h INTEGER,
    prior_24h INTEGER,

    velocity_15m REAL,
    velocity_60m REAL,

    acceleration REAL,

    PRIMARY KEY (
        token_mint,
        level,
        canonical_key
    )
)
""")

db.commit()


# ============================================================
# LOAD
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


tokens_data = []

for r in rows:

    c = classify(
        r["name"],
        r["symbol"]
    )

    tokens_data.append({
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

        "class":
            c,
    })


# ============================================================
# CHRONOLOGICAL WAVES
# ============================================================

history = defaultdict(list)

derived = []


def add_feature_row(
    x,
    level,
    key
):

    ts = x[
        "first_seen_at"
    ]

    hkey = (
        level,
        key
    )

    prior = history[
        hkey
    ]

    ages = [
        ts - p
        for p in prior
        if p <= ts
    ]

    p15 = sum(
        1 for a in ages
        if 0 <= a <= 15*60
    )

    p60 = sum(
        1 for a in ages
        if 0 <= a <= 60*60
    )

    p6 = sum(
        1 for a in ages
        if 0 <= a <= 6*3600
    )

    p24 = sum(
        1 for a in ages
        if 0 <= a <= 24*3600
    )

    rank = len(prior) + 1

    age = (
        ts - prior[0]
        if prior
        else 0.0
    )

    v15 = p15 / 0.25
    v60 = float(p60)

    accel = (
        v15 - v60
    )

    derived.append({
        "token_mint":
            x["token_mint"],

        "first_event_id":
            x["first_event_id"],

        "first_seen_at":
            ts,

        "name":
            x["name"],

        "symbol":
            x["symbol"],

        "exact_key":
            x["exact_key"],

        "level":
            level,

        "key":
            key,

        "rank":
            rank,

        "age":
            age,

        "p15":
            p15,

        "p60":
            p60,

        "p6":
            p6,

        "p24":
            p24,

        "v15":
            v15,

        "v60":
            v60,

        "accel":
            accel,
    })


for x in tokens_data:

    entries = []

    for key in x["class"]["broad"]:
        entries.append(
            (
                "BROAD_THEME",
                key
            )
        )

    for key in x["class"]["entities"]:
        entries.append(
            (
                "ENTITY",
                key
            )
        )

    for key in x["class"]["narratives"]:
        entries.append(
            (
                "NARRATIVE",
                key
            )
        )

    # Exact fallback is intentionally
    # lower priority / descriptive.
    if x["class"]["exact_fallback"]:
        entries.append(
            (
                "EXACT_FALLBACK",
                x["class"]["exact_fallback"]
            )
        )

    # compute first
    for level,key in entries:

        add_feature_row(
            x,
            level,
            key
        )

    # then add current token to histories
    for level,key in entries:

        history[
            (
                level,
                key
            )
        ].append(
            x["first_seen_at"]
        )


# ============================================================
# REBUILD TABLE
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

        level,
        canonical_key,

        rank,

        age_sec,

        prior_15m,
        prior_60m,
        prior_6h,
        prior_24h,

        velocity_15m,
        velocity_60m,

        acceleration
    )

    VALUES (
        ?, ?, ?,
        ?, ?,
        ?,
        ?, ?,
        ?,
        ?,
        ?, ?, ?, ?,
        ?, ?,
        ?
    )
    """, (

        x["token_mint"],

        x["first_event_id"],
        x["first_seen_at"],

        x["name"],
        x["symbol"],

        x["exact_key"],

        x["level"],
        x["key"],

        x["rank"],

        x["age"],

        x["p15"],
        x["p60"],
        x["p6"],
        x["p24"],

        x["v15"],
        x["v60"],

        x["accel"]
    ))

db.commit()


# ============================================================
# OUTPUT
# ============================================================

print("=" * 160)
print(
    "MEMECOIN LAB — T74B.2 BLIND NARRATIVE CANONICALIZATION"
)
print("=" * 160)


stats = db.execute(f"""
SELECT
    level,

    COUNT(*) AS rows,

    COUNT(
        DISTINCT canonical_key
    ) AS keys,

    COUNT(
        DISTINCT token_mint
    ) AS tokens

FROM {OUT}

GROUP BY level

ORDER BY level
""").fetchall()


for r in stats:

    print(
        f"{r['level']:16} "
        f"| ROWS={r['rows']:4d} "
        f"| KEYS={r['keys']:4d} "
        f"| TOKENS={r['tokens']:4d}"
    )


print()
print(
    "LABELS USED      : NO"
)

print(
    "MODEL FITTING    : NO"
)

print(
    "OUTCOME ACCESS   : NO"
)

print(
    "T59              : UNTOUCHED"
)


# ============================================================
# A) NARRATIVES
# ============================================================

print()
print("=" * 160)
print("A) CANONICAL NARRATIVES")
print("=" * 160)


narr = db.execute(f"""
SELECT
    canonical_key,

    COUNT(
        DISTINCT token_mint
    ) AS n,

    COUNT(
        DISTINCT exact_key
    ) AS exact_keys,

    MAX(rank) AS max_rank,

    MAX(prior_15m) AS max_p15,

    MAX(prior_60m) AS max_p60,

    MAX(acceleration) AS max_accel

FROM {OUT}

WHERE
    level='NARRATIVE'

GROUP BY
    canonical_key

ORDER BY
    n DESC,
    max_accel DESC,
    canonical_key
""").fetchall()


if not narr:
    print(
        "No canonical narratives detected."
    )


for r in narr:

    print(
        f"{r['canonical_key'][:34]:34} "
        f"| TOK={r['n']:3d} "
        f"| EXACT={r['exact_keys']:3d} "
        f"| RANK={r['max_rank']:3d} "
        f"| P15={r['max_p15']:3d} "
        f"| P60={r['max_p60']:3d} "
        f"| ACC={r['max_accel']:+7.2f}"
    )


# ============================================================
# B) NARRATIVE CONTENT
# ============================================================

print()
print("=" * 160)
print("B) CONTENT OF CANONICAL NARRATIVES")
print("=" * 160)


for n in narr:

    key = n[
        "canonical_key"
    ]

    rr = db.execute(f"""
    SELECT
        name,
        symbol,
        exact_key,

        rank,
        prior_15m,
        prior_60m,
        acceleration

    FROM {OUT}

    WHERE
        level='NARRATIVE'
        AND canonical_key=?

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
            f"  #{r['rank']:02d} "
            f"| {str(r['symbol'] or '-')[:12]:12} "
            f"| {str(r['name'] or '-')[:35]:35} "
            f"| exact={str(r['exact_key'] or '-')[:28]:28} "
            f"| P15={r['prior_15m']:2d} "
            f"| P60={r['prior_60m']:2d} "
            f"| ACC={r['acceleration']:+6.2f}"
        )


# ============================================================
# C) ENTITIES
# ============================================================

print()
print("=" * 160)
print("C) ENTITY WAVES")
print("=" * 160)


entities = db.execute(f"""
SELECT
    canonical_key,

    COUNT(
        DISTINCT token_mint
    ) AS n,

    COUNT(
        DISTINCT exact_key
    ) AS exact_keys,

    MAX(prior_15m) AS max_p15,

    MAX(prior_60m) AS max_p60,

    MAX(acceleration) AS max_accel

FROM {OUT}

WHERE
    level='ENTITY'

GROUP BY
    canonical_key

HAVING
    n >= 2

ORDER BY
    n DESC,
    max_accel DESC
""").fetchall()


for r in entities:

    print(
        f"{r['canonical_key'][:34]:34} "
        f"| TOK={r['n']:3d} "
        f"| EXACT={r['exact_keys']:3d} "
        f"| P15={r['max_p15']:3d} "
        f"| P60={r['max_p60']:3d} "
        f"| ACC={r['max_accel']:+7.2f}"
    )


# ============================================================
# D) BROAD THEMES
# ============================================================

print()
print("=" * 160)
print("D) BROAD THEMES")
print("=" * 160)


broad = db.execute(f"""
SELECT
    canonical_key,

    COUNT(
        DISTINCT token_mint
    ) AS n,

    COUNT(
        DISTINCT exact_key
    ) AS exact_keys,

    MAX(prior_15m) AS max_p15,

    MAX(prior_60m) AS max_p60

FROM {OUT}

WHERE
    level='BROAD_THEME'

GROUP BY
    canonical_key

ORDER BY
    n DESC
""").fetchall()


for r in broad:

    print(
        f"{r['canonical_key'][:34]:34} "
        f"| TOK={r['n']:3d} "
        f"| EXACT={r['exact_keys']:3d} "
        f"| P15={r['max_p15']:3d} "
        f"| P60={r['max_p60']:3d}"
    )


# ============================================================
# E) MULTI-LEVEL TOKENS
# ============================================================

print()
print("=" * 160)
print("E) MULTI-LEVEL TOKEN EXAMPLES")
print("=" * 160)


multi = db.execute(f"""
SELECT
    token_mint,
    name,
    symbol,

    GROUP_CONCAT(
        level || ':' || canonical_key,
        ' | '
    ) AS assignments

FROM {OUT}

GROUP BY
    token_mint

HAVING
    COUNT(*) >= 2

ORDER BY
    COUNT(*) DESC,
    name

LIMIT 50
""").fetchall()


for r in multi:

    print(
        f"{str(r['symbol'] or '-')[:12]:12} "
        f"| {str(r['name'] or '-')[:35]:35} "
        f"| {r['assignments']}"
    )


# ============================================================
# F) NOISE CHECK
# ============================================================

print()
print("=" * 160)
print("F) NOISE / FORBIDDEN KEY CHECK")
print("=" * 160)


forbidden = sorted(
    STOPWORDS
)


bad = db.execute(f"""
SELECT DISTINCT
    level,
    canonical_key

FROM {OUT}
""").fetchall()


bad_found = [
    (
        r["level"],
        r["canonical_key"]
    )
    for r in bad
    if r["canonical_key"] in STOPWORDS
]


if bad_found:

    print(
        "❌ Forbidden generic keys found:"
    )

    for level,key in bad_found:
        print(
            f"  {level} | {key}"
        )

else:

    print(
        "✅ No forbidden generic key appears "
        "as canonical broad/entity/narrative assignment."
    )


print()
print("=" * 160)
print("G) IMPORTANT")
print("=" * 160)

print(
    "T74B.2 is still blind to outcomes."
)

print(
    "Broad themes, entities and narratives are separate levels."
)

print(
    "Generic language is filtered before clustering."
)

print(
    "Symbols are not automatically promoted to narratives."
)

print(
    "Wave metrics are strictly chronological."
)

print(
    "Current token enters history only after its own metrics are computed."
)

print(
    "Do NOT run performance analysis until cluster quality is accepted."
)

db.close()
