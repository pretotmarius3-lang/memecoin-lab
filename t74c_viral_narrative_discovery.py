#!/usr/bin/env python3

import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"
CANON = "t74_narrative_canonical"

RUNNER = 10.0
DUMP = -10.0


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def label_r60(x):

    if not valid(x):
        return None

    if x >= RUNNER:
        return 1

    if x <= DUMP:
        return 0

    return None


def auc_directional(y, x):

    pairs = [
        (yy, xx)
        for yy, xx in zip(y, x)
        if yy is not None and valid(xx)
    ]

    pos = [
        xx for yy, xx in pairs
        if yy == 1
    ]

    neg = [
        xx for yy, xx in pairs
        if yy == 0
    ]

    if not pos or not neg:
        return None, None, None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:

            total += 1

            if a > b:
                wins += 1.0

            elif a == b:
                wins += 0.5

    raw = wins / total

    if raw >= 0.50:
        return raw, raw, "HIGHER"

    return raw, 1.0-raw, "LOWER"


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
# LOAD FIRST-EVENT OUTCOMES
#
# One outcome per token.
# Canonical features were defined before labels were opened.
# ============================================================

outcomes = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,
    e.dex_return_60s

FROM events e

JOIN (
    SELECT
        token_mint,
        MIN(id) AS first_event_id

    FROM events

    WHERE
        token_mint IS NOT NULL
        AND timestamp IS NOT NULL

    GROUP BY
        token_mint
) f

ON
    f.token_mint=e.token_mint
    AND f.first_event_id=e.id

WHERE
    e.dex_return_60s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


outcome_by_token = {}

for r in outcomes:

    y = label_r60(
        r["dex_return_60s"]
    )

    if y is None:
        continue

    outcome_by_token[
        r["token_mint"]
    ] = {
        "id":
            r["id"],

        "timestamp":
            r["timestamp"],

        "token_mint":
            r["token_mint"],

        "r60":
            r["dex_return_60s"],

        "y":
            y,
    }


# ============================================================
# LOAD CANONICAL WAVE FEATURES
# ============================================================

rows = db.execute(f"""
SELECT
    token_mint,

    first_event_id,
    first_seen_at,

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

FROM {CANON}

WHERE
    level IN (
        'NARRATIVE',
        'ENTITY',
        'BROAD_THEME'
    )

ORDER BY
    first_seen_at,
    first_event_id
""").fetchall()


# ============================================================
# AGGREGATE PER TOKEN / LEVEL
#
# Blind aggregation:
# max activity/acceleration, earliest rank,
# minimum age.
# No outcome-dependent choice.
# ============================================================

bucket = defaultdict(list)


for r in rows:

    bucket[
        (
            r["token_mint"],
            r["level"]
        )
    ].append(r)


records = []


for (
    token,
    level
), rr in bucket.items():

    if token not in outcome_by_token:
        continue


    def vals(col):
        return [
            r[col]
            for r in rr
            if valid(r[col])
        ]


    prior15 = vals(
        "prior_15m"
    )

    prior60 = vals(
        "prior_60m"
    )

    prior6 = vals(
        "prior_6h"
    )

    prior24 = vals(
        "prior_24h"
    )

    v15 = vals(
        "velocity_15m"
    )

    v60 = vals(
        "velocity_60m"
    )

    accel = vals(
        "acceleration"
    )

    ranks = vals(
        "rank"
    )

    ages = vals(
        "age_sec"
    )


    features = {
        "prior_15m":
            max(prior15)
            if prior15
            else None,

        "prior_60m":
            max(prior60)
            if prior60
            else None,

        "prior_6h":
            max(prior6)
            if prior6
            else None,

        "prior_24h":
            max(prior24)
            if prior24
            else None,

        "velocity_15m":
            max(v15)
            if v15
            else None,

        "velocity_60m":
            max(v60)
            if v60
            else None,

        "acceleration":
            max(accel)
            if accel
            else None,

        # low rank = earlier in narrative wave
        "rank":
            min(ranks)
            if ranks
            else None,

        # younger wave among assignments
        "age_sec":
            min(ages)
            if ages
            else None,

        "assignment_count":
            len(rr),
    }


    o = outcome_by_token[
        token
    ]


    records.append({
        "id":
            o["id"],

        "timestamp":
            o["timestamp"],

        "token_mint":
            token,

        "r60":
            o["r60"],

        "y":
            o["y"],

        "level":
            level,

        "features":
            features,
    })


# ============================================================
# FEATURE FAMILY
# ============================================================

FEATURES = [
    "prior_15m",
    "prior_60m",
    "prior_6h",
    "prior_24h",
    "velocity_15m",
    "velocity_60m",
    "acceleration",
    "rank",
    "age_sec",
    "assignment_count",
]


LEVELS = [
    "NARRATIVE",
    "ENTITY",
    "BROAD_THEME",
]


# ============================================================
# GLOBAL ANALYSIS
# ============================================================

global_results = []


for level in LEVELS:

    level_rows = [
        r for r in records
        if r["level"] == level
    ]


    for feature in FEATURES:

        rr = [
            r for r in level_rows
            if valid(
                r["features"].get(
                    feature
                )
            )
        ]


        y = [
            r["y"]
            for r in rr
        ]

        x = [
            r["features"][
                feature
            ]
            for r in rr
        ]


        _, da, direction = auc_directional(
            y,
            x
        )


        run = [
            xx
            for yy,xx in zip(y,x)
            if yy == 1
        ]

        dump = [
            xx
            for yy,xx in zip(y,x)
            if yy == 0
        ]


        global_results.append({
            "level":
                level,

            "feature":
                feature,

            "n":
                len(rr),

            "run":
                len(run),

            "dump":
                len(dump),

            "run_med":
                med(run),

            "dump_med":
                med(dump),

            "diff":
                (
                    med(run)-med(dump)
                    if run and dump
                    else None
                ),

            "auc":
                da,

            "direction":
                direction,
        })


global_results.sort(
    key=lambda r: (
        -(r["auc"] or 0),
        -r["n"]
    )
)


# ============================================================
# CHRONOLOGICAL HALVES
# ============================================================

half_results = {}


for level in LEVELS:

    level_rows = sorted(
        [
            r for r in records
            if r["level"] == level
        ],
        key=lambda r: (
            r["timestamp"],
            r["id"]
        )
    )


    cut = (
        len(level_rows)//2
    )


    halves = {
        "EARLY":
            level_rows[:cut],

        "LATE":
            level_rows[cut:],
    }


    for feature in FEATURES:

        key = (
            level,
            feature
        )

        half_results[
            key
        ] = {}


        for hname, rr0 in halves.items():

            rr = [
                r for r in rr0
                if valid(
                    r["features"].get(
                        feature
                    )
                )
            ]


            y = [
                r["y"]
                for r in rr
            ]

            x = [
                r["features"][
                    feature
                ]
                for r in rr
            ]


            _, da, direction = auc_directional(
                y,
                x
            )


            half_results[
                key
            ][
                hname
            ] = {
                "n":
                    len(rr),

                "auc":
                    da,

                "direction":
                    direction,
            }


# ============================================================
# COVERAGE
# ============================================================

all_binary_tokens = set(
    outcome_by_token.keys()
)


level_token_sets = {
    level:
        set(
            r["token_mint"]
            for r in records
            if r["level"] == level
        )
    for level in LEVELS
}


# ============================================================
# OUTPUT
# ============================================================

print("=" * 185)
print(
    "MEMECOIN LAB — T74C VIRAL NARRATIVE DISCOVERY"
)
print("=" * 185)

print(
    f"ALL FIRST-EVENT BINARY TOKENS : "
    f"{len(all_binary_tokens)}"
)

print(
    f"CANONICAL BINARY RECORDS       : "
    f"{len(records)}"
)

print()

for level in LEVELS:

    n = len(
        level_token_sets[
            level
        ]
    )

    coverage = (
        100*n/len(all_binary_tokens)
        if all_binary_tokens
        else 0
    )

    print(
        f"{level:14} "
        f"TOKENS={n:3d} "
        f"| COVERAGE={coverage:5.1f}%"
    )


print()
print(
    "LABEL              : first-event dex_return_60s"
)

print(
    "RUN                : >= +10%"
)

print(
    "DUMP               : <= -10%"
)

print(
    "MODEL FITTING      : NO"
)

print(
    "THRESHOLD SEARCH   : NO"
)

print(
    "CANONICALIZATION   : FROZEN BEFORE LABEL ACCESS"
)

print(
    "T59                : UNTOUCHED"
)


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 185)
print("A) GLOBAL UNIVARIATE RANKING")
print("=" * 185)


for r in global_results:

    print(
        f"{r['level']:12} "
        f"| {r['feature']:18} "
        f"| N={r['n']:3d} "
        f"RUN={r['run']:3d} "
        f"DUMP={r['dump']:3d} "
        f"| RUN_MED={fmt(r['run_med']):>9} "
        f"DUMP_MED={fmt(r['dump_med']):>9} "
        f"| DIFF={fmt(r['diff']):>9} "
        f"| DIR={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])}"
    )


# ============================================================
# B) CHRONOLOGICAL STABILITY
# ============================================================

print()
print("=" * 185)
print("B) CHRONOLOGICAL HALF STABILITY")
print("=" * 185)


top = [
    r
    for r in global_results
    if r["auc"] is not None
][:20]


for r in top:

    key = (
        r["level"],
        r["feature"]
    )

    e = half_results[
        key
    ][
        "EARLY"
    ]

    l = half_results[
        key
    ][
        "LATE"
    ]


    print(
        f"{r['level']:12} "
        f"| {r['feature']:18} "
        f"| GLOBAL DIR={r['direction']:6} "
        f"AUC={fmt(r['auc'])} "
        f"| EARLY N={e['n']:3d} "
        f"DIR={str(e['direction']):6} "
        f"AUC={fmt(e['auc'])} "
        f"| LATE N={l['n']:3d} "
        f"DIR={str(l['direction']):6} "
        f"AUC={fmt(l['auc'])}"
    )


# ============================================================
# C) HYPOTHESIS AUDIT
# ============================================================

print()
print("=" * 185)
print("C) PRE-DECLARED HYPOTHESES")
print("=" * 185)


hypotheses = [
    (
        "H1_NARRATIVE_ACCELERATION",
        "NARRATIVE",
        "acceleration",
        "HIGHER"
    ),

    (
        "H2_NARRATIVE_RANK",
        "NARRATIVE",
        "rank",
        "LOWER"
    ),

    (
        "H3_NARRATIVE_PRIOR15",
        "NARRATIVE",
        "prior_15m",
        "HIGHER"
    ),

    (
        "H4_ENTITY_ACCELERATION",
        "ENTITY",
        "acceleration",
        "HIGHER"
    ),

    (
        "H5_BROAD_ACCELERATION",
        "BROAD_THEME",
        "acceleration",
        "HIGHER"
    ),
]


result_lookup = {
    (
        r["level"],
        r["feature"]
    ):
        r
    for r in global_results
}


for (
    name,
    level,
    feature,
    expected
) in hypotheses:

    r = result_lookup[
        (
            level,
            feature
        )
    ]

    direction_match = (
        r["direction"]
        == expected
    )

    print(
        f"{name:30} "
        f"| N={r['n']:3d} "
        f"| EXPECT={expected:6} "
        f"| OBS={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])} "
        f"| DIR_MATCH="
        f"{'YES' if direction_match else 'NO'}"
    )


# ============================================================
# D) CONSERVATIVE DISCOVERY GATE
# ============================================================

print()
print("=" * 185)
print("D) CONSERVATIVE DISCOVERY GATE")
print("=" * 185)


survivors = []


for g in global_results:

    # Require enough examples and both classes.
    if (
        g["n"] < 15
        or g["run"] < 4
        or g["dump"] < 4
        or g["auc"] is None
    ):
        continue


    h = half_results[
        (
            g["level"],
            g["feature"]
        )
    ]

    e = h[
        "EARLY"
    ]

    l = h[
        "LATE"
    ]


    same_direction = (
        g["direction"] is not None
        and e["direction"] is not None
        and l["direction"] is not None
        and g["direction"]
            == e["direction"]
            == l["direction"]
    )


    passes = (
        g["auc"] >= 0.57

        and e["auc"] is not None
        and e["auc"] >= 0.53

        and l["auc"] is not None
        and l["auc"] >= 0.53

        and same_direction
    )


    if passes:

        survivors.append({
            **g,

            "early_auc":
                e["auc"],

            "late_auc":
                l["auc"],
        })


survivors.sort(
    key=lambda r: (
        -min(
            r["auc"],
            r["early_auc"],
            r["late_auc"]
        ),
        -r["n"]
    )
)


if survivors:

    for r in survivors:

        print(
            f"{r['level']:12} "
            f"| {r['feature']:18} "
            f"| DIR={r['direction']:6} "
            f"| N={r['n']:3d} "
            f"| GLOBAL={r['auc']:.3f} "
            f"| EARLY={r['early_auc']:.3f} "
            f"| LATE={r['late_auc']:.3f}"
        )

else:

    print(
        "No viral/narrative feature passes "
        "the conservative discovery gate."
    )


# ============================================================
# E) LEVEL COMPARISON
# ============================================================

print()
print("=" * 185)
print("E) LEVEL COMPARISON — ACCELERATION")
print("=" * 185)


for level in LEVELS:

    r = result_lookup[
        (
            level,
            "acceleration"
        )
    ]

    print(
        f"{level:14} "
        f"| N={r['n']:3d} "
        f"| DIR={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])}"
    )


# ============================================================
# F) DECISION
# ============================================================

print()
print("=" * 185)
print("F) DECISION SUPPORT")
print("=" * 185)


if survivors:

    best = survivors[0]

    print(
        "🟡 VIRAL/NARRATIVE FAMILY CONTAINS "
        "A ROBUSTNESS CANDIDATE."
    )

    print(
        f"PRIMARY T75 CANDIDATE = "
        f"{best['level']}.{best['feature']}"
    )

    print(
        f"FROZEN DIRECTION      = "
        f"{best['direction']}"
    )

    print(
        "Next = T75 robustness audit."
    )

    print(
        "Do NOT optimize thresholds."
    )

else:

    print(
        "🔴 NO VIRAL/NARRATIVE FEATURE "
        "SURVIVES THIS DISCOVERY GATE."
    )

    print(
        "Do not force the family."
    )

    print(
        "T59 remains untouched."
    )


print()
print("IMPORTANT:")
print("• T74B.2 canonicalization was completed blind to outcomes.")
print("• T74C uses one first-event outcome per token.")
print("• Neutral outcomes between -10% and +10% are excluded.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• No narrative-specific performance selection.")
print("• T59 remains frozen and untouched.")
print("• T74C writes nothing to DB.")

db.close()
