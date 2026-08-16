#!/usr/bin/env python3

import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

PRIMARY = "median_abs_r300_prior_30m"

ACTIVATION_THRESHOLD = 3.0
LOOKBACK = 1800.0
OUTCOME_DELAY = 300.0
MIN_PRIOR_OUTCOMES = 5

BOOT_N = 5000
SEED = 94


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    n = len(xs)

    if n % 2:
        return xs[n//2]

    return (
        xs[n//2-1]
        + xs[n//2]
    ) / 2.0


def avg(xs):
    xs = [x for x in xs if valid(x)]
    return sum(xs)/len(xs) if xs else None


def quantile(xs, q):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    pos = (len(xs)-1)*q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos-lo

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def raw_auc(rows):

    pos = [
        r[PRIMARY]
        for r in rows
        if (
            r["activation"] == 1
            and valid(r[PRIMARY])
        )
    ]

    neg = [
        r[PRIMARY]
        for r in rows
        if (
            r["activation"] == 0
            and valid(r[PRIMARY])
        )
    ]

    if not pos or not neg:
        return None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:

            total += 1

            if a > b:
                wins += 1.0

            elif a == b:
                wins += 0.5

    return wins / total


def directional(raw, frozen_direction):

    if raw is None:
        return None

    if frozen_direction == "HIGHER":
        return raw

    return 1.0-raw


def med_diff(rows):

    yes = [
        r[PRIMARY]
        for r in rows
        if (
            r["activation"] == 1
            and valid(r[PRIMARY])
        )
    ]

    no = [
        r[PRIMARY]
        for r in rows
        if (
            r["activation"] == 0
            and valid(r[PRIMARY])
        )
    ]

    if not yes or not no:
        return None

    return med(yes)-med(no)


# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


boundary = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()[0]

if boundary is None:
    raise RuntimeError(
        "Cannot determine T59 boundary."
    )

boundary = int(boundary)


rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    dex_return_30s,
    dex_done_30s,

    dex_return_300s,
    dex_done_300s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


# ============================================================
# BUILD PROSPECTIVE-SAFE FEATURE
# ============================================================

records = []


for i, current in enumerate(rows):

    t = current["timestamp"]

    activation = None

    if (
        current["dex_done_30s"] == 1
        and valid(current["dex_return_30s"])
    ):

        activation = int(
            abs(current["dex_return_30s"])
            >= ACTIVATION_THRESHOLD
        )


    prior_r300 = []

    for j in range(i-1, -1, -1):

        previous = rows[j]

        if previous["timestamp"] < t-LOOKBACK:
            break

        # Critical provenance rule:
        # this R300 outcome must ALREADY have matured.
        if previous["timestamp"] > t-OUTCOME_DELAY:
            continue

        if (
            previous["dex_done_300s"] == 1
            and valid(
                previous["dex_return_300s"]
            )
        ):

            prior_r300.append(
                abs(
                    previous[
                        "dex_return_300s"
                    ]
                )
            )


    feature = None

    if len(prior_r300) >= MIN_PRIOR_OUTCOMES:

        feature = med(
            prior_r300
        )


    records.append({
        "id":
            current["id"],

        "timestamp":
            current["timestamp"],

        "token_mint":
            current["token_mint"],

        "historical":
            current["id"] <= boundary,

        "activation":
            activation,

        PRIMARY:
            feature,

        "prior_count":
            len(prior_r300),
    })


usable = [
    r for r in records
    if (
        r["activation"] is not None
        and valid(r[PRIMARY])
    )
]


hist = [
    r for r in usable
    if r["historical"]
]

pros = [
    r for r in usable
    if not r["historical"]
]


# ============================================================
# FREEZE DIRECTION FROM T93 HYPOTHESIS
#
# We infer orientation from HIST only here and then NEVER
# flip it for any subsequent audit.
# ============================================================

hist_raw = raw_auc(hist)

if hist_raw is None:
    raise RuntimeError(
        "Insufficient historical classes."
    )


FROZEN_DIRECTION = (
    "HIGHER"
    if hist_raw >= 0.5
    else "LOWER"
)


# ============================================================
# HEADER
# ============================================================

print("=" * 185)
print(
    "MEMECOIN LAB — T94 FROZEN GLOBAL MARKET-STATE ACTIVATION ROBUSTNESS AUDIT"
)
print("=" * 185)

print(
    f"PRIMARY FEATURE       : {PRIMARY}"
)

print(
    f"DEFINITION            : median |R300| of matured prior events in previous 30m"
)

print(
    f"OUTCOME SAFETY        : source event must be >=300s old"
)

print(
    f"MIN PRIOR OUTCOMES    : {MIN_PRIOR_OUTCOMES}"
)

print(
    f"ACTIVATION TARGET     : |R30| >= {ACTIVATION_THRESHOLD:.1f}%"
)

print(
    f"FROZEN DIRECTION      : {FROZEN_DIRECTION}"
)

print()

print(
    f"USABLE EVENTS         : {len(usable)}"
)

print(
    f"UNIQUE TOKENS         : "
    f"{len(set(r['token_mint'] for r in usable))}"
)

print()

print("MODEL FITTING         : NONE")
print("THRESHOLD SEARCH      : NONE")
print("DB WRITES             : NONE")
print("T59/T78/T82/T86       : UNTOUCHED")


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 185)
print("A) GLOBAL")
print("=" * 185)

global_raw = raw_auc(
    usable
)

global_auc = directional(
    global_raw,
    FROZEN_DIRECTION
)

print(
    f"N={len(usable)} "
    f"| YES={sum(r['activation'] for r in usable)} "
    f"| NO={sum(1-r['activation'] for r in usable)}"
)

print(
    f"RAW HIGHER AUC={fmt(global_raw)}"
)

print(
    f"FROZEN DIR AUC={fmt(global_auc)}"
)

print(
    f"MEDIAN DIFF YES-NO={fmt(med_diff(usable))}"
)


# ============================================================
# B) REGIMES
# ============================================================

print()
print("=" * 185)
print("B) HISTORICAL / PROSPECTIVE")
print("=" * 185)

for name, rr in [
    ("HISTORICAL", hist),
    ("PROSPECTIVE", pros),
]:

    raw = raw_auc(rr)

    print(
        f"{name:12} "
        f"| N={len(rr):4d} "
        f"| YES={sum(r['activation'] for r in rr):4d} "
        f"| NO={sum(1-r['activation'] for r in rr):4d} "
        f"| DIFF={fmt(med_diff(rr)):>8} "
        f"| DIR_AUC={fmt(directional(raw,FROZEN_DIRECTION))}"
    )


# ============================================================
# C) CHRONO BLOCKS
# ============================================================

print()
print("=" * 185)
print("C) CHRONOLOGICAL BLOCK ROBUSTNESS")
print("=" * 185)

N = len(usable)

blocks = [
    ("T1", usable[:N//3]),
    ("T2", usable[N//3:(2*N)//3]),
    ("T3", usable[(2*N)//3:]),

    ("Q1", usable[:N//4]),
    ("Q2", usable[N//4:N//2]),
    ("Q3", usable[N//2:(3*N)//4]),
    ("Q4", usable[(3*N)//4:]),
]


audits = []


for name, rr in blocks:

    yes = sum(
        r["activation"]
        for r in rr
    )

    no = len(rr)-yes

    raw = raw_auc(rr)

    da = directional(
        raw,
        FROZEN_DIRECTION
    )

    print(
        f"{name:4} "
        f"| N={len(rr):4d} "
        f"| YES={yes:3d} "
        f"| NO={no:3d} "
        f"| DIFF={fmt(med_diff(rr)):>8} "
        f"| DIR_AUC={fmt(da)}"
    )

    if (
        yes >= 3
        and no >= 3
        and da is not None
    ):
        audits.append(
            (name, da)
        )


# ============================================================
# D) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 185)
print("D) FIRST-EVENT/TOKEN")
print("=" * 185)

seen = set()
first = []

for r in usable:

    if r["token_mint"] in seen:
        continue

    seen.add(
        r["token_mint"]
    )

    first.append(r)


first_raw = raw_auc(
    first
)

first_auc = directional(
    first_raw,
    FROZEN_DIRECTION
)


print(
    f"N={len(first)} "
    f"| YES={sum(r['activation'] for r in first)} "
    f"| NO={sum(1-r['activation'] for r in first)} "
    f"| DIFF={fmt(med_diff(first))} "
    f"| DIR_AUC={fmt(first_auc)}"
)


# ============================================================
# E) FIRST TOKEN BY REGIME
# ============================================================

print()
print("=" * 185)
print("E) FIRST-EVENT/TOKEN BY REGIME")
print("=" * 185)


for name, base in [
    ("HIST", hist),
    ("PROS", pros),
]:

    seen = set()
    rr = []

    for r in base:

        if r["token_mint"] in seen:
            continue

        seen.add(
            r["token_mint"]
        )

        rr.append(r)


    raw = raw_auc(
        rr
    )

    print(
        f"{name:5} "
        f"| TOK={len(rr):3d} "
        f"| YES={sum(r['activation'] for r in rr):3d} "
        f"| NO={sum(1-r['activation'] for r in rr):3d} "
        f"| DIR_AUC="
        f"{fmt(directional(raw,FROZEN_DIRECTION))}"
    )


# ============================================================
# F) TOKEN BOOTSTRAP
# ============================================================

print()
print("=" * 185)
print("F) TOKEN-LEVEL BOOTSTRAP")
print("=" * 185)

by_token = defaultdict(list)

for r in usable:

    by_token[
        r["token_mint"]
    ].append(r)


tokens = list(
    by_token
)


rng = random.Random(
    SEED
)

boots = []


for _ in range(BOOT_N):

    sampled = [
        rng.choice(tokens)
        for _ in range(len(tokens))
    ]

    rr = []

    for tok in sampled:

        rr.extend(
            by_token[tok]
        )

    raw = raw_auc(
        rr
    )

    da = directional(
        raw,
        FROZEN_DIRECTION
    )

    if da is not None:
        boots.append(da)


print(
    f"BOOT N={len(boots)}"
)

print(
    f"MED AUC={fmt(med(boots))}"
)

print(
    f"95% CI=["
    f"{fmt(quantile(boots,0.025))}, "
    f"{fmt(quantile(boots,0.975))}]"
)

print(
    f"P(AUC>0.50)="
    f"{100*sum(x>0.50 for x in boots)/len(boots):.1f}%"
)

print(
    f"P(AUC>0.55)="
    f"{100*sum(x>0.55 for x in boots)/len(boots):.1f}%"
)

print(
    f"P(AUC>0.60)="
    f"{100*sum(x>0.60 for x in boots)/len(boots):.1f}%"
)


# ============================================================
# G) LEAVE ONE TOKEN OUT
# ============================================================

print()
print("=" * 185)
print("G) LEAVE-ONE-TOKEN-OUT")
print("=" * 185)

loo = []

for tok in tokens:

    rr = [
        r for r in usable
        if r["token_mint"] != tok
    ]

    raw = raw_auc(
        rr
    )

    da = directional(
        raw,
        FROZEN_DIRECTION
    )

    if da is not None:

        loo.append(
            (
                da,
                tok,
                len(by_token[tok])
            )
        )


loo.sort()

vals = [
    x[0]
    for x in loo
]


print(
    f"TOKENS={len(vals)} "
    f"| MED={fmt(med(vals))} "
    f"| WORST={fmt(min(vals))} "
    f"| BEST={fmt(max(vals))}"
)


# ============================================================
# H) PRIOR-SAMPLE DEPTH
# ============================================================

print()
print("=" * 185)
print("H) PRIOR OUTCOME DEPTH SENSITIVITY")
print("=" * 185)

for minimum in [
    5,
    8,
    10,
    15,
    20,
]:

    rr = [
        r for r in usable
        if r["prior_count"] >= minimum
    ]

    yes = sum(
        r["activation"]
        for r in rr
    )

    no = len(rr)-yes

    raw = raw_auc(
        rr
    )

    print(
        f"MIN_PRIOR={minimum:2d} "
        f"| N={len(rr):4d} "
        f"| YES={yes:3d} "
        f"| NO={no:3d} "
        f"| DIR_AUC="
        f"{fmt(directional(raw,FROZEN_DIRECTION))}"
    )


# ============================================================
# I) STATE DISTRIBUTION BY REGIME
# ============================================================

print()
print("=" * 185)
print("I) MARKET-STATE DISTRIBUTION")
print("=" * 185)

for name, rr in [
    ("HIST", hist),
    ("PROS", pros),
]:

    vals = [
        r[PRIMARY]
        for r in rr
        if valid(r[PRIMARY])
    ]

    print(
        f"{name:5} "
        f"| N={len(vals):4d} "
        f"| MED={fmt(med(vals)):>8} "
        f"| P10={fmt(quantile(vals,0.10)):>8} "
        f"| P90={fmt(quantile(vals,0.90)):>8}"
    )


# ============================================================
# J) ROBUSTNESS SCORECARD
# ============================================================

print()
print("=" * 185)
print("J) ROBUSTNESS SCORECARD")
print("=" * 185)


for name, rr in [
    ("HIST", hist),
    ("PROS", pros),
    ("FIRST", first),
]:

    yes = sum(
        r["activation"]
        for r in rr
    )

    no = len(rr)-yes

    da = directional(
        raw_auc(rr),
        FROZEN_DIRECTION
    )

    if (
        yes >= 3
        and no >= 3
        and da is not None
    ):

        audits.append(
            (name, da)
        )


auc55 = sum(
    a >= 0.55
    for _,a in audits
)

auc60 = sum(
    a >= 0.60
    for _,a in audits
)


print(
    f"USABLE AUDITS      = {len(audits)}"
)

print(
    f"AUC >=0.55         = "
    f"{auc55}/{len(audits)}"
)

print(
    f"AUC >=0.60         = "
    f"{auc60}/{len(audits)}"
)

print(
    f"MED AUDIT AUC      = "
    f"{fmt(med([a for _,a in audits]))}"
)

print(
    f"WORST AUDIT AUC    = "
    f"{fmt(min(a for _,a in audits))}"
)

print(
    f"BEST AUDIT AUC     = "
    f"{fmt(max(a for _,a in audits))}"
)


# ============================================================
# K) DECISION
# ============================================================

print()
print("=" * 185)
print("K) DECISION SUPPORT")
print("=" * 185)


hist_auc = directional(
    raw_auc(hist),
    FROZEN_DIRECTION
)

pros_auc = directional(
    raw_auc(pros),
    FROZEN_DIRECTION
)


boot50 = (
    sum(x > 0.50 for x in boots)
    / len(boots)
)


print(
    f"GLOBAL DIR-AUC      = {fmt(global_auc)}"
)

print(
    f"HIST DIR-AUC        = {fmt(hist_auc)}"
)

print(
    f"PROS DIR-AUC        = {fmt(pros_auc)}"
)

print(
    f"FIRST TOKEN AUC     = {fmt(first_auc)}"
)

print(
    f"BOOT P(AUC>0.50)    = {100*boot50:.1f}%"
)

print()


robust = (
    global_auc is not None
    and global_auc >= 0.58

    and hist_auc is not None
    and hist_auc >= 0.58

    and pros_auc is not None
    and pros_auc >= 0.58

    and first_auc is not None
    and first_auc >= 0.55

    and len(audits) >= 7

    and auc55 / len(audits) >= 0.70

    and boot50 >= 0.95
)


if robust:

    print(
        "🟢 FROZEN GLOBAL MARKET-STATE FEATURE "
        "SURVIVES ACTIVATION ROBUSTNESS."
    )

    print(
        "Candidate next = incremental/staged audit against token-level context."
    )

    print(
        "Do NOT optimize a market-state threshold yet."
    )

else:

    print(
        "🔴 GLOBAL MARKET-STATE FEATURE DOES NOT "
        "SURVIVE THE ROBUSTNESS GATE."
    )

    print(
        "Do not build a market permission filter from it."
    )


print()
print("IMPORTANT:")
print("• Candidate discovered in T93.")
print("• Direction frozen before robustness comparisons.")
print("• Feature uses only matured prior R300 outcomes.")
print("• No current/future event outcome enters its state.")
print("• Activation remains |R30| >= 3%.")
print("• No threshold optimization.")
print("• No model fitting.")
print("• Bootstrap resamples whole tokens.")
print("• T94 writes nothing to DB.")
print("• Frozen prospective experiments remain untouched.")

db.close()
