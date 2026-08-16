#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"

T59 = "t59_capv2_prospective"
T82 = "t82_target5_prospective"

# T59 historical/prospective boundary
# Read dynamically from table where possible.

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


def stdev(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.pstdev(xs) if len(xs) >= 2 else None


def quantile(xs, q):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    p = (len(xs)-1)*q

    lo = int(
        math.floor(p)
    )

    hi = int(
        math.ceil(p)
    )

    if lo == hi:
        return xs[lo]

    w = p-lo

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def standardized_diff(a, b):
    """
    Difference in means divided by pooled population SD.
    Descriptive only.
    """

    aa = [x for x in a if valid(x)]
    bb = [x for x in b if valid(x)]

    if len(aa) < 2 or len(bb) < 2:
        return None

    ma = avg(aa)
    mb = avg(bb)

    va = statistics.pvariance(aa)
    vb = statistics.pvariance(bb)

    pooled = math.sqrt(
        (va + vb) / 2.0
    )

    if pooled <= 1e-12:
        return None

    return (
        mb - ma
    ) / pooled


def tail(xs, t):

    xs = [
        x for x in xs
        if valid(x)
    ]

    up = sum(
        x >= t
        for x in xs
    )

    down = sum(
        x <= -t
        for x in xs
    )

    return {
        "n": len(xs),
        "up": up,
        "down": down,
        "either": up + down,
    }


# ============================================================
# DB READ ONLY
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute(
    "PRAGMA busy_timeout=5000"
)


# ============================================================
# BOUNDARIES
# ============================================================

t59_boundary_row = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()

t82_boundary_row = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T82}
""").fetchone()


T59_BOUNDARY = int(
    t59_boundary_row[0]
)

T82_BOUNDARY = int(
    t82_boundary_row[0]
)


# ============================================================
# BASE QUERY
# ============================================================

base_sql = """
SELECT
    e.id,
    e.timestamp,
    e.token_mint,

    e.fa,
    e.new_wallets30,

    e.swaps5,
    e.swaps10,
    e.swaps30,
    e.swaps60,

    e.buyers5,
    e.buyers10,
    e.buyers30,
    e.buyers60,

    e.wallets30,
    e.wallets60,

    e.buyer_growth,
    e.wallet_growth,

    e.dex_return_60s,
    e.dex_done_60s,
    e.dex_delay_60s,

    s.recent_buy_share,
    s.recent_net_share,
    s.breadth_score,
    s.late_chase_score,

    s.early_price_return,
    s.early_net_sol

FROM events e

LEFT JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
"""


all_rows = db.execute(
    base_sql
).fetchall()


# ============================================================
# BUILD DERIVED
# ============================================================

records = []


for r in all_rows:

    early_div = None

    if (
        valid(r["early_price_return"])
        and valid(r["early_net_sol"])
    ):
        early_div = (
            r["early_price_return"]
            - r["early_net_sol"]
        )


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],

        "fa": r["fa"],
        "new_wallets30": r["new_wallets30"],

        "swaps5": r["swaps5"],
        "swaps10": r["swaps10"],
        "swaps30": r["swaps30"],
        "swaps60": r["swaps60"],

        "buyers5": r["buyers5"],
        "buyers10": r["buyers10"],
        "buyers30": r["buyers30"],
        "buyers60": r["buyers60"],

        "wallets30": r["wallets30"],
        "wallets60": r["wallets60"],

        "buyer_growth": r["buyer_growth"],
        "wallet_growth": r["wallet_growth"],

        "recent_buy_share": r["recent_buy_share"],
        "recent_net_share": r["recent_net_share"],
        "breadth_score": r["breadth_score"],
        "late_chase_score": r["late_chase_score"],

        "early_div": early_div,

        "dex_return_60s": r["dex_return_60s"],
        "dex_done_60s": r["dex_done_60s"],
        "dex_delay_60s": r["dex_delay_60s"],
    })


# ============================================================
# COHORTS
# ============================================================

historical = [
    r for r in records
    if r["id"] <= T59_BOUNDARY
]

t59_forward = [
    r for r in records
    if r["id"] > T59_BOUNDARY
]

t82_forward = [
    r for r in records
    if r["id"] > T82_BOUNDARY
]


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

def first_per_token(rows):

    seen = set()
    out = []

    for r in sorted(
        rows,
        key=lambda x: (
            x["timestamp"],
            x["id"]
        )
    ):

        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


historical_first = first_per_token(
    historical
)

t59_first = first_per_token(
    t59_forward
)

t82_first = first_per_token(
    t82_forward
)


# ============================================================
# FEATURES
# ============================================================

FEATURES = [
    "fa",
    "new_wallets30",

    "swaps5",
    "swaps10",
    "swaps30",
    "swaps60",

    "buyers5",
    "buyers10",
    "buyers30",
    "buyers60",

    "wallets30",
    "wallets60",

    "buyer_growth",
    "wallet_growth",

    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",

    "early_div",
]


# ============================================================
# HEADER
# ============================================================

print("=" * 175)
print(
    "MEMECOIN LAB — T83 HISTORICAL vs PROSPECTIVE REGIME-SHIFT AUDIT"
)
print("=" * 175)

print("MODE              : READ-ONLY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")
print("DB WRITES         : NONE")
print("T59               : UNTOUCHED")
print("T78               : UNTOUCHED")
print("T82               : UNTOUCHED")

print()

print(
    f"T59 BOUNDARY      : {T59_BOUNDARY}"
)

print(
    f"T82 BOUNDARY      : {T82_BOUNDARY}"
)


# ============================================================
# A) COHORT SIZE
# ============================================================

print()
print("=" * 175)
print("A) COHORT SIZES")
print("=" * 175)

for name, rr in [
    ("HISTORICAL", historical),
    ("T59_FORWARD", t59_forward),
    ("T82_FORWARD", t82_forward),
]:

    print(
        f"{name:14} "
        f"| EVENTS={len(rr):4d} "
        f"| TOKENS={len(set(r['token_mint'] for r in rr)):4d}"
    )


# ============================================================
# B) FEATURE SHIFT
# ============================================================

print()
print("=" * 175)
print("B) FEATURE DISTRIBUTION SHIFT — HISTORICAL vs T59 FORWARD")
print("=" * 175)

shift_rows = []


for f in FEATURES:

    h = [
        r[f]
        for r in historical
        if valid(r[f])
    ]

    p = [
        r[f]
        for r in t59_forward
        if valid(r[f])
    ]

    sd = standardized_diff(
        h,
        p
    )

    shift_rows.append(
        (
            abs(sd)
            if sd is not None
            else -1,
            f,
            len(h),
            len(p),
            med(h),
            med(p),
            avg(h),
            avg(p),
            sd
        )
    )


shift_rows.sort(
    reverse=True
)


for (
    _,
    f,
    nh,
    np,
    mh,
    mp,
    ah,
    ap,
    sd
) in shift_rows:

    print(
        f"{f:24} "
        f"| HIST N={nh:4d} MED={fmt(mh):>8} AVG={fmt(ah):>8} "
        f"| FWD N={np:4d} MED={fmt(mp):>8} AVG={fmt(ap):>8} "
        f"| STD_DIFF={fmt(sd):>8}"
    )


# ============================================================
# C) FIRST TOKEN SHIFT
# ============================================================

print()
print("=" * 175)
print("C) FIRST-EVENT/TOKEN SHIFT")
print("=" * 175)


for f in FEATURES:

    h = [
        r[f]
        for r in historical_first
        if valid(r[f])
    ]

    p = [
        r[f]
        for r in t59_first
        if valid(r[f])
    ]

    sd = standardized_diff(
        h,
        p
    )

    print(
        f"{f:24} "
        f"| HIST MED={fmt(med(h)):>8} "
        f"| FWD MED={fmt(med(p)):>8} "
        f"| STD_DIFF={fmt(sd):>8}"
    )


# ============================================================
# D) RETURN DISTRIBUTION SHIFT
# ============================================================

print()
print("=" * 175)
print("D) DEX RETURN 60s DISTRIBUTION SHIFT")
print("=" * 175)


for name, rr in [
    ("HISTORICAL", historical),
    ("T59_FORWARD", t59_forward),
    ("T82_FORWARD", t82_forward),
]:

    xs = [
        r["dex_return_60s"]
        for r in rr
        if valid(
            r["dex_return_60s"]
        )
    ]

    print()
    print(name)

    print(
        f"N={len(xs)} "
        f"| MEAN={fmt(avg(xs))} "
        f"| MED={fmt(med(xs))} "
        f"| P05={fmt(quantile(xs,0.05))} "
        f"| P95={fmt(quantile(xs,0.95))}"
    )

    for t in [
        3.0,
        5.0,
        7.5,
        10.0,
    ]:

        s = tail(
            xs,
            t
        )

        print(
            f"  ±{t:4.1f}% "
            f"| UP={s['up']:3d} "
            f"| DOWN={s['down']:3d} "
            f"| EITHER={s['either']:3d} "
            f"| RATE={pct(s['either'],s['n']):>6}"
        )


# ============================================================
# E) FIRST TOKEN RETURN SHIFT
# ============================================================

print()
print("=" * 175)
print("E) FIRST-EVENT/TOKEN RETURN SHIFT")
print("=" * 175)


for name, rr in [
    ("HISTORICAL", historical_first),
    ("T59_FORWARD", t59_first),
    ("T82_FORWARD", t82_first),
]:

    xs = [
        r["dex_return_60s"]
        for r in rr
        if valid(
            r["dex_return_60s"]
        )
    ]

    print()
    print(
        f"{name:14} "
        f"| N={len(xs):3d} "
        f"| MEAN={fmt(avg(xs)):>8} "
        f"| MED={fmt(med(xs)):>8}"
    )

    for t in [
        5.0,
        10.0,
    ]:

        s = tail(
            xs,
            t
        )

        print(
            f"  ±{t:4.1f}% "
            f"| UP={s['up']:3d} "
            f"| DOWN={s['down']:3d} "
            f"| RATE={pct(s['either'],s['n']):>6}"
        )


# ============================================================
# F) ZERO MASS
# ============================================================

print()
print("=" * 175)
print("F) ZERO / NEAR-ZERO MASS")
print("=" * 175)


for name, rr in [
    ("HISTORICAL", historical),
    ("T59_FORWARD", t59_forward),
    ("T82_FORWARD", t82_forward),
]:

    xs = [
        r["dex_return_60s"]
        for r in rr
        if valid(
            r["dex_return_60s"]
        )
    ]

    print()
    print(name)

    for eps in [
        0.0,
        0.10,
        0.25,
        0.50,
        1.00,
    ]:

        n = sum(
            abs(x) <= eps
            for x in xs
        )

        print(
            f"  |R60| <= {eps:4.2f}% "
            f"| {n:4d}/{len(xs):4d} "
            f"| {pct(n,len(xs)):>6}"
        )


# ============================================================
# G) DEX DELAYS
# ============================================================

print()
print("=" * 175)
print("G) DEX DELAY SHIFT")
print("=" * 175)


for name, rr in [
    ("HISTORICAL", historical),
    ("T59_FORWARD", t59_forward),
    ("T82_FORWARD", t82_forward),
]:

    ds = [
        r["dex_delay_60s"]
        for r in rr
        if valid(
            r["dex_delay_60s"]
        )
    ]

    print(
        f"{name:14} "
        f"| N={len(ds):4d} "
        f"| MED={fmt(med(ds)):>8} "
        f"| P90={fmt(quantile(ds,0.90)):>8} "
        f"| P99={fmt(quantile(ds,0.99)):>8} "
        f"| MAX={fmt(max(ds) if ds else None):>8}"
    )


# ============================================================
# H) HISTORICAL CHRONO BLOCKS
# ============================================================

print()
print("=" * 175)
print("H) HISTORICAL CHRONOLOGICAL RETURN REGIMES")
print("=" * 175)


N = len(
    historical
)

blocks = [
    (
        "HIST_Q1",
        historical[:N//4]
    ),

    (
        "HIST_Q2",
        historical[N//4:N//2]
    ),

    (
        "HIST_Q3",
        historical[N//2:(3*N)//4]
    ),

    (
        "HIST_Q4",
        historical[(3*N)//4:]
    ),
]


for name, rr in blocks:

    xs = [
        r["dex_return_60s"]
        for r in rr
        if valid(
            r["dex_return_60s"]
        )
    ]

    s5 = tail(
        xs,
        5.0
    )

    s10 = tail(
        xs,
        10.0
    )

    print(
        f"{name:8} "
        f"| N={len(xs):4d} "
        f"| MEAN={fmt(avg(xs)):>8} "
        f"| ±5={pct(s5['either'],s5['n']):>6} "
        f"| ±10={pct(s10['either'],s10['n']):>6}"
    )


# ============================================================
# I) SHIFT SCORECARD
# ============================================================

print()
print("=" * 175)
print("I) SHIFT SCORECARD")
print("=" * 175)


big_shifts = [
    x
    for x in shift_rows
    if (
        x[-1] is not None
        and abs(x[-1]) >= 0.50
    )
]


moderate_shifts = [
    x
    for x in shift_rows
    if (
        x[-1] is not None
        and abs(x[-1]) >= 0.25
    )
]


hist_r60 = [
    r["dex_return_60s"]
    for r in historical
    if valid(
        r["dex_return_60s"]
    )
]


fwd_r60 = [
    r["dex_return_60s"]
    for r in t59_forward
    if valid(
        r["dex_return_60s"]
    )
]


hist5 = tail(
    hist_r60,
    5.0
)

fwd5 = tail(
    fwd_r60,
    5.0
)

hist10 = tail(
    hist_r60,
    10.0
)

fwd10 = tail(
    fwd_r60,
    10.0
)


hist5_rate = (
    hist5["either"]
    / hist5["n"]
    if hist5["n"]
    else None
)

fwd5_rate = (
    fwd5["either"]
    / fwd5["n"]
    if fwd5["n"]
    else None
)

hist10_rate = (
    hist10["either"]
    / hist10["n"]
    if hist10["n"]
    else None
)

fwd10_rate = (
    fwd10["either"]
    / fwd10["n"]
    if fwd10["n"]
    else None
)


print(
    f"FEATURE |STD DIFF| >=0.25 : "
    f"{len(moderate_shifts)}"
)

print(
    f"FEATURE |STD DIFF| >=0.50 : "
    f"{len(big_shifts)}"
)

print()

print(
    f"HIST ±5 RATE       : "
    f"{fmt(hist5_rate,3)}"
)

print(
    f"FWD ±5 RATE        : "
    f"{fmt(fwd5_rate,3)}"
)

print(
    f"HIST ±10 RATE      : "
    f"{fmt(hist10_rate,3)}"
)

print(
    f"FWD ±10 RATE       : "
    f"{fmt(fwd10_rate,3)}"
)


# ============================================================
# J) DECISION
# ============================================================

print()
print("=" * 175)
print("J) DECISION SUPPORT")
print("=" * 175)


major_return_drop = (
    hist10_rate is not None
    and fwd10_rate is not None
    and hist10_rate > 0
    and fwd10_rate
        <= 0.50 * hist10_rate
)


moderate_feature_shift = (
    len(moderate_shifts) >= 3
)


if (
    major_return_drop
    and moderate_feature_shift
):

    print(
        "🟠 PROSPECTIVE DATA APPEARS TO BE IN A "
        "DIFFERENT MARKET / EVENT REGIME."
    )

    print(
        "Historical edge estimates should not be assumed stationary."
    )

elif major_return_drop:

    print(
        "🟡 RETURN REGIME HAS SHIFTED MATERIALLY, "
        "BUT INPUT-FEATURE SHIFT IS LIMITED."
    )

elif moderate_feature_shift:

    print(
        "🟡 INPUT DISTRIBUTION SHIFT DETECTED."
    )

else:

    print(
        "🟢 NO LARGE STRUCTURAL REGIME SHIFT DETECTED "
        "BY THIS AUDIT."
    )


print()
print(
    "T83 does NOT modify any model, target or threshold."
)

print(
    "T83 is descriptive only."
)

db.close()
