#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

WINDOW = 30.0


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


def sdiv(a, b):
    if not valid(a) or not valid(b) or abs(b) < 1e-12:
        return None
    return a / b


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

    pos = [xx for yy, xx in pairs if yy == 1]
    neg = [xx for yy, xx in pairs if yy == 0]

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

    if raw >= 0.5:
        return raw, raw, "HIGHER"

    return raw, 1.0 - raw, "LOWER"


def pearson(xs, ys):

    pairs = [
        (x, y)
        for x, y in zip(xs, ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xx = [x for x, _ in pairs]
    yy = [y for _, y in pairs]

    mx = avg(xx)
    my = avg(yy)

    dx = math.sqrt(sum((x-mx)**2 for x in xx))
    dy = math.sqrt(sum((y-my)**2 for y in yy))

    if dx == 0 or dy == 0:
        return None

    return sum(
        (x-mx)*(y-my)
        for x, y in pairs
    ) / (dx*dy)


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s,
    fa,
    new_wallets30

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


# ============================================================
# BUILD
# ============================================================

records = []


for e in events:

    y = label_r60(
        e["dex_return_60s"]
    )

    if y is None:
        continue


    ts = e["timestamp"]


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
        e["token_mint"],
        ts-WINDOW,
        ts
    )).fetchall()


    if len(buys) < 2:
        continue


    # first appearance per buyer inside 30s window
    first_by_wallet = {}

    for r in buys:
        first_by_wallet.setdefault(
            r["wallet"],
            r["timestamp"]
        )


    arrivals = sorted(
        first_by_wallet.values()
    )


    if len(arrivals) < 2:
        continue


    # relative time within pre-event window
    rel = [
        t - (ts-WINDOW)
        for t in arrivals
    ]


    gaps = [
        arrivals[i] - arrivals[i-1]
        for i in range(1, len(arrivals))
    ]


    # ========================================================
    # WINDOW COUNTS
    # ========================================================

    # last 5 / 10 / 15 / 30 seconds
    n5 = sum(
        t >= ts-5
        for t in arrivals
    )

    n10 = sum(
        t >= ts-10
        for t in arrivals
    )

    n15 = sum(
        t >= ts-15
        for t in arrivals
    )

    n30 = len(arrivals)


    # ========================================================
    # VELOCITY / ACCELERATION
    # ========================================================

    velocity_5 = n5 / 5.0
    velocity_10 = n10 / 10.0
    velocity_15 = n15 / 15.0
    velocity_30 = n30 / 30.0

    accel_5_vs_30 = (
        velocity_5 - velocity_30
    )

    accel_10_vs_30 = (
        velocity_10 - velocity_30
    )

    accel_15_vs_30 = (
        velocity_15 - velocity_30
    )


    # ========================================================
    # BURST / INTER-ARRIVAL STRUCTURE
    # ========================================================

    gap_mean = avg(gaps)
    gap_std = stdev(gaps)

    gap_cv = sdiv(
        gap_std,
        gap_mean
    )

    gap_med = med(gaps)

    latest_gap = (
        gaps[-1]
        if gaps
        else None
    )

    recent_gap_mean = (
        avg(gaps[-3:])
        if gaps
        else None
    )

    early_gap_mean = (
        avg(gaps[:3])
        if gaps
        else None
    )

    gap_acceleration = (
        early_gap_mean - recent_gap_mean
        if valid(early_gap_mean)
        and valid(recent_gap_mean)
        else None
    )


    # ========================================================
    # ARRIVAL CONCENTRATION
    # ========================================================

    last10_share = sdiv(
        n10,
        n30
    )

    last5_share = sdiv(
        n5,
        n30
    )

    recent_half_share = sdiv(
        n15,
        n30
    )


    # ========================================================
    # ARRIVAL POSITION / SPAN
    # ========================================================

    span = (
        arrivals[-1]
        - arrivals[0]
    )

    first_arrival_age = (
        ts - arrivals[0]
    )

    last_arrival_age = (
        ts - arrivals[-1]
    )


    # ========================================================
    # BUYER TURNOVER
    # ========================================================

    early_wallets = set(
        r["wallet"]
        for r in buys
        if r["timestamp"] < ts-15
    )

    recent_wallets = set(
        r["wallet"]
        for r in buys
        if r["timestamp"] >= ts-15
    )

    union_wallets = (
        early_wallets
        | recent_wallets
    )

    overlap = (
        early_wallets
        & recent_wallets
    )

    recent_new = (
        recent_wallets
        - early_wallets
    )


    turnover_overlap = sdiv(
        len(overlap),
        len(union_wallets)
    )

    recent_new_share = sdiv(
        len(recent_new),
        len(recent_wallets)
    )


    features = {
        "buyer_velocity_5":
            velocity_5,

        "buyer_velocity_10":
            velocity_10,

        "buyer_velocity_15":
            velocity_15,

        "buyer_velocity_30":
            velocity_30,

        "buyer_accel_5_vs_30":
            accel_5_vs_30,

        "buyer_accel_10_vs_30":
            accel_10_vs_30,

        "buyer_accel_15_vs_30":
            accel_15_vs_30,

        "buyer_gap_mean":
            gap_mean,

        "buyer_gap_median":
            gap_med,

        "buyer_gap_cv":
            gap_cv,

        "buyer_latest_gap":
            latest_gap,

        "buyer_gap_acceleration":
            gap_acceleration,

        "buyer_last5_share":
            last5_share,

        "buyer_last10_share":
            last10_share,

        "buyer_recent_half_share":
            recent_half_share,

        "buyer_arrival_span":
            span,

        "buyer_first_arrival_age":
            first_arrival_age,

        "buyer_last_arrival_age":
            last_arrival_age,

        "buyer_turnover_overlap":
            turnover_overlap,

        "buyer_recent_new_share":
            recent_new_share,

        "buyer_unique_30":
            n30,
    }


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "y":
            y,

        "fa":
            e["fa"],

        "new_wallets30":
            e["new_wallets30"],

        "features":
            features,
    })


feature_names = sorted(
    set(
        k
        for r in records
        for k in r["features"]
    )
)


# ============================================================
# GLOBAL
# ============================================================

results = []


for feature in feature_names:

    rr = [
        r for r in records
        if valid(
            r["features"].get(feature)
        )
    ]

    y = [
        r["y"]
        for r in rr
    ]

    x = [
        r["features"][feature]
        for r in rr
    ]

    _, da, direction = auc_directional(
        y,
        x
    )

    run = [
        xx
        for yy, xx in zip(y, x)
        if yy == 1
    ]

    dump = [
        xx
        for yy, xx in zip(y, x)
        if yy == 0
    ]

    results.append({
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


results.sort(
    key=lambda r: (
        -(r["auc"] or 0),
        -r["n"]
    )
)


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

seen = set()
first = []

for r in records:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


first_results = {}


for feature in feature_names:

    rr = [
        r for r in first
        if valid(
            r["features"].get(feature)
        )
    ]

    y = [
        r["y"]
        for r in rr
    ]

    x = [
        r["features"][feature]
        for r in rr
    ]

    _, da, direction = auc_directional(
        y,
        x
    )

    first_results[
        feature
    ] = {
        "n":
            len(rr),

        "auc":
            da,

        "direction":
            direction,
    }


# ============================================================
# CHRONOLOGICAL HALVES
# ============================================================

midpoint = len(records)//2

halves = {
    "EARLY":
        records[:midpoint],

    "LATE":
        records[midpoint:],
}


half_results = {}


for feature in feature_names:

    half_results[
        feature
    ] = {}

    for hname, rr0 in halves.items():

        rr = [
            r for r in rr0
            if valid(
                r["features"].get(feature)
            )
        ]

        y = [
            r["y"]
            for r in rr
        ]

        x = [
            r["features"][feature]
            for r in rr
        ]

        _, da, direction = auc_directional(
            y,
            x
        )

        half_results[
            feature
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
# CONTEXT REDUNDANCY
# ============================================================

redundancy = {}


for feature in feature_names:

    redundancy[
        feature
    ] = {}

    for ctx in [
        "fa",
        "new_wallets30"
    ]:

        xs = []
        ys = []

        for r in records:

            x = r["features"].get(
                feature
            )

            y = r[ctx]

            if valid(x) and valid(y):
                xs.append(x)
                ys.append(y)

        redundancy[
            feature
        ][
            ctx
        ] = pearson(
            xs,
            ys
        )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 185)
print(
    "MEMECOIN LAB — T75 BUYER ARRIVAL DYNAMICS DISCOVERY"
)
print("=" * 185)

print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in records))}"
)

print(
    f"FEATURES       : {len(feature_names)}"
)

print(
    "BUYER WINDOW   : STRICT PRE-EVENT 30s"
)

print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


print()
print("=" * 185)
print("A) GLOBAL UNIVARIATE RANKING")
print("=" * 185)

for r in results:

    print(
        f"{r['feature']:30} "
        f"N={r['n']:3d} "
        f"RUN={r['run']:3d} "
        f"DUMP={r['dump']:3d} "
        f"RUN_MED={fmt(r['run_med']):>8} "
        f"DUMP_MED={fmt(r['dump_med']):>8} "
        f"DIFF={fmt(r['diff']):>8} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['auc'])}"
    )


top15 = [
    r["feature"]
    for r in results[:15]
]


print()
print("=" * 185)
print("B) TOP 15 — FIRST EVENT / TOKEN")
print("=" * 185)

for feature in top15:

    r = first_results[
        feature
    ]

    print(
        f"{feature:30} "
        f"N={r['n']:3d} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['auc'])}"
    )


print()
print("=" * 185)
print("C) TOP 15 — CHRONOLOGICAL HALF STABILITY")
print("=" * 185)

for feature in top15:

    a = half_results[
        feature
    ][
        "EARLY"
    ]

    b = half_results[
        feature
    ][
        "LATE"
    ]

    print(
        f"{feature:30} "
        f"| EARLY N={a['n']:3d} "
        f"DIR={str(a['direction']):6} "
        f"AUC={fmt(a['auc'])} "
        f"| LATE N={b['n']:3d} "
        f"DIR={str(b['direction']):6} "
        f"AUC={fmt(b['auc'])}"
    )


print()
print("=" * 185)
print("D) TOP 15 — CONTEXT REDUNDANCY")
print("=" * 185)

for feature in top15:

    vals = [
        (ctx,c)
        for ctx,c in redundancy[
            feature
        ].items()
        if valid(c)
    ]

    if not vals:
        continue

    ctx,c = max(
        vals,
        key=lambda x:
            abs(x[1])
    )

    print(
        f"{feature:30} "
        f"MAX|CORR|={abs(c):.3f} "
        f"| WITH={ctx:15} "
        f"| CORR={c:+.3f}"
    )


# ============================================================
# GATE
# ============================================================

print()
print("=" * 185)
print("E) CONSERVATIVE DISCOVERY GATE")
print("=" * 185)

survivors = []

for g in results:

    feature = g[
        "feature"
    ]

    f = first_results[
        feature
    ]

    e = half_results[
        feature
    ][
        "EARLY"
    ]

    l = half_results[
        feature
    ][
        "LATE"
    ]

    corrs = [
        abs(c)
        for c in redundancy[
            feature
        ].values()
        if valid(c)
    ]

    maxcorr = (
        max(corrs)
        if corrs
        else None
    )


    same_dir = (
        g["direction"] is not None
        and f["direction"] is not None
        and e["direction"] is not None
        and l["direction"] is not None
        and g["direction"]
            == f["direction"]
            == e["direction"]
            == l["direction"]
    )


    passes = (
        g["n"] >= 80

        and g["auc"] is not None
        and g["auc"] >= 0.57

        and f["auc"] is not None
        and f["auc"] >= 0.55

        and e["auc"] is not None
        and e["auc"] >= 0.53

        and l["auc"] is not None
        and l["auc"] >= 0.53

        and same_dir

        and (
            maxcorr is None
            or maxcorr <= 0.75
        )
    )


    if passes:

        survivors.append({
            "feature":
                feature,

            "direction":
                g["direction"],

            "global_auc":
                g["auc"],

            "first_auc":
                f["auc"],

            "early_auc":
                e["auc"],

            "late_auc":
                l["auc"],

            "maxcorr":
                maxcorr,
        })


survivors.sort(
    key=lambda r: (
        -min(
            r["global_auc"],
            r["first_auc"],
            r["early_auc"],
            r["late_auc"]
        ),
        r["maxcorr"]
        if r["maxcorr"] is not None
        else 0
    )
)


if survivors:

    for r in survivors:

        print(
            f"{r['feature']:30} "
            f"| DIR={r['direction']:6} "
            f"| GLOBAL={r['global_auc']:.3f} "
            f"| FIRST={r['first_auc']:.3f} "
            f"| EARLY={r['early_auc']:.3f} "
            f"| LATE={r['late_auc']:.3f} "
            f"| MAXCORR={fmt(r['maxcorr'])}"
        )

else:

    print(
        "No buyer-arrival feature passes the gate."
    )


print()
print("=" * 185)
print("F) DECISION SUPPORT")
print("=" * 185)

if survivors:

    best = survivors[0]

    print(
        "🟡 BUYER-ARRIVAL FAMILY CONTAINS "
        "A ROBUSTNESS CANDIDATE."
    )

    print(
        f"PRIMARY T76 CANDIDATE = "
        f"{best['feature']}"
    )

    print(
        f"FROZEN DIRECTION      = "
        f"{best['direction']}"
    )

    print(
        "Next = T76 robustness audit."
    )

    print(
        "No threshold optimization."
    )

else:

    print(
        "🔴 NO BUYER-ARRIVAL FEATURE SURVIVES."
    )

    print(
        "Do not force this family."
    )

    print(
        "T59 remains untouched."
    )


print()
print("IMPORTANT:")
print("• Only BUY arrivals strictly before event timestamp are used.")
print("• Buyer identity is deduplicated by first appearance in the 30s window.")
print("• No future swaps.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No DB writes.")
print("• T59 remains frozen and untouched.")
print("• Historical discovery only.")

db.close()
