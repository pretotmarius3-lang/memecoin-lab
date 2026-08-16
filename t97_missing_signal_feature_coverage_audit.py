#!/usr/bin/env python3

import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

ACT = 3.0

FEATURES = [
    "fa", "nf30", "imbalance30", "price_change30",
    "swaps5", "swaps10", "swaps30", "swaps60",
    "buyers5", "buyers10", "buyers30", "buyers60",
    "sellers5", "sellers10", "sellers30", "sellers60",
    "wallets30", "wallets60",
    "new_wallets10", "new_wallets30",
    "buyer_growth", "wallet_growth",
    "buy_volume30", "sell_volume30",
    "largest_buy30", "buy_concentration30",
]

DEX_FEATURES = [
    "liquidity_usd",
    "market_cap",
    "fdv",
    "volume_m5",
    "buys_m5",
    "sells_m5",
]


def valid(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


def median(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def auc(rows, feature, target="activation"):
    pos = [r[feature] for r in rows
           if r[target] == 1 and valid(r.get(feature))]
    neg = [r[feature] for r in rows
           if r[target] == 0 and valid(r.get(feature))]

    if not pos or not neg:
        return None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:
            total += 1
            if a > b:
                wins += 1
            elif a == b:
                wins += .5

    raw = wins / total
    return max(raw, 1-raw)


def overlap(rows, feature):
    """
    Robust central-range overlap:
    overlap of activation/nonactivation IQRs divided by union.
    1 = heavy overlap, 0 = separated.
    """
    a = sorted(r[feature] for r in rows
               if r["activation"] == 1 and valid(r.get(feature)))
    b = sorted(r[feature] for r in rows
               if r["activation"] == 0 and valid(r.get(feature)))

    if len(a) < 4 or len(b) < 4:
        return None

    def q(x, p):
        k = (len(x)-1)*p
        lo = int(math.floor(k))
        hi = int(math.ceil(k))
        if lo == hi:
            return x[lo]
        w = k-lo
        return x[lo]*(1-w)+x[hi]*w

    a1, a3 = q(a,.25), q(a,.75)
    b1, b3 = q(b,.25), q(b,.75)

    intersection = max(0, min(a3,b3)-max(a1,b1))
    union = max(a3,b3)-min(a1,b1)

    if union == 0:
        return 1.0

    return intersection/union


def corr(x, y):
    pairs = [(a,b) for a,b in zip(x,y) if valid(a) and valid(b)]

    if len(pairs) < 3:
        return None

    xx = [p[0] for p in pairs]
    yy = [p[1] for p in pairs]

    mx = sum(xx)/len(xx)
    my = sum(yy)/len(yy)

    sx = math.sqrt(sum((z-mx)**2 for z in xx))
    sy = math.sqrt(sum((z-my)**2 for z in yy))

    if sx == 0 or sy == 0:
        return None

    return sum(
        (a-mx)*(b-my)
        for a,b in pairs
    )/(sx*sy)


db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
db.row_factory = sqlite3.Row

boundary = db.execute(
    f"SELECT MIN(boundary_id) FROM {T59}"
).fetchone()[0]

boundary = int(boundary)


# ============================================================
# EVENTS
# ============================================================

cols = ", ".join(FEATURES)

events = db.execute(f"""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_30s,
    dex_done_30s,
    {cols}
FROM events
WHERE timestamp IS NOT NULL
  AND token_mint IS NOT NULL
  AND dex_done_30s=1
  AND dex_return_30s IS NOT NULL
ORDER BY timestamp,id
""").fetchall()


records = []

for x in events:
    r = dict(x)
    r["activation"] = int(abs(r["dex_return_30s"]) >= ACT)
    r["historical"] = r["id"] <= boundary
    records.append(r)


# ============================================================
# NEAREST DEX SNAPSHOT AT EVENT TIME
# no future snapshot allowed
# ============================================================

dex = db.execute("""
SELECT
    event_id,
    timestamp,
    liquidity_usd,
    market_cap,
    fdv,
    volume_m5,
    buys_m5,
    sells_m5,
    dex_id,
    pair_address
FROM dex_prices
ORDER BY event_id,timestamp
""").fetchall()

dex_by_event = defaultdict(list)

for d in dex:
    dex_by_event[d["event_id"]].append(d)


dex_matched = 0

for r in records:

    candidates = [
        d for d in dex_by_event.get(r["id"], [])
        if d["timestamp"] <= r["timestamp"]
    ]

    # strict no-future match
    if not candidates:
        for f in DEX_FEATURES:
            r[f] = None
        r["dex_id"] = None
        r["pair_address"] = None
        continue

    d = max(candidates, key=lambda z:z["timestamp"])

    dex_matched += 1

    for f in DEX_FEATURES:
        r[f] = d[f]

    r["dex_id"] = d["dex_id"]
    r["pair_address"] = d["pair_address"]


hist = [r for r in records if r["historical"]]
pros = [r for r in records if not r["historical"]]


def first_token(rr):
    seen = set()
    out = []

    for r in rr:
        if r["token_mint"] in seen:
            continue
        seen.add(r["token_mint"])
        out.append(r)

    return out


hf = first_token(hist)
pf = first_token(pros)


print("="*180)
print("MEMECOIN LAB — T97 MISSING SIGNAL / FEATURE COVERAGE AUDIT")
print("="*180)

print("MODE                 : READ-ONLY")
print("MODEL FITTING        : NONE")
print("THRESHOLD SEARCH     : NONE")
print("DB WRITES            : NONE")
print("T59/T78/T82/T86      : UNTOUCHED")
print()
print(f"T59 BOUNDARY         : {boundary}")
print(f"TARGET               : |R30| >= {ACT:.1f}%")
print(f"EVENTS               : {len(records)}")
print(f"TOKENS               : {len(set(r['token_mint'] for r in records))}")
print(f"DEX MATCHED          : {dex_matched}/{len(records)}")


# ============================================================
# A) CLASS BALANCE
# ============================================================

print()
print("="*180)
print("A) CLASS BALANCE")
print("="*180)

for name, rr in [("HIST",hist),("PROS",pros)]:
    yes = sum(r["activation"] for r in rr)
    print(
        f"{name:5} | N={len(rr):4d} "
        f"| ACTIVE={yes:4d} ({100*yes/len(rr):5.1f}%) "
        f"| QUIET={len(rr)-yes:4d}"
    )


# ============================================================
# B) EXISTING FEATURE SPACE
# ============================================================

print()
print("="*180)
print("B) EXISTING FEATURE SPACE — DISCRIMINATION / OVERLAP")
print("="*180)

existing_results = []

for f in FEATURES:

    ha = auc(hist,f)
    pa = auc(pros,f)
    ho = overlap(hist,f)
    po = overlap(pros,f)

    existing_results.append((f,ha,pa,ho,po))

    print(
        f"{f:24} "
        f"| HIST AUC={fmt(ha)} OVL={fmt(ho)} "
        f"| PROS AUC={fmt(pa)} OVL={fmt(po)}"
    )


# ============================================================
# C) FIRST TOKEN
# ============================================================

print()
print("="*180)
print("C) EXISTING FEATURES — FIRST-EVENT/TOKEN")
print("="*180)

first_survivors = []

for f in FEATURES:

    ha = auc(hf,f)
    pa = auc(pf,f)

    if (
        ha is not None and pa is not None
        and ha >= .55 and pa >= .55
    ):
        first_survivors.append((min(ha,pa),f,ha,pa))

first_survivors.sort(reverse=True)

if first_survivors:
    for _,f,ha,pa in first_survivors:
        print(
            f"{f:24} | HIST={ha:.3f} | PROS={pa:.3f}"
        )
else:
    print("No existing feature reaches AUC >=0.55 in both first-token regimes.")


# ============================================================
# D) REDUNDANCY
# ============================================================

print()
print("="*180)
print("D) EXISTING FEATURE REDUNDANCY")
print("="*180)

high_corr = []

for i,f1 in enumerate(FEATURES):
    for f2 in FEATURES[i+1:]:

        c = corr(
            [r.get(f1) for r in records],
            [r.get(f2) for r in records]
        )

        if c is not None and abs(c) >= .75:
            high_corr.append((abs(c),c,f1,f2))

high_corr.sort(reverse=True)

print(f"|CORR| >= 0.75 PAIRS : {len(high_corr)}")

for _,c,f1,f2 in high_corr[:30]:
    print(f"{f1:22} <-> {f2:22} | CORR={c:+.3f}")


# ============================================================
# E) DEX / MARKET STRUCTURE COVERAGE
# ============================================================

print()
print("="*180)
print("E) DEX / MARKET-STRUCTURE COVERAGE")
print("="*180)

for f in DEX_FEATURES:

    h = sum(valid(r.get(f)) for r in hist)
    p = sum(valid(r.get(f)) for r in pros)

    print(
        f"{f:20} "
        f"| HIST={h:4d}/{len(hist):4d} "
        f"({100*h/len(hist):5.1f}%) "
        f"| PROS={p:4d}/{len(pros):4d} "
        f"({100*p/len(pros):5.1f}%)"
    )


# ============================================================
# F) DEX FEATURE DISCOVERY
# descriptive only
# ============================================================

print()
print("="*180)
print("F) DEX / MARKET-STRUCTURE DESCRIPTIVE DISCOVERY")
print("="*180)

dex_candidates = []

for f in DEX_FEATURES:

    ha = auc(hist,f)
    pa = auc(pros,f)

    hh = auc(hf,f)
    pp = auc(pf,f)

    print(
        f"{f:20} "
        f"| HIST={fmt(ha)} PROS={fmt(pa)} "
        f"| FIRST HIST={fmt(hh)} PROS={fmt(pp)}"
    )

    if (
        ha is not None
        and pa is not None
        and hh is not None
        and pp is not None
        and ha >= .55
        and pa >= .55
        and hh >= .55
        and pp >= .55
    ):
        dex_candidates.append(
            (
                min(ha,pa,hh,pp),
                f,ha,pa,hh,pp
            )
        )

dex_candidates.sort(reverse=True)


# ============================================================
# G) DEX CATEGORICAL CONTEXT
# ============================================================

print()
print("="*180)
print("G) DEX / PAIR CONTEXT")
print("="*180)

for regime_name, rr in [("HIST",hist),("PROS",pros)]:

    dex_groups = defaultdict(list)

    for r in rr:
        if r.get("dex_id"):
            dex_groups[r["dex_id"]].append(r)

    print()
    print(regime_name)

    for dex_id, xs in sorted(
        dex_groups.items(),
        key=lambda z:-len(z[1])
    ):

        yes = sum(r["activation"] for r in xs)

        print(
            f"  {str(dex_id):18} "
            f"| N={len(xs):4d} "
            f"| ACTIVE={yes:3d} "
            f"| RATE={100*yes/len(xs):5.1f}%"
        )


# ============================================================
# H) INFORMATION SCORECARD
# ============================================================

print()
print("="*180)
print("H) INFORMATION SCORECARD")
print("="*180)

existing_cross = [
    x for x in existing_results
    if (
        x[1] is not None and x[2] is not None
        and x[1] >= .55 and x[2] >= .55
    )
]

heavy_overlap = [
    x for x in existing_results
    if (
        x[3] is not None and x[4] is not None
        and x[3] >= .50 and x[4] >= .50
    )
]

print(
    f"EXISTING FEATURES >=.55 BOTH REGIMES : "
    f"{len(existing_cross)}/{len(FEATURES)}"
)

print(
    f"FIRST-TOKEN SURVIVORS               : "
    f"{len(first_survivors)}/{len(FEATURES)}"
)

print(
    f"HEAVY-IQR-OVERLAP FEATURES          : "
    f"{len(heavy_overlap)}/{len(FEATURES)}"
)

print(
    f"HIGH-CORRELATION PAIRS              : "
    f"{len(high_corr)}"
)

print(
    f"DEX FIRST+REGIME CANDIDATES         : "
    f"{len(dex_candidates)}/{len(DEX_FEATURES)}"
)


# ============================================================
# I) DECISION
# ============================================================

print()
print("="*180)
print("I) DECISION SUPPORT")
print("="*180)

if dex_candidates:

    best = dex_candidates[0]

    print(
        "🟡 DEX / MARKET-STRUCTURE DATA CONTAINS A MISSING-SIGNAL CANDIDATE."
    )

    print(
        f"TOP DESCRIPTIVE CANDIDATE = {best[1]}"
    )

    print(
        "Next = freeze this hypothesis and run a dedicated robustness audit."
    )

    print(
        "Do NOT optimize a threshold."
    )

elif len(first_survivors) >= 2 and len(existing_cross) >= 4:

    print(
        "🟢 EXISTING FEATURE SPACE STILL CONTAINS CROSS-TOKEN INFORMATION."
    )

    print(
        "Failure of single-feature gates may reflect representation/combination rather than missing data."
    )

    print(
        "Next = controlled multivariate information audit, not another single-feature search."
    )

else:

    print(
        "🔴 CURRENT OBSERVABLE SPACE DOES NOT ROBUSTLY SEPARATE ACTIVATION."
    )

    print(
        "Existing flow features are insufficient at first-token / cross-regime level."
    )

    if dex_matched < len(records)*0.5:
        print(
            "DEX market-structure coverage is also too sparse to rule that family in or out."
        )

    print(
        "Next = enrich prospective collection before further activation-model fitting."
    )


print()
print("IMPORTANT:")
print("• T97 is an information/coverage audit, not a model-selection test.")
print("• DEX snapshots are matched at or BEFORE event timestamp only.")
print("• No future DEX snapshot is allowed.")
print("• No feature threshold optimization.")
print("• No model fitting.")
print("• No interaction search.")
print("• No candidate is promoted by T97 alone.")
print("• T97 writes nothing to DB.")
print("• Frozen prospective branches remain untouched.")

db.close()
