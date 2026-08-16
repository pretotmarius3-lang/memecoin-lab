#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

# We reconstruct prices around the event.
OFFSETS = [-300, -120, -60, -30, -10, 0, 60, 300]

# Maximum distance between requested timestamp and actual dex_prices snapshot.
MAX_DELAY = 15.0


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
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def avg(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def quantile(xs, q):
    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    p = (len(xs)-1)*q
    lo = int(math.floor(p))
    hi = int(math.ceil(p))

    if lo == hi:
        return xs[lo]

    w = p-lo
    return xs[lo]*(1-w) + xs[hi]*w


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def ret(a, b):
    """
    Percentage return FROM price a TO price b.
    """
    if (
        not valid(a)
        or not valid(b)
        or a <= 0
        or b <= 0
    ):
        return None

    return 100.0 * (b/a - 1.0)


def logret(a, b):
    if (
        not valid(a)
        or not valid(b)
        or a <= 0
        or b <= 0
    ):
        return None

    return math.log(b/a)


def tail_rate(xs, t):
    xs = [x for x in xs if valid(x)]

    if not xs:
        return None

    return (
        sum(abs(x) >= t for x in xs)
        / len(xs)
    )


# ============================================================
# DB
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
    raise RuntimeError("Cannot determine T59 boundary.")

boundary = int(boundary)


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s,
    dex_return_300s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


# ============================================================
# CACHE PRICE SERIES BY TOKEN
# ============================================================

price_cache = {}


def load_prices(token):

    if token in price_cache:
        return price_cache[token]

    rr = db.execute("""
    SELECT
        timestamp,
        price_usd,
        pair_address,
        dex_id

    FROM dex_prices

    WHERE
        token_mint=?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp
    """, (
        token,
    )).fetchall()

    price_cache[token] = rr

    return rr


def nearest_snapshot(token, target_ts):

    rr = load_prices(token)

    if not rr:
        return None

    best = None
    best_delay = None

    # Dataset is not huge, simple scan keeps logic transparent.
    for r in rr:

        delay = abs(
            r["timestamp"] - target_ts
        )

        if (
            best_delay is None
            or delay < best_delay
        ):
            best = r
            best_delay = delay

    if (
        best is None
        or best_delay > MAX_DELAY
    ):
        return None

    return {
        "timestamp": best["timestamp"],
        "price": best["price_usd"],
        "pair": best["pair_address"],
        "dex": best["dex_id"],
        "delay": best_delay,
    }


# ============================================================
# EVENT RECONSTRUCTION
# ============================================================

records = []


for e in events:

    points = {}

    for offset in OFFSETS:

        points[offset] = nearest_snapshot(
            e["token_mint"],
            e["timestamp"] + offset
        )


    def price(offset):
        x = points.get(offset)
        return x["price"] if x else None


    p300m = price(-300)
    p120m = price(-120)
    p60m = price(-60)
    p30m = price(-30)
    p10m = price(-10)
    p0 = price(0)
    p60 = price(60)
    p300 = price(300)


    pre300 = ret(p300m, p0)
    pre120 = ret(p120m, p0)
    pre60 = ret(p60m, p0)
    pre30 = ret(p30m, p0)
    pre10 = ret(p10m, p0)

    post60 = ret(p0, p60)
    post300 = ret(p0, p300)

    total_m60_p300 = ret(
        p60m,
        p300
    )


    # ========================================================
    # Fraction of absolute path already completed by event
    #
    # Uses log returns so path components add cleanly.
    # 0   = virtually all movement after event
    # 1   = virtually all movement before event
    # ========================================================

    lr_pre60 = logret(
        p60m,
        p0
    )

    lr_post300 = logret(
        p0,
        p300
    )

    premove_share = None

    if (
        valid(lr_pre60)
        and valid(lr_post300)
    ):

        denom = (
            abs(lr_pre60)
            + abs(lr_post300)
        )

        if denom > 1e-12:
            premove_share = (
                abs(lr_pre60)
                / denom
            )


    # Same pair across -60 / 0 / +300
    core_points = [
        points.get(-60),
        points.get(0),
        points.get(300),
    ]

    core_pairs = {
        x["pair"]
        for x in core_points
        if x is not None
        and x["pair"] is not None
    }

    pair_stable = (
        len(core_points) == 3
        and all(x is not None for x in core_points)
        and len(core_pairs) == 1
    )


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "historical":
            e["id"] <= boundary,

        "pre300":
            pre300,

        "pre120":
            pre120,

        "pre60":
            pre60,

        "pre30":
            pre30,

        "pre10":
            pre10,

        "post60_rebuilt":
            post60,

        "post300_rebuilt":
            post300,

        "post60_source":
            e["dex_return_60s"],

        "post300_source":
            e["dex_return_300s"],

        "total_m60_p300":
            total_m60_p300,

        "premove_share":
            premove_share,

        "pair_stable":
            pair_stable,

        "points":
            points,
    })


historical = [
    r for r in records
    if r["historical"]
]

prospective = [
    r for r in records
    if not r["historical"]
]


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

def first_token(rr):

    seen = set()
    out = []

    for r in rr:

        if r["token_mint"] in seen:
            continue

        seen.add(
            r["token_mint"]
        )

        out.append(r)

    return out


hist_first = first_token(
    historical
)

pros_first = first_token(
    prospective
)


# ============================================================
# OUTPUT
# ============================================================

print("=" * 165)
print(
    "MEMECOIN LAB — T87 EVENT TIMING / PRE-MOVE AUDIT"
)
print("=" * 165)

print("MODE              : READ-ONLY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")
print("DB WRITES         : NONE")
print("T59/T78/T82/T86   : UNTOUCHED")
print()
print(f"T59 BOUNDARY      : {boundary}")
print(f"PRICE SOURCE      : dex_prices.price_usd")
print(f"MAX SNAPSHOT GAP  : ±{MAX_DELAY:.1f}s")


# ============================================================
# A) COVERAGE
# ============================================================

print()
print("=" * 165)
print("A) PRICE-POINT COVERAGE")
print("=" * 165)


for offset in OFFSETS:

    h = sum(
        r["points"].get(offset)
        is not None
        for r in historical
    )

    p = sum(
        r["points"].get(offset)
        is not None
        for r in prospective
    )

    print(
        f"{offset:+5d}s "
        f"| HIST={h:4d}/{len(historical):4d} "
        f"({pct(h,len(historical)):>6}) "
        f"| PROS={p:4d}/{len(prospective):4d} "
        f"({pct(p,len(prospective)):>6})"
    )


# ============================================================
# B) MATCH DELAYS
# ============================================================

print()
print("=" * 165)
print("B) SNAPSHOT MATCH DELAYS")
print("=" * 165)


for offset in OFFSETS:

    hd = [
        r["points"][offset]["delay"]
        for r in historical
        if r["points"].get(offset)
    ]

    pd = [
        r["points"][offset]["delay"]
        for r in prospective
        if r["points"].get(offset)
    ]

    print(
        f"{offset:+5d}s "
        f"| HIST MED={fmt(med(hd)):>7} "
        f"P95={fmt(quantile(hd,0.95)):>7} "
        f"| PROS MED={fmt(med(pd)):>7} "
        f"P95={fmt(quantile(pd,0.95)):>7}"
    )


# ============================================================
# C) PRE-EVENT RETURNS
# ============================================================

print()
print("=" * 165)
print("C) PRE-EVENT PRICE MOVEMENT")
print("=" * 165)


for feature in [
    "pre300",
    "pre120",
    "pre60",
    "pre30",
    "pre10",
]:

    h = [
        r[feature]
        for r in historical
        if valid(r[feature])
    ]

    p = [
        r[feature]
        for r in prospective
        if valid(r[feature])
    ]

    print(
        f"{feature:12} "
        f"| HIST N={len(h):4d} "
        f"MED={fmt(med(h)):>8} "
        f"P10={fmt(quantile(h,0.10)):>8} "
        f"P90={fmt(quantile(h,0.90)):>8} "
        f"| PROS N={len(p):4d} "
        f"MED={fmt(med(p)):>8} "
        f"P10={fmt(quantile(p,0.10)):>8} "
        f"P90={fmt(quantile(p,0.90)):>8}"
    )


# ============================================================
# D) PRE-MOVE TAIL RATES
# ============================================================

print()
print("=" * 165)
print("D) HOW OFTEN A LARGE MOVE ALREADY HAPPENED BEFORE EVENT")
print("=" * 165)


for feature in [
    "pre60",
    "pre30",
    "pre10",
]:

    h = [
        r[feature]
        for r in historical
        if valid(r[feature])
    ]

    p = [
        r[feature]
        for r in prospective
        if valid(r[feature])
    ]

    print()
    print(feature)

    for t in [3.0, 5.0, 10.0]:

        print(
            f"  |PRE| >= {t:4.1f}% "
            f"| HIST={fmt(tail_rate(h,t),3):>6} "
            f"| PROS={fmt(tail_rate(p,t),3):>6}"
        )


# ============================================================
# E) REBUILT POST RETURNS
# ============================================================

print()
print("=" * 165)
print("E) REBUILT POST-EVENT MOVEMENT")
print("=" * 165)


for feature in [
    "post60_rebuilt",
    "post300_rebuilt",
]:

    h = [
        r[feature]
        for r in historical
        if valid(r[feature])
    ]

    p = [
        r[feature]
        for r in prospective
        if valid(r[feature])
    ]

    print(
        f"{feature:18} "
        f"| HIST N={len(h):4d} "
        f"MED={fmt(med(h)):>8} "
        f"| PROS N={len(p):4d} "
        f"MED={fmt(med(p)):>8}"
    )

    for t in [5.0, 10.0]:

        print(
            f"  ±{t:4.1f}% "
            f"| HIST={fmt(tail_rate(h,t),3)} "
            f"| PROS={fmt(tail_rate(p,t),3)}"
        )


# ============================================================
# F) FRACTION MOVE ALREADY DONE
# ============================================================

print()
print("=" * 165)
print("F) PRE-MOVE SHARE — -60s / EVENT / +300s")
print("=" * 165)


for name, rr in [
    ("HISTORICAL", historical),
    ("PROSPECTIVE", prospective),
]:

    xs = [
        r["premove_share"]
        for r in rr
        if valid(r["premove_share"])
        and r["pair_stable"]
    ]

    print(
        f"{name:12} "
        f"| N={len(xs):4d} "
        f"| MED={fmt(med(xs)):>7} "
        f"| P25={fmt(quantile(xs,0.25)):>7} "
        f"| P75={fmt(quantile(xs,0.75)):>7}"
    )

    if xs:

        mostly_before = sum(
            x >= 0.67
            for x in xs
        )

        mostly_after = sum(
            x <= 0.33
            for x in xs
        )

        print(
            f"             "
            f"| >=67% BEFORE={mostly_before:4d} "
            f"({pct(mostly_before,len(xs)):>6}) "
            f"| <=33% BEFORE={mostly_after:4d} "
            f"({pct(mostly_after,len(xs)):>6})"
        )


# ============================================================
# G) PAIR STABILITY
# ============================================================

print()
print("=" * 165)
print("G) PAIR STABILITY — -60s / EVENT / +300s")
print("=" * 165)


for name, rr in [
    ("HISTORICAL", historical),
    ("PROSPECTIVE", prospective),
]:

    complete = [
        r
        for r in rr
        if (
            r["points"].get(-60)
            and r["points"].get(0)
            and r["points"].get(300)
        )
    ]

    stable = sum(
        r["pair_stable"]
        for r in complete
    )

    print(
        f"{name:12} "
        f"| COMPLETE={len(complete):4d} "
        f"| SAME_PAIR={stable:4d} "
        f"| RATE={pct(stable,len(complete))}"
    )


# ============================================================
# H) FIRST-EVENT/TOKEN
# ============================================================

print()
print("=" * 165)
print("H) FIRST-EVENT/TOKEN TIMING")
print("=" * 165)


for feature in [
    "pre60",
    "pre30",
    "pre10",
    "post300_rebuilt",
    "premove_share",
]:

    h = [
        r[feature]
        for r in hist_first
        if valid(r[feature])
    ]

    p = [
        r[feature]
        for r in pros_first
        if valid(r[feature])
    ]

    print(
        f"{feature:18} "
        f"| HIST N={len(h):3d} MED={fmt(med(h)):>8} "
        f"| PROS N={len(p):3d} MED={fmt(med(p)):>8}"
    )


# ============================================================
# I) SOURCE-vs-REBUILT SANITY CHECK
# ============================================================

print()
print("=" * 165)
print("I) SOURCE RETURN vs REBUILT RETURN SANITY CHECK")
print("=" * 165)


for rebuilt, source in [
    ("post60_rebuilt", "post60_source"),
    ("post300_rebuilt", "post300_source"),
]:

    diffs_h = []
    diffs_p = []

    for r in historical:

        if (
            valid(r[rebuilt])
            and valid(r[source])
        ):
            diffs_h.append(
                abs(
                    r[rebuilt]
                    - r[source]
                )
            )

    for r in prospective:

        if (
            valid(r[rebuilt])
            and valid(r[source])
        ):
            diffs_p.append(
                abs(
                    r[rebuilt]
                    - r[source]
                )
            )

    print(
        f"{rebuilt:18} "
        f"| HIST MAE={fmt(avg(diffs_h)):>8} "
        f"MEDAE={fmt(med(diffs_h)):>8} "
        f"| PROS MAE={fmt(avg(diffs_p)):>8} "
        f"MEDAE={fmt(med(diffs_p)):>8}"
    )


# ============================================================
# J) DECISION SUPPORT
# ============================================================

print()
print("=" * 165)
print("J) DECISION SUPPORT")
print("=" * 165)


hist_share = [
    r["premove_share"]
    for r in historical
    if (
        valid(r["premove_share"])
        and r["pair_stable"]
    )
]

pros_share = [
    r["premove_share"]
    for r in prospective
    if (
        valid(r["premove_share"])
        and r["pair_stable"]
    )
]


hist_pre60 = [
    r["pre60"]
    for r in historical
    if valid(r["pre60"])
]

pros_pre60 = [
    r["pre60"]
    for r in prospective
    if valid(r["pre60"])
]


late_share_shift = (
    hist_share
    and pros_share
    and med(pros_share)
        >= med(hist_share) + 0.10
)


pre_tail_shift = False

if hist_pre60 and pros_pre60:

    h = tail_rate(
        hist_pre60,
        5.0
    )

    p = tail_rate(
        pros_pre60,
        5.0
    )

    if (
        h is not None
        and p is not None
        and p >= h * 1.5
    ):
        pre_tail_shift = True


if (
    late_share_shift
    and pre_tail_shift
):

    print(
        "🟠 STRONG EVIDENCE EVENTS ARE ARRIVING LATER "
        "IN THE MOVE PROSPECTIVELY."
    )

    print(
        "Event-trigger timing should be investigated before "
        "changing labels/models."
    )

elif (
    late_share_shift
    or pre_tail_shift
):

    print(
        "🟡 PARTIAL EVIDENCE OF LATER EVENT TIMING."
    )

    print(
        "Inspect trigger mechanics and timing provenance next."
    )

else:

    print(
        "🟢 NO CLEAR EVIDENCE THAT PROSPECTIVE EVENTS "
        "ARE SIMPLY TRIGGERING LATER."
    )

    print(
        "Neutral dominance likely has another cause."
    )


print()
print("IMPORTANT:")
print("• Prices reconstructed directly from dex_prices.")
print("• No model fitting.")
print("• No target optimization.")
print("• Pre-event prices are used only for timing diagnosis.")
print("• Post-event prices are diagnostic only.")
print("• Pair consistency is explicitly audited.")
print("• T87 writes nothing to DB.")

db.close()
