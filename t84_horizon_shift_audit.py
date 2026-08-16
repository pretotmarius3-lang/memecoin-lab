#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

THRESHOLDS = [5.0, 7.5, 10.0]


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


def quantile(xs, q):
    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    pos = (len(xs)-1)*q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos-lo
    return xs[lo]*(1-w) + xs[hi]*w


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

    dx = math.sqrt(
        sum((x-mx)**2 for x in xx)
    )

    dy = math.sqrt(
        sum((y-my)**2 for y in yy)
    )

    if dx == 0 or dy == 0:
        return None

    return sum(
        (x-mx)*(y-my)
        for x, y in pairs
    ) / (dx*dy)


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def tail(xs, threshold):
    xs = [x for x in xs if valid(x)]

    up = sum(x >= threshold for x in xs)
    down = sum(x <= -threshold for x in xs)

    return {
        "n": len(xs),
        "up": up,
        "down": down,
        "either": up + down,
    }


def distribution(name, xs):
    xs = [x for x in xs if valid(x)]

    print()
    print(name)
    print("-" * 100)

    if not xs:
        print("NO VALID VALUES")
        return

    print(
        f"N={len(xs)} "
        f"| MEAN={fmt(avg(xs))} "
        f"| MED={fmt(med(xs))} "
        f"| P05={fmt(quantile(xs,0.05))} "
        f"| P95={fmt(quantile(xs,0.95))}"
    )

    for t in THRESHOLDS:
        s = tail(xs, t)

        print(
            f"±{t:4.1f}% "
            f"| UP={s['up']:3d} "
            f"| DOWN={s['down']:3d} "
            f"| EITHER={s['either']:3d} "
            f"| RATE={pct(s['either'],s['n']):>6}"
        )


# ============================================================
# READ-ONLY DB
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


boundary_row = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()

if (
    boundary_row is None
    or boundary_row[0] is None
):
    raise RuntimeError(
        "Cannot determine T59 boundary"
    )

BOUNDARY = int(boundary_row[0])


# ============================================================
# LOAD EVENTS
# ============================================================

rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    dex_return_60s,
    dex_done_60s,
    dex_delay_60s,

    dex_return_300s,
    dex_done_300s,
    dex_delay_300s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


historical = [
    r for r in rows
    if r["id"] <= BOUNDARY
]

prospective = [
    r for r in rows
    if r["id"] > BOUNDARY
]


# ============================================================
# SAME COMPLETE-CASE COHORT
# ============================================================

def complete(rows):
    return [
        r for r in rows
        if (
            valid(r["dex_return_60s"])
            and valid(r["dex_return_300s"])
            and r["dex_done_60s"] == 1
            and r["dex_done_300s"] == 1
        )
    ]


hist_cc = complete(historical)
pros_cc = complete(prospective)


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

def first_token(rows):
    seen = set()
    out = []

    for r in rows:
        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


hist_first = first_token(hist_cc)
pros_first = first_token(pros_cc)


# ============================================================
# CONDITIONAL ANALYSIS
# ============================================================

def conditional_stats(rows, threshold):

    # Neutral at 60s under the SAME threshold.
    neutral60 = [
        r for r in rows
        if abs(r["dex_return_60s"]) < threshold
    ]

    later_run = sum(
        r["dex_return_300s"] >= threshold
        for r in neutral60
    )

    later_dump = sum(
        r["dex_return_300s"] <= -threshold
        for r in neutral60
    )

    later_tail = later_run + later_dump

    return {
        "neutral60": len(neutral60),
        "later_run": later_run,
        "later_dump": later_dump,
        "later_tail": later_tail,
    }


# ============================================================
# HEADER
# ============================================================

print("=" * 150)
print("MEMECOIN LAB — T84 HORIZON SHIFT AUDIT — 60s vs 300s")
print("=" * 150)

print("MODE              : READ-ONLY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")
print("DB WRITES         : NONE")
print("T59               : UNTOUCHED")
print("T78               : UNTOUCHED")
print("T82               : UNTOUCHED")
print()
print(f"T59 BOUNDARY      : {BOUNDARY}")


# ============================================================
# A
# ============================================================

print()
print("=" * 150)
print("A) SAME COMPLETE-CASE COVERAGE")
print("=" * 150)

print(
    f"HISTORICAL | N={len(hist_cc):4d} "
    f"| TOKENS={len(set(r['token_mint'] for r in hist_cc)):4d}"
)

print(
    f"PROSPECTIVE | N={len(pros_cc):4d} "
    f"| TOKENS={len(set(r['token_mint'] for r in pros_cc)):4d}"
)

print(
    f"HIST FIRST | N={len(hist_first):4d}"
)

print(
    f"PROS FIRST | N={len(pros_first):4d}"
)


# ============================================================
# B
# ============================================================

print()
print("=" * 150)
print("B) HISTORICAL HORIZON DISTRIBUTIONS")
print("=" * 150)

distribution(
    "HISTORICAL R60",
    [r["dex_return_60s"] for r in hist_cc]
)

distribution(
    "HISTORICAL R300",
    [r["dex_return_300s"] for r in hist_cc]
)


# ============================================================
# C
# ============================================================

print()
print("=" * 150)
print("C) PROSPECTIVE HORIZON DISTRIBUTIONS")
print("=" * 150)

distribution(
    "PROSPECTIVE R60",
    [r["dex_return_60s"] for r in pros_cc]
)

distribution(
    "PROSPECTIVE R300",
    [r["dex_return_300s"] for r in pros_cc]
)


# ============================================================
# D
# ============================================================

print()
print("=" * 150)
print("D) R60 / R300 CORRELATION")
print("=" * 150)

for name, rr in [
    ("HISTORICAL", hist_cc),
    ("PROSPECTIVE", pros_cc),
]:

    corr = pearson(
        [r["dex_return_60s"] for r in rr],
        [r["dex_return_300s"] for r in rr]
    )

    print(
        f"{name:12} "
        f"| N={len(rr):4d} "
        f"| CORR={fmt(corr)}"
    )


# ============================================================
# E
# ============================================================

print()
print("=" * 150)
print("E) 60s NEUTRAL → 300s LARGE MOVE")
print("=" * 150)

for t in THRESHOLDS:

    print()
    print(f"THRESHOLD ±{t:.1f}%")

    for name, rr in [
        ("HIST", hist_cc),
        ("PROS", pros_cc),
    ]:

        s = conditional_stats(
            rr,
            t
        )

        print(
            f"{name:5} "
            f"| R60_NEUTRAL={s['neutral60']:4d} "
            f"| →R300_RUN={s['later_run']:3d} "
            f"| →R300_DUMP={s['later_dump']:3d} "
            f"| →ANY={s['later_tail']:3d} "
            f"| RATE={pct(s['later_tail'],s['neutral60']):>6}"
        )


# ============================================================
# F
# ============================================================

print()
print("=" * 150)
print("F) FIRST-EVENT/TOKEN — 60s vs 300s")
print("=" * 150)

for name, rr in [
    ("HIST_FIRST", hist_first),
    ("PROS_FIRST", pros_first),
]:

    print()
    print(name)

    for horizon in [
        "dex_return_60s",
        "dex_return_300s",
    ]:

        xs = [
            r[horizon]
            for r in rr
        ]

        s5 = tail(xs, 5.0)
        s10 = tail(xs, 10.0)

        print(
            f"{horizon:16} "
            f"| N={len(xs):3d} "
            f"| MED={fmt(med(xs)):>8} "
            f"| ±5={s5['either']:3d} ({pct(s5['either'],s5['n']):>6}) "
            f"| ±10={s10['either']:3d} ({pct(s10['either'],s10['n']):>6})"
        )


# ============================================================
# G
# ============================================================

print()
print("=" * 150)
print("G) PROSPECTIVE HORIZON EXPANSION RATIOS")
print("=" * 150)

pros60 = [
    r["dex_return_60s"]
    for r in pros_cc
]

pros300 = [
    r["dex_return_300s"]
    for r in pros_cc
]

for t in THRESHOLDS:

    s60 = tail(pros60, t)
    s300 = tail(pros300, t)

    rate60 = (
        s60["either"] / s60["n"]
        if s60["n"]
        else None
    )

    rate300 = (
        s300["either"] / s300["n"]
        if s300["n"]
        else None
    )

    ratio = None

    if (
        rate60 is not None
        and rate300 is not None
        and rate60 > 0
    ):
        ratio = rate300 / rate60

    print(
        f"±{t:4.1f}% "
        f"| R60_RATE={fmt(rate60,3)} "
        f"| R300_RATE={fmt(rate300,3)} "
        f"| EXPANSION={fmt(ratio,2)}x"
    )


# ============================================================
# H
# ============================================================

print()
print("=" * 150)
print("H) PROSPECTIVE 300s DELAY QUALITY")
print("=" * 150)

delays300 = [
    r["dex_delay_300s"]
    for r in pros_cc
    if valid(r["dex_delay_300s"])
]

print(
    f"N       : {len(delays300)}"
)

print(
    f"MED     : {fmt(med(delays300))}"
)

print(
    f"P90     : {fmt(quantile(delays300,0.90))}"
)

print(
    f"P95     : {fmt(quantile(delays300,0.95))}"
)

print(
    f"P99     : {fmt(quantile(delays300,0.99))}"
)

print(
    f"MAX     : {fmt(max(delays300) if delays300 else None)}"
)

for cut in [5, 10, 30, 60]:

    n = sum(
        d > cut
        for d in delays300
    )

    print(
        f"DELAY > {cut:2d}s "
        f"| N={n:3d} "
        f"| SHARE={pct(n,len(delays300))}"
    )


# ============================================================
# I
# ============================================================

print()
print("=" * 150)
print("I) HORIZON-SHIFT SCORECARD")
print("=" * 150)

for t in [5.0, 10.0]:

    h60 = tail(
        [r["dex_return_60s"] for r in hist_cc],
        t
    )

    h300 = tail(
        [r["dex_return_300s"] for r in hist_cc],
        t
    )

    p60 = tail(
        [r["dex_return_60s"] for r in pros_cc],
        t
    )

    p300 = tail(
        [r["dex_return_300s"] for r in pros_cc],
        t
    )

    h60r = h60["either"]/h60["n"] if h60["n"] else None
    h300r = h300["either"]/h300["n"] if h300["n"] else None
    p60r = p60["either"]/p60["n"] if p60["n"] else None
    p300r = p300["either"]/p300["n"] if p300["n"] else None

    print(
        f"±{t:4.1f}% "
        f"| HIST60={fmt(h60r,3)} "
        f"| HIST300={fmt(h300r,3)} "
        f"| PROS60={fmt(p60r,3)} "
        f"| PROS300={fmt(p300r,3)}"
    )


# ============================================================
# J) DECISION SUPPORT
# ============================================================

print()
print("=" * 150)
print("J) DECISION SUPPORT")
print("=" * 150)

p60_5 = tail(
    [r["dex_return_60s"] for r in pros_cc],
    5.0
)

p300_5 = tail(
    [r["dex_return_300s"] for r in pros_cc],
    5.0
)

p60_10 = tail(
    [r["dex_return_60s"] for r in pros_cc],
    10.0
)

p300_10 = tail(
    [r["dex_return_300s"] for r in pros_cc],
    10.0
)

rate60_5 = (
    p60_5["either"] / p60_5["n"]
    if p60_5["n"]
    else None
)

rate300_5 = (
    p300_5["either"] / p300_5["n"]
    if p300_5["n"]
    else None
)

rate60_10 = (
    p60_10["either"] / p60_10["n"]
    if p60_10["n"]
    else None
)

rate300_10 = (
    p300_10["either"] / p300_10["n"]
    if p300_10["n"]
    else None
)


shift5 = (
    rate60_5 is not None
    and rate300_5 is not None
    and rate60_5 > 0
    and rate300_5 >= 2.0 * rate60_5
)

shift10 = (
    rate60_10 is not None
    and rate300_10 is not None
    and rate60_10 > 0
    and rate300_10 >= 2.0 * rate60_10
)


if shift5 and shift10:

    print(
        "🟢 STRONG EVIDENCE OF HORIZON SHIFT."
    )

    print(
        "Large prospective moves are materially more common at 300s than 60s."
    )

elif shift5 or shift10:

    print(
        "🟡 PARTIAL EVIDENCE OF HORIZON SHIFT."
    )

    print(
        "One large-move threshold expands materially by 300s."
    )

else:

    print(
        "🔴 NO CLEAR EVIDENCE THAT THE MISSING 60s TAILS "
        "SIMPLY MOVED TO 300s."
    )


print()
print(
    "No horizon change is authorized by T84."
)

print(
    "Any 300s experiment requires a new prospective freeze."
)

db.close()
