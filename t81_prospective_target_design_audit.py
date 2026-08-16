#!/usr/bin/env python3

import sqlite3
import math
import statistics
from collections import Counter

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

THRESHOLDS = [3.0, 5.0, 7.5, 10.0]


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
    return statistics.mean(xs) if xs else None


def med(xs):
    return statistics.median(xs) if xs else None


def stdev(xs):
    return statistics.pstdev(xs) if len(xs) >= 2 else None


def quantile(xs, q):
    xs = sorted(xs)

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


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def summarize_threshold(xs, t):

    run = sum(
        x >= t
        for x in xs
    )

    dump = sum(
        x <= -t
        for x in xs
    )

    neutral = sum(
        -t < x < t
        for x in xs
    )

    binary = (
        run + dump
    )

    balance = None

    if binary:
        balance = min(
            run,
            dump
        ) / binary

    return {
        "run": run,
        "dump": dump,
        "neutral": neutral,
        "binary": binary,
        "balance": balance,
    }


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row

rows = db.execute(f"""
SELECT
    t.event_id,
    t.token_mint,
    t.event_timestamp,

    e.dex_return_60s,
    e.dex_done_60s,
    e.dex_delay_60s

FROM {T59} t

LEFT JOIN events e
    ON e.id=t.event_id

ORDER BY
    t.event_timestamp,
    t.event_id
""").fetchall()


valid_rows = [
    r
    for r in rows
    if valid(
        r["dex_return_60s"]
    )
]


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

seen = set()
first_rows = []

for r in valid_rows:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first_rows.append(r)


all_returns = [
    r["dex_return_60s"]
    for r in valid_rows
]

first_returns = [
    r["dex_return_60s"]
    for r in first_rows
]


# ============================================================
# CHRONO BLOCKS
# ============================================================

N = len(valid_rows)

blocks = {
    "T1":
        valid_rows[:N//3],

    "T2":
        valid_rows[N//3:(2*N)//3],

    "T3":
        valid_rows[(2*N)//3:],
}


# ============================================================
# OUTPUT
# ============================================================

print("=" * 150)
print(
    "MEMECOIN LAB — T81 PROSPECTIVE TARGET-DESIGN AUDIT"
)
print("=" * 150)

print("MODE              : READ-ONLY")
print("DB WRITES         : NONE")
print("MODEL FITTING     : NONE")
print("THRESHOLD TUNING  : NONE")
print("T59               : UNTOUCHED")
print("T78               : UNTOUCHED")
print()
print(
    "IMPORTANT: alternative thresholds are diagnostic only."
)
print(
    "Frozen T59/T78 label remains ±10%."
)


# ============================================================
# A
# ============================================================

print()
print("=" * 150)
print("A) SAMPLE")
print("=" * 150)

print(
    f"ALL VALID EVENTS      : {len(valid_rows)}"
)

print(
    f"FIRST-EVENT TOKENS    : {len(first_rows)}"
)


# ============================================================
# B
# ============================================================

print()
print("=" * 150)
print(
    "B) TARGET DENSITY — ALL EVENTS"
)
print("=" * 150)


for t in THRESHOLDS:

    s = summarize_threshold(
        all_returns,
        t
    )

    print(
        f"±{t:4.1f}% "
        f"| RUN={s['run']:3d} "
        f"| DUMP={s['dump']:3d} "
        f"| BINARY={s['binary']:3d} "
        f"({pct(s['binary'],len(all_returns)):>6}) "
        f"| NEUTRAL={s['neutral']:3d} "
        f"| MINORITY_SHARE="
        f"{fmt(s['balance'],3)}"
    )


# ============================================================
# C
# ============================================================

print()
print("=" * 150)
print(
    "C) TARGET DENSITY — FIRST EVENT/TOKEN"
)
print("=" * 150)


for t in THRESHOLDS:

    s = summarize_threshold(
        first_returns,
        t
    )

    print(
        f"±{t:4.1f}% "
        f"| RUN={s['run']:3d} "
        f"| DUMP={s['dump']:3d} "
        f"| BINARY={s['binary']:3d} "
        f"({pct(s['binary'],len(first_returns)):>6}) "
        f"| NEUTRAL={s['neutral']:3d} "
        f"| MINORITY_SHARE="
        f"{fmt(s['balance'],3)}"
    )


# ============================================================
# D
# ============================================================

print()
print("=" * 150)
print(
    "D) CHRONOLOGICAL DENSITY"
)
print("=" * 150)


for t in THRESHOLDS:

    print()
    print(
        f"THRESHOLD ±{t:.1f}%"
    )

    for name, rr in blocks.items():

        xs = [
            r["dex_return_60s"]
            for r in rr
        ]

        s = summarize_threshold(
            xs,
            t
        )

        print(
            f"{name} "
            f"| N={len(xs):3d} "
            f"| RUN={s['run']:3d} "
            f"| DUMP={s['dump']:3d} "
            f"| BIN={s['binary']:3d} "
            f"| RATE={pct(s['binary'],len(xs)):>6}"
        )


# ============================================================
# E
# ============================================================

print()
print("=" * 150)
print(
    "E) CONTINUOUS R60 DISTRIBUTION"
)
print("=" * 150)

print(
    f"N       : {len(all_returns)}"
)

print(
    f"MEAN    : {fmt(avg(all_returns))}"
)

print(
    f"STD     : {fmt(stdev(all_returns))}"
)

print(
    f"MIN     : {fmt(min(all_returns))}"
)

print(
    f"P01     : {fmt(quantile(all_returns,0.01))}"
)

print(
    f"P05     : {fmt(quantile(all_returns,0.05))}"
)

print(
    f"P10     : {fmt(quantile(all_returns,0.10))}"
)

print(
    f"P25     : {fmt(quantile(all_returns,0.25))}"
)

print(
    f"P50     : {fmt(quantile(all_returns,0.50))}"
)

print(
    f"P75     : {fmt(quantile(all_returns,0.75))}"
)

print(
    f"P90     : {fmt(quantile(all_returns,0.90))}"
)

print(
    f"P95     : {fmt(quantile(all_returns,0.95))}"
)

print(
    f"P99     : {fmt(quantile(all_returns,0.99))}"
)

print(
    f"MAX     : {fmt(max(all_returns))}"
)


# ============================================================
# F
# ============================================================

print()
print("=" * 150)
print(
    "F) ZERO / NEAR-ZERO MASS"
)
print("=" * 150)

for eps in [
    0.0,
    0.1,
    0.25,
    0.5,
    1.0,
]:

    n = sum(
        abs(x) <= eps
        for x in all_returns
    )

    print(
        f"|R60| <= {eps:4.2f}% "
        f"| N={n:3d} "
        f"| SHARE={pct(n,len(all_returns))}"
    )


# ============================================================
# G
# ============================================================

print()
print("=" * 150)
print(
    "G) DEX DELAY OUTLIERS"
)
print("=" * 150)

delays = [
    r["dex_delay_60s"]
    for r in valid_rows
    if valid(
        r["dex_delay_60s"]
    )
]

for cut in [
    5,
    10,
    30,
    60,
    120,
]:

    n = sum(
        d > cut
        for d in delays
    )

    print(
        f"DELAY > {cut:3d}s "
        f"| N={n:3d} "
        f"| SHARE={pct(n,len(delays))}"
    )


# ============================================================
# H
# ============================================================

print()
print("=" * 150)
print(
    "H) DELAY-SENSITIVITY OF RETURN DISTRIBUTION"
)
print("=" * 150)

for cut in [
    5,
    10,
    30,
]:

    rr = [
        r
        for r in valid_rows
        if (
            valid(r["dex_delay_60s"])
            and r["dex_delay_60s"] <= cut
        )
    ]

    xs = [
        r["dex_return_60s"]
        for r in rr
    ]

    if not xs:
        continue

    s10 = summarize_threshold(
        xs,
        10.0
    )

    print(
        f"DELAY <= {cut:2d}s "
        f"| N={len(xs):3d} "
        f"| MEAN={fmt(avg(xs)):>8} "
        f"| MED={fmt(med(xs)):>8} "
        f"| ±10 BINARY={s10['binary']:3d} "
        f"({pct(s10['binary'],len(xs)):>6})"
    )


# ============================================================
# I
# ============================================================

print()
print("=" * 150)
print(
    "I) TARGET-DESIGN SCORECARD"
)
print("=" * 150)

for t in THRESHOLDS:

    all_s = summarize_threshold(
        all_returns,
        t
    )

    first_s = summarize_threshold(
        first_returns,
        t
    )

    chrono_rates = []

    for rr in blocks.values():

        xs = [
            r["dex_return_60s"]
            for r in rr
        ]

        ss = summarize_threshold(
            xs,
            t
        )

        if xs:
            chrono_rates.append(
                ss["binary"]
                / len(xs)
            )

    min_chrono = (
        min(chrono_rates)
        if chrono_rates
        else None
    )

    print(
        f"±{t:4.1f}% "
        f"| ALL_BIN={all_s['binary']:3d} "
        f"({pct(all_s['binary'],len(all_returns)):>6}) "
        f"| FIRST_BIN={first_s['binary']:3d} "
        f"({pct(first_s['binary'],len(first_returns)):>6}) "
        f"| MIN_CHRONO_RATE="
        f"{fmt(min_chrono,3)}"
    )


# ============================================================
# J
# ============================================================

print()
print("=" * 150)
print(
    "J) DECISION SUPPORT"
)
print("=" * 150)

s10 = summarize_threshold(
    all_returns,
    10.0
)

s75 = summarize_threshold(
    all_returns,
    7.5
)

s5 = summarize_threshold(
    all_returns,
    5.0
)

print(
    f"±10% binary density : "
    f"{pct(s10['binary'],len(all_returns))}"
)

print(
    f"±7.5% density       : "
    f"{pct(s75['binary'],len(all_returns))}"
)

print(
    f"±5% density         : "
    f"{pct(s5['binary'],len(all_returns))}"
)

print()

if (
    s10["binary"] < 10
    and s5["binary"] >= 10
):

    print(
        "🟡 FROZEN ±10% TARGET IS TOO SPARSE FOR "
        "A NEAR-TERM PROSPECTIVE DECISION."
    )

    print(
        "A SEPARATE future target experiment may be justified."
    )

elif s10["binary"] >= 10:

    print(
        "🟢 ±10% target density is becoming usable."
    )

else:

    print(
        "🔴 ALL TESTED BINARY TARGETS REMAIN VERY SPARSE."
    )

print()
print(
    "T81 does NOT authorize changing T59/T78 labels."
)

print(
    "Any alternative target must receive its own future boundary/freeze."
)

db.close()
