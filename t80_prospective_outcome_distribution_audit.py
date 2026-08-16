#!/usr/bin/env python3

import sqlite3
import math
from collections import Counter, defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

# FROZEN production/research labels remain ±10%.
# 5% and 7.5% below are DESCRIPTIVE ONLY.
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
    return sum(xs) / len(xs) if xs else None


def quantile(xs, q):
    xs = sorted(xs)

    if not xs:
        return None

    pos = (len(xs) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return (
        xs[lo] * (1-w)
        + xs[hi] * w
    )


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def print_distribution(title, xs):

    print()
    print("=" * 140)
    print(title)
    print("=" * 140)

    if not xs:
        print("NO VALID VALUES")
        return

    qs = [
        ("MIN", 0.00),
        ("P05", 0.05),
        ("P10", 0.10),
        ("P25", 0.25),
        ("P50", 0.50),
        ("P75", 0.75),
        ("P90", 0.90),
        ("P95", 0.95),
        ("MAX", 1.00),
    ]

    print(f"N      : {len(xs)}")
    print(f"MEAN   : {fmt(avg(xs))}")

    for name, q in qs:
        print(
            f"{name:6} : "
            f"{fmt(quantile(xs, q))}"
        )


def threshold_table(xs):

    print()
    print("-" * 100)
    print("DESCRIPTIVE TAIL COUNTS")
    print("-" * 100)

    n = len(xs)

    for t in THRESHOLDS:

        up = sum(
            x >= t
            for x in xs
        )

        down = sum(
            x <= -t
            for x in xs
        )

        either = sum(
            abs(x) >= t
            for x in xs
        )

        print(
            f"|R60| >= {t:4.1f}% "
            f"| UP={up:3d} ({pct(up,n):>6}) "
            f"| DOWN={down:3d} ({pct(down,n):>6}) "
            f"| EITHER={either:3d} ({pct(either,n):>6})"
        )


# ============================================================
# DB — READ ONLY
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
# LOAD T59
# ============================================================

rows = db.execute(f"""
SELECT
    t.event_id,
    t.token_mint,
    t.event_timestamp,
    t.captured_at,
    t.dex_return_60s,
    t.status,
    t.binary_label,

    e.dex_return_60s AS source_r60,
    e.dex_done_60s,
    e.dex_delay_60s

FROM {T59} t

LEFT JOIN events e
    ON e.id=t.event_id

ORDER BY
    t.event_timestamp,
    t.event_id
""").fetchall()


# ============================================================
# BASIC COUNTS
# ============================================================

tokens = {
    r["token_mint"]
    for r in rows
    if r["token_mint"] is not None
}

statuses = Counter(
    r["status"]
    for r in rows
)

valid_source = [
    r
    for r in rows
    if valid(r["source_r60"])
]

source_returns = [
    r["source_r60"]
    for r in valid_source
]

stored_returns = [
    r["dex_return_60s"]
    for r in rows
    if valid(r["dex_return_60s"])
]


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

first_by_token = {}

for r in rows:

    tok = r["token_mint"]

    if tok is None:
        continue

    if tok not in first_by_token:
        first_by_token[tok] = r


first_rows = list(
    first_by_token.values()
)

first_valid = [
    r
    for r in first_rows
    if valid(r["source_r60"])
]

first_returns = [
    r["source_r60"]
    for r in first_valid
]


# ============================================================
# SOURCE / STORED CONSISTENCY
# ============================================================

mismatches = []

for r in rows:

    a = r["dex_return_60s"]
    b = r["source_r60"]

    if valid(a) and valid(b):

        if abs(a-b) > 1e-9:

            mismatches.append(
                (
                    r["event_id"],
                    a,
                    b
                )
            )


# ============================================================
# DELAYS
# ============================================================

dex_delays = [
    r["dex_delay_60s"]
    for r in rows
    if valid(r["dex_delay_60s"])
]

capture_delays = [
    r["captured_at"]
    - r["event_timestamp"]
    for r in rows
    if (
        valid(r["captured_at"])
        and valid(r["event_timestamp"])
    )
]


# ============================================================
# DEX DONE
# ============================================================

done_counts = Counter(
    r["dex_done_60s"]
    for r in rows
)


# ============================================================
# OUTPUT
# ============================================================

print("=" * 140)
print(
    "MEMECOIN LAB — T80 PROSPECTIVE OUTCOME DISTRIBUTION AUDIT"
)
print("=" * 140)

print("MODE                : READ-ONLY")
print("DB WRITES           : NONE")
print("MODEL REFIT         : NONE")
print("THRESHOLD TUNING    : NONE")
print("T59                 : UNTOUCHED")
print("T78                 : UNTOUCHED")
print()
print(
    "IMPORTANT: ±5% and ±7.5% are descriptive diagnostics ONLY."
)
print(
    "Frozen RUN/DUMP definition remains ±10%."
)


# ============================================================
# A
# ============================================================

print()
print("=" * 140)
print("A) T59 PROSPECTIVE COVERAGE")
print("=" * 140)

print(f"ROWS                : {len(rows)}")
print(f"UNIQUE TOKENS       : {len(tokens)}")
print(f"SOURCE R60 VALID    : {len(source_returns)}")
print(f"SOURCE R60 MISSING  : {len(rows)-len(source_returns)}")

print()
print("STATUS")

for k in [
    "RUN",
    "DUMP",
    "NEUTRAL",
    "WAIT"
]:
    print(
        f"{k:10} : {statuses.get(k,0)}"
    )


# ============================================================
# B
# ============================================================

print_distribution(
    "B) ALL-EVENT DEX RETURN 60s DISTRIBUTION",
    source_returns
)

threshold_table(
    source_returns
)


neutral10 = sum(
    -10.0 < x < 10.0
    for x in source_returns
)

print()
print(
    f"STRICT INSIDE (-10%, +10%) : "
    f"{neutral10}/{len(source_returns)} "
    f"({pct(neutral10,len(source_returns))})"
)


# ============================================================
# C
# ============================================================

print_distribution(
    "C) FIRST-EVENT/TOKEN DEX RETURN 60s DISTRIBUTION",
    first_returns
)

threshold_table(
    first_returns
)


# ============================================================
# D
# ============================================================

print()
print("=" * 140)
print("D) FROZEN ±10% LABEL CHECK")
print("=" * 140)

expected_run = sum(
    x >= 10.0
    for x in source_returns
)

expected_dump = sum(
    x <= -10.0
    for x in source_returns
)

expected_neutral = sum(
    -10.0 < x < 10.0
    for x in source_returns
)

print(
    f"EXPECTED RUN       : {expected_run}"
)

print(
    f"EXPECTED DUMP      : {expected_dump}"
)

print(
    f"EXPECTED NEUTRAL   : {expected_neutral}"
)

print()

print(
    f"STORED RUN         : {statuses.get('RUN',0)}"
)

print(
    f"STORED DUMP        : {statuses.get('DUMP',0)}"
)

print(
    f"STORED NEUTRAL     : {statuses.get('NEUTRAL',0)}"
)


label_ok = (
    expected_run
    == statuses.get("RUN",0)
    and expected_dump
    == statuses.get("DUMP",0)
    and expected_neutral
    == statuses.get("NEUTRAL",0)
)

print()
print(
    "LABEL COUNT CHECK   : "
    + (
        "✅ CONSISTENT"
        if label_ok
        else "❌ REVIEW"
    )
)


# ============================================================
# E
# ============================================================

print()
print("=" * 140)
print("E) SOURCE / T59 OUTCOME CONSISTENCY")
print("=" * 140)

print(
    f"STORED VALID R60    : {len(stored_returns)}"
)

print(
    f"SOURCE VALID R60    : {len(source_returns)}"
)

print(
    f"VALUE MISMATCHES    : {len(mismatches)}"
)

if mismatches:

    print()
    print("FIRST MISMATCHES")

    for event_id, stored, source in mismatches[:10]:

        print(
            f"ID={event_id} "
            f"| T59={fmt(stored)} "
            f"| SOURCE={fmt(source)}"
        )

else:

    print(
        "✅ Stored T59 outcomes match source outcomes "
        "on all mutually complete rows."
    )


# ============================================================
# F
# ============================================================

print()
print("=" * 140)
print("F) DEX 60s COMPLETION")
print("=" * 140)

print(
    f"dex_done_60s = 1    : "
    f"{done_counts.get(1,0)}"
)

print(
    f"dex_done_60s = 0    : "
    f"{done_counts.get(0,0)}"
)

print(
    f"dex_done_60s = NULL : "
    f"{done_counts.get(None,0)}"
)


# ============================================================
# G
# ============================================================

print_distribution(
    "G) DEX 60s DELAY DISTRIBUTION",
    dex_delays
)


# ============================================================
# H
# ============================================================

print_distribution(
    "H) T59 CAPTURE DELAY DISTRIBUTION",
    capture_delays
)


# ============================================================
# I
# ============================================================

print()
print("=" * 140)
print("I) FIRST-EVENT/TOKEN FROZEN LABEL BALANCE")
print("=" * 140)

first_run = sum(
    x >= 10.0
    for x in first_returns
)

first_dump = sum(
    x <= -10.0
    for x in first_returns
)

first_neutral = sum(
    -10.0 < x < 10.0
    for x in first_returns
)

print(
    f"FIRST TOKENS        : {len(first_rows)}"
)

print(
    f"VALID FIRST R60     : {len(first_returns)}"
)

print(
    f"RUN                 : {first_run}"
)

print(
    f"DUMP                : {first_dump}"
)

print(
    f"NEUTRAL             : {first_neutral}"
)


# ============================================================
# J
# ============================================================

print()
print("=" * 140)
print("J) DIAGNOSTIC SUMMARY")
print("=" * 140)

tail5 = sum(
    abs(x) >= 5.0
    for x in source_returns
)

tail75 = sum(
    abs(x) >= 7.5
    for x in source_returns
)

tail10 = sum(
    abs(x) >= 10.0
    for x in source_returns
)

print(
    f"|R60| >= 5.0%       : "
    f"{tail5}/{len(source_returns)} "
    f"({pct(tail5,len(source_returns))})"
)

print(
    f"|R60| >= 7.5%       : "
    f"{tail75}/{len(source_returns)} "
    f"({pct(tail75,len(source_returns))})"
)

print(
    f"|R60| >= 10.0%      : "
    f"{tail10}/{len(source_returns)} "
    f"({pct(tail10,len(source_returns))})"
)

print()

if (
    len(mismatches) == 0
    and done_counts.get(1,0) > 0
):

    print(
        "🟢 OUTCOME PIPELINE APPEARS INTERNALLY CONSISTENT."
    )

else:

    print(
        "🟡 OUTCOME PIPELINE REQUIRES REVIEW."
    )

print()
print(
    "NO DECISION ABOUT CHANGING ±10% IS MADE BY T80."
)

print(
    "T80 is descriptive only."
)

db.close()
