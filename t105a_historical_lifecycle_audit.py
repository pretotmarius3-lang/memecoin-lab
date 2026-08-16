#!/usr/bin/env python3

import sqlite3
import math
from collections import defaultdict

DB = "validation_v090.db"

RUN = 100.0
CRASH = -50.0
HORIZONS = [30, 60, 300, 900, 1800]

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
        and x > 0
    )

def pct(a, b):
    if not valid(a) or not valid(b):
        return None
    return 100.0 * (b / a - 1.0)

rows = db.execute("""
SELECT
    token_mint,
    timestamp,
    price_usd
FROM dex_prices
WHERE
    token_mint IS NOT NULL
    AND price_usd IS NOT NULL
    AND price_usd > 0
ORDER BY token_mint, timestamp
""").fetchall()

by_token = defaultdict(list)

for r in rows:
    by_token[r["token_mint"]].append(
        (r["timestamp"], r["price_usd"])
    )

results = []

for mint, hist in by_token.items():

    if len(hist) < 2:
        continue

    first_ts, first_price = hist[0]

    run_i = None

    for i, (ts, price) in enumerate(hist):
        move = pct(first_price, price)

        if move is not None and move >= RUN:
            run_i = i
            break

    if run_i is None:
        results.append({
            "mint": mint,
            "rows": len(hist),
            "run": False
        })
        continue

    run_ts, run_price = hist[run_i]

    peak_price = run_price
    peak_ts = run_ts
    peak_i = run_i

    crash_i = None
    crash_price = None
    crash_ts = None
    crash_dd = None

    for i in range(run_i, len(hist)):

        ts, price = hist[i]

        if price > peak_price:
            peak_price = price
            peak_ts = ts
            peak_i = i

        dd = pct(peak_price, price)

        if dd is not None and dd <= CRASH:
            crash_i = i
            crash_ts = ts
            crash_price = price
            crash_dd = dd
            break

    if crash_i is None:
        results.append({
            "mint": mint,
            "rows": len(hist),
            "run": True,
            "crash": False
        })
        continue

    future = {}

    for h in HORIZONS:

        target = crash_ts + h

        match = None

        for ts, price in hist[crash_i:]:
            if ts >= target:
                match = (ts, price)
                break

        if match:
            ts, price = match

            future[h] = {
                "delay": ts - target,
                "ret_crash": pct(
                    crash_price,
                    price
                ),
                "ret_peak": pct(
                    peak_price,
                    price
                )
            }

        else:
            future[h] = None

    results.append({
        "mint": mint,
        "rows": len(hist),
        "run": True,
        "crash": True,
        "first_ts": first_ts,
        "run_ts": run_ts,
        "peak_ts": peak_ts,
        "crash_ts": crash_ts,
        "first_price": first_price,
        "peak_price": peak_price,
        "crash_price": crash_price,
        "run_return": pct(
            first_price,
            peak_price
        ),
        "crash_dd": crash_dd,
        "future": future
    })


total = len(by_token)

runs = [
    r for r in results
    if r.get("run")
]

crashes = [
    r for r in results
    if r.get("crash")
]

print("=" * 150)
print("MEMECOIN LAB — T105A HISTORICAL LIFECYCLE RECOVERABILITY AUDIT")
print("=" * 150)

print(f"DEX TOKENS             : {total}")
print(f"RUN >= +100%           : {len(runs)}")
print(f"RUN -> CRASH <= -50%   : {len(crashes)}")

print()
print("=" * 150)
print("POST-CRASH COVERAGE")
print("=" * 150)

for h in HORIZONS:

    usable = [
        r for r in crashes
        if r["future"].get(h) is not None
    ]

    good_delay = [
        r for r in usable
        if abs(
            r["future"][h]["delay"]
        ) <= 15
    ]

    recovered = [
        r for r in good_delay
        if (
            r["future"][h]["ret_crash"]
            is not None
            and r["future"][h]["ret_crash"] >= 50
        )
    ]

    reclaimed = [
        r for r in good_delay
        if (
            r["future"][h]["ret_peak"]
            is not None
            and r["future"][h]["ret_peak"] >= 0
        )
    ]

    print(
        f"+{h:4d}s "
        f"| AVAILABLE={len(usable):4d}"
        f" | <=15s GAP={len(good_delay):4d}"
        f" | RECOVERY50={len(recovered):4d}"
        f" | RECLAIM_PEAK={len(reclaimed):4d}"
    )

print()
print("=" * 150)
print("CRASH COHORT SAMPLE")
print("=" * 150)

for r in crashes[:30]:

    print(
        f"{r['mint'][:18]:18}"
        f" | ROWS={r['rows']:5d}"
        f" | RUN={r['run_return']:8.1f}%"
        f" | CRASH={r['crash_dd']:7.1f}%"
    )

print()
print("=" * 150)

if len(crashes) >= 100:
    print("🟢 LARGE HISTORICAL RESURRECTION COHORT EXISTS.")
    print("Next = strict provenance / feature-availability audit around crash timestamps.")
elif len(crashes) >= 30:
    print("🟡 USEFUL HISTORICAL RESURRECTION COHORT EXISTS.")
    print("Next = strict provenance / feature-availability audit around crash timestamps.")
else:
    print("🔴 HISTORICAL CRASH COHORT IS SMALL.")
    print("Keep prospective T104 collection as primary source.")

print("=" * 150)

db.close()
