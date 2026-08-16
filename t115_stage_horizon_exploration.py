#!/usr/bin/env python3

import sqlite3
import os
import math
import statistics

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

DUMPS = "t108_dump_events"
DEX = "dex_prices"

STAGES = [30, 60, 90, 120, 180]
HORIZONS = [300, 900]

MAX_ENTRY_DELAY = 40
MAX_PATH_GAP = 65

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# HELPERS
# ============================================================

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
    return (b / a - 1.0) * 100.0


def avg(xs):
    xs = [x for x in xs if x is not None and math.isfinite(x)]
    return sum(xs) / len(xs) if xs else None


def med(xs):
    xs = [x for x in xs if x is not None and math.isfinite(x)]
    return statistics.median(xs) if xs else None


def fmt(x, n=1):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def price_at_or_after(mint, ts, max_delay=MAX_ENTRY_DELAY):
    return db.execute("""
        SELECT timestamp, price_usd
        FROM dex_prices
        WHERE token_mint=?
          AND timestamp >= ?
          AND timestamp <= ?
          AND price_usd IS NOT NULL
          AND price_usd > 0
        ORDER BY timestamp ASC
        LIMIT 1
    """, (mint, ts, ts + max_delay)).fetchone()


def path_after(mint, start_ts, horizon):
    rows = db.execute("""
        SELECT timestamp, price_usd
        FROM dex_prices
        WHERE token_mint=?
          AND timestamp >= ?
          AND timestamp <= ?
          AND price_usd IS NOT NULL
          AND price_usd > 0
        ORDER BY timestamp ASC
    """, (mint, start_ts, start_ts + horizon)).fetchall()

    return rows


def max_gap(rows, start_ts, end_ts):
    if not rows:
        return None

    times = [start_ts]
    times += [r["timestamp"] for r in rows]
    times += [end_ts]

    return max(
        times[i + 1] - times[i]
        for i in range(len(times) - 1)
    )


# ============================================================
# LOAD DUMPS
# ============================================================

dumps = db.execute("""
    SELECT *
    FROM t108_dump_events
    ORDER BY trigger_timestamp
""").fetchall()

print()
print("=" * 180)
print("MEMECOIN LAB — T115 STAGE HORIZON EXPLORATION")
print("=" * 180)

print(f"DUMP EVENTS       : {len(dumps)}")
print(f"STAGES            : {STAGES}")
print(f"FORWARD HORIZONS  : {HORIZONS}")
print()
print("MODE              : EXPLORATION ONLY")
print("T114              : UNTOUCHED / FROZEN")
print("ENTRY             : FIRST DEX SNAPSHOT AT/AFTER STAGE")
print("FUTURE            : STRICTLY AFTER ACTUAL ENTRY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")


# ============================================================
# BUILD RESULTS
# ============================================================

results = []

for d in dumps:

    mint = d["token_mint"]
    trigger = d["trigger_timestamp"]

    trigger_price = d["trigger_price"]

    if not valid(trigger_price):
        continue

    for stage in STAGES:

        requested_ts = trigger + stage

        entry = price_at_or_after(
            mint,
            requested_ts
        )

        if not entry:
            continue

        entry_ts = entry["timestamp"]
        entry_price = entry["price_usd"]

        if not valid(entry_price):
            continue

        delay = entry_ts - requested_ts

        if delay > MAX_ENTRY_DELAY:
            continue

        row = {
            "mint": mint,
            "level": d["dump_level"],
            "trigger": trigger,
            "stage": stage,
            "entry_ts": entry_ts,
            "entry_price": entry_price,
            "delay": delay,
            "stage_return": pct(
                trigger_price,
                entry_price
            ),
        }

        for horizon in HORIZONS:

            path = path_after(
                mint,
                entry_ts,
                horizon
            )

            if not path:
                row[f"ready_{horizon}"] = False
                continue

            gap = max_gap(
                path,
                entry_ts,
                entry_ts + horizon
            )

            # Need coverage reaching close to horizon.
            last_age = (
                entry_ts + horizon
                - path[-1]["timestamp"]
            )

            ready = (
                gap is not None
                and gap <= MAX_PATH_GAP
                and last_age <= MAX_PATH_GAP
            )

            row[f"ready_{horizon}"] = ready
            row[f"n_{horizon}"] = len(path)
            row[f"gap_{horizon}"] = gap

            if not ready:
                continue

            prices = [
                r["price_usd"]
                for r in path
                if valid(r["price_usd"])
            ]

            if not prices:
                row[f"ready_{horizon}"] = False
                continue

            end_price = prices[-1]
            high = max(prices)
            low = min(prices)

            row[f"end_{horizon}"] = pct(
                entry_price,
                end_price
            )

            row[f"max_{horizon}"] = pct(
                entry_price,
                high
            )

            row[f"min_{horizon}"] = pct(
                entry_price,
                low
            )

            row[f"hit20_{horizon}"] = int(
                row[f"max_{horizon}"] >= 20
            )

            row[f"hit50_{horizon}"] = int(
                row[f"max_{horizon}"] >= 50
            )

        results.append(row)


# ============================================================
# STAGE SUMMARY
# ============================================================

print()
print("=" * 180)
print("STAGE COMPARISON — STRICT ENTRY-RELATIVE OUTCOMES")
print("=" * 180)

print(
    f"{'STAGE':>6} "
    f"{'ROWS':>6} "
    f"{'R300':>6} "
    f"{'MAX300 AVG':>11} "
    f"{'MAX300 MED':>11} "
    f"{'MIN300 AVG':>11} "
    f"{'END300 AVG':>11} "
    f"{'+20':>8} "
    f"{'+50':>8}"
)

print("-" * 105)

for stage in STAGES:

    rs = [
        r for r in results
        if r["stage"] == stage
    ]

    mature = [
        r for r in rs
        if r.get("ready_300")
    ]

    maxs = [
        r.get("max_300")
        for r in mature
    ]

    mins = [
        r.get("min_300")
        for r in mature
    ]

    ends = [
        r.get("end_300")
        for r in mature
    ]

    hit20 = sum(
        r.get("hit20_300", 0)
        for r in mature
    )

    hit50 = sum(
        r.get("hit50_300", 0)
        for r in mature
    )

    n = len(mature)

    print(
        f"{stage:>5}s "
        f"{len(rs):>6} "
        f"{n:>6} "
        f"{fmt(avg(maxs)):>11} "
        f"{fmt(med(maxs)):>11} "
        f"{fmt(avg(mins)):>11} "
        f"{fmt(avg(ends)):>11} "
        f"{hit20}/{n:<5} "
        f"{hit50}/{n:<5}"
    )


# ============================================================
# 900 SECOND SUMMARY
# ============================================================

print()
print("=" * 180)
print("LONGER FOLLOW-THROUGH — 900s AFTER ACTUAL ENTRY")
print("=" * 180)

print(
    f"{'STAGE':>6} "
    f"{'READY':>7} "
    f"{'MAX900 AVG':>12} "
    f"{'MAX900 MED':>12} "
    f"{'MIN900 AVG':>12} "
    f"{'END900 AVG':>12}"
)

print("-" * 75)

for stage in STAGES:

    mature = [
        r for r in results
        if r["stage"] == stage
        and r.get("ready_900")
    ]

    print(
        f"{stage:>5}s "
        f"{len(mature):>7} "
        f"{fmt(avg([r.get('max_900') for r in mature])):>12} "
        f"{fmt(med([r.get('max_900') for r in mature])):>12} "
        f"{fmt(avg([r.get('min_900') for r in mature])):>12} "
        f"{fmt(avg([r.get('end_900') for r in mature])):>12}"
    )


# ============================================================
# MATCHED DUMPS
# ============================================================

print()
print("=" * 180)
print("MATCHED-DUMP COMPARISON")
print("=" * 180)

# Only dumps having usable 300s paths at every stage.
by_event = {}

for r in results:

    key = (
        r["mint"],
        r["trigger"],
        r["level"],
    )

    by_event.setdefault(
        key,
        {}
    )[r["stage"]] = r


matched = []

for key, stages in by_event.items():

    if all(
        s in stages
        and stages[s].get("ready_300")
        for s in STAGES
    ):
        matched.append(
            (key, stages)
        )


print(
    f"DUMPS USABLE AT ALL {len(STAGES)} STAGES : "
    f"{len(matched)}"
)

print()

if matched:

    base_stage = 30

    print(
        f"{'STAGE':>6} "
        f"{'ΔMAX vs30':>12} "
        f"{'ΔMIN vs30':>12} "
        f"{'ΔEND vs30':>12} "
        f"{'ENTRY MOVE':>12}"
    )

    print("-" * 65)

    for stage in STAGES:

        dmax = []
        dmin = []
        dend = []
        entry_move = []

        for _, stages in matched:

            base = stages[
                base_stage
            ]

            cur = stages[
                stage
            ]

            dmax.append(
                cur["max_300"]
                - base["max_300"]
            )

            dmin.append(
                cur["min_300"]
                - base["min_300"]
            )

            dend.append(
                cur["end_300"]
                - base["end_300"]
            )

            entry_move.append(
                pct(
                    base["entry_price"],
                    cur["entry_price"]
                )
            )

        print(
            f"{stage:>5}s "
            f"{fmt(avg(dmax)):>12} "
            f"{fmt(avg(dmin)):>12} "
            f"{fmt(avg(dend)):>12} "
            f"{fmt(avg(entry_move)):>12}"
        )


# ============================================================
# INDIVIDUAL PATHS
# ============================================================

print()
print("=" * 180)
print("LATEST MATURE DUMPS — STAGE TRADE-OFF")
print("=" * 180)

keys = sorted(
    by_event.keys(),
    key=lambda x: x[1],
    reverse=True
)[:12]

for key in keys:

    mint, trigger, level = key

    print()
    print(
        f"{mint[:20]} | D={level}%"
    )

    for stage in STAGES:

        r = by_event[
            key
        ].get(stage)

        if not r:
            print(
                f"  {stage:>3}s | NO ENTRY SNAPSHOT"
            )
            continue

        if not r.get(
            "ready_300"
        ):
            print(
                f"  {stage:>3}s | "
                f"ENTRYΔ={fmt(r['stage_return']):>7}% "
                f"| 300s NOT MATURE"
            )
            continue

        print(
            f"  {stage:>3}s "
            f"| ENTRYΔ={fmt(r['stage_return']):>7}% "
            f"| MAX={fmt(r['max_300']):>7}% "
            f"| MIN={fmt(r['min_300']):>7}% "
            f"| END={fmt(r['end_300']):>7}% "
            f"| GAP={fmt(r.get('gap_300'),0):>4}s"
        )


print()
print("=" * 180)
print("INTERPRETATION")
print("=" * 180)

print("""
T115 is exploratory and MUST NOT modify T114.

The key comparison is not simply which stage has the largest return.

We are measuring the trade-off:

EARLY ENTRY
    more remaining upside
    less confirmation
    potentially more downside

LATE ENTRY
    more path information
    potentially fewer false reversals
    but some rebound may already be consumed

30 / 60 / 90 / 120 / 180 seconds are therefore treated as competing
observation horizons.

Do not select a new live stage from this historical cohort alone.

If 90/120/180 looks materially better, it becomes a candidate for a
future frozen prospective test rather than retroactively changing T114.
""")

db.close()
