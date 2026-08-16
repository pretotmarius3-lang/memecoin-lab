#!/usr/bin/env python3

import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

WINDOWS = [
    2.0,
    5.0,
    10.0,
    30.0,
    60.0,
]

FEATURES = [
    "liquidity_usd",
    "market_cap",
    "fdv",
    "volume_m5",
    "buys_m5",
    "sells_m5",
]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


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
    return "NA" if x is None else f"{x:.{n}f}"


def pct(n, d):
    return "NA" if not d else f"{100*n/d:.1f}%"


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

boundary = int(boundary)


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint
FROM events
WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
ORDER BY timestamp,id
""").fetchall()


dex = db.execute("""
SELECT
    id,
    event_id,
    token_mint,
    timestamp,

    liquidity_usd,
    market_cap,
    fdv,
    volume_m5,
    buys_m5,
    sells_m5,

    pair_address,
    dex_id

FROM dex_prices

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY token_mint,timestamp,id
""").fetchall()


by_token = defaultdict(list)

for d in dex:
    by_token[d["token_mint"]].append(d)


# ============================================================
# DIRECT event_id JOIN CHECK
# ============================================================

event_ids = {
    e["id"]
    for e in events
}

direct_rows = [
    d for d in dex
    if d["event_id"] in event_ids
]

direct_unique_events = len({
    d["event_id"]
    for d in direct_rows
})


# ============================================================
# TEMPORAL MATCHING
#
# Strict pre-event:
# latest DEX snapshot timestamp <= event timestamp.
# ============================================================

matches = []


for e in events:

    candidates = by_token.get(
        e["token_mint"],
        []
    )

    best = None

    # Data sorted ascending.
    for d in candidates:

        if d["timestamp"] > e["timestamp"]:
            break

        best = d


    if best is None:

        matches.append({
            "event_id": e["id"],
            "historical": e["id"] <= boundary,
            "matched": False,
        })

        continue


    delay = (
        e["timestamp"]
        - best["timestamp"]
    )


    rec = {
        "event_id":
            e["id"],

        "historical":
            e["id"] <= boundary,

        "matched":
            True,

        "delay":
            delay,

        "dex_event_id":
            best["event_id"],

        "same_event_id":
            best["event_id"] == e["id"],

        "pair_address":
            best["pair_address"],

        "dex_id":
            best["dex_id"],
    }


    for f in FEATURES:
        rec[f] = best[f]


    matches.append(rec)


matched = [
    r for r in matches
    if r["matched"]
]

hist = [
    r for r in matches
    if r["historical"]
]

pros = [
    r for r in matches
    if not r["historical"]
]


print("=" * 175)
print(
    "MEMECOIN LAB — T99 DEX MATCHING / PROVENANCE AUDIT"
)
print("=" * 175)

print("MODE              : READ-ONLY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")
print("DB WRITES         : NONE")
print("T59/T78/T82/T86   : UNTOUCHED")
print()
print(f"T59 BOUNDARY      : {boundary}")

print()
print("=" * 175)
print("A) RAW TABLE COVERAGE")
print("=" * 175)

print(
    f"EVENTS                : {len(events)}"
)

print(
    f"DEX PRICE ROWS        : {len(dex)}"
)

print(
    f"EVENT TOKENS          : "
    f"{len(set(e['token_mint'] for e in events))}"
)

print(
    f"DEX TOKENS            : "
    f"{len(set(d['token_mint'] for d in dex))}"
)

common_tokens = (
    set(e["token_mint"] for e in events)
    &
    set(d["token_mint"] for d in dex)
)

print(
    f"COMMON TOKENS         : {len(common_tokens)}"
)


print()
print("=" * 175)
print("B) DEX event_id PROVENANCE")
print("=" * 175)

print(
    f"DEX ROWS whose event_id exists in events : "
    f"{len(direct_rows)}/{len(dex)} "
    f"({pct(len(direct_rows),len(dex))})"
)

print(
    f"UNIQUE events covered by direct event_id : "
    f"{direct_unique_events}/{len(events)} "
    f"({pct(direct_unique_events,len(events))})"
)


same_temporal_eventid = sum(
    r.get("same_event_id", False)
    for r in matched
)

print(
    f"TEMPORAL MATCH also has same event_id    : "
    f"{same_temporal_eventid}/{len(matched)} "
    f"({pct(same_temporal_eventid,len(matched))})"
)


print()
print("=" * 175)
print("C) STRICT PRE-EVENT TEMPORAL MATCHING")
print("=" * 175)

for name, rr in [
    ("ALL", matches),
    ("HIST", hist),
    ("PROS", pros),
]:

    mm = [
        r for r in rr
        if r["matched"]
    ]

    print(
        f"{name:5} "
        f"| MATCHED={len(mm):4d}/{len(rr):4d} "
        f"| RATE={pct(len(mm),len(rr)):>6}"
    )


print()
print("=" * 175)
print("D) MATCH DELAY DISTRIBUTION")
print("=" * 175)

for name, rr in [
    ("ALL", matched),
    ("HIST", [
        r for r in matched
        if r["historical"]
    ]),
    ("PROS", [
        r for r in matched
        if not r["historical"]
    ]),
]:

    ds = [
        r["delay"]
        for r in rr
        if valid(r.get("delay"))
    ]

    print(
        f"{name:5} "
        f"| N={len(ds):4d} "
        f"| MED={fmt(med(ds)):>8} "
        f"| P90={fmt(quantile(ds,.90)):>8} "
        f"| P95={fmt(quantile(ds,.95)):>8} "
        f"| MAX={fmt(max(ds) if ds else None):>8}"
    )


print()
print("=" * 175)
print("E) COVERAGE BY MAXIMUM SNAPSHOT AGE")
print("=" * 175)

for window in WINDOWS:

    print()
    print(
        f"MAX AGE <= {window:.0f}s"
    )

    for name, rr in [
        ("ALL", matches),
        ("HIST", hist),
        ("PROS", pros),
    ]:

        n = sum(
            r["matched"]
            and r["delay"] <= window
            for r in rr
        )

        print(
            f"  {name:5} "
            f"| {n:4d}/{len(rr):4d} "
            f"| {pct(n,len(rr)):>6}"
        )


print()
print("=" * 175)
print("F) FEATURE COMPLETENESS — MATCHED ROWS")
print("=" * 175)

for f in FEATURES:

    n = sum(
        valid(r.get(f))
        for r in matched
    )

    print(
        f"{f:20} "
        f"| COMPLETE={n:4d}/{len(matched):4d} "
        f"| RATE={pct(n,len(matched))}"
    )


print()
print("=" * 175)
print("G) DEX / PAIR DISTRIBUTION")
print("=" * 175)

dex_counts = defaultdict(int)
pair_counts = defaultdict(int)

for r in matched:

    dex_counts[
        str(r.get("dex_id"))
    ] += 1

    pair_counts[
        str(r.get("pair_address"))
    ] += 1


print("DEX IDS")

for k,v in sorted(
    dex_counts.items(),
    key=lambda x:-x[1]
)[:20]:

    print(
        f"  {k:20} | N={v:4d}"
    )


print()
print(
    f"UNIQUE PAIRS = {len(pair_counts)}"
)


print()
print("=" * 175)
print("H) PROVENANCE EXAMPLES")
print("=" * 175)

for r in matched[:20]:

    print(
        f"EVENT={r['event_id']:5d} "
        f"| DEX_EVENT={str(r['dex_event_id']):>5} "
        f"| SAME={str(r['same_event_id']):5} "
        f"| AGE={r['delay']:8.3f}s "
        f"| DEX={str(r['dex_id']):12} "
        f"| PAIR={str(r['pair_address'])[:20]}"
    )


print()
print("=" * 175)
print("I) DECISION SUPPORT")
print("=" * 175)

coverage10 = (
    sum(
        r["matched"]
        and r["delay"] <= 10
        for r in matches
    )
    / len(matches)
)

proscoverage10 = (
    sum(
        r["matched"]
        and r["delay"] <= 10
        for r in pros
    )
    / len(pros)
    if pros else 0
)


print(
    f"ALL <=10s COVERAGE  = {100*coverage10:.1f}%"
)

print(
    f"PROS <=10s COVERAGE = {100*proscoverage10:.1f}%"
)

print()


if (
    coverage10 >= .70
    and proscoverage10 >= .70
):

    print(
        "🟢 DEX MARKET-STRUCTURE DATA IS USABLE VIA "
        "TOKEN+STRICT-PRE-EVENT TIME MATCHING."
    )

    print(
        "T97 failed because event_id was the wrong matching assumption."
    )

    print(
        "Next = T100 market-structure discovery with frozen matching rule."
    )


elif (
    coverage10 >= .40
    or proscoverage10 >= .40
):

    print(
        "🟡 DEX DATA IS PARTIALLY USABLE."
    )

    print(
        "Need a coverage-aware discovery audit before any promotion."
    )


else:

    print(
        "🔴 DEX SNAPSHOT COVERAGE IS TOO SPARSE FOR "
        "RELIABLE MARKET-STRUCTURE DISCOVERY."
    )

    print(
        "Enrich prospective DEX collection first."
    )


print()
print("IMPORTANT:")
print("• Temporal matching is token_mint + latest snapshot <= event timestamp.")
print("• Future DEX snapshots are strictly forbidden.")
print("• No model fitting.")
print("• No feature threshold search.")
print("• T99 writes nothing.")
print("• Frozen prospective branches remain untouched.")

db.close()
