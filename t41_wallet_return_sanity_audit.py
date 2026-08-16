import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

HORIZON = 60.0
MAX_AFTER_DELAY = 15.0

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )

def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None

def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")

print("="*150)
print("MEMECOIN LAB — T41 WALLET POST-BUY RETURN SANITY AUDIT")
print("="*150)

# ============================================================
# LOAD DEX PRICES
# ============================================================

dex = defaultdict(list)

for r in db.execute("""
    SELECT
        token_mint,
        timestamp,
        price_usd,
        pair_address,
        liquidity_usd,
        market_cap

    FROM dex_prices

    WHERE
        token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY token_mint, timestamp
""").fetchall():

    dex[r["token_mint"]].append(r)

# ============================================================
# PRICE LOOKUP
# ============================================================

def nearest_after(token, target_ts, max_delay=MAX_AFTER_DELAY):

    arr = dex.get(token, [])

    best = None

    for r in arr:

        if r["timestamp"] < target_ts:
            continue

        delay = r["timestamp"] - target_ts

        if delay > max_delay:
            break

        best = r
        break

    return best

# ============================================================
# BUY SWAPS
# ============================================================

buys = db.execute("""
    SELECT
        signature,
        timestamp,
        wallet,
        token_mint,
        clean_price,
        raw_price,
        sol_delta,
        token_delta,
        program

    FROM swaps

    WHERE
        side='BUY'
        AND wallet IS NOT NULL
        AND token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND clean_price IS NOT NULL
        AND clean_price > 0

    ORDER BY timestamp
""").fetchall()

records = []

for b in buys:

    p1 = nearest_after(
        b["token_mint"],
        b["timestamp"] + HORIZON
    )

    if not p1:
        continue

    p0 = b["clean_price"]
    px1 = p1["price_usd"]

    if not valid(p0) or p0 <= 0 or not valid(px1):
        continue

    ret = (px1 / p0 - 1) * 100

    ratio = px1 / p0

    records.append({
        "signature": b["signature"],
        "timestamp": b["timestamp"],
        "wallet": b["wallet"],
        "token": b["token_mint"],
        "program": b["program"],

        "p0": p0,
        "raw_p0": b["raw_price"],
        "p1": px1,

        "ret": ret,
        "ratio": ratio,

        "target_ts": b["timestamp"] + HORIZON,
        "p1_ts": p1["timestamp"],
        "delay": p1["timestamp"] - (b["timestamp"] + HORIZON),

        "pair": p1["pair_address"],
        "liq": p1["liquidity_usd"],
        "mc": p1["market_cap"],
    })

print(f"BUY SWAPS                  : {len(buys)}")
print(f"WITH 60s COMPARABLE PRICE : {len(records)}")

if not records:
    raise RuntimeError("No comparable post-buy records.")

rets = [r["ret"] for r in records]
ratios = [r["ratio"] for r in records]

print()
print("="*150)
print("A) GLOBAL RETURN DISTRIBUTION")
print("="*150)

for q in [0,1,5,10,25,50,75,90,95,99,100]:
    vals = sorted(rets)
    idx = int(round((q/100) * (len(vals)-1)))
    print(f"P{q:>3} = {vals[idx]:+12.2f}%")

print()
print(f"AVG RETURN = {avg(rets):+.2f}%")
print(f"MED RETURN = {med(rets):+.2f}%")

# ============================================================
# EXTREME COUNTS
# ============================================================

print()
print("="*150)
print("B) EXTREME RETURN COUNTS")
print("="*150)

thresholds = [
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000
]

for t in thresholds:
    n = sum(abs(r["ret"]) >= t for r in records)
    print(
        f"|RET| >= {t:>5}% : "
        f"{n:5d} / {len(records)} "
        f"({100*n/len(records):5.2f}%)"
    )

# ============================================================
# CLEAN VS RAW ENTRY PRICE
# ============================================================

raw_diff = []

for r in records:
    if valid(r["raw_p0"]) and r["raw_p0"] > 0:
        d = abs(r["p0"] / r["raw_p0"] - 1) * 100
        raw_diff.append(d)

print()
print("="*150)
print("C) CLEAN PRICE VS RAW PRICE")
print("="*150)

if raw_diff:
    print(f"N={len(raw_diff)}")
    print(f"MED ABS DIFF = {med(raw_diff):.6f}%")
    print(f"AVG ABS DIFF = {avg(raw_diff):.6f}%")
    print(f">1%  = {sum(x>1 for x in raw_diff)}")
    print(f">10% = {sum(x>10 for x in raw_diff)}")
else:
    print("NO RAW PRICE COMPARISON")

# ============================================================
# TOP EXTREMES
# ============================================================

extremes = sorted(
    records,
    key=lambda r: abs(r["ret"]),
    reverse=True
)

print()
print("="*150)
print("D) TOP 30 ABSOLUTE RETURN EXTREMES")
print("="*150)

print(
    f"{'RET':>12} "
    f"{'RATIO':>10} "
    f"{'P0':>14} "
    f"{'P1':>14} "
    f"{'DELAY':>8} "
    f"{'PROGRAM':>10} "
    f"{'TOKEN':20} "
    f"{'WALLET':20}"
)

print("-"*135)

for r in extremes[:30]:

    print(
        f"{r['ret']:+11.2f}% "
        f"{r['ratio']:9.2f}x "
        f"{r['p0']:13.6g} "
        f"{r['p1']:13.6g} "
        f"{r['delay']:7.2f}s "
        f"{str(r['program'])[:10]:>10} "
        f"{r['token'][:20]:20} "
        f"{r['wallet'][:20]:20}"
    )

# ============================================================
# TOKEN-LEVEL EXTREME CONCENTRATION
# ============================================================

token_stats = defaultdict(list)

for r in records:
    token_stats[r["token"]].append(r["ret"])

ranked_tokens = []

for token, vals in token_stats.items():

    ranked_tokens.append({
        "token": token,
        "n": len(vals),
        "med": med(vals),
        "avg": avg(vals),
        "max_abs": max(abs(x) for x in vals),
        "huge": sum(abs(x) >= 1000 for x in vals),
    })

ranked_tokens.sort(
    key=lambda x: x["max_abs"],
    reverse=True
)

print()
print("="*150)
print("E) TOKENS WITH LARGEST RETURN ANOMALIES")
print("="*150)

print(
    f"{'TOKEN':22} "
    f"{'N':>5} "
    f"{'MED':>12} "
    f"{'AVG':>12} "
    f"{'MAXABS':>12} "
    f"{'>=1000%':>9}"
)

print("-"*85)

for x in ranked_tokens[:25]:

    print(
        f"{x['token'][:22]:22} "
        f"{x['n']:5d} "
        f"{x['med']:+11.2f}% "
        f"{x['avg']:+11.2f}% "
        f"{x['max_abs']:11.2f}% "
        f"{x['huge']:9d}"
    )

# ============================================================
# ENTRY PRICE SCALE AUDIT
# ============================================================

print()
print("="*150)
print("F) ENTRY PRICE SCALE AUDIT")
print("="*150)

bands = [
    (0, 1e-9),
    (1e-9, 1e-7),
    (1e-7, 1e-5),
    (1e-5, 1e-3),
    (1e-3, 1e-1),
    (1e-1, float("inf")),
]

for lo, hi in bands:

    subset = [
        r for r in records
        if r["p0"] >= lo and r["p0"] < hi
    ]

    if not subset:
        continue

    vals = [r["ret"] for r in subset]

    print(
        f"P0 [{lo:.1e}, {hi:.1e}) "
        f"| N={len(subset):5d} "
        f"| MED={med(vals):+10.2f}% "
        f"| AVG={avg(vals):+10.2f}% "
        f"| >=1000%={sum(abs(x)>=1000 for x in vals):4d}"
    )

# ============================================================
# DELAY AUDIT
# ============================================================

print()
print("="*150)
print("G) TARGET DELAY AUDIT")
print("="*150)

delays = [r["delay"] for r in records]

print(f"MED DELAY = {med(delays):.3f}s")
print(f"AVG DELAY = {avg(delays):.3f}s")
print(f"MAX DELAY = {max(delays):.3f}s")

for t in [1,2,5,10,15]:
    print(
        f"DELAY > {t:2d}s : "
        f"{sum(x>t for x in delays):5d}"
    )

# ============================================================
# SAME TOKEN PRICE-RANGE AUDIT
# ============================================================

print()
print("="*150)
print("H) DEX TOKEN PRICE RANGE AUDIT")
print("="*150)

token_ranges = []

for token, arr in dex.items():

    prices = [
        x["price_usd"]
        for x in arr
        if valid(x["price_usd"]) and x["price_usd"] > 0
    ]

    if len(prices) < 2:
        continue

    lo = min(prices)
    hi = max(prices)

    ratio = hi / lo if lo > 0 else None

    token_ranges.append(
        (ratio, token, lo, hi, len(prices))
    )

token_ranges.sort(
    reverse=True,
    key=lambda x: x[0]
)

print(
    f"{'RANGE':>12} "
    f"{'MIN':>14} "
    f"{'MAX':>14} "
    f"{'PTS':>6} "
    f"{'TOKEN':22}"
)

print("-"*80)

for ratio, token, lo, hi, n in token_ranges[:25]:

    print(
        f"{ratio:11.2f}x "
        f"{lo:13.6g} "
        f"{hi:13.6g} "
        f"{n:6d} "
        f"{token[:22]:22}"
    )

# ============================================================
# SANITY FLAGS
# ============================================================

huge = [
    r for r in records
    if abs(r["ret"]) >= 1000
]

extreme_token_count = len(
    set(r["token"] for r in huge)
)

print()
print("="*150)
print("I) DECISION SUPPORT")
print("="*150)

print(
    f"HUGE |RET|>=1000% RECORDS : "
    f"{len(huge)}"
)

print(
    f"HUGE-RETURN TOKENS         : "
    f"{extreme_token_count}"
)

huge_rate = (
    len(huge) / len(records)
)

if huge_rate >= 0.05:

    print()
    print("⚠️ STRONG DATA-QUALITY WARNING")
    print(
        "At least 5% of post-buy observations exceed "
        "1000% absolute return."
    )
    print(
        "Do NOT use wallet post-buy returns for modeling "
        "until price continuity is repaired/audited."
    )

elif huge:
    print()
    print("⚠️ LOCALIZED EXTREME RETURN ISSUE")
    print(
        "Large returns exist but appear limited to a subset "
        "of observations/tokens."
    )
    print(
        "Inspect token-level price continuity before T42."
    )

else:
    print()
    print("✅ NO >=1000% RETURN ANOMALIES")
    print(
        "Post-buy return scale looks materially cleaner."
    )

print()
print("IMPORTANT:")
print("• T41 writes nothing to the DB.")
print("• T23/T31/T32 remain untouched.")
print("• This is data-quality analysis only.")
print("• Do not tune a trading rule from T41.")

db.close()
