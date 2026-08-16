import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"
MAX_DELAY = 5.0


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


# ============================================================
# LOAD DEX PRICES
# ============================================================

dex = defaultdict(list)

for r in db.execute("""
    SELECT
        token_mint,
        timestamp,
        price_usd,
        pair_address
    FROM dex_prices
    WHERE
        token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND price_usd IS NOT NULL
        AND price_usd > 0
    ORDER BY token_mint, timestamp
""").fetchall():

    dex[r["token_mint"]].append(r)


def nearest(token, ts):

    arr = dex.get(token, [])

    best = None
    best_err = None

    for r in arr:

        err = abs(r["timestamp"] - ts)

        if best_err is None or err < best_err:
            best = r
            best_err = err

        if r["timestamp"] > ts and err > MAX_DELAY:
            break

    if best is None:
        return None

    if best_err > MAX_DELAY:
        return None

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
        program
    FROM swaps
    WHERE
        side='BUY'
        AND token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND clean_price IS NOT NULL
        AND clean_price > 0
    ORDER BY timestamp
""").fetchall()


records = []

for b in buys:

    d = nearest(
        b["token_mint"],
        b["timestamp"]
    )

    if not d:
        continue

    sp = b["clean_price"]
    dp = d["price_usd"]

    if not valid(sp) or sp <= 0 or not valid(dp) or dp <= 0:
        continue

    ratio = dp / sp

    records.append({
        "token": b["token_mint"],
        "wallet": b["wallet"],
        "program": b["program"],

        "swap_ts": b["timestamp"],
        "dex_ts": d["timestamp"],
        "delay": d["timestamp"] - b["timestamp"],

        "swap_price": sp,
        "dex_price": dp,
        "ratio": ratio,

        "pair": d["pair_address"],
    })


print("=" * 150)
print("MEMECOIN LAB — T41.1 SAME-TIMESTAMP PRICE SCALE DIAGNOSTIC")
print("=" * 150)

print(f"BUY SWAPS                : {len(buys)}")
print(f"SAME-TIME COMPARISONS    : {len(records)}")

if not records:
    raise RuntimeError("No same-time price comparisons found.")


ratios = [r["ratio"] for r in records]
delays = [abs(r["delay"]) for r in records]


# ============================================================
# A) RATIO DISTRIBUTION
# ============================================================

print()
print("=" * 150)
print("A) DEX_PRICE / SWAP_PRICE RATIO")
print("=" * 150)

vals = sorted(ratios)

for q in [0,1,5,10,25,50,75,90,95,99,100]:
    idx = int(round(
        (q / 100) * (len(vals)-1)
    ))

    print(
        f"P{q:>3} = "
        f"{vals[idx]:12.4f}x"
    )

print()
print(
    f"AVG RATIO = {avg(ratios):.4f}x"
)

print(
    f"MED RATIO = {med(ratios):.4f}x"
)


# ============================================================
# B) DISTANCE FROM GLOBAL MEDIAN SCALE
# ============================================================

scale = med(ratios)

relative_errors = [
    abs(r / scale - 1) * 100
    for r in ratios
]

print()
print("=" * 150)
print("B) STABILITY AROUND MEDIAN SCALE")
print("=" * 150)

print(
    f"MEDIAN SCALE = {scale:.6f}x"
)

print(
    f"MED ABS REL ERROR = "
    f"{med(relative_errors):.4f}%"
)

print(
    f"AVG ABS REL ERROR = "
    f"{avg(relative_errors):.4f}%"
)

for t in [1,2,5,10,20,50]:
    n = sum(
        x <= t
        for x in relative_errors
    )

    print(
        f"WITHIN ±{t:>2}% OF SCALE : "
        f"{n:5d}/{len(records)} "
        f"({100*n/len(records):5.1f}%)"
    )


# ============================================================
# C) TOKEN-LEVEL MEDIAN RATIOS
# ============================================================

by_token = defaultdict(list)

for r in records:
    by_token[r["token"]].append(
        r["ratio"]
    )


token_stats = []

for token, vals in by_token.items():

    token_stats.append({
        "token": token,
        "n": len(vals),
        "med": med(vals),
        "avg": avg(vals),
        "min": min(vals),
        "max": max(vals),
    })


token_stats.sort(
    key=lambda x:
        abs(x["med"] - scale),
    reverse=True
)


print()
print("=" * 150)
print("C) TOKEN-LEVEL SCALE")
print("=" * 150)

print(
    f"{'TOKEN':22} "
    f"{'N':>6} "
    f"{'MED':>12} "
    f"{'AVG':>12} "
    f"{'MIN':>12} "
    f"{'MAX':>12}"
)

print("-" * 85)

for x in token_stats[:40]:

    print(
        f"{x['token'][:22]:22} "
        f"{x['n']:6d} "
        f"{x['med']:11.3f}x "
        f"{x['avg']:11.3f}x "
        f"{x['min']:11.3f}x "
        f"{x['max']:11.3f}x"
    )


# ============================================================
# D) PROGRAM SCALE
# ============================================================

by_program = defaultdict(list)

for r in records:
    by_program[
        r["program"] or "NA"
    ].append(r["ratio"])


print()
print("=" * 150)
print("D) SCALE BY PROGRAM")
print("=" * 150)

for program, vals in sorted(
    by_program.items(),
    key=lambda x: -len(x[1])
):

    print(
        f"{program:15} "
        f"| N={len(vals):5d} "
        f"| MED={med(vals):10.3f}x "
        f"| AVG={avg(vals):10.3f}x"
    )


# ============================================================
# E) DELAY
# ============================================================

print()
print("=" * 150)
print("E) SAME-TIME MATCH DELAY")
print("=" * 150)

print(
    f"MED ABS DELAY = "
    f"{med(delays):.3f}s"
)

print(
    f"AVG ABS DELAY = "
    f"{avg(delays):.3f}s"
)

print(
    f"MAX ABS DELAY = "
    f"{max(delays):.3f}s"
)


# ============================================================
# F) EXAMPLES AROUND MEDIAN
# ============================================================

records_sorted = sorted(
    records,
    key=lambda r:
        abs(r["ratio"] - scale)
)

print()
print("=" * 150)
print("F) EXAMPLES CLOSE TO MEDIAN SCALE")
print("=" * 150)

for r in records_sorted[:20]:

    print(
        f"RATIO={r['ratio']:9.3f}x "
        f"| SWAP={r['swap_price']:.8g} "
        f"| DEX={r['dex_price']:.8g} "
        f"| DELAY={r['delay']:+.2f}s "
        f"| TOKEN={r['token'][:20]} "
        f"| PROGRAM={r['program']}"
    )


# ============================================================
# G) DECISION SUPPORT
# ============================================================

within_10 = (
    sum(
        x <= 10
        for x in relative_errors
    )
    / len(relative_errors)
)

print()
print("=" * 150)
print("G) DECISION SUPPORT")
print("=" * 150)

if (
    scale >= 10
    and within_10 >= 0.80
):

    print(
        "✅ STRONG CONSTANT-SCALE MISMATCH DETECTED"
    )

    print(
        f"DEX price is approximately "
        f"{scale:.3f}x swap clean_price."
    )

    print(
        f"{100*within_10:.1f}% of observations "
        f"are within ±10% of this scale."
    )

    print()
    print(
        "This strongly suggests the two price sources "
        "are expressed in different units."
    )

    print(
        "Do NOT mix swaps.clean_price and dex_prices.price_usd "
        "for return calculations."
    )

elif scale >= 3:

    print(
        "⚠️ NON-TRIVIAL PRICE SCALE MISMATCH"
    )

    print(
        f"Median DEX/SWAP ratio = "
        f"{scale:.3f}x"
    )

    print(
        "Mismatch exists, but it is not sufficiently "
        "constant to apply a single correction factor yet."
    )

else:

    print(
        "❌ NO LARGE CONSTANT SCALE MISMATCH"
    )

    print(
        "The T41 anomaly likely comes from another "
        "price-continuity or matching issue."
    )


print()
print("IMPORTANT:")
print("• T41.1 writes nothing to DB.")
print("• T23/T31/T32 remain untouched.")
print("• Do not apply any correction factor automatically yet.")
print("• First diagnose the unit source.")

db.close()
