import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

HORIZON = 60.0

# We audit several tolerances rather than silently accepting
# a distant swap as the +60s price.
TOLERANCES = [2.0, 5.0, 10.0, 15.0]

PRIMARY_TOLERANCE = 5.0


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


def percentile(vals, q):
    vals = sorted(x for x in vals if valid(x))

    if not vals:
        return None

    idx = int(round(
        (q / 100.0) * (len(vals) - 1)
    ))

    return vals[idx]


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# LOAD SWAPS
# ============================================================

rows = db.execute("""
    SELECT
        signature,
        timestamp,
        wallet,
        side,
        token_mint,
        token_delta,
        sol_delta,
        raw_price,
        clean_price,
        price_valid,
        reject_reason,
        program
    FROM swaps
    WHERE
        token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND clean_price IS NOT NULL
        AND clean_price > 0
        AND (
            price_valid IS NULL
            OR price_valid = 1
        )
    ORDER BY token_mint, timestamp
""").fetchall()


by_token = defaultdict(list)

for r in rows:

    if not valid(r["clean_price"]):
        continue

    if r["clean_price"] <= 0:
        continue

    by_token[r["token_mint"]].append(r)


# ============================================================
# NEAREST SAME-UNIT SWAP
# ============================================================

def nearest_swap(token, target_ts, tolerance):

    arr = by_token.get(token, [])

    if not arr:
        return None

    best = None
    best_error = None

    for r in arr:

        err = abs(r["timestamp"] - target_ts)

        if best_error is None or err < best_error:
            best = r
            best_error = err

        # Since array is chronological, once we are clearly
        # beyond target+tolerance there is no need to continue.
        if r["timestamp"] > target_ts + tolerance:
            break

    if best is None:
        return None

    if best_error > tolerance:
        return None

    return best


# ============================================================
# ENTRY BUY SWAPS
# ============================================================

buys = [
    r for r in rows
    if str(r["side"]).upper() == "BUY"
]


print("=" * 155)
print("MEMECOIN LAB — T41.3 SAME-UNIT WALLET RETURN REPAIR AUDIT")
print("=" * 155)

print("PRICE FAMILY:")
print("ENTRY = swaps.clean_price")
print("EXIT  = swaps.clean_price from same token around entry +60s")
print("UNIT  = SOL/token -> SOL/token")
print()

print(f"VALID SWAPS LOADED : {len(rows)}")
print(f"BUY ENTRIES        : {len(buys)}")
print(f"UNIQUE TOKENS      : {len(by_token)}")


# ============================================================
# A) COVERAGE BY MATCH TOLERANCE
# ============================================================

print()
print("=" * 155)
print("A) +60s SAME-UNIT PRICE COVERAGE")
print("=" * 155)

coverage = {}

for tol in TOLERANCES:

    matched = []

    for b in buys:

        target = b["timestamp"] + HORIZON

        x = nearest_swap(
            b["token_mint"],
            target,
            tol
        )

        if x is not None:
            matched.append(x)

    coverage[tol] = len(matched)

    pct = (
        100 * len(matched) / len(buys)
        if buys else 0
    )

    print(
        f"TOL ±{tol:>4.1f}s "
        f"| MATCH={len(matched):5d}/{len(buys)} "
        f"| COVERAGE={pct:6.2f}%"
    )


# ============================================================
# BUILD PRIMARY RECORDS
# ============================================================

records = []

for b in buys:

    target = b["timestamp"] + HORIZON

    x = nearest_swap(
        b["token_mint"],
        target,
        PRIMARY_TOLERANCE
    )

    if x is None:
        continue

    p0 = b["clean_price"]
    p1 = x["clean_price"]

    if (
        not valid(p0)
        or not valid(p1)
        or p0 <= 0
        or p1 <= 0
    ):
        continue

    ret = (p1 / p0 - 1.0) * 100.0

    records.append({
        "entry_sig": b["signature"],
        "exit_sig": x["signature"],

        "token": b["token_mint"],
        "wallet": b["wallet"],

        "entry_program": b["program"],
        "exit_program": x["program"],

        "entry_ts": b["timestamp"],
        "target_ts": target,
        "exit_ts": x["timestamp"],

        "delay": x["timestamp"] - target,

        "entry_price": p0,
        "exit_price": p1,

        "entry_side": b["side"],
        "exit_side": x["side"],

        "ret": ret,
    })


if not records:
    raise RuntimeError(
        "No +60s same-unit matches found."
    )


rets = [r["ret"] for r in records]
delays = [abs(r["delay"]) for r in records]


# ============================================================
# B) REPAIRED RETURN DISTRIBUTION
# ============================================================

print()
print("=" * 155)
print(
    f"B) REPAIRED +60s RETURN DISTRIBUTION — "
    f"PRIMARY TOLERANCE ±{PRIMARY_TOLERANCE:.1f}s"
)
print("=" * 155)

print(f"N = {len(records)}")
print()

for q in [0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100]:

    v = percentile(rets, q)

    print(
        f"P{q:>3} = {v:+12.2f}%"
    )

print()
print(f"AVG = {avg(rets):+.2f}%")
print(f"MED = {med(rets):+.2f}%")
print(
    f"WIN = "
    f"{100 * sum(x > 0 for x in rets) / len(rets):.1f}%"
)


# ============================================================
# C) EXTREME RETURN AUDIT
# ============================================================

print()
print("=" * 155)
print("C) EXTREME RETURN AUDIT")
print("=" * 155)

for threshold in [
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
]:

    n = sum(
        abs(x) >= threshold
        for x in rets
    )

    print(
        f"|RET| >= {threshold:>5}% "
        f": {n:5d}/{len(rets)} "
        f"({100*n/len(rets):6.2f}%)"
    )


# ============================================================
# D) MATCH DELAY QUALITY
# ============================================================

print()
print("=" * 155)
print("D) +60s MATCH DELAY QUALITY")
print("=" * 155)

print(
    f"MED ABS DELAY = {med(delays):.3f}s"
)

print(
    f"AVG ABS DELAY = {avg(delays):.3f}s"
)

print(
    f"MAX ABS DELAY = {max(delays):.3f}s"
)

for t in [0.5, 1, 2, 3, 5]:

    n = sum(
        x <= t
        for x in delays
    )

    print(
        f"WITHIN ±{t:>3.1f}s "
        f": {n:5d}/{len(delays)} "
        f"({100*n/len(delays):5.1f}%)"
    )


# ============================================================
# E) RETURN BY ENTRY PROGRAM
# ============================================================

print()
print("=" * 155)
print("E) RETURN BY ENTRY PROGRAM")
print("=" * 155)

by_program = defaultdict(list)

for r in records:
    by_program[
        r["entry_program"] or "NA"
    ].append(r["ret"])


for program, vals in sorted(
    by_program.items(),
    key=lambda x: -len(x[1])
):

    print(
        f"{program:15} "
        f"| N={len(vals):5d} "
        f"| AVG={avg(vals):+9.2f}% "
        f"| MED={med(vals):+9.2f}% "
        f"| WIN={100*sum(x>0 for x in vals)/len(vals):5.1f}%"
    )


# ============================================================
# F) EXIT SIDE AUDIT
# ============================================================

print()
print("=" * 155)
print("F) EXIT SWAP SIDE AUDIT")
print("=" * 155)

by_exit_side = defaultdict(list)

for r in records:
    by_exit_side[
        str(r["exit_side"])
    ].append(r["ret"])


for side, vals in sorted(
    by_exit_side.items(),
    key=lambda x: -len(x[1])
):

    print(
        f"{side:8} "
        f"| N={len(vals):5d} "
        f"| AVG={avg(vals):+9.2f}% "
        f"| MED={med(vals):+9.2f}%"
    )


# ============================================================
# G) TOP ABSOLUTE EXTREMES
# ============================================================

print()
print("=" * 155)
print("G) TOP 30 REPAIRED ABSOLUTE RETURN EXTREMES")
print("=" * 155)

extremes = sorted(
    records,
    key=lambda r: abs(r["ret"]),
    reverse=True
)

print(
    f"{'RET':>12} "
    f"{'P0':>14} "
    f"{'P60':>14} "
    f"{'DELAY':>8} "
    f"{'E-SIDE':>7} "
    f"{'PROGRAM':>10} "
    f"{'TOKEN':22}"
)

print("-" * 100)

for r in extremes[:30]:

    print(
        f"{r['ret']:+11.2f}% "
        f"{r['entry_price']:13.6g} "
        f"{r['exit_price']:13.6g} "
        f"{r['delay']:+7.2f}s "
        f"{str(r['exit_side']):>7} "
        f"{str(r['entry_program'])[:10]:>10} "
        f"{r['token'][:22]:22}"
    )


# ============================================================
# H) TOKEN-LEVEL SANITY
# ============================================================

print()
print("=" * 155)
print("H) TOKEN-LEVEL REPAIRED RETURNS")
print("=" * 155)

token_returns = defaultdict(list)

for r in records:
    token_returns[
        r["token"]
    ].append(r["ret"])


token_stats = []

for token, vals in token_returns.items():

    token_stats.append({
        "token": token,
        "n": len(vals),
        "med": med(vals),
        "avg": avg(vals),
        "maxabs": max(abs(x) for x in vals),
        "huge": sum(abs(x) >= 1000 for x in vals),
    })


token_stats.sort(
    key=lambda x: x["maxabs"],
    reverse=True
)


print(
    f"{'TOKEN':22} "
    f"{'N':>6} "
    f"{'MED':>12} "
    f"{'AVG':>12} "
    f"{'MAXABS':>12} "
    f"{'>=1000':>8}"
)

print("-" * 80)

for x in token_stats[:30]:

    print(
        f"{x['token'][:22]:22} "
        f"{x['n']:6d} "
        f"{x['med']:+11.2f}% "
        f"{x['avg']:+11.2f}% "
        f"{x['maxabs']:11.2f}% "
        f"{x['huge']:8d}"
    )


# ============================================================
# I) PRICE CONTINUITY CHECK
# ============================================================

print()
print("=" * 155)
print("I) SAME-UNIT PRICE CONTINUITY CHECK")
print("=" * 155)

reasonable = sum(
    abs(x) < 1000
    for x in rets
)

print(
    f"|RET| < 1000% : "
    f"{reasonable}/{len(rets)} "
    f"({100*reasonable/len(rets):.2f}%)"
)

under_500 = sum(
    abs(x) < 500
    for x in rets
)

print(
    f"|RET| < 500%  : "
    f"{under_500}/{len(rets)} "
    f"({100*under_500/len(rets):.2f}%)"
)

under_250 = sum(
    abs(x) < 250
    for x in rets
)

print(
    f"|RET| < 250%  : "
    f"{under_250}/{len(rets)} "
    f"({100*under_250/len(rets):.2f}%)"
)


# ============================================================
# J) DECISION SUPPORT
# ============================================================

huge = sum(
    abs(x) >= 1000
    for x in rets
)

huge_rate = huge / len(rets)

coverage_primary = (
    len(records) / len(buys)
    if buys else 0
)


print()
print("=" * 155)
print("J) DECISION SUPPORT")
print("=" * 155)

print(
    f"PRIMARY COVERAGE = "
    f"{100*coverage_primary:.2f}%"
)

print(
    f"|RET|>=1000% RATE = "
    f"{100*huge_rate:.3f}%"
)

print()

if huge_rate <= 0.01:

    print(
        "✅ ORIGINAL ~+7000% SCALE BUG IS REMOVED"
    )

    print(
        "Same-unit SOL/token -> SOL/token returns "
        "do not reproduce the systematic T40 anomaly."
    )

    if coverage_primary >= 0.50:

        print()
        print(
            "✅ COVERAGE IS SUFFICIENT FOR A FIRST "
            "CORRECTED WALLET-BEHAVIOR AUDIT."
        )

        print(
            "Next candidate: T40B using only "
            "same-unit chronological wallet returns."
        )

    else:

        print()
        print(
            "⚠️ RETURN SCALE IS FIXED, BUT +60s "
            "SWAP COVERAGE IS LIMITED."
        )

        print(
            "Before T40B, improve the same-unit "
            "post-buy price construction without mixing USD/SOL."
        )

else:

    print(
        "⚠️ LARGE SAME-UNIT EXTREMES STILL EXIST."
    )

    print(
        "The USD/SOL mismatch was real, but it was "
        "not the only price-quality issue."
    )

    print(
        "Inspect section G/H before rebuilding T40."
    )


print()
print("IMPORTANT:")
print("• No dex_prices.price_usd is used for returns.")
print("• No hardcoded SOL/USD factor is used.")
print("• T41.3 writes nothing to the database.")
print("• T23/T31/T32 remain untouched.")
print("• This is a data-quality / repair audit, not a trading rule.")

db.close()
