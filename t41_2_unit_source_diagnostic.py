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
# LOAD DEX
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
# LOAD SWAPS
# ============================================================

swaps = db.execute("""
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
        program
    FROM swaps
    WHERE
        token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND token_delta IS NOT NULL
        AND sol_delta IS NOT NULL
        AND token_delta != 0
        AND sol_delta != 0
        AND clean_price IS NOT NULL
        AND clean_price > 0
    ORDER BY timestamp
""").fetchall()


records = []

for s in swaps:

    token_delta = abs(s["token_delta"])
    sol_delta = abs(s["sol_delta"])

    if token_delta <= 0 or sol_delta <= 0:
        continue

    reconstructed = sol_delta / token_delta

    d = nearest(
        s["token_mint"],
        s["timestamp"]
    )

    if not d:
        continue

    dex_price = d["price_usd"]

    rec = {
        "token": s["token_mint"],
        "wallet": s["wallet"],
        "side": s["side"],
        "program": s["program"],

        "token_delta": token_delta,
        "sol_delta": sol_delta,

        "reconstructed": reconstructed,
        "raw_price": s["raw_price"],
        "clean_price": s["clean_price"],
        "dex_price": dex_price,

        "delay": d["timestamp"] - s["timestamp"],
    }

    # ratios
    rec["raw_over_recon"] = (
        s["raw_price"] / reconstructed
        if valid(s["raw_price"]) and s["raw_price"] > 0
        else None
    )

    rec["clean_over_recon"] = (
        s["clean_price"] / reconstructed
        if valid(s["clean_price"]) and s["clean_price"] > 0
        else None
    )

    rec["dex_over_recon"] = (
        dex_price / reconstructed
        if valid(dex_price) and dex_price > 0
        else None
    )

    rec["dex_over_clean"] = (
        dex_price / s["clean_price"]
        if valid(s["clean_price"]) and s["clean_price"] > 0
        else None
    )

    records.append(rec)


print("=" * 150)
print("MEMECOIN LAB — T41.2 UNIT SOURCE DIAGNOSTIC")
print("=" * 150)

print(f"SWAPS CHECKED       : {len(swaps)}")
print(f"COMPARABLE RECORDS  : {len(records)}")

if not records:
    raise RuntimeError("No comparable records.")


# ============================================================
# A) RECONSTRUCTION CHECK
# ============================================================

print()
print("=" * 150)
print("A) PRICE RECONSTRUCTION FROM |SOL_DELTA| / |TOKEN_DELTA|")
print("=" * 150)

raw_ratios = [
    r["raw_over_recon"]
    for r in records
    if valid(r["raw_over_recon"])
]

clean_ratios = [
    r["clean_over_recon"]
    for r in records
    if valid(r["clean_over_recon"])
]

print(
    f"RAW / RECON   | N={len(raw_ratios)} "
    f"| MED={med(raw_ratios):.6f}x "
    f"| AVG={avg(raw_ratios):.6f}x"
)

print(
    f"CLEAN / RECON | N={len(clean_ratios)} "
    f"| MED={med(clean_ratios):.6f}x "
    f"| AVG={avg(clean_ratios):.6f}x"
)


# ============================================================
# B) DEX VS RECONSTRUCTED
# ============================================================

dex_recon = [
    r["dex_over_recon"]
    for r in records
    if valid(r["dex_over_recon"])
]

dex_clean = [
    r["dex_over_clean"]
    for r in records
    if valid(r["dex_over_clean"])
]

print()
print("=" * 150)
print("B) DEX PRICE VS RECONSTRUCTED / CLEAN")
print("=" * 150)

print(
    f"DEX / RECON   | MED={med(dex_recon):.4f}x "
    f"| AVG={avg(dex_recon):.4f}x"
)

print(
    f"DEX / CLEAN   | MED={med(dex_clean):.4f}x "
    f"| AVG={avg(dex_clean):.4f}x"
)


# ============================================================
# C) QUANTILES OF DEX / RECON
# ============================================================

print()
print("=" * 150)
print("C) DEX / RECON QUANTILES")
print("=" * 150)

vals = sorted(dex_recon)

for q in [0,1,5,10,25,50,75,90,95,99,100]:
    idx = int(round(
        (q / 100) * (len(vals) - 1)
    ))

    print(
        f"P{q:>3} = {vals[idx]:12.4f}x"
    )


# ============================================================
# D) PROGRAM BREAKDOWN
# ============================================================

print()
print("=" * 150)
print("D) SCALE BY PROGRAM")
print("=" * 150)

by_program = defaultdict(list)

for r in records:
    if valid(r["dex_over_recon"]):
        by_program[
            r["program"] or "NA"
        ].append(
            r["dex_over_recon"]
        )

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
# E) SIDE BREAKDOWN
# ============================================================

print()
print("=" * 150)
print("E) SCALE BY SIDE")
print("=" * 150)

by_side = defaultdict(list)

for r in records:
    if valid(r["dex_over_recon"]):
        by_side[r["side"]].append(
            r["dex_over_recon"]
        )

for side, vals in by_side.items():
    print(
        f"{side:6} "
        f"| N={len(vals):5d} "
        f"| MED={med(vals):10.3f}x "
        f"| AVG={avg(vals):10.3f}x"
    )


# ============================================================
# F) EXAMPLES
# ============================================================

print()
print("=" * 150)
print("F) EXAMPLES")
print("=" * 150)

scale = med(dex_recon)

examples = sorted(
    records,
    key=lambda r:
        abs(
            r["dex_over_recon"] - scale
        )
        if valid(r["dex_over_recon"])
        else 999999
)

for r in examples[:20]:

    print(
        f"SIDE={r['side']:4} "
        f"| PROGRAM={str(r['program']):8} "
        f"| RECON={r['reconstructed']:.8g} "
        f"| RAW={r['raw_price']:.8g} "
        f"| CLEAN={r['clean_price']:.8g} "
        f"| DEX={r['dex_price']:.8g} "
        f"| DEX/RECON={r['dex_over_recon']:.3f}x "
        f"| TOKEN={r['token'][:20]}"
    )


# ============================================================
# G) SOL/USD-LIKE FACTOR AUDIT
# ============================================================

print()
print("=" * 150)
print("G) POSSIBLE QUOTE-CURRENCY CONVERSION FACTOR")
print("=" * 150)

m = med(dex_recon)

print(
    f"MEDIAN DEX / RECON FACTOR = {m:.4f}x"
)

if 20 <= m <= 500:
    print(
        "Factor magnitude is compatible with a quote-currency "
        "conversion-type mismatch (for example token/SOL vs token/USD)."
    )
else:
    print(
        "Factor magnitude is not obviously consistent with a "
        "simple quote-currency conversion."
    )


# ============================================================
# H) DECISION SUPPORT
# ============================================================

raw_close = [
    abs(x - 1)
    for x in raw_ratios
]

clean_close = [
    abs(x - 1)
    for x in clean_ratios
]

raw_reconstructed_ok = (
    med(raw_close) is not None
    and med(raw_close) < 0.001
)

clean_reconstructed_ok = (
    med(clean_close) is not None
    and med(clean_close) < 0.001
)


print()
print("=" * 150)
print("H) DECISION SUPPORT")
print("=" * 150)

if raw_reconstructed_ok and clean_reconstructed_ok:

    print(
        "✅ SWAP RAW/CLEAN PRICE IS CONSISTENT WITH SOL_DELTA/TOKEN_DELTA"
    )

    print(
        "This means swaps.clean_price is effectively a TOKEN-IN-SOL style price."
    )

    print()

    print(
        f"DEX price is ~{med(dex_recon):.3f}x larger."
    )

    print(
        "Most likely interpretation: dex_prices.price_usd is TOKEN-IN-USD "
        "while swaps.clean_price is TOKEN-IN-SOL."
    )

    print()

    print(
        "Do NOT compare them directly for returns."
    )

    print(
        "For wallet post-buy returns, use the SAME price family "
        "at entry and exit."
    )

elif clean_reconstructed_ok:

    print(
        "✅ CLEAN PRICE matches reconstructed swap economics."
    )

    print(
        "RAW/CLEAN handling may differ, but the DEX mismatch remains external."
    )

else:

    print(
        "⚠️ SWAP PRICE DOES NOT CLEANLY MATCH SOL_DELTA/TOKEN_DELTA."
    )

    print(
        "Need deeper decimal/program-specific audit before repair."
    )


print()
print("IMPORTANT:")
print("• T41.2 writes nothing to DB.")
print("• T23/T31/T32 remain untouched.")
print("• Do not apply a hardcoded correction factor.")
print("• Repair should use same-unit prices at entry and exit.")

db.close()
