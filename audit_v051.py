import sqlite3
import statistics

DB = "memecoin_lab_sampler.db"

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

print()
print("=" * 100)
print("MEMECOIN LAB — DATA AUDIT V0.5.1")
print("=" * 100)

# ============================================================
# BASIC COUNTS
# ============================================================

swaps = db.execute("""
SELECT COUNT(*) FROM swaps
""").fetchone()[0]

tokens = db.execute("""
SELECT COUNT(DISTINCT token_mint)
FROM swaps
""").fetchone()[0]

snapshots = db.execute("""
SELECT COUNT(*)
FROM feature_snapshots
""").fetchone()[0]

print()
print(f"SWAPS     : {swaps:,}")
print(f"TOKENS    : {tokens:,}")
print(f"SNAPSHOTS : {snapshots:,}")


# ============================================================
# MOST ACTIVE TOKENS
# ============================================================

print()
print("=" * 100)
print("TOP 20 TOKENS BY SWAPS")
print("=" * 100)

rows = db.execute("""
SELECT
    token_mint,
    COUNT(*) AS n,
    SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) AS buys,
    SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) AS sells,
    COUNT(DISTINCT wallet) AS wallets,
    SUM(ABS(sol_delta)) AS sol_volume

FROM swaps

GROUP BY token_mint
ORDER BY n DESC
LIMIT 20
""").fetchall()

for r in rows:

    print(
        f"{r['token_mint']} | "
        f"N={r['n']:>4} | "
        f"B={r['buys']:>4} | "
        f"S={r['sells']:>4} | "
        f"W={r['wallets']:>4} | "
        f"SOL={r['sol_volume']:.3f}"
    )


# ============================================================
# RETURN DISTRIBUTIONS
# ============================================================

print()
print("=" * 100)
print("RETURN DISTRIBUTIONS")
print("=" * 100)

for horizon in [
    "return_10s",
    "return_30s",
    "return_60s",
    "return_300s"
]:

    vals = [
        r[0]
        for r in db.execute(
            f"""
            SELECT {horizon}
            FROM feature_snapshots
            WHERE {horizon} IS NOT NULL
            """
        )
    ]

    if not vals:
        continue

    vals = sorted(vals)

    def pct(p):

        i = int(
            (len(vals)-1) * p
        )

        return vals[i]

    print()
    print(horizon)

    print(
        f"N={len(vals)}"
        f" | MIN={min(vals):+.2f}%"
        f" | P01={pct(.01):+.2f}%"
        f" | P05={pct(.05):+.2f}%"
        f" | MED={statistics.median(vals):+.2f}%"
        f" | P95={pct(.95):+.2f}%"
        f" | P99={pct(.99):+.2f}%"
        f" | MAX={max(vals):+.2f}%"
    )


# ============================================================
# EXTREME RETURNS
# ============================================================

print()
print("=" * 100)
print("TOP EXTREME +60s RETURNS")
print("=" * 100)

rows = db.execute("""
SELECT
    token_mint,
    timestamp,
    last_price,
    return_10s,
    return_30s,
    return_60s,
    return_300s

FROM feature_snapshots

WHERE return_60s IS NOT NULL

ORDER BY return_60s DESC

LIMIT 20
""").fetchall()

for r in rows:

    print(
        f"{r['token_mint']} | "
        f"PRICE={r['last_price']:.12g} | "
        f"R10={r['return_10s']} | "
        f"R30={r['return_30s']} | "
        f"R60={r['return_60s']:+.2f}% | "
        f"R300={r['return_300s']}"
    )


print()
print("=" * 100)
print("WORST EXTREME +60s RETURNS")
print("=" * 100)

rows = db.execute("""
SELECT
    token_mint,
    timestamp,
    last_price,
    return_10s,
    return_30s,
    return_60s,
    return_300s

FROM feature_snapshots

WHERE return_60s IS NOT NULL

ORDER BY return_60s ASC

LIMIT 20
""").fetchall()

for r in rows:

    print(
        f"{r['token_mint']} | "
        f"PRICE={r['last_price']:.12g} | "
        f"R10={r['return_10s']} | "
        f"R30={r['return_30s']} | "
        f"R60={r['return_60s']:+.2f}% | "
        f"R300={r['return_300s']}"
    )


# ============================================================
# SUSPICIOUS TOKENS
# ============================================================

print()
print("=" * 100)
print("TOKENS RESPONSIBLE FOR >100% RETURNS")
print("=" * 100)

rows = db.execute("""
SELECT
    token_mint,
    COUNT(*) AS extreme_count,
    MAX(return_60s) AS max_return,
    AVG(return_60s) AS avg_return

FROM feature_snapshots

WHERE return_60s > 100

GROUP BY token_mint

ORDER BY extreme_count DESC
""").fetchall()

if not rows:

    print("NONE")

else:

    for r in rows:

        print(
            f"{r['token_mint']} | "
            f"EXTREMES={r['extreme_count']} | "
            f"MAX={r['max_return']:+.2f}% | "
            f"AVG={r['avg_return']:+.2f}%"
        )


# ============================================================
# ROBUST SIGNAL CHECK
# Remove absurd observations temporarily.
# NOT a final trading filter.
# ============================================================

print()
print("=" * 100)
print("ROBUST 60s TEST — RETURNS CLIPPED TO [-90%, +100%]")
print("=" * 100)

rows = db.execute("""
SELECT *
FROM feature_snapshots
WHERE
    return_60s IS NOT NULL
    AND return_60s BETWEEN -90 AND 100
""").fetchall()

if not rows:

    print("NO DATA")
    raise SystemExit

fa = sorted(
    r["flow_accel_fast"]
    for r in rows
)

nf = sorted(
    r["net_flow_30"]
    for r in rows
)

imb = sorted(
    r["imbalance_30"]
    for r in rows
)

def percentile(x, p):

    return x[
        int((len(x)-1)*p)
    ]

fa90 = percentile(
    fa,
    .90
)

nf75 = percentile(
    nf,
    .75
)

imb75 = percentile(
    imb,
    .75
)

tests = {
    "BASELINE":
        lambda r: True,

    "FA>P90":
        lambda r:
            r["flow_accel_fast"] >= fa90,

    "NETFLOW>P75":
        lambda r:
            r["net_flow_30"] >= nf75,

    "IMBALANCE>P75":
        lambda r:
            r["imbalance_30"] >= imb75,
}

for name, fn in tests.items():

    selected = [
        r["return_60s"]
        for r in rows
        if fn(r)
    ]

    if not selected:
        continue

    print(
        f"{name:16} | "
        f"N={len(selected):4} | "
        f"AVG={statistics.mean(selected):+8.3f}% | "
        f"MED={statistics.median(selected):+8.3f}% | "
        f"WIN={100*sum(x>0 for x in selected)/len(selected):5.1f}%"
    )

print()
print("=" * 100)
print("AUDIT DONE")
print("=" * 100)

db.close()
