#!/usr/bin/env python3

import sqlite3
import statistics
from pathlib import Path
from collections import Counter

ROOT = Path.home() / "memecoin_lab"
DB = ROOT / "validation_v090.db"

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30,
)

db.row_factory = sqlite3.Row


# ============================================================
# HELPERS
# ============================================================

def cols(table):
    return {
        r["name"]
        for r in db.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()
    }


def pct(n, total):
    return (
        100.0 * n / total
        if total
        else 0.0
    )


def percentile(values, q):
    values = sorted(values)

    if not values:
        return None

    i = int(
        round(
            (len(values) - 1) * q
        )
    )

    return values[i]


def fmt(x):
    if x is None:
        return "NA"

    return f"{x:,.2f}"


# ============================================================
# DISCOVER SCHEMA
# ============================================================

TABLE = "t116_pump_swaps"
C = cols(TABLE)

ALIASES = {
    "mint": [
        "token_mint",
        "mint",
    ],

    "ts": [
        "timestamp",
        "block_time",
        "created_at",
        "ts",
    ],

    "wallet": [
        "wallet",
        "trader",
        "owner",
        "signer",
        "fee_payer",
    ],

    "signature": [
        "signature",
        "tx_signature",
        "txid",
    ],

    "price": [
        "raw_price_sol",
        "raw_price",
        "price_sol",
        "price",
    ],

    "side": [
        "side",
        "direction",
    ],

    "sol": [
        "sol_delta",
        "sol_amount",
        "sol",
    ],
}


def pick(name):
    for candidate in ALIASES[name]:
        if candidate in C:
            return candidate

    return None


M = {
    name: pick(name)
    for name in ALIASES
}


print("=" * 130)
print("MEMECOIN LAB — V4 DATA COVERAGE AUDIT")
print("=" * 130)

print()
print("SOURCE COLUMN MAPPING")
print("-" * 90)

for k, v in M.items():
    print(
        f"{k:<12} -> {v or 'MISSING'}"
    )


if not M["mint"] or not M["ts"]:
    raise SystemExit(
        "Cannot audit without mint + timestamp."
    )


# ============================================================
# RAW TOTALS
# ============================================================

mint = M["mint"]
ts = M["ts"]

total_rows = db.execute(
    f"SELECT COUNT(*) FROM {TABLE}"
).fetchone()[0]

total_tokens = db.execute(
    f"""
    SELECT COUNT(DISTINCT {mint})
    FROM {TABLE}
    WHERE {mint} IS NOT NULL
    """
).fetchone()[0]


print()
print("=" * 130)
print("GLOBAL COVERAGE")
print("=" * 130)

print(
    f"RAW SWAP ROWS      : {total_rows:,}"
)

print(
    f"UNIQUE TOKENS      : {total_tokens:,}"
)

print(
    f"SWAPS / TOKEN AVG  : "
    f"{total_rows / total_tokens if total_tokens else 0:.2f}"
)


# ============================================================
# TOKEN-LEVEL COVERAGE
# ============================================================

rows = db.execute(
    f"""
    SELECT
        {mint} AS token_mint,

        COUNT(*) AS swaps,

        MIN({ts}) AS first_ts,
        MAX({ts}) AS last_ts,

        MAX({ts}) - MIN({ts}) AS duration_s

    FROM {TABLE}

    WHERE
        {mint} IS NOT NULL
        AND {ts} IS NOT NULL

    GROUP BY {mint}
    """
).fetchall()


swap_counts = [
    int(r["swaps"])
    for r in rows
]

durations = [
    float(r["duration_s"])
    for r in rows
    if r["duration_s"] is not None
]


print()
print("=" * 130)
print("SWAPS PER TOKEN")
print("=" * 130)


buckets = [
    ("1",       lambda n: n == 1),
    ("2-5",     lambda n: 2 <= n <= 5),
    ("6-20",    lambda n: 6 <= n <= 20),
    ("21-100",  lambda n: 21 <= n <= 100),
    ("101+",    lambda n: n >= 101),
]


for label, fn in buckets:
    n = sum(
        fn(x)
        for x in swap_counts
    )

    print(
        f"{label:<12}"
        f"{n:>7,}"
        f"   {pct(n, len(swap_counts)):>6.2f}%"
    )


print()
print("SWAP COUNT DISTRIBUTION")
print("-" * 90)

for q in [
    0.00,
    0.10,
    0.25,
    0.50,
    0.75,
    0.90,
    0.95,
    0.99,
    1.00,
]:
    print(
        f"P{int(q*100):>3} "
        f"{percentile(swap_counts, q):>10,}"
    )


# ============================================================
# DURATION
# ============================================================

print()
print("=" * 130)
print("OBSERVED TOKEN LIFETIME")
print("=" * 130)


for q in [
    0.00,
    0.10,
    0.25,
    0.50,
    0.75,
    0.90,
    0.95,
    0.99,
    1.00,
]:
    value = percentile(
        durations,
        q
    )

    print(
        f"P{int(q*100):>3} "
        f"{fmt(value):>14} sec"
    )


duration_buckets = [
    ("0 sec",       lambda x: x == 0),
    ("< 10 sec",    lambda x: 0 < x < 10),
    ("10-60 sec",   lambda x: 10 <= x < 60),
    ("1-5 min",     lambda x: 60 <= x < 300),
    ("5-30 min",    lambda x: 300 <= x < 1800),
    ("30m-2h",      lambda x: 1800 <= x < 7200),
    ("2h+",         lambda x: x >= 7200),
]


print()
print("DURATION BUCKETS")
print("-" * 90)

for label, fn in duration_buckets:
    n = sum(
        fn(x)
        for x in durations
    )

    print(
        f"{label:<14}"
        f"{n:>7,}"
        f"   {pct(n, len(durations)):>6.2f}%"
    )


# ============================================================
# FIELD COMPLETENESS
# ============================================================

print()
print("=" * 130)
print("FIELD COMPLETENESS")
print("=" * 130)


for logical in [
    "wallet",
    "signature",
    "price",
    "side",
    "sol",
]:

    col = M[logical]

    if not col:
        print(
            f"{logical:<15} MISSING"
        )
        continue

    nonnull = db.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE {col} IS NOT NULL
        """
    ).fetchone()[0]

    print(
        f"{logical:<15}"
        f"{nonnull:>8,}"
        f"/{total_rows:,}"
        f"   {pct(nonnull,total_rows):>6.2f}%"
    )


# ============================================================
# UNIQUE WALLETS
# ============================================================

if M["wallet"]:

    wallet = M["wallet"]

    unique_wallets = db.execute(
        f"""
        SELECT COUNT(DISTINCT {wallet})
        FROM {TABLE}
        WHERE {wallet} IS NOT NULL
        """
    ).fetchone()[0]


    recurring_wallets = db.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {wallet}
            FROM {TABLE}
            WHERE {wallet} IS NOT NULL
            GROUP BY {wallet}
            HAVING COUNT(DISTINCT {mint}) > 1
        )
        """
    ).fetchone()[0]


    print()
    print("=" * 130)
    print("WALLET COVERAGE")
    print("=" * 130)

    print(
        f"UNIQUE WALLETS     : {unique_wallets:,}"
    )

    print(
        f"MULTI-TOKEN WALLETS: {recurring_wallets:,}"
    )

    print(
        f"RECURRING RATE     : "
        f"{pct(recurring_wallets, unique_wallets):.2f}%"
    )


# ============================================================
# TEMPORAL GAPS
# ============================================================

print()
print("=" * 130)
print("INTER-SWAP GAP AUDIT")
print("=" * 130)


all_gaps = []

tokens_with_gap_60 = 0
tokens_with_gap_300 = 0


for row in rows:

    token = row["token_mint"]

    timestamps = [
        x[0]
        for x in db.execute(
            f"""
            SELECT {ts}
            FROM {TABLE}
            WHERE {mint}=?
              AND {ts} IS NOT NULL
            ORDER BY {ts}
            """,
            (token,)
        ).fetchall()
    ]

    if len(timestamps) < 2:
        continue

    gaps = [
        float(b) - float(a)
        for a, b in zip(
            timestamps,
            timestamps[1:]
        )
    ]

    gaps = [
        g
        for g in gaps
        if g >= 0
    ]

    if not gaps:
        continue

    all_gaps.extend(gaps)

    if max(gaps) >= 60:
        tokens_with_gap_60 += 1

    if max(gaps) >= 300:
        tokens_with_gap_300 += 1


if all_gaps:

    for q in [
        0.50,
        0.75,
        0.90,
        0.95,
        0.99,
        1.00,
    ]:

        print(
            f"GAP P{int(q*100):>3} "
            f"{fmt(percentile(all_gaps,q)):>12} sec"
        )


print()

print(
    f"TOKENS WITH >=60s GAP  : "
    f"{tokens_with_gap_60:,}"
)

print(
    f"TOKENS WITH >=300s GAP : "
    f"{tokens_with_gap_300:,}"
)


# ============================================================
# HIGH COVERAGE TOKENS
# ============================================================

print()
print("=" * 130)
print("POTENTIALLY RESEARCH-GRADE TRAJECTORIES")
print("=" * 130)


criteria = [
    (
        ">= 5 swaps",
        lambda r:
            r["swaps"] >= 5
    ),

    (
        ">= 10 swaps",
        lambda r:
            r["swaps"] >= 10
    ),

    (
        ">= 20 swaps",
        lambda r:
            r["swaps"] >= 20
    ),

    (
        ">= 5 swaps + >=60s",
        lambda r:
            r["swaps"] >= 5
            and r["duration_s"] >= 60
    ),

    (
        ">=10 swaps + >=120s",
        lambda r:
            r["swaps"] >= 10
            and r["duration_s"] >= 120
    ),

    (
        ">=20 swaps + >=300s",
        lambda r:
            r["swaps"] >= 20
            and r["duration_s"] >= 300
    ),
]


for label, fn in criteria:

    n = sum(
        fn(r)
        for r in rows
    )

    print(
        f"{label:<25}"
        f"{n:>7,}"
        f"   {pct(n,len(rows)):>6.2f}%"
    )


# ============================================================
# TOP TOKENS
# ============================================================

print()
print("=" * 130)
print("TOP 15 MOST OBSERVED TOKENS")
print("=" * 130)


top = sorted(
    rows,
    key=lambda r: (
        r["swaps"],
        r["duration_s"] or 0
    ),
    reverse=True
)[:15]


for i, r in enumerate(
    top,
    1
):

    token = str(
        r["token_mint"]
    )

    print(
        f"#{i:02d} "
        f"{token[:16]:<18}"
        f" swaps={r['swaps']:>6}"
        f" duration={fmt(r['duration_s']):>12}s"
    )


db.close()


print()
print("=" * 130)
print("AUDIT COMPLETE")
print("=" * 130)

print("""
INTERPRETATION TARGET:

A) If most tokens have 1-5 swaps and near-zero duration:
   current DB is an EVENT SAMPLE, not exhaustive lifecycle data.

B) If substantial numbers have 20-100+ swaps over minutes/hours:
   we already possess useful transaction-level trajectories.

C) Wallet completeness + recurring wallets determines whether
   wallet-history / smart-wallet / clustering research is viable.

D) The next collector will be designed from these results.
""")
