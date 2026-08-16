import sqlite3
import statistics
import math

DB = "validation_v090.db"

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row

# ============================================================
# COST SCENARIOS
#
# All-in round-trip assumptions in %
# These are intentionally simple stress tests.
# ============================================================

SCENARIOS = {
    "LIGHT": 0.50,
    "MEDIUM": 1.00,
    "HEAVY": 2.00,
    "UGLY": 4.00,
}

HORIZONS = [10, 20, 30, 60]

# ============================================================
# HELPERS
# ============================================================

def percentile(vals, p):
    vals = sorted(vals)

    if not vals:
        return None

    k = (len(vals) - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return vals[lo] * (hi-k) + vals[hi] * (k-lo)


def summary(vals):
    vals = [
        x for x in vals
        if x is not None
        and math.isfinite(x)
    ]

    if not vals:
        return None

    return {
        "n": len(vals),
        "avg": statistics.mean(vals),
        "med": statistics.median(vals),
        "win": 100 * sum(x > 0 for x in vals) / len(vals),
        "p10": percentile(vals, .10),
        "p90": percentile(vals, .90),
        "worst": min(vals),
        "best": max(vals),
    }


def report(name, rows):
    print()
    print(name)
    print("-" * 110)

    print(
        f"EVENTS={len(rows)} | "
        f"TOKENS={len(set(r['token_mint'] for r in rows))}"
    )

    for h in HORIZONS:
        col = f"dex_return_{h}s"

        gross = [
            r[col]
            for r in rows
            if r[col] is not None
        ]

        if not gross:
            print(f"{h:>3}s | N=0")
            continue

        g = summary(gross)

        print(
            f"{h:>3}s GROSS | "
            f"N={g['n']:>3} | "
            f"AVG={g['avg']:+7.2f}% | "
            f"MED={g['med']:+7.2f}% | "
            f"WIN={g['win']:5.1f}%"
        )

        for label, cost in SCENARIOS.items():
            net = [
                x - cost
                for x in gross
            ]

            s = summary(net)

            print(
                f"     {label:6} | "
                f"COST={cost:>4.1f}% | "
                f"AVG={s['avg']:+7.2f}% | "
                f"MED={s['med']:+7.2f}% | "
                f"WIN={s['win']:5.1f}% | "
                f"P10={s['p10']:+7.2f}%"
            )


# ============================================================
# LOAD FIRST DEX SNAPSHOT
# ============================================================

rows = db.execute("""
WITH first_dex AS (
    SELECT d.*
    FROM dex_prices d
    JOIN (
        SELECT
            event_id,
            MIN(timestamp) AS first_time
        FROM dex_prices
        GROUP BY event_id
    ) x
    ON d.event_id = x.event_id
    AND d.timestamp = x.first_time
)

SELECT
    e.*,
    d.liquidity_usd,
    d.market_cap,
    d.fdv,
    d.volume_m5,
    d.buys_m5,
    d.sells_m5,

    CASE
        WHEN (
            COALESCE(d.buys_m5,0)
            + COALESCE(d.sells_m5,0)
        ) > 0
        THEN
            1.0 * COALESCE(d.buys_m5,0)
            /
            (
                COALESCE(d.buys_m5,0)
                + COALESCE(d.sells_m5,0)
            )
        ELSE NULL
    END AS dex_buy_ratio

FROM events e
LEFT JOIN first_dex d
ON d.event_id = e.id

WHERE
    e.fa95 = 1
    AND e.dex_return_60s IS NOT NULL

ORDER BY e.timestamp
""").fetchall()

print()
print("=" * 110)
print("MEMECOIN LAB — V1.6 EXECUTION / SLIPPAGE ANALYZER")
print("=" * 110)

print(f"FA95 EVENTS : {len(rows)}")
print(
    f"TOKENS      : "
    f"{len(set(r['token_mint'] for r in rows))}"
)

if len(rows) < 10:
    print("Pas assez de données.")
    raise SystemExit

# ============================================================
# BASE
# ============================================================

report(
    "FA95 — ALL",
    rows
)

# ============================================================
# LIQUIDITY BUCKETS
# ============================================================

liq_vals = [
    r["liquidity_usd"]
    for r in rows
    if r["liquidity_usd"] is not None
]

if liq_vals:
    liq25 = percentile(liq_vals, .25)
    liq50 = percentile(liq_vals, .50)
    liq75 = percentile(liq_vals, .75)

    print()
    print("=" * 110)
    print("LIQUIDITY BUCKETS")
    print("=" * 110)

    print(
        f"P25=${liq25:,.0f} | "
        f"P50=${liq50:,.0f} | "
        f"P75=${liq75:,.0f}"
    )

    buckets = [
        (
            "LOW LIQ",
            [
                r for r in rows
                if (
                    r["liquidity_usd"] is not None
                    and r["liquidity_usd"] < liq25
                )
            ]
        ),
        (
            "MID-LOW LIQ",
            [
                r for r in rows
                if (
                    r["liquidity_usd"] is not None
                    and liq25 <= r["liquidity_usd"] < liq50
                )
            ]
        ),
        (
            "MID-HIGH LIQ",
            [
                r for r in rows
                if (
                    r["liquidity_usd"] is not None
                    and liq50 <= r["liquidity_usd"] < liq75
                )
            ]
        ),
        (
            "HIGH LIQ",
            [
                r for r in rows
                if (
                    r["liquidity_usd"] is not None
                    and r["liquidity_usd"] >= liq75
                )
            ]
        ),
    ]

    for name, subset in buckets:
        if len(subset) >= 3:
            report(name, subset)

# ============================================================
# MARKET CAP BUCKETS
# ============================================================

mc_vals = [
    r["market_cap"]
    for r in rows
    if r["market_cap"] is not None
]

if mc_vals:
    mc50 = percentile(mc_vals, .50)

    print()
    print("=" * 110)
    print("MARKET CAP SPLIT")
    print("=" * 110)

    print(
        f"MEDIAN MARKET CAP = "
        f"${mc50:,.0f}"
    )

    report(
        "MCAP BELOW MEDIAN",
        [
            r for r in rows
            if (
                r["market_cap"] is not None
                and r["market_cap"] < mc50
            )
        ]
    )

    report(
        "MCAP ABOVE MEDIAN",
        [
            r for r in rows
            if (
                r["market_cap"] is not None
                and r["market_cap"] >= mc50
            )
        ]
    )

# ============================================================
# VOLUME FILTER
# ============================================================

vol_vals = [
    r["volume_m5"]
    for r in rows
    if r["volume_m5"] is not None
]

if vol_vals:
    vol50 = percentile(vol_vals, .50)

    print()
    print("=" * 110)
    print("DEX M5 VOLUME SPLIT")
    print("=" * 110)

    print(
        f"MEDIAN VOLUME M5 = "
        f"${vol50:,.0f}"
    )

    report(
        "VOLUME HIGH",
        [
            r for r in rows
            if (
                r["volume_m5"] is not None
                and r["volume_m5"] >= vol50
            )
        ]
    )

    report(
        "VOLUME LOW",
        [
            r for r in rows
            if (
                r["volume_m5"] is not None
                and r["volume_m5"] < vol50
            )
        ]
    )

# ============================================================
# NEW30 + VOLUME — OUR V2 IDEA
# ============================================================

combo = [
    r for r in rows
    if (
        r["new_wallets30"] is not None
        and r["new_wallets30"] >= 2
        and r["volume_m5"] is not None
        and r["volume_m5"] >= 8837.925
    )
]

print()
print("=" * 110)
print("V2 CANDIDATE — FA95 + NEW30>=2 + VOLUME_M5>=8837.925")
print("=" * 110)

report(
    "V2 CANDIDATE",
    combo
)

# ============================================================
# CRASH RISK
# ============================================================

print()
print("=" * 110)
print("CRASH RISK @ 60s")
print("=" * 110)

r60 = [
    r["dex_return_60s"]
    for r in rows
]

for threshold in [
    -2,
    -5,
    -10,
    -20,
]:

    n = sum(
        x <= threshold
        for x in r60
    )

    print(
        f"R60 <= {threshold:>4}% : "
        f"{n}/{len(r60)} "
        f"({100*n/len(r60):.1f}%)"
    )

# ============================================================
# SIMPLE POSITION-SIZE STRESS
# ============================================================

print()
print("=" * 110)
print("POSITION SIZE STRESS EXAMPLE")
print("=" * 110)

print(
    "If portfolio risk allocated to one event is:"
)

for risk_fraction in [
    0.25,
    0.50,
    1.00,
    2.00,
]:

    worst = min(r60)

    portfolio_hit = (
        abs(worst)
        / 100
        * risk_fraction
    )

    print(
        f"Position={risk_fraction:>4.2f}% portfolio "
        f"| worst observed token move "
        f"{worst:+.2f}% "
        f"| portfolio hit ≈ "
        f"{portfolio_hit:.3f}%"
    )

print()
print("=" * 110)
print("DONE")
print("=" * 110)

print(
    "Reminder: this is a stress test, not a real slippage model."
)

db.close()
