import sqlite3
import statistics
import math
from collections import defaultdict, Counter

DB = "validation_v090.db"

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")

# ============================================================
# HELPERS
# ============================================================

def median(vals):
    vals = [x for x in vals if x is not None]
    return statistics.median(vals) if vals else None


def mean(vals):
    vals = [x for x in vals if x is not None]
    return statistics.mean(vals) if vals else None


def percentile(vals, p):
    vals = sorted(x for x in vals if x is not None)

    if not vals:
        return None

    k = (len(vals)-1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return (
        vals[lo] * (hi-k)
        + vals[hi] * (k-lo)
    )


def fmt(x):
    if x is None:
        return "NA"
    return f"{x:+.4f}"


def pct(x):
    if x is None:
        return "NA"
    return f"{x:+.2f}%"


def outcome_report(name, rows):
    print()
    print(name)
    print("-" * 105)
    print(
        f"N={len(rows)} | "
        f"TOKENS={len(set(r['token_mint'] for r in rows))}"
    )

    for h in [10,20,30,60,300]:
        col = f"dex_return_{h}s"

        vals = [
            r[col]
            for r in rows
            if r[col] is not None
        ]

        if not vals:
            print(f"{h:>3}s | N=0")
            continue

        print(
            f"{h:>3}s | "
            f"N={len(vals):>3} | "
            f"AVG={statistics.mean(vals):+7.2f}% | "
            f"MED={statistics.median(vals):+7.2f}% | "
            f"WIN={100*sum(x>0 for x in vals)/len(vals):5.1f}% | "
            f"P10={percentile(vals,.10):+7.2f}% | "
            f"P90={percentile(vals,.90):+7.2f}%"
        )


# ============================================================
# LOAD FA95 + FIRST DEX SNAPSHOT
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

ORDER BY e.timestamp ASC
""").fetchall()

print()
print("=" * 105)
print("MEMECOIN LAB — FA95 WINNER / LOSER DIAGNOSTIC V1.4")
print("=" * 105)

print(f"FA95 USABLE 60s : {len(rows)}")
print(
    f"UNIQUE TOKENS   : "
    f"{len(set(r['token_mint'] for r in rows))}"
)

if len(rows) < 10:
    print("Pas assez de FA95.")
    raise SystemExit


# ============================================================
# GROUPS
# ============================================================

winners = [
    r for r in rows
    if r["dex_return_60s"] > 0
]

losers = [
    r for r in rows
    if r["dex_return_60s"] <= 0
]

strong_winners = [
    r for r in rows
    if r["dex_return_60s"] >= 5
]

big_losers = [
    r for r in rows
    if r["dex_return_60s"] <= -10
]


print()
print("=" * 105)
print("1. GROUP COUNTS")
print("=" * 105)

print(
    f"WINNERS       : {len(winners)}"
)

print(
    f"LOSERS        : {len(losers)}"
)

print(
    f"STRONG +5%    : {len(strong_winners)}"
)

print(
    f"CRASH <= -10% : {len(big_losers)}"
)


# ============================================================
# FEATURE COMPARISON
# ============================================================

features = [
    ("FA", "fa"),
    ("NET FLOW 30", "nf30"),
    ("IMBALANCE", "imbalance30"),

    ("NEW WALLETS 10", "new_wallets10"),
    ("NEW WALLETS 30", "new_wallets30"),

    ("WALLET GROWTH", "wallet_growth"),
    ("BUYER GROWTH", "buyer_growth"),

    ("BUY CONCENTRATION", "buy_concentration30"),

    ("BUYERS 5", "buyers5"),
    ("BUYERS 10", "buyers10"),
    ("BUYERS 30", "buyers30"),

    ("BUY VOLUME 30", "buy_volume30"),
    ("SELL VOLUME 30", "sell_volume30"),

    ("LIQUIDITY USD", "liquidity_usd"),
    ("MARKET CAP", "market_cap"),
    ("VOLUME M5", "volume_m5"),

    ("DEX BUYS M5", "buys_m5"),
    ("DEX SELLS M5", "sells_m5"),
    ("DEX BUY RATIO", "dex_buy_ratio"),
]


print()
print("=" * 105)
print("2. WINNERS VS LOSERS — MEDIANS")
print("=" * 105)

print(
    f"{'FEATURE':24} | "
    f"{'WINNER':>14} | "
    f"{'LOSER':>14} | "
    f"{'STRONG+5':>14} | "
    f"{'CRASH-10':>14}"
)

print("-" * 105)

for label, col in features:

    w = median(
        r[col] for r in winners
    )

    l = median(
        r[col] for r in losers
    )

    sw = median(
        r[col] for r in strong_winners
    )

    bl = median(
        r[col] for r in big_losers
    )

    print(
        f"{label:24} | "
        f"{str(round(w,4) if w is not None else 'NA'):>14} | "
        f"{str(round(l,4) if l is not None else 'NA'):>14} | "
        f"{str(round(sw,4) if sw is not None else 'NA'):>14} | "
        f"{str(round(bl,4) if bl is not None else 'NA'):>14}"
    )


# ============================================================
# QUANTILE TESTS
#
# EXPLORATORY ONLY.
# These thresholds are NOT for modifying live FA95 yet.
# ============================================================

print()
print("=" * 105)
print("3. SINGLE FEATURE SPLITS — EXPLORATORY")
print("=" * 105)

test_features = [
    "fa",
    "nf30",
    "new_wallets10",
    "new_wallets30",
    "wallet_growth",
    "buyer_growth",
    "buy_concentration30",
    "buy_volume30",
    "liquidity_usd",
    "volume_m5",
    "dex_buy_ratio",
]

thresholds = {}

for col in test_features:

    vals = [
        r[col]
        for r in rows
        if r[col] is not None
    ]

    if len(vals) < 8:
        continue

    q50 = percentile(vals, .50)
    q75 = percentile(vals, .75)

    thresholds[col] = {
        "p50": q50,
        "p75": q75
    }

    print()
    print(
        f">>> {col}"
        f" | P50={q50}"
        f" | P75={q75}"
    )

    high = [
        r for r in rows
        if (
            r[col] is not None
            and r[col] >= q50
        )
    ]

    low = [
        r for r in rows
        if (
            r[col] is not None
            and r[col] < q50
        )
    ]

    outcome_report(
        f"{col} >= P50",
        high
    )

    outcome_report(
        f"{col} < P50",
        low
    )


# ============================================================
# HAND-CHECKED COMBINATIONS
#
# Still exploratory.
# We keep minimum N to avoid being fooled by 1-2 trades.
# ============================================================

print()
print("=" * 105)
print("4. COMBINATION SEARCH")
print("=" * 105)

fa50 = thresholds.get(
    "fa", {}
).get("p50")

nf50 = thresholds.get(
    "nf30", {}
).get("p50")

nw10_50 = thresholds.get(
    "new_wallets10", {}
).get("p50")

wg50 = thresholds.get(
    "wallet_growth", {}
).get("p50")

bv50 = thresholds.get(
    "buy_volume30", {}
).get("p50")

liq50 = thresholds.get(
    "liquidity_usd", {}
).get("p50")

dexbr50 = thresholds.get(
    "dex_buy_ratio", {}
).get("p50")

conc50 = thresholds.get(
    "buy_concentration30", {}
).get("p50")


tests = []


def add_test(name, fn):

    selected = []

    for r in rows:

        try:
            if fn(r):
                selected.append(r)

        except (
            TypeError,
            KeyError
        ):
            pass

    if len(selected) >= 5:

        r60 = [
            r["dex_return_60s"]
            for r in selected
            if r["dex_return_60s"]
            is not None
        ]

        if not r60:
            return

        tests.append({
            "name": name,
            "rows": selected,
            "n": len(r60),
            "avg60":
                statistics.mean(r60),

            "med60":
                statistics.median(r60),

            "win60":
                100
                * sum(x > 0 for x in r60)
                / len(r60),

            "crash":
                100
                * sum(x <= -10 for x in r60)
                / len(r60)
        })


if fa50 is not None:

    add_test(
        "FA >= median",
        lambda r:
            r["fa"] >= fa50
    )


if nf50 is not None:

    add_test(
        "NF >= median",
        lambda r:
            r["nf30"] >= nf50
    )


if (
    fa50 is not None
    and nf50 is not None
):

    add_test(
        "FA high + NF high",
        lambda r:
            (
                r["fa"] >= fa50
                and r["nf30"] >= nf50
            )
    )


if nw10_50 is not None:

    add_test(
        "NEW10 high",
        lambda r:
            r["new_wallets10"]
            >= nw10_50
    )


if (
    fa50 is not None
    and nw10_50 is not None
):

    add_test(
        "FA high + NEW10 high",
        lambda r:
            (
                r["fa"] >= fa50
                and r["new_wallets10"]
                >= nw10_50
            )
    )


if (
    nf50 is not None
    and nw10_50 is not None
):

    add_test(
        "NF high + NEW10 high",
        lambda r:
            (
                r["nf30"] >= nf50
                and r["new_wallets10"]
                >= nw10_50
            )
    )


if (
    wg50 is not None
    and nw10_50 is not None
):

    add_test(
        "Wallet growth + NEW10",
        lambda r:
            (
                r["wallet_growth"] >= wg50
                and r["new_wallets10"]
                >= nw10_50
            )
    )


if bv50 is not None:

    add_test(
        "BuyVol >= median",
        lambda r:
            r["buy_volume30"]
            >= bv50
    )


if (
    fa50 is not None
    and bv50 is not None
):

    add_test(
        "FA high + BuyVol high",
        lambda r:
            (
                r["fa"] >= fa50
                and r["buy_volume30"]
                >= bv50
            )
    )


if liq50 is not None:

    add_test(
        "Liquidity >= median",
        lambda r:
            (
                r["liquidity_usd"]
                is not None
                and r["liquidity_usd"]
                >= liq50
            )
    )


if dexbr50 is not None:

    add_test(
        "DEX buy ratio high",
        lambda r:
            (
                r["dex_buy_ratio"]
                is not None
                and r["dex_buy_ratio"]
                >= dexbr50
            )
    )


if (
    dexbr50 is not None
    and nf50 is not None
):

    add_test(
        "NF high + DEX buy ratio high",
        lambda r:
            (
                r["nf30"] >= nf50
                and r["dex_buy_ratio"]
                is not None
                and r["dex_buy_ratio"]
                >= dexbr50
            )
    )


if conc50 is not None:

    add_test(
        "Concentration high",
        lambda r:
            r["buy_concentration30"]
            >= conc50
    )

    add_test(
        "Concentration low",
        lambda r:
            r["buy_concentration30"]
            < conc50
    )


tests.sort(
    key=lambda x: (
        x["med60"],
        x["win60"]
    ),
    reverse=True
)


print(
    f"{'FILTER':38} | "
    f"{'N':>3} | "
    f"{'AVG60':>9} | "
    f"{'MED60':>9} | "
    f"{'WIN':>7} | "
    f"{'CRASH':>7}"
)

print("-" * 105)

for t in tests:

    print(
        f"{t['name'][:38]:38} | "
        f"{t['n']:>3} | "
        f"{t['avg60']:+8.2f}% | "
        f"{t['med60']:+8.2f}% | "
        f"{t['win60']:6.1f}% | "
        f"{t['crash']:6.1f}%"
    )


# ============================================================
# TOKEN INDEPENDENCE
# ============================================================

print()
print("=" * 105)
print("5. TOKEN INDEPENDENCE")
print("=" * 105)

token_groups = defaultdict(list)

for r in rows:
    token_groups[
        r["token_mint"]
    ].append(
        r["dex_return_60s"]
    )

token_medians = []

for mint, values in token_groups.items():

    token_medians.append(
        statistics.median(values)
    )

print(
    f"TOKENS            : "
    f"{len(token_groups)}"
)

print(
    f"EVENTS/TOKEN MED  : "
    f"{statistics.median(len(v) for v in token_groups.values()):.1f}"
)

print(
    f"TOKEN MEDIAN R60  : "
    f"{statistics.median(token_medians):+.2f}%"
)

print(
    f"TOKENS POSITIVE   : "
    f"{sum(x>0 for x in token_medians)}/"
    f"{len(token_medians)} "
    f"({100*sum(x>0 for x in token_medians)/len(token_medians):.1f}%)"
)


# ============================================================
# INDIVIDUAL FA95 EVENTS
# ============================================================

print()
print("=" * 105)
print("6. INDIVIDUAL FA95 EVENTS")
print("=" * 105)

ranked = sorted(
    rows,
    key=lambda r:
        r["dex_return_60s"],
    reverse=True
)

for r in ranked:

    print(
        f"{r['token_mint'][:14]}... | "
        f"R30={pct(r['dex_return_30s'])} | "
        f"R60={pct(r['dex_return_60s'])} | "
        f"FA={r['fa']:+.3f} | "
        f"NF={r['nf30']:+.2f} | "
        f"NEW10={r['new_wallets10']} | "
        f"WG={r['wallet_growth']:+.2f} | "
        f"CONC={r['buy_concentration30']:.2f} | "
        f"LIQ={r['liquidity_usd'] if r['liquidity_usd'] is not None else 'NA'} | "
        f"DEXBR={round(r['dex_buy_ratio'],3) if r['dex_buy_ratio'] is not None else 'NA'}"
    )


print()
print("=" * 105)
print("IMPORTANT")
print("=" * 105)

print(
    "Cette analyse est EXPLORATOIRE."
)

print(
    "Ne modifie pas FA95 live sur la base de ces sous-groupes."
)

print(
    "On cherche une hypothese V2 a valider sur les FUTURS events."
)

print("=" * 105)

db.close()
