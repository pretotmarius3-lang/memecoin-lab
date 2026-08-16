import sqlite3
import statistics
import math
from collections import Counter, defaultdict

DB = "validation_v090.db"

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row

HORIZONS = [5, 10, 20, 30, 60, 300]


# ============================================================
# HELPERS
# ============================================================

def percentile(vals, p):
    vals = sorted(vals)

    if not vals:
        return None

    k = (len(vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return vals[f]

    return (
        vals[f] * (c-k)
        + vals[c] * (k-f)
    )


def fmt(x, digits=2):
    if x is None:
        return "NA"

    return f"{x:+.{digits}f}%"


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
        "p25": percentile(vals, .25),
        "p75": percentile(vals, .75),
        "p90": percentile(vals, .90),
        "min": min(vals),
        "max": max(vals),
    }


def print_summary(name, rows):
    print()
    print(name)
    print("-" * 110)

    print(
        f"EVENTS={len(rows):,} | "
        f"TOKENS={len(set(r['token_mint'] for r in rows)):,}"
    )

    for h in HORIZONS:
        col = f"dex_return_{h}s"

        vals = [
            r[col]
            for r in rows
            if r[col] is not None
        ]

        s = summary(vals)

        if not s:
            print(
                f"{h:>3}s | N=0"
            )
            continue

        print(
            f"{h:>3}s | "
            f"N={s['n']:>5} | "
            f"AVG={s['avg']:+8.2f}% | "
            f"MED={s['med']:+8.2f}% | "
            f"WIN={s['win']:5.1f}% | "
            f"P10={s['p10']:+8.2f}% | "
            f"P90={s['p90']:+8.2f}%"
        )


def split_quantile(rows, column, p=.5):

    vals = [
        r[column]
        for r in rows
        if r[column] is not None
    ]

    if not vals:
        return None, [], []

    threshold = percentile(
        vals,
        p
    )

    low = [
        r for r in rows
        if (
            r[column] is not None
            and r[column] < threshold
        )
    ]

    high = [
        r for r in rows
        if (
            r[column] is not None
            and r[column] >= threshold
        )
    ]

    return threshold, low, high


# ============================================================
# LOAD
# ============================================================

events = db.execute("""
SELECT *
FROM events
ORDER BY timestamp ASC
""").fetchall()

dex_points = db.execute("""
SELECT COUNT(*)
FROM dex_prices
""").fetchone()[0]

dex_events = db.execute("""
SELECT COUNT(DISTINCT event_id)
FROM dex_prices
""").fetchone()[0]

print()
print("=" * 110)
print("MEMECOIN LAB — V1.2 FULL DATA ANALYSIS")
print("=" * 110)

print(
    f"EVENTS            : {len(events):,}"
)

print(
    f"UNIQUE TOKENS     : "
    f"{len(set(r['token_mint'] for r in events)):,}"
)

print(
    f"DEX PRICE POINTS  : {dex_points:,}"
)

print(
    f"DEX TRACKED EVENTS: {dex_events:,}"
)

if not events:
    print("AUCUNE DATA.")
    raise SystemExit


# ============================================================
# COVERAGE
# ============================================================

print()
print("=" * 110)
print("1. DEX OUTCOME COVERAGE")
print("=" * 110)

for h in HORIZONS:

    available = sum(
        r[f"dex_return_{h}s"] is not None
        for r in events
    )

    coverage = (
        100 * available / len(events)
    )

    delays = [
        r[f"dex_delay_{h}s"]
        for r in events
        if r[f"dex_delay_{h}s"] is not None
    ]

    med_delay = (
        statistics.median(delays)
        if delays
        else None
    )

    print(
        f"{h:>3}s | "
        f"{available:>5}/{len(events):<5} | "
        f"COVERAGE={coverage:6.2f}% | "
        f"MED_DELAY="
        f"{med_delay if med_delay is not None else 'NA'}"
    )


# ============================================================
# ALL EVENTS
# ============================================================

print()
print("=" * 110)
print("2. ALL SIGNAL EVENTS")
print("=" * 110)

print_summary(
    "ALL EVENTS",
    events
)


# ============================================================
# SIGNAL FLAGS
# ============================================================

print()
print("=" * 110)
print("3. SIGNAL COMPARISON")
print("=" * 110)

signals = [
    ("FA90", "fa90"),
    ("FA95", "fa95"),
    ("FPA", "fpa"),
    ("EXTREME", "extreme"),
]

for name, column in signals:

    selected = [
        r for r in events
        if r[column] == 1
    ]

    print_summary(
        name,
        selected
    )


# ============================================================
# FA REGIMES
# ============================================================

print()
print("=" * 110)
print("4. FLOW REGIME")
print("=" * 110)

regimes = sorted(
    set(
        r["flow_regime"]
        for r in events
        if r["flow_regime"]
    )
)

for regime in regimes:

    selected = [
        r for r in events
        if r["flow_regime"] == regime
    ]

    print_summary(
        regime,
        selected
    )


# ============================================================
# FA STRENGTH
# ============================================================

fa_values = [
    r["fa"]
    for r in events
    if r["fa"] is not None
]

if fa_values:

    fa50 = percentile(
        fa_values, .50
    )

    fa75 = percentile(
        fa_values, .75
    )

    fa90_data = percentile(
        fa_values, .90
    )

    print()
    print("=" * 110)
    print("5. FLOW ACCELERATION STRENGTH")
    print("=" * 110)

    print(
        f"DATA P50={fa50:+.5f} | "
        f"P75={fa75:+.5f} | "
        f"P90={fa90_data:+.5f}"
    )

    buckets = [
        (
            "FA < DATA P50",
            lambda r:
                r["fa"] < fa50
        ),

        (
            "FA P50-P75",
            lambda r:
                fa50 <= r["fa"] < fa75
        ),

        (
            "FA P75-P90",
            lambda r:
                fa75 <= r["fa"] < fa90_data
        ),

        (
            "FA > DATA P90",
            lambda r:
                r["fa"] >= fa90_data
        ),
    ]

    for name, fn in buckets:

        selected = [
            r for r in events
            if (
                r["fa"] is not None
                and fn(r)
            )
        ]

        print_summary(
            name,
            selected
        )


# ============================================================
# ADOPTION — NEW WALLETS
# ============================================================

print()
print("=" * 110)
print("6. ADOPTION — NEW WALLETS")
print("=" * 110)

for col in [
    "new_wallets10",
    "new_wallets30",
    "wallet_growth",
    "buyer_growth",
]:

    threshold, low, high = split_quantile(
        events,
        col,
        .50
    )

    if threshold is None:
        continue

    print()
    print(
        f">>> {col} MEDIAN = {threshold:.4f}"
    )

    print_summary(
        f"{col} LOW",
        low
    )

    print_summary(
        f"{col} HIGH",
        high
    )


# ============================================================
# CONCENTRATION
# ============================================================

print()
print("=" * 110)
print("7. BUY CONCENTRATION")
print("=" * 110)

conc_values = [
    r["buy_concentration30"]
    for r in events
    if r["buy_concentration30"] is not None
]

if conc_values:

    conc50 = percentile(
        conc_values,
        .50
    )

    conc75 = percentile(
        conc_values,
        .75
    )

    print(
        f"CONC P50={conc50:.3f}"
        f" | P75={conc75:.3f}"
    )

    diversified = [
        r for r in events
        if (
            r["buy_concentration30"]
            is not None
            and r["buy_concentration30"]
            < conc50
        )
    ]

    concentrated = [
        r for r in events
        if (
            r["buy_concentration30"]
            is not None
            and r["buy_concentration30"]
            >= conc75
        )
    ]

    print_summary(
        "DIVERSIFIED BUYING",
        diversified
    )

    print_summary(
        "WHALE-CONCENTRATED",
        concentrated
    )


# ============================================================
# COMMUNITY / ADOPTION COMPOSITE
# ============================================================

print()
print("=" * 110)
print("8. ADOPTION COMPOSITE")
print("=" * 110)

nw10_vals = sorted(
    r["new_wallets10"]
    for r in events
    if r["new_wallets10"] is not None
)

wg_vals = sorted(
    r["wallet_growth"]
    for r in events
    if r["wallet_growth"] is not None
)

bg_vals = sorted(
    r["buyer_growth"]
    for r in events
    if r["buyer_growth"] is not None
)

if (
    nw10_vals
    and wg_vals
    and bg_vals
    and conc_values
):

    nw10_50 = percentile(
        nw10_vals, .50
    )

    wg50 = percentile(
        wg_vals, .50
    )

    bg50 = percentile(
        bg_vals, .50
    )

    adoption_high = []

    adoption_low = []

    for r in events:

        score = 0

        if (
            r["new_wallets10"]
            is not None
            and r["new_wallets10"]
            >= nw10_50
        ):
            score += 1

        if (
            r["wallet_growth"]
            is not None
            and r["wallet_growth"]
            >= wg50
        ):
            score += 1

        if (
            r["buyer_growth"]
            is not None
            and r["buyer_growth"]
            >= bg50
        ):
            score += 1

        if (
            r["buy_concentration30"]
            is not None
            and r["buy_concentration30"]
            < conc50
        ):
            score += 1

        if score >= 3:
            adoption_high.append(
                r
            )

        if score <= 1:
            adoption_low.append(
                r
            )

    print(
        f"HIGH = >=3/4 adoption conditions"
    )

    print_summary(
        "HIGH ADOPTION",
        adoption_high
    )

    print_summary(
        "LOW ADOPTION",
        adoption_low
    )


# ============================================================
# FA95 + ADOPTION
# ============================================================

print()
print("=" * 110)
print("9. FA95 + ADOPTION TEST")
print("=" * 110)

if (
    nw10_vals
    and wg_vals
    and bg_vals
    and conc_values
):

    fa95_events = [
        r for r in events
        if r["fa95"] == 1
    ]

    good = []
    bad = []

    for r in fa95_events:

        score = 0

        if (
            r["new_wallets10"]
            is not None
            and r["new_wallets10"]
            >= nw10_50
        ):
            score += 1

        if (
            r["wallet_growth"]
            is not None
            and r["wallet_growth"]
            >= wg50
        ):
            score += 1

        if (
            r["buyer_growth"]
            is not None
            and r["buyer_growth"]
            >= bg50
        ):
            score += 1

        if (
            r["buy_concentration30"]
            is not None
            and r["buy_concentration30"]
            < conc50
        ):
            score += 1

        if score >= 3:
            good.append(r)

        if score <= 1:
            bad.append(r)

    print_summary(
        "FA95 + HIGH ADOPTION",
        good
    )

    print_summary(
        "FA95 + LOW ADOPTION",
        bad
    )


# ============================================================
# SCALP VS RUNNER
# ============================================================

print()
print("=" * 110)
print("10. SCALP VS RUNNER")
print("=" * 110)

usable = [
    r for r in events
    if (
        r["dex_return_10s"] is not None
        and r["dex_return_60s"] is not None
    )
]

scalp = [
    r for r in usable
    if (
        r["dex_return_10s"] > 0
        and r["dex_return_60s"]
        <= r["dex_return_10s"]
    )
]

runner = [
    r for r in usable
    if (
        r["dex_return_60s"]
        > r["dex_return_10s"]
    )
]

dump = [
    r for r in usable
    if (
        r["dex_return_10s"] <= 0
        and r["dex_return_60s"] <= 0
    )
]

print(
    f"USABLE={len(usable):,}"
    f" | SCALP={len(scalp):,}"
    f" | RUNNER={len(runner):,}"
    f" | DUMP={len(dump):,}"
)

print_summary(
    "RUNNERS",
    runner
)

print_summary(
    "SCALP / EARLY PEAK",
    scalp
)

print_summary(
    "DUMPS",
    dump
)


# ============================================================
# RUNNER FEATURE PROFILE
# ============================================================

print()
print("=" * 110)
print("11. RUNNER FEATURE PROFILE")
print("=" * 110)

features = [
    "fa",
    "nf30",
    "imbalance30",
    "new_wallets10",
    "new_wallets30",
    "buyer_growth",
    "wallet_growth",
    "buy_concentration30",
    "buyers10",
    "buyers30",
    "wallets30",
    "buy_volume30",
]


def feature_med(rows, col):

    vals = [
        r[col]
        for r in rows
        if r[col] is not None
    ]

    if not vals:
        return None

    return statistics.median(
        vals
    )


for col in features:

    runner_med = feature_med(
        runner,
        col
    )

    nonrunner_med = feature_med(
        [
            r for r in usable
            if r not in runner
        ],
        col
    )

    print(
        f"{col:22} | "
        f"RUNNER={str(runner_med):>12} | "
        f"OTHER={str(nonrunner_med):>12}"
    )


# ============================================================
# TOKEN CONCENTRATION / INDEPENDENCE
# ============================================================

print()
print("=" * 110)
print("12. EVENT INDEPENDENCE")
print("=" * 110)

token_counts = Counter(
    r["token_mint"]
    for r in events
)

print(
    f"UNIQUE TOKENS = "
    f"{len(token_counts):,}"
)

print(
    f"EVENTS/TOKEN MEDIAN = "
    f"{statistics.median(token_counts.values()):.2f}"
)

print(
    f"MAX EVENTS ONE TOKEN = "
    f"{max(token_counts.values())}"
)

print()
print("TOP TOKENS")

for mint, n in token_counts.most_common(
    15
):

    rows = [
        r for r in events
        if r["token_mint"] == mint
    ]

    r60 = [
        r["dex_return_60s"]
        for r in rows
        if r["dex_return_60s"]
        is not None
    ]

    med60 = (
        statistics.median(r60)
        if r60
        else None
    )

    print(
        f"{mint[:16]}..."
        f" | EVENTS={n:>4}"
        f" | R60_MED="
        f"{fmt(med60)}"
    )


# ============================================================
# EXTREME OUTLIER CHECK
# ============================================================

print()
print("=" * 110)
print("13. RETURN OUTLIER CHECK")
print("=" * 110)

for h in HORIZONS:

    vals = [
        r[f"dex_return_{h}s"]
        for r in events
        if r[f"dex_return_{h}s"]
        is not None
    ]

    if not vals:
        continue

    print(
        f"{h:>3}s | "
        f"N={len(vals):,} | "
        f"MIN={min(vals):+.2f}% | "
        f"P01={percentile(vals,.01):+.2f}% | "
        f"MED={statistics.median(vals):+.2f}% | "
        f"P99={percentile(vals,.99):+.2f}% | "
        f"MAX={max(vals):+.2f}%"
    )


# ============================================================
# TOP / WORST EVENTS
# ============================================================

print()
print("=" * 110)
print("14. TOP 20 EVENTS @ 60s")
print("=" * 110)

ranked = sorted(
    [
        r for r in events
        if r["dex_return_60s"]
        is not None
    ],
    key=lambda r:
        r["dex_return_60s"],
    reverse=True
)

for r in ranked[:20]:

    print(
        f"{r['token_mint'][:14]}... | "
        f"R5={fmt(r['dex_return_5s'])} | "
        f"R10={fmt(r['dex_return_10s'])} | "
        f"R30={fmt(r['dex_return_30s'])} | "
        f"R60={fmt(r['dex_return_60s'])} | "
        f"R300={fmt(r['dex_return_300s'])} | "
        f"FA={r['fa']:+.3f} | "
        f"NEW10={r['new_wallets10']} | "
        f"CONC={r['buy_concentration30']:.2f}"
    )


print()
print("=" * 110)
print("15. WORST 20 EVENTS @ 60s")
print("=" * 110)

for r in ranked[-20:]:

    print(
        f"{r['token_mint'][:14]}... | "
        f"R5={fmt(r['dex_return_5s'])} | "
        f"R10={fmt(r['dex_return_10s'])} | "
        f"R30={fmt(r['dex_return_30s'])} | "
        f"R60={fmt(r['dex_return_60s'])} | "
        f"R300={fmt(r['dex_return_300s'])} | "
        f"FA={r['fa']:+.3f} | "
        f"NEW10={r['new_wallets10']} | "
        f"CONC={r['buy_concentration30']:.2f}"
    )


print()
print("=" * 110)
print("ANALYSIS COMPLETE")
print("=" * 110)

db.close()
