import sqlite3
import statistics
import math

DB = "validation_v090.db"

MAX_ID = 545

VOLUME_CUT = 8837.925

# Approximation déjà utilisée dans T24
TOTAL_EXECUTION_COST = 3.0

# Petite grille volontairement limitée
ACTIVATIONS = [5.0, 10.0, 15.0]
TRAILS = [3.0, 5.0, 8.0]
HORIZONS = [60, 120, 300]

DISCOVERY_FRAC = 0.60


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def percentile(vals, p):
    vals = sorted(x for x in vals if valid(x))

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


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# FIRST V2 SIGNAL / TOKEN
# ============================================================

signals = db.execute("""
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

      ON d.event_id=x.event_id
     AND d.timestamp=x.first_time
)

SELECT
    e.id,
    e.timestamp,
    e.token_mint,

    d.price_usd,
    d.timestamp AS dex_entry_timestamp,
    d.volume_m5

FROM events e

JOIN first_dex d
ON d.event_id=e.id

WHERE
    e.id <= ?
    AND e.fa95=1
    AND e.new_wallets30 >= 2
    AND d.volume_m5 >= ?

ORDER BY e.id ASC
""", (
    MAX_ID,
    VOLUME_CUT
)).fetchall()


first_by_token = []
seen = set()

for r in signals:

    token = r["token_mint"]

    if token in seen:
        continue

    seen.add(token)
    first_by_token.append(r)


# ============================================================
# PRICE PATH
# ============================================================

def load_path(signal, horizon):

    start = signal["dex_entry_timestamp"]
    end = start + horizon

    rows = db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND timestamp >= ?
            AND timestamp <= ?
            AND price_usd IS NOT NULL
            AND price_usd > 0

        ORDER BY timestamp ASC
    """, (
        signal["id"],
        start,
        end
    )).fetchall()

    return rows


def raw_return(entry, price):

    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    return (
        price / entry - 1
    ) * 100


# ============================================================
# TRAILING SIMULATION
# ============================================================

def simulate(signal, activation, trail, horizon):

    path = load_path(
        signal,
        horizon
    )

    if not path:
        return None

    entry = signal["price_usd"]

    if not valid(entry) or entry <= 0:
        return None

    peak_price = entry

    activated = False
    activation_time = None

    exit_price = None
    exit_time = None
    exit_reason = "TIME"

    max_return = 0.0

    for row in path:

        price = row["price_usd"]

        if not valid(price) or price <= 0:
            continue

        if price > peak_price:
            peak_price = price

        peak_ret = raw_return(
            entry,
            peak_price
        )

        if valid(peak_ret):
            max_return = max(
                max_return,
                peak_ret
            )

        if (
            not activated
            and valid(peak_ret)
            and peak_ret >= activation
        ):
            activated = True
            activation_time = (
                row["timestamp"]
                - signal["dex_entry_timestamp"]
            )

        if activated:

            # True % trailing from peak price
            stop_price = (
                peak_price
                * (
                    1 - trail/100
                )
            )

            if price <= stop_price:

                # Conservative:
                # exit at observed snapshot price,
                # not ideal stop price.
                exit_price = price
                exit_time = (
                    row["timestamp"]
                    - signal["dex_entry_timestamp"]
                )
                exit_reason = "TRAIL"
                break

    if exit_price is None:

        # Time stop = last available observed price
        last = path[-1]

        exit_price = last["price_usd"]
        exit_time = (
            last["timestamp"]
            - signal["dex_entry_timestamp"]
        )

    gross = raw_return(
        entry,
        exit_price
    )

    if not valid(gross):
        return None

    net = (
        gross
        - TOTAL_EXECUTION_COST
    )

    return {
        "event_id":
            signal["id"],

        "token":
            signal["token_mint"],

        "gross":
            gross,

        "net":
            net,

        "max_return":
            max_return,

        "activated":
            activated,

        "activation_time":
            activation_time,

        "exit_time":
            exit_time,

        "exit_reason":
            exit_reason,

        "points":
            len(path),

        "last_delay":
            path[-1]["timestamp"]
            - signal["dex_entry_timestamp"],
    }


# ============================================================
# FIXED EXIT BASELINE FROM PATH
# ============================================================

def fixed_exit(signal, horizon):

    path = load_path(
        signal,
        horizon
    )

    if not path:
        return None

    entry = signal["price_usd"]
    price = path[-1]["price_usd"]

    gross = raw_return(
        entry,
        price
    )

    if not valid(gross):
        return None

    return {
        "gross":
            gross,

        "net":
            gross
            - TOTAL_EXECUTION_COST,

        "points":
            len(path),

        "exit_time":
            path[-1]["timestamp"]
            - signal["dex_entry_timestamp"],
    }


# ============================================================
# RESULT STATS
# ============================================================

def result_stats(results):

    results = [
        x for x in results
        if x is not None
    ]

    if not results:
        return None

    vals = [
        x["net"]
        for x in results
    ]

    gross = [
        x["gross"]
        for x in results
    ]

    activated = [
        x for x in results
        if x.get(
            "activated",
            False
        )
    ]

    trails = [
        x for x in results
        if x.get(
            "exit_reason"
        ) == "TRAIL"
    ]

    return {
        "n":
            len(vals),

        "avg":
            avg(vals),

        "med":
            med(vals),

        "win":
            100
            * sum(x > 0 for x in vals)
            / len(vals),

        "worst":
            min(vals),

        "best":
            max(vals),

        "p10":
            percentile(vals,.10),

        "gross_avg":
            avg(gross),

        "activated_pct":
            100
            * len(activated)
            / len(vals),

        "trail_exit_pct":
            100
            * len(trails)
            / len(vals),

        "avg_points":
            avg([
                x["points"]
                for x in results
            ]),

        "avg_exit_time":
            avg([
                x["exit_time"]
                for x in results
            ]),
    }


# ============================================================
# SPLIT TOKENS CHRONOLOGICALLY
# ============================================================

n = len(first_by_token)

cut = int(
    n * DISCOVERY_FRAC
)

discovery = (
    first_by_token[:cut]
)

validation = (
    first_by_token[cut:]
)


# ============================================================
# COVERAGE
# ============================================================

print("=" * 135)
print(
    "MEMECOIN LAB — "
    "T25 PATH-BASED TRAILING STOP LAB"
)
print("=" * 135)

print()
print(
    f"V2 FIRST-SIGNAL TOKENS : "
    f"{len(first_by_token)}"
)

print(
    f"DISCOVERY              : "
    f"{len(discovery)}"
)

print(
    f"VALIDATION             : "
    f"{len(validation)}"
)

print(
    f"HISTORICAL BOUNDARY    : "
    f"ID <= {MAX_ID}"
)

print(
    f"TOTAL EXECUTION COST   : "
    f"{TOTAL_EXECUTION_COST:.2f}%"
)

print()

print("=" * 135)
print("A) DEX PATH COVERAGE")
print("=" * 135)

for horizon in HORIZONS:

    cover = []

    for s in first_by_token:

        path = load_path(
            s,
            horizon
        )

        if path:

            cover.append(
                (
                    len(path),
                    path[-1]["timestamp"]
                    - s["dex_entry_timestamp"]
                )
            )

    if not cover:

        print(
            f"{horizon:>4}s | N=0"
        )

        continue

    print(
        f"{horizon:>4}s | "
        f"TOKENS={len(cover):>3} | "
        f"MED POINTS={med([x[0] for x in cover]):6.1f} | "
        f"MED LAST DELAY={med([x[1] for x in cover]):7.2f}s"
    )


# ============================================================
# BASELINES
# ============================================================

print()
print("=" * 135)
print(
    "B) FIXED TIME-STOP BASELINES "
    "(PATH DATA, 3% COST)"
)
print("=" * 135)

for horizon in HORIZONS:

    for name, subset in [
        ("DISC", discovery),
        ("VALID", validation),
    ]:

        results = []

        for s in subset:

            r = fixed_exit(
                s,
                horizon
            )

            if r:

                results.append({
                    "net":
                        r["net"],

                    "gross":
                        r["gross"],

                    "points":
                        r["points"],

                    "exit_time":
                        r["exit_time"],

                    "activated":
                        False,

                    "exit_reason":
                        "TIME",
                })

        st = result_stats(
            results
        )

        if not st:
            continue

        print(
            f"{name:5} | "
            f"H={horizon:>3}s | "
            f"N={st['n']:>2} | "
            f"AVG={st['avg']:+7.2f}% | "
            f"MED={st['med']:+7.2f}% | "
            f"WIN={st['win']:5.1f}% | "
            f"P10={st['p10']:+7.2f}% | "
            f"WORST={st['worst']:+7.2f}% | "
            f"BEST={st['best']:+7.2f}%"
        )


# ============================================================
# GRID
# ============================================================

grid = []

for horizon in HORIZONS:

    for activation in ACTIVATIONS:

        for trail in TRAILS:

            disc_results = [
                simulate(
                    s,
                    activation,
                    trail,
                    horizon
                )
                for s in discovery
            ]

            val_results = [
                simulate(
                    s,
                    activation,
                    trail,
                    horizon
                )
                for s in validation
            ]

            ds = result_stats(
                disc_results
            )

            vs = result_stats(
                val_results
            )

            if not ds or not vs:
                continue

            # Discovery ranking only.
            # Reward average + median,
            # penalize bad left tail.
            score = (
                ds["avg"]
                + 0.50 * ds["med"]
                + 0.10 * ds["p10"]
            )

            grid.append({
                "horizon":
                    horizon,

                "activation":
                    activation,

                "trail":
                    trail,

                "score":
                    score,

                "disc":
                    ds,

                "valid":
                    vs,

                "disc_results":
                    disc_results,

                "val_results":
                    val_results,
            })


grid.sort(
    key=lambda x:
        x["score"],
    reverse=True
)


print()
print("=" * 135)
print(
    "C) TRAILING GRID — "
    "RANKED ON DISCOVERY ONLY"
)
print("=" * 135)

print(
    f"{'H':>4} "
    f"{'ACT':>5} "
    f"{'TRL':>5} | "
    f"{'D AVG':>8} "
    f"{'D MED':>8} "
    f"{'D WIN':>7} "
    f"{'D P10':>8} | "
    f"{'V AVG':>8} "
    f"{'V MED':>8} "
    f"{'V WIN':>7} "
    f"{'V P10':>8}"
)

print("-" * 115)

for x in grid:

    d = x["disc"]
    v = x["valid"]

    print(
        f"{x['horizon']:4d} "
        f"{x['activation']:5.1f} "
        f"{x['trail']:5.1f} | "
        f"{d['avg']:+7.2f}% "
        f"{d['med']:+7.2f}% "
        f"{d['win']:6.1f}% "
        f"{d['p10']:+7.2f}% | "
        f"{v['avg']:+7.2f}% "
        f"{v['med']:+7.2f}% "
        f"{v['win']:6.1f}% "
        f"{v['p10']:+7.2f}%"
    )


# ============================================================
# TOP DISCOVERY RULE
# ============================================================

if grid:

    best = grid[0]

    print()
    print("=" * 135)
    print(
        "D) TOP DISCOVERY RULE — "
        "UNTOUCHED VALIDATION"
    )
    print("=" * 135)

    print(
        f"TIME STOP   : "
        f"{best['horizon']}s"
    )

    print(
        f"ACTIVATION  : "
        f"+{best['activation']:.1f}%"
    )

    print(
        f"TRAIL       : "
        f"{best['trail']:.1f}%"
    )

    print()

    for name, st in [
        ("DISCOVERY", best["disc"]),
        ("VALIDATION", best["valid"]),
    ]:

        print(
            f"{name:11} | "
            f"N={st['n']} | "
            f"AVG={st['avg']:+.2f}% | "
            f"MED={st['med']:+.2f}% | "
            f"WIN={st['win']:.1f}% | "
            f"P10={st['p10']:+.2f}% | "
            f"WORST={st['worst']:+.2f}% | "
            f"BEST={st['best']:+.2f}%"
        )

        print(
            f"{'':11} | "
            f"TRAIL ACTIVATED="
            f"{st['activated_pct']:.1f}% | "
            f"TRAIL EXIT="
            f"{st['trail_exit_pct']:.1f}% | "
            f"AVG EXIT="
            f"{st['avg_exit_time']:.1f}s"
        )


    # ========================================================
    # VALIDATION TOKEN DETAIL
    # ========================================================

    print()
    print("=" * 135)
    print(
        "E) TOP RULE — VALIDATION TOKEN DETAIL"
    )
    print("=" * 135)

    print(
        f"{'TOKEN':20} "
        f"{'NET':>9} "
        f"{'MAX':>9} "
        f"{'EXIT':>8} "
        f"{'TIME':>8}"
    )

    print("-" * 65)

    valid_results = [
        r for r in best["val_results"]
        if r is not None
    ]

    for r in sorted(
        valid_results,
        key=lambda x:
            x["net"],
        reverse=True
    ):

        print(
            f"{r['token'][:20]:20} "
            f"{r['net']:+8.2f}% "
            f"{r['max_return']:+8.2f}% "
            f"{r['exit_reason']:>8} "
            f"{r['exit_time']:7.1f}s"
        )


# ============================================================
# ROBUST CROSS-SPLIT CANDIDATES
# ============================================================

print()
print("=" * 135)
print(
    "F) RULES POSITIVE IN BOTH "
    "DISCOVERY AND VALIDATION"
)
print("=" * 135)

robust = [
    x for x in grid
    if (
        x["disc"]["avg"] > 0
        and x["valid"]["avg"] > 0
    )
]

robust.sort(
    key=lambda x:
        (
            x["valid"]["avg"]
            + x["valid"]["med"]
        ),
    reverse=True
)

if not robust:

    print(
        "No rule has positive average "
        "in both splits."
    )

else:

    for x in robust[:10]:

        d = x["disc"]
        v = x["valid"]

        print(
            f"H={x['horizon']:>3}s "
            f"| ACT={x['activation']:>4.0f}% "
            f"| TRAIL={x['trail']:>3.0f}% "
            f"| DISC AVG/MED="
            f"{d['avg']:+.2f}/{d['med']:+.2f}% "
            f"| VALID AVG/MED="
            f"{v['avg']:+.2f}/{v['med']:+.2f}%"
        )


print()
print("=" * 135)
print("IMPORTANT")
print("=" * 135)

print("""
• Historical data only: ID <= 545.
• T23 remains untouched and prospective.
• Trailing activation is based on observed DEX snapshots.
• If price gaps through the trailing level, exit uses the
  observed price, not the theoretical stop price.
• 3% execution cost is deducted from every completed trade.
• The grid is ranked ONLY on the Discovery tokens.
• Validation must not be used to endlessly retune this grid.
• With only 21 historical tokens, this remains exploratory.
""")

db.close()
