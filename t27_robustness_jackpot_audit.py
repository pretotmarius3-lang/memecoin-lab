import sqlite3
import statistics
import math
import random

DB = "validation_v090.db"

MAX_ID = 545
VOLUME_CUT = 8837.925

HORIZON = 120
ACTIVATION = 5.0
TRAIL = 3.0
TOTAL_EXECUTION_COST = 3.0

DISCOVERY_FRAC = 0.60

BOOTSTRAP_N = 10000
RANDOM_SEED = 42


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

    return vals[lo]*(hi-k) + vals[hi]*(k-lo)


def trimmed_mean(vals, trim_frac):

    vals = sorted(
        x for x in vals
        if valid(x)
    )

    if not vals:
        return None

    k = int(
        len(vals) * trim_frac
    )

    if 2*k >= len(vals):
        return None

    trimmed = vals[
        k:len(vals)-k
    ]

    return avg(trimmed)


def winsorized_mean(vals, trim_frac):

    vals = sorted(
        x for x in vals
        if valid(x)
    )

    if not vals:
        return None

    k = int(
        len(vals) * trim_frac
    )

    if k == 0:
        return avg(vals)

    lo = vals[k]
    hi = vals[-k-1]

    wins = [
        min(
            max(x,lo),
            hi
        )
        for x in vals
    ]

    return avg(wins)


db = sqlite3.connect(
    DB,
    timeout=30
)

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
    e.token_mint,

    d.timestamp AS entry_timestamp,
    d.price_usd AS entry_price,
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


first = []
seen = set()

for r in signals:

    token = r["token_mint"]

    if token in seen:
        continue

    seen.add(token)
    first.append(r)


cut = int(
    len(first)
    * DISCOVERY_FRAC
)

discovery = first[:cut]
validation = first[cut:]


# ============================================================
# PATH + FROZEN EXECUTION
# ============================================================

def path(signal):

    return db.execute("""
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

        ORDER BY timestamp
    """, (
        signal["id"],
        signal["entry_timestamp"],
        signal["entry_timestamp"] + HORIZON,
    )).fetchall()


def ret(entry, price):

    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    return (
        price / entry - 1
    ) * 100


def simulate(signal):

    rows = path(signal)

    if not rows:
        return None

    entry = signal[
        "entry_price"
    ]

    if not valid(entry) or entry <= 0:
        return None

    peak = entry
    trail_active = False

    exit_price = None
    exit_time = None
    reason = "TIME"

    max_ret = 0.0
    min_ret = 0.0

    for row in rows:

        price = row[
            "price_usd"
        ]

        if not valid(price) or price <= 0:
            continue

        current = ret(
            entry,
            price
        )

        if not valid(current):
            continue

        max_ret = max(
            max_ret,
            current
        )

        min_ret = min(
            min_ret,
            current
        )

        if price > peak:
            peak = price

        peak_ret = ret(
            entry,
            peak
        )

        if (
            not trail_active
            and valid(peak_ret)
            and peak_ret >= ACTIVATION
        ):
            trail_active = True

        if trail_active:

            trail_price = (
                peak
                * (
                    1 - TRAIL/100
                )
            )

            if price <= trail_price:

                exit_price = price
                exit_time = (
                    row["timestamp"]
                    - signal["entry_timestamp"]
                )
                reason = "TRAIL"
                break

    if exit_price is None:

        exit_price = rows[-1][
            "price_usd"
        ]

        exit_time = (
            rows[-1]["timestamp"]
            - signal["entry_timestamp"]
        )

    gross = ret(
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
        "token":
            signal["token_mint"],

        "event_id":
            signal["id"],

        "gross":
            gross,

        "net":
            net,

        "max_ret":
            max_ret,

        "min_ret":
            min_ret,

        "reason":
            reason,

        "exit_time":
            exit_time,
    }


def simulate_group(group):

    return [
        x for x in (
            simulate(s)
            for s in group
        )
        if x is not None
    ]


disc_results = simulate_group(
    discovery
)

val_results = simulate_group(
    validation
)

all_results = (
    disc_results
    + val_results
)


# ============================================================
# ROBUSTNESS
# ============================================================

def basic_stats(results):

    vals = [
        x["net"]
        for x in results
    ]

    return {
        "n": len(vals),
        "avg": avg(vals),
        "med": med(vals),
        "win": 100*sum(x > 0 for x in vals)/len(vals),
        "p10": percentile(vals,.10),
        "p25": percentile(vals,.25),
        "worst": min(vals),
        "best": max(vals),
        "trim10": trimmed_mean(vals,.10),
        "winsor10": winsorized_mean(vals,.10),
    }


def top_removed(results, k):

    ranked = sorted(
        results,
        key=lambda x:
            x["net"],
        reverse=True
    )

    if k >= len(ranked):
        return []

    return ranked[k:]


def bottom_removed(results, k):

    ranked = sorted(
        results,
        key=lambda x:
            x["net"]
    )

    if k >= len(ranked):
        return []

    return ranked[k:]


def bootstrap_mean_ci(results):

    vals = [
        x["net"]
        for x in results
    ]

    if len(vals) < 2:
        return None

    random.seed(
        RANDOM_SEED
    )

    means = []

    for _ in range(
        BOOTSTRAP_N
    ):

        sample = [
            random.choice(vals)
            for _ in range(
                len(vals)
            )
        ]

        means.append(
            avg(sample)
        )

    means.sort()

    return {
        "p025":
            percentile(
                means,
                .025
            ),

        "p50":
            percentile(
                means,
                .50
            ),

        "p975":
            percentile(
                means,
                .975
            ),

        "prob_positive":
            100
            * sum(x > 0 for x in means)
            / len(means),
    }


def leave_one_out(results):

    out = []

    for i,r in enumerate(results):

        remaining = (
            results[:i]
            + results[i+1:]
        )

        vals = [
            x["net"]
            for x in remaining
        ]

        out.append({
            "removed_token":
                r["token"],

            "removed_net":
                r["net"],

            "remaining_avg":
                avg(vals),

            "remaining_med":
                med(vals),
        })

    return sorted(
        out,
        key=lambda x:
            x["remaining_avg"]
    )


def contribution(results):

    vals = sorted(
        [
            x["net"]
            for x in results
        ],
        reverse=True
    )

    total = sum(vals)

    if total == 0:
        return None

    out = {}

    for k in [1,2,3,5]:

        if k > len(vals):
            continue

        top_sum = sum(
            vals[:k]
        )

        out[k] = (
            100
            * top_sum
            / total
        )

    return out


# ============================================================
# OUTPUT
# ============================================================

print("="*145)
print(
    "MEMECOIN LAB — "
    "T27 ROBUSTNESS / JACKPOT DEPENDENCE AUDIT"
)
print("="*145)

print(
    f"FROZEN RULE:"
)

print(
    f"V2 first signal/token "
    f"| ACT=+{ACTIVATION:.0f}% "
    f"| TRAIL={TRAIL:.0f}% "
    f"| TIME={HORIZON}s "
    f"| COST={TOTAL_EXECUTION_COST:.0f}%"
)

print(
    f"HISTORICAL BOUNDARY: "
    f"ID <= {MAX_ID}"
)

print()


# ============================================================
# A) BASE
# ============================================================

print("="*145)
print("A) BASE RESULTS")
print("="*145)

for name,results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    s = basic_stats(
        results
    )

    print(
        f"{name:11} | "
        f"N={s['n']:>2} | "
        f"AVG={s['avg']:+7.2f}% | "
        f"MED={s['med']:+7.2f}% | "
        f"WIN={s['win']:5.1f}% | "
        f"P10={s['p10']:+7.2f}% | "
        f"P25={s['p25']:+7.2f}% | "
        f"TRIM10={s['trim10']:+7.2f}% | "
        f"WINSOR10={s['winsor10']:+7.2f}%"
    )


# ============================================================
# B) REMOVE BEST TRADES
# ============================================================

print()
print("="*145)
print(
    "B) JACKPOT DEPENDENCE — "
    "REMOVE BEST TOKENS"
)
print("="*145)

for name,results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    print()
    print(name)
    print("-"*100)

    for k in [0,1,2,3,5]:

        rr = (
            results
            if k == 0
            else top_removed(
                results,
                k
            )
        )

        if not rr:
            continue

        s = basic_stats(
            rr
        )

        print(
            f"REMOVE TOP {k:<2} | "
            f"N={s['n']:>2} | "
            f"AVG={s['avg']:+7.2f}% | "
            f"MED={s['med']:+7.2f}% | "
            f"WIN={s['win']:5.1f}% | "
            f"WORST={s['worst']:+7.2f}% | "
            f"BEST={s['best']:+7.2f}%"
        )


# ============================================================
# C) REMOVE WORST TRADES
# ============================================================

print()
print("="*145)
print(
    "C) LEFT-TAIL DEPENDENCE — "
    "REMOVE WORST TOKENS"
)
print("="*145)

for name,results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    print()
    print(name)
    print("-"*100)

    for k in [1,2,3]:

        rr = bottom_removed(
            results,
            k
        )

        if not rr:
            continue

        s = basic_stats(
            rr
        )

        print(
            f"REMOVE WORST {k:<2} | "
            f"N={s['n']:>2} | "
            f"AVG={s['avg']:+7.2f}% | "
            f"MED={s['med']:+7.2f}% | "
            f"WIN={s['win']:5.1f}%"
        )


# ============================================================
# D) CONTRIBUTION
# ============================================================

print()
print("="*145)
print(
    "D) PROFIT CONTRIBUTION OF TOP WINNERS"
)
print("="*145)

for name,results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    c = contribution(
        results
    )

    print()
    print(name)

    if c is None:
        print(
            "Total net = 0"
        )
        continue

    for k,v in c.items():

        print(
            f"TOP {k:<2} contribution "
            f"to total net result = "
            f"{v:+.1f}%"
        )


# ============================================================
# E) BOOTSTRAP
# ============================================================

print()
print("="*145)
print(
    "E) BOOTSTRAP MEAN EXPECTANCY"
)
print("="*145)

for name,results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    b = bootstrap_mean_ci(
        results
    )

    if not b:
        continue

    print(
        f"{name:11} | "
        f"MEAN BOOT MED={b['p50']:+7.2f}% | "
        f"95% CI=[{b['p025']:+7.2f}%, "
        f"{b['p975']:+7.2f}%] | "
        f"P(MEAN>0)={b['prob_positive']:5.1f}%"
    )


# ============================================================
# F) LEAVE ONE TOKEN OUT
# ============================================================

print()
print("="*145)
print(
    "F) LEAVE-ONE-TOKEN-OUT"
)
print("="*145)

for name,results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
]:

    loo = leave_one_out(
        results
    )

    print()
    print(name)
    print("-"*100)

    print(
        "Worst remaining average after removing one token:"
    )

    for x in loo[:5]:

        print(
            f"remove {x['removed_token'][:20]:20} "
            f"(trade={x['removed_net']:+7.2f}%) "
            f"=> AVG={x['remaining_avg']:+7.2f}% "
            f"| MED={x['remaining_med']:+7.2f}%"
        )


# ============================================================
# G) TOKEN DETAIL
# ============================================================

print()
print("="*145)
print(
    "G) ALL TOKEN RESULTS — FROZEN RULE"
)
print("="*145)

print(
    f"{'SPLIT':10} "
    f"{'TOKEN':20} "
    f"{'NET':>9} "
    f"{'MAX':>9} "
    f"{'MIN':>9} "
    f"{'EXIT':>8}"
)

print("-"*75)

for split_name, results in [
    ("DISC", disc_results),
    ("VALID", val_results),
]:

    for r in sorted(
        results,
        key=lambda x:
            x["net"],
        reverse=True
    ):

        print(
            f"{split_name:10} "
            f"{r['token'][:20]:20} "
            f"{r['net']:+8.2f}% "
            f"{r['max_ret']:+8.2f}% "
            f"{r['min_ret']:+8.2f}% "
            f"{r['reason']:>8}"
        )


print()
print("="*145)
print("INTERPRETATION")
print("="*145)

print("""
Strong result:
• trimmed / winsorized means remain positive
• removing top 1 winner does not destroy expectancy
• removing top 2-3 does not immediately collapse everything
• bootstrap P(mean>0) remains high
• leave-one-token-out stays mostly positive

Fragile result:
• top 1-3 winners explain almost all profit
• AVG becomes negative when best token is removed
• bootstrap CI is very wide around zero
• one token determines the conclusion

T23 remains the true prospective validator.
Do NOT change the frozen rule from this audit.
""")

db.close()
