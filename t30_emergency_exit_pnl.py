import sqlite3
import statistics
import math

DB = "validation_v090.db"

MAX_ID = 545
VOLUME_CUT = 8837.925

HORIZON = 120
ACTIVATION = 5.0
TRAIL = 3.0

EMERGENCY_TIME = 75
EMERGENCY_THRESHOLD = -5.0

TOTAL_EXECUTION_COST = 3.0

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

    return vals[lo]*(hi-k) + vals[hi]*(k-lo)


def ret(entry, price):
    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    return (price/entry - 1) * 100


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
        SELECT event_id, MIN(timestamp) AS first_time
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

ORDER BY e.id
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
    len(first) * DISCOVERY_FRAC
)

discovery = first[:cut]
validation = first[cut:]


# ============================================================
# PATH
# ============================================================

def load_path(signal):

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


# ============================================================
# SIMULATION CORE
# ============================================================

def simulate(signal, use_emergency=False):

    rows = load_path(signal)

    if not rows:
        return None

    entry = signal["entry_price"]
    start = signal["entry_timestamp"]

    if not valid(entry) or entry <= 0:
        return None

    peak = entry
    trail_active = False

    exit_price = None
    exit_time = None
    reason = "TIME"

    max_ret = 0.0
    min_ret = 0.0

    emergency_checked = False
    emergency_snapshot_ret = None

    for row in rows:

        price = row["price_usd"]
        ts = row["timestamp"]

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

        # ----------------------------------------------------
        # ACTIVATE TRAIL
        # ----------------------------------------------------

        if (
            not trail_active
            and valid(peak_ret)
            and peak_ret >= ACTIVATION
        ):
            trail_active = True

        # ----------------------------------------------------
        # TRAILING EXIT
        # ----------------------------------------------------

        if trail_active:

            trail_price = (
                peak
                * (
                    1 - TRAIL/100
                )
            )

            if price <= trail_price:

                exit_price = price
                exit_time = ts - start
                reason = "TRAIL"
                break

        # ----------------------------------------------------
        # EMERGENCY CHECK @ 75s
        #
        # Apply only once we have reached/passed 75s
        # and only if trade is still open.
        # ----------------------------------------------------

        elapsed = ts - start

        if (
            use_emergency
            and not emergency_checked
            and elapsed >= EMERGENCY_TIME
        ):

            emergency_checked = True
            emergency_snapshot_ret = current

            if current < EMERGENCY_THRESHOLD:

                exit_price = price
                exit_time = elapsed
                reason = "EMERGENCY"
                break

    if exit_price is None:

        exit_price = rows[-1]["price_usd"]
        exit_time = (
            rows[-1]["timestamp"]
            - start
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

        "net":
            net,

        "gross":
            gross,

        "max_ret":
            max_ret,

        "min_ret":
            min_ret,

        "reason":
            reason,

        "exit_time":
            exit_time,

        "emergency_checked":
            emergency_checked,

        "emergency_ret":
            emergency_snapshot_ret,
    }


# ============================================================
# RUN BOTH SYSTEMS
# ============================================================

def run_group(group):

    out = []

    for s in group:

        base = simulate(
            s,
            use_emergency=False
        )

        candidate = simulate(
            s,
            use_emergency=True
        )

        if base is None or candidate is None:
            continue

        out.append({
            "token":
                s["token_mint"],

            "event_id":
                s["id"],

            "base":
                base,

            "candidate":
                candidate,

            "delta":
                candidate["net"]
                - base["net"],
        })

    return out


disc_results = run_group(
    discovery
)

val_results = run_group(
    validation
)

all_results = (
    disc_results
    + val_results
)


# ============================================================
# STATS
# ============================================================

def system_stats(results, key):

    vals = [
        r[key]["net"]
        for r in results
    ]

    if not vals:
        return None

    reasons = {}

    for r in results:

        reason = r[key]["reason"]

        reasons[reason] = (
            reasons.get(reason, 0)
            + 1
        )

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

        "p10":
            percentile(vals,.10),

        "p25":
            percentile(vals,.25),

        "worst":
            min(vals),

        "best":
            max(vals),

        "reasons":
            reasons,

        "avg_exit":
            avg([
                r[key]["exit_time"]
                for r in results
            ]),
    }


def delta_stats(results):

    vals = [
        r["delta"]
        for r in results
    ]

    improved = [
        r for r in results
        if r["delta"] > 0
    ]

    worsened = [
        r for r in results
        if r["delta"] < 0
    ]

    unchanged = [
        r for r in results
        if abs(r["delta"]) < 1e-9
    ]

    return {
        "avg_delta":
            avg(vals),

        "med_delta":
            med(vals),

        "improved":
            len(improved),

        "worsened":
            len(worsened),

        "unchanged":
            len(unchanged),

        "best_improvement":
            max(vals),

        "worst_damage":
            min(vals),
    }


# ============================================================
# OUTPUT
# ============================================================

print("="*155)
print(
    "MEMECOIN LAB — "
    "T30 EMERGENCY EXIT P&L IMPACT"
)
print("="*155)

print(
    f"FIRST V2 TOKENS : "
    f"{len(first)}"
)

print(
    f"DISCOVERY       : "
    f"{len(discovery)}"
)

print(
    f"VALIDATION      : "
    f"{len(validation)}"
)

print()

print(
    "BASE:"
)

print(
    f"ACT +{ACTIVATION:.0f}% "
    f"| TRAIL {TRAIL:.0f}% "
    f"| TIME {HORIZON}s "
    f"| COST {TOTAL_EXECUTION_COST:.0f}%"
)

print()

print(
    "CANDIDATE:"
)

print(
    f"BASE + emergency @ {EMERGENCY_TIME}s "
    f"if return < {EMERGENCY_THRESHOLD:.0f}%"
)

print()


# ============================================================
# A) SPLIT RESULTS
# ============================================================

print("="*155)
print(
    "A) BASE VS CANDIDATE"
)
print("="*155)

print(
    f"{'SPLIT':11} "
    f"{'SYSTEM':10} "
    f"{'N':>3} "
    f"{'AVG':>9} "
    f"{'MED':>9} "
    f"{'WIN':>8} "
    f"{'P10':>9} "
    f"{'P25':>9} "
    f"{'WORST':>9} "
    f"{'BEST':>9} "
    f"{'AVG EXIT':>10}"
)

print("-"*125)

for split_name, results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    for key, label in [
        ("base", "BASE"),
        ("candidate", "CAND"),
    ]:

        s = system_stats(
            results,
            key
        )

        print(
            f"{split_name:11} "
            f"{label:10} "
            f"{s['n']:3d} "
            f"{s['avg']:+8.2f}% "
            f"{s['med']:+8.2f}% "
            f"{s['win']:7.1f}% "
            f"{s['p10']:+8.2f}% "
            f"{s['p25']:+8.2f}% "
            f"{s['worst']:+8.2f}% "
            f"{s['best']:+8.2f}% "
            f"{s['avg_exit']:9.1f}s"
        )


# ============================================================
# B) DELTA
# ============================================================

print()
print("="*155)
print(
    "B) EMERGENCY EXIT IMPACT"
)
print("="*155)

for split_name, results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
    ("ALL", all_results),
]:

    d = delta_stats(
        results
    )

    print(
        f"{split_name:11} | "
        f"AVG DELTA={d['avg_delta']:+7.2f}% | "
        f"MED DELTA={d['med_delta']:+7.2f}% | "
        f"IMPROVED={d['improved']:2d} | "
        f"WORSENED={d['worsened']:2d} | "
        f"UNCHANGED={d['unchanged']:2d} | "
        f"BEST SAVE={d['best_improvement']:+7.2f}% | "
        f"WORST DAMAGE={d['worst_damage']:+7.2f}%"
    )


# ============================================================
# C) EXIT REASONS
# ============================================================

print()
print("="*155)
print(
    "C) EXIT REASONS"
)
print("="*155)

for split_name, results in [
    ("DISCOVERY", disc_results),
    ("VALIDATION", val_results),
]:

    b = system_stats(
        results,
        "base"
    )

    c = system_stats(
        results,
        "candidate"
    )

    print()
    print(split_name)
    print("-"*80)

    print(
        f"BASE      : "
        f"{b['reasons']}"
    )

    print(
        f"CANDIDATE : "
        f"{c['reasons']}"
    )


# ============================================================
# D) TOKEN DETAIL
# ============================================================

print()
print("="*155)
print(
    "D) TOKEN DETAIL"
)
print("="*155)

print(
    f"{'SPLIT':7} "
    f"{'TOKEN':20} "
    f"{'BASE':>9} "
    f"{'CAND':>9} "
    f"{'DELTA':>9} "
    f"{'B EXIT':>10} "
    f"{'C EXIT':>10} "
    f"{'C TIME':>8} "
    f"{'E75 RET':>9}"
)

print("-"*112)

for split_name, results in [
    ("DISC", disc_results),
    ("VALID", val_results),
]:

    for r in sorted(
        results,
        key=lambda x:
            x["delta"],
        reverse=True
    ):

        cand = r["candidate"]

        e75 = (
            cand["emergency_ret"]
            if valid(
                cand["emergency_ret"]
            )
            else None
        )

        print(
            f"{split_name:7} "
            f"{r['token'][:20]:20} "
            f"{r['base']['net']:+8.2f}% "
            f"{r['candidate']['net']:+8.2f}% "
            f"{r['delta']:+8.2f}% "
            f"{r['base']['reason']:>10} "
            f"{r['candidate']['reason']:>10} "
            f"{r['candidate']['exit_time']:7.1f}s "
            f"{(e75 if e75 is not None else 0):+8.2f}%"
        )


# ============================================================
# E) EMERGENCY-EXIT ONLY
# ============================================================

print()
print("="*155)
print(
    "E) TRADES ACTUALLY CHANGED BY EMERGENCY EXIT"
)
print("="*155)

changed = [
    r for r in all_results
    if r["candidate"]["reason"]
    == "EMERGENCY"
]

if not changed:

    print(
        "No trade triggered emergency exit."
    )

else:

    for r in changed:

        print(
            f"{r['token'][:20]:20} "
            f"| BASE={r['base']['net']:+7.2f}% "
            f"| EMERGENCY={r['candidate']['net']:+7.2f}% "
            f"| SAVE={r['delta']:+7.2f}% "
            f"| EXIT={r['candidate']['exit_time']:.1f}s "
            f"| RET_AT_EXIT={r['candidate']['gross']:+7.2f}%"
        )


# ============================================================
# F) DECISION
# ============================================================

print()
print("="*155)
print(
    "F) DECISION SUPPORT"
)
print("="*155)

ds = delta_stats(
    disc_results
)

vs = delta_stats(
    val_results
)

base_d = system_stats(
    disc_results,
    "base"
)

cand_d = system_stats(
    disc_results,
    "candidate"
)

base_v = system_stats(
    val_results,
    "base"
)

cand_v = system_stats(
    val_results,
    "candidate"
)

good = (
    cand_d["avg"] >= base_d["avg"]
    and cand_v["avg"] >= base_v["avg"]
    and cand_d["p10"] >= base_d["p10"]
    and cand_v["p10"] >= base_v["p10"]
    and vs["worsened"] == 0
)

if good:

    print(
        "CANDIDATE EMERGENCY EXIT PASSES HISTORICAL P&L CHECK"
    )

    print(
        "Next step: freeze it separately "
        "for prospective validation."
    )

else:

    print(
        "KEEP BASE EXECUTION"
    )

    print(
        "Emergency exit does not improve "
        "both splits cleanly enough."
    )

print()
print(
    "IMPORTANT:"
)

print(
    "• No thresholds were tuned in T30."
)

print(
    "• Emergency rule came from T29 only."
)

print(
    "• Historical boundary remains ID <= 545."
)

print(
    "• T23 remains untouched."
)

print(
    "• Do not modify the V2 signal from this result."
)

db.close()
