import sqlite3
import statistics
import math

DB = "validation_v090.db"

MAX_ID = 545
VOLUME_CUT = 8837.925

HORIZON = 120
TRAIL = 3.0

# On teste uniquement le hard stop.
HARD_STOPS = [None, -5.0, -10.0, -15.0, -20.0]

# Trailing activations déjà vues dans T25.
ACTIVATIONS = [5.0, 10.0, 15.0]

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
    e.timestamp,
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


# ============================================================
# SIMULATOR
# ============================================================

def simulate(signal, activation, hard_stop):

    rows = path(signal)

    if not rows:
        return None

    entry = signal["entry_price"]

    if not valid(entry) or entry <= 0:
        return None

    peak = entry

    trail_active = False

    max_ret = 0.0
    min_ret = 0.0

    exit_price = None
    exit_time = None
    reason = "TIME"

    runner_before_exit = False

    for row in rows:

        price = row["price_usd"]

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

        if current >= 10:
            runner_before_exit = True

        # ----------------------------------------------------
        # HARD STOP
        # ----------------------------------------------------

        if (
            hard_stop is not None
            and current <= hard_stop
        ):

            # Conservative execution:
            # use observed snapshot, not theoretical stop.
            exit_price = price
            exit_time = (
                row["timestamp"]
                - signal["entry_timestamp"]
            )

            reason = "STOP"
            break

        # ----------------------------------------------------
        # TRAILING
        # ----------------------------------------------------

        if price > peak:
            peak = price

        peak_ret = ret(
            entry,
            peak
        )

        if (
            not trail_active
            and valid(peak_ret)
            and peak_ret >= activation
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

        exit_price = rows[-1]["price_usd"]

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

        "trail_active":
            trail_active,

        "runner_before_exit":
            runner_before_exit,

        # Did a stop kill something
        # that had been or would appear runner-like?
        "stopped_after_runner":
            (
                reason == "STOP"
                and runner_before_exit
            ),
    }


# ============================================================
# STATS
# ============================================================

def stats(results):

    rr = [
        x for x in results
        if x is not None
    ]

    if not rr:
        return None

    vals = [
        x["net"]
        for x in rr
    ]

    stops = [
        x for x in rr
        if x["reason"] == "STOP"
    ]

    trails = [
        x for x in rr
        if x["reason"] == "TRAIL"
    ]

    timeouts = [
        x for x in rr
        if x["reason"] == "TIME"
    ]

    killed = [
        x for x in rr
        if x["stopped_after_runner"]
    ]

    return {
        "n":
            len(rr),

        "avg":
            avg(vals),

        "med":
            med(vals),

        "win":
            100*sum(x > 0 for x in vals)/len(vals),

        "p10":
            percentile(vals,.10),

        "worst":
            min(vals),

        "best":
            max(vals),

        "stops":
            len(stops),

        "trails":
            len(trails),

        "timeouts":
            len(timeouts),

        "killed":
            len(killed),

        "avg_exit":
            avg([
                x["exit_time"]
                for x in rr
            ]),
    }


def run_subset(
    subset,
    activation,
    hard_stop
):

    return [
        simulate(
            s,
            activation,
            hard_stop
        )
        for s in subset
    ]


# ============================================================
# OUTPUT
# ============================================================

print("="*150)
print(
    "MEMECOIN LAB — "
    "T26 HARD STOP + TRAILING 120s LAB"
)
print("="*150)

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

print(
    f"TRAIL           : "
    f"{TRAIL:.1f}%"
)

print(
    f"TIME STOP       : "
    f"{HORIZON}s"
)

print(
    f"EXECUTION COST  : "
    f"{TOTAL_EXECUTION_COST:.1f}%"
)

print()


# ============================================================
# GRID
# ============================================================

grid = []

for activation in ACTIVATIONS:

    for hard_stop in HARD_STOPS:

        dr = run_subset(
            discovery,
            activation,
            hard_stop
        )

        vr = run_subset(
            validation,
            activation,
            hard_stop
        )

        ds = stats(dr)
        vs = stats(vr)

        if not ds or not vs:
            continue

        grid.append({
            "activation":
                activation,

            "stop":
                hard_stop,

            "disc":
                ds,

            "valid":
                vs,

            "disc_results":
                dr,

            "val_results":
                vr,
        })


print("="*150)
print(
    "A) HARD STOP GRID"
)
print("="*150)

print(
    f"{'ACT':>5} "
    f"{'STOP':>7} | "
    f"{'D AVG':>8} "
    f"{'D MED':>8} "
    f"{'D P10':>8} "
    f"{'D WST':>8} "
    f"{'D WIN':>7} | "
    f"{'V AVG':>8} "
    f"{'V MED':>8} "
    f"{'V P10':>8} "
    f"{'V WST':>8} "
    f"{'V WIN':>7}"
)

print("-"*130)

for x in grid:

    d = x["disc"]
    v = x["valid"]

    stop_txt = (
        "NONE"
        if x["stop"] is None
        else f"{x['stop']:.0f}%"
    )

    print(
        f"{x['activation']:5.0f} "
        f"{stop_txt:>7} | "
        f"{d['avg']:+7.2f}% "
        f"{d['med']:+7.2f}% "
        f"{d['p10']:+7.2f}% "
        f"{d['worst']:+7.2f}% "
        f"{d['win']:6.1f}% | "
        f"{v['avg']:+7.2f}% "
        f"{v['med']:+7.2f}% "
        f"{v['p10']:+7.2f}% "
        f"{v['worst']:+7.2f}% "
        f"{v['win']:6.1f}%"
    )


# ============================================================
# ROBUST RULES
# ============================================================

robust = [
    x for x in grid
    if (
        x["disc"]["avg"] > 0
        and x["valid"]["avg"] > 0
        and x["disc"]["med"] > 0
        and x["valid"]["med"] > 0
    )
]

robust.sort(
    key=lambda x:
        (
            x["valid"]["p10"],
            x["valid"]["avg"],
            x["valid"]["med"],
        ),
    reverse=True
)

print()
print("="*150)
print(
    "B) ROBUST RULES — "
    "AVG + MED POSITIVE IN BOTH SPLITS"
)
print("="*150)

if not robust:

    print(
        "No rule passes."
    )

else:

    for x in robust:

        stop_txt = (
            "NONE"
            if x["stop"] is None
            else f"{x['stop']:.0f}%"
        )

        d = x["disc"]
        v = x["valid"]

        print(
            f"ACT={x['activation']:.0f}% "
            f"| STOP={stop_txt:>5} "
            f"| DISC AVG/MED/P10="
            f"{d['avg']:+.2f}/"
            f"{d['med']:+.2f}/"
            f"{d['p10']:+.2f}% "
            f"| VALID AVG/MED/P10="
            f"{v['avg']:+.2f}/"
            f"{v['med']:+.2f}/"
            f"{v['p10']:+.2f}%"
        )


# ============================================================
# BEST ROBUST
# ============================================================

if robust:

    best = robust[0]

    print()
    print("="*150)
    print(
        "C) BEST ROBUST DOWNSIDE RULE"
    )
    print("="*150)

    stop_txt = (
        "NONE"
        if best["stop"] is None
        else f"{best['stop']:.0f}%"
    )

    print(
        f"ACTIVATION : "
        f"+{best['activation']:.0f}%"
    )

    print(
        f"TRAIL      : "
        f"{TRAIL:.0f}%"
    )

    print(
        f"HARD STOP  : "
        f"{stop_txt}"
    )

    print(
        f"TIME STOP  : "
        f"{HORIZON}s"
    )

    for name, st in [
        ("DISCOVERY", best["disc"]),
        ("VALIDATION", best["valid"]),
    ]:

        print()
        print(
            f"{name}"
        )

        print(
            f"AVG={st['avg']:+.2f}% | "
            f"MED={st['med']:+.2f}% | "
            f"WIN={st['win']:.1f}% | "
            f"P10={st['p10']:+.2f}% | "
            f"WORST={st['worst']:+.2f}% | "
            f"BEST={st['best']:+.2f}%"
        )

        print(
            f"STOP exits={st['stops']} | "
            f"TRAIL exits={st['trails']} | "
            f"TIME exits={st['timeouts']} | "
            f"AVG EXIT={st['avg_exit']:.1f}s"
        )

        print(
            f"STOPPED AFTER ALREADY HITTING +10%="
            f"{st['killed']}"
        )


# ============================================================
# VALIDATION DETAIL
# ============================================================

if robust:

    print()
    print("="*150)
    print(
        "D) BEST RULE — VALIDATION TOKEN DETAIL"
    )
    print("="*150)

    print(
        f"{'TOKEN':20} "
        f"{'NET':>9} "
        f"{'MAX':>9} "
        f"{'MIN':>9} "
        f"{'EXIT':>8} "
        f"{'TIME':>8}"
    )

    print("-"*72)

    for r in sorted(
        [
            x for x in
            best["val_results"]
            if x is not None
        ],
        key=lambda x:
            x["net"],
        reverse=True
    ):

        print(
            f"{r['token'][:20]:20} "
            f"{r['net']:+8.2f}% "
            f"{r['max_ret']:+8.2f}% "
            f"{r['min_ret']:+8.2f}% "
            f"{r['reason']:>8} "
            f"{r['exit_time']:7.1f}s"
        )


print()
print("="*150)
print("IMPORTANT")
print("="*150)

print("""
• Historical ID <= 545 only.
• T23 remains the prospective signal validator.
• We are testing ONLY hard-stop sensitivity.
• Stop execution uses the observed DEX snapshot, therefore
  real gap/slippage through the stop is preserved approximately.
• A hard stop is useful only if downside improves WITHOUT
  destroying the runner tail.
• With 21 tokens this is still exploratory.
""")

db.close()
