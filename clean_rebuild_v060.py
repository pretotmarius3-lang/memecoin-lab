import sqlite3
import statistics
import math
from collections import defaultdict

DB = "memecoin_lab_sampler.db"

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA busy_timeout=5000")

print()
print("=" * 90)
print("MEMECOIN LAB — CLEAN REBUILD V0.6")
print("=" * 90)

# ============================================================
# RESET CLEAN FEATURES
# ============================================================

db.execute("DROP TABLE IF EXISTS clean_feature_snapshots")

db.execute("""
CREATE TABLE clean_feature_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    timestamp REAL,
    token_mint TEXT,
    last_price REAL,

    swaps_5 INTEGER,
    swaps_10 INTEGER,
    swaps_30 INTEGER,
    swaps_60 INTEGER,

    buyers_5 INTEGER,
    buyers_10 INTEGER,
    buyers_30 INTEGER,
    buyers_60 INTEGER,

    sellers_5 INTEGER,
    sellers_10 INTEGER,
    sellers_30 INTEGER,
    sellers_60 INTEGER,

    buy_vol_5 REAL,
    buy_vol_10 REAL,
    buy_vol_30 REAL,
    buy_vol_60 REAL,

    sell_vol_5 REAL,
    sell_vol_10 REAL,
    sell_vol_30 REAL,
    sell_vol_60 REAL,

    net_flow_5 REAL,
    net_flow_10 REAL,
    net_flow_30 REAL,
    net_flow_60 REAL,

    imbalance_5 REAL,
    imbalance_10 REAL,
    imbalance_30 REAL,
    imbalance_60 REAL,

    buyer_velocity_5 REAL,
    buyer_velocity_10 REAL,
    buyer_velocity_30 REAL,

    buyer_accel_fast REAL,
    buyer_accel_slow REAL,

    flow_velocity_5 REAL,
    flow_velocity_10 REAL,
    flow_velocity_30 REAL,

    flow_accel_fast REAL,
    flow_accel_slow REAL,

    price_change_10 REAL,
    price_change_30 REAL,
    price_change_60 REAL,

    return_10s REAL,
    return_30s REAL,
    return_60s REAL,
    return_300s REAL
)
""")

db.execute("""
CREATE INDEX idx_clean_features_token_time
ON clean_feature_snapshots(token_mint, timestamp)
""")

db.commit()

# ============================================================
# LOAD CLEAN SWAPS
# ============================================================

rows = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    ABS(sol_delta) AS sol,
    clean_price AS price
FROM clean_swaps
WHERE
    price_valid = 1
    AND clean_price IS NOT NULL
ORDER BY token_mint, timestamp
""").fetchall()

tokens = defaultdict(list)

for r in rows:
    tokens[r["token_mint"]].append({
        "t": r["timestamp"],
        "wallet": r["wallet"],
        "side": r["side"],
        "sol": r["sol"],
        "price": r["price"],
    })

print(f"CLEAN SWAPS  : {len(rows):,}")
print(f"CLEAN TOKENS : {len(tokens):,}")

# ============================================================
# HELPERS
# ============================================================

def window(events, now, seconds):

    data = [
        x for x in events
        if now - seconds <= x["t"] <= now
    ]

    buys = [x for x in data if x["side"] == "BUY"]
    sells = [x for x in data if x["side"] == "SELL"]

    buyers = len(set(x["wallet"] for x in buys))
    sellers = len(set(x["wallet"] for x in sells))

    bv = sum(x["sol"] for x in buys)
    sv = sum(x["sol"] for x in sells)

    nf = bv - sv
    tv = bv + sv

    imb = nf / tv if tv else 0.0

    return {
        "swaps": len(data),
        "buyers": buyers,
        "sellers": sellers,
        "bv": bv,
        "sv": sv,
        "nf": nf,
        "imb": imb
    }


def price_before(events, target):

    candidate = None

    for x in events:
        if x["t"] <= target:
            candidate = x["price"]
        else:
            break

    return candidate


def future_price(events, target, tolerance=15):

    for x in events:

        if x["t"] >= target:

            if x["t"] - target <= tolerance:
                return x["price"]

            return None

    return None


def ret(a, b):

    if (
        a is None
        or b is None
        or a <= 0
        or b <= 0
    ):
        return None

    return (b / a - 1) * 100


def historical_change(events, now, current, seconds):

    old = price_before(
        events,
        now - seconds
    )

    return ret(old, current)

# ============================================================
# CREATE SNAPSHOTS
#
# IMPORTANT:
# We create snapshots at actual swap timestamps.
# No duplicated 5-second snapshots of stale state.
# ============================================================

created = 0

for token, events in tokens.items():

    if len(events) < 2:
        continue

    for i, event in enumerate(events):

        now = event["t"]
        price = event["price"]

        history = events[:i+1]

        s5 = window(history, now, 5)
        s10 = window(history, now, 10)
        s30 = window(history, now, 30)
        s60 = window(history, now, 60)

        bv5 = s5["buyers"] / 5
        bv10 = s10["buyers"] / 10
        bv30 = s30["buyers"] / 30

        ba_fast = bv5 - bv10
        ba_slow = bv10 - bv30

        fv5 = s5["nf"] / 5
        fv10 = s10["nf"] / 10
        fv30 = s30["nf"] / 30

        fa_fast = fv5 - fv10
        fa_slow = fv10 - fv30

        pc10 = historical_change(
            history, now, price, 10
        )

        pc30 = historical_change(
            history, now, price, 30
        )

        pc60 = historical_change(
            history, now, price, 60
        )

        fp10 = future_price(events, now + 10)
        fp30 = future_price(events, now + 30)
        fp60 = future_price(events, now + 60)
        fp300 = future_price(events, now + 300)

        r10 = ret(price, fp10)
        r30 = ret(price, fp30)
        r60 = ret(price, fp60)
        r300 = ret(price, fp300)

        db.execute("""
        INSERT INTO clean_feature_snapshots (
            timestamp,
            token_mint,
            last_price,

            swaps_5,
            swaps_10,
            swaps_30,
            swaps_60,

            buyers_5,
            buyers_10,
            buyers_30,
            buyers_60,

            sellers_5,
            sellers_10,
            sellers_30,
            sellers_60,

            buy_vol_5,
            buy_vol_10,
            buy_vol_30,
            buy_vol_60,

            sell_vol_5,
            sell_vol_10,
            sell_vol_30,
            sell_vol_60,

            net_flow_5,
            net_flow_10,
            net_flow_30,
            net_flow_60,

            imbalance_5,
            imbalance_10,
            imbalance_30,
            imbalance_60,

            buyer_velocity_5,
            buyer_velocity_10,
            buyer_velocity_30,

            buyer_accel_fast,
            buyer_accel_slow,

            flow_velocity_5,
            flow_velocity_10,
            flow_velocity_30,

            flow_accel_fast,
            flow_accel_slow,

            price_change_10,
            price_change_30,
            price_change_60,

            return_10s,
            return_30s,
            return_60s,
            return_300s
        )

        VALUES (
            ?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,
            ?,?,
            ?,?,?,
            ?,?,
            ?,?,?,
            ?,?,?,?
        )
        """, (
            now,
            token,
            price,

            s5["swaps"],
            s10["swaps"],
            s30["swaps"],
            s60["swaps"],

            s5["buyers"],
            s10["buyers"],
            s30["buyers"],
            s60["buyers"],

            s5["sellers"],
            s10["sellers"],
            s30["sellers"],
            s60["sellers"],

            s5["bv"],
            s10["bv"],
            s30["bv"],
            s60["bv"],

            s5["sv"],
            s10["sv"],
            s30["sv"],
            s60["sv"],

            s5["nf"],
            s10["nf"],
            s30["nf"],
            s60["nf"],

            s5["imb"],
            s10["imb"],
            s30["imb"],
            s60["imb"],

            bv5,
            bv10,
            bv30,

            ba_fast,
            ba_slow,

            fv5,
            fv10,
            fv30,

            fa_fast,
            fa_slow,

            pc10,
            pc30,
            pc60,

            r10,
            r30,
            r60,
            r300
        ))

        created += 1

db.commit()

print(f"SNAPSHOTS    : {created:,}")

# ============================================================
# ANALYSIS
# ============================================================

analysis = db.execute("""
SELECT *
FROM clean_feature_snapshots
WHERE return_60s IS NOT NULL
""").fetchall()

print(f"60s USABLE   : {len(analysis):,}")

if len(analysis) < 20:
    print("Pas assez de données.")
    db.close()
    raise SystemExit

# ============================================================
# STAT HELPERS
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


def report(name, selected):

    print()
    print(name)
    print("-" * 90)

    print(f"N = {len(selected):,}")

    for label, col in [
        ("10s", "return_10s"),
        ("30s", "return_30s"),
        ("60s", "return_60s"),
        ("300s", "return_300s"),
    ]:

        vals = [
            x[col]
            for x in selected
            if x[col] is not None
        ]

        if not vals:
            continue

        mean = statistics.mean(vals)
        med = statistics.median(vals)
        win = (
            sum(x > 0 for x in vals)
            / len(vals)
            * 100
        )

        p10 = percentile(vals, .10)
        p90 = percentile(vals, .90)

        print(
            f"{label:>4} | "
            f"N={len(vals):>4} | "
            f"AVG={mean:+8.3f}% | "
            f"MED={med:+8.3f}% | "
            f"WIN={win:5.1f}% | "
            f"P10={p10:+8.2f}% | "
            f"P90={p90:+8.2f}%"
        )

# ============================================================
# THRESHOLDS
# ============================================================

fa_values = [
    x["flow_accel_fast"]
    for x in analysis
]

ba_values = [
    x["buyer_accel_fast"]
    for x in analysis
]

nf_values = [
    x["net_flow_30"]
    for x in analysis
]

imb_values = [
    x["imbalance_30"]
    for x in analysis
]

fa75 = percentile(fa_values, .75)
fa90 = percentile(fa_values, .90)
fa95 = percentile(fa_values, .95)

ba90 = percentile(ba_values, .90)
nf75 = percentile(nf_values, .75)
imb75 = percentile(imb_values, .75)

print()
print("=" * 90)
print("CLEAN THRESHOLDS")
print("=" * 90)

print(f"FA P75  : {fa75:+.8f}")
print(f"FA P90  : {fa90:+.8f}")
print(f"FA P95  : {fa95:+.8f}")
print(f"BA P90  : {ba90:+.8f}")
print(f"NF P75  : {nf75:+.8f}")
print(f"IMB P75 : {imb75:+.8f}")

# ============================================================
# SIGNALS
# ============================================================

report(
    "BASELINE",
    analysis
)

report(
    "FLOW ACCEL > P75",
    [
        x for x in analysis
        if x["flow_accel_fast"] >= fa75
    ]
)

report(
    "FLOW ACCEL > P90",
    [
        x for x in analysis
        if x["flow_accel_fast"] >= fa90
    ]
)

report(
    "FLOW ACCEL > P95",
    [
        x for x in analysis
        if x["flow_accel_fast"] >= fa95
    ]
)

report(
    "BUYER ACCEL > P90",
    [
        x for x in analysis
        if x["buyer_accel_fast"] >= ba90
    ]
)

report(
    "NET FLOW > P75",
    [
        x for x in analysis
        if x["net_flow_30"] >= nf75
    ]
)

report(
    "IMBALANCE > P75",
    [
        x for x in analysis
        if x["imbalance_30"] >= imb75
    ]
)

# Flow acceleration WITHOUT already extended price
fpa = [
    x for x in analysis

    if (
        x["flow_accel_fast"] >= fa90

        and x["net_flow_30"] > 0

        and x["imbalance_30"] > 0

        and (
            x["price_change_30"] is None
            or x["price_change_30"] < 10
        )
    )
]

report(
    "FPA CLEAN CANDIDATE",
    fpa
)

# ============================================================
# CONTRARIAN TEST
# ============================================================

contrarian = [
    x for x in analysis

    if (
        x["net_flow_30"] >= nf75
        and x["imbalance_30"] >= imb75
    )
]

report(
    "EXTREME BUY PRESSURE",
    contrarian
)

print()
print("=" * 90)
print("CLEAN REBUILD DONE")
print("=" * 90)

db.close()
