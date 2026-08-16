import sqlite3
import statistics
import math

DB = "memecoin_lab_sampler.db"

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row


def avg(xs):
    return sum(xs) / len(xs) if xs else None


def median(xs):
    return statistics.median(xs) if xs else None


def pct_positive(xs):
    return (
        100 * sum(x > 0 for x in xs) / len(xs)
        if xs else None
    )


def percentile(values, p):
    values = sorted(values)

    if not values:
        return None

    k = (len(values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return values[int(k)]

    return (
        values[f] * (c - k)
        + values[c] * (k - f)
    )


rows = db.execute("""
SELECT *
FROM feature_snapshots
WHERE return_60s IS NOT NULL
""").fetchall()

print()
print("=" * 90)
print("MEMECOIN LAB — EDGE ANALYSIS V0.5")
print("=" * 90)

print(f"Usable snapshots : {len(rows):,}")

if len(rows) < 30:
    print()
    print("PAS ASSEZ DE DONNEES.")
    print("Relance la collecte plus longtemps.")
    raise SystemExit


# ============================================================
# BASELINE
# ============================================================

horizons = [
    ("10s", "return_10s"),
    ("30s", "return_30s"),
    ("60s", "return_60s"),
    ("300s", "return_300s"),
]

print()
print("BASELINE")
print("-" * 90)

for label, col in horizons:

    vals = [
        r[col]
        for r in rows
        if r[col] is not None
    ]

    if not vals:
        continue

    print(
        f"{label:>5} | "
        f"N={len(vals):>5} | "
        f"AVG={avg(vals):>+9.3f}% | "
        f"MED={median(vals):>+9.3f}% | "
        f"WIN={pct_positive(vals):>6.1f}%"
    )


# ============================================================
# THRESHOLDS
# ============================================================

ba_values = [
    r["buyer_accel_fast"]
    for r in rows
    if r["buyer_accel_fast"] is not None
]

fa_values = [
    r["flow_accel_fast"]
    for r in rows
    if r["flow_accel_fast"] is not None
]

nf_values = [
    r["net_flow_30"]
    for r in rows
    if r["net_flow_30"] is not None
]

imb_values = [
    r["imbalance_30"]
    for r in rows
    if r["imbalance_30"] is not None
]

ba75 = percentile(ba_values, .75)
ba90 = percentile(ba_values, .90)

fa75 = percentile(fa_values, .75)
fa90 = percentile(fa_values, .90)

nf75 = percentile(nf_values, .75)

imb75 = percentile(imb_values, .75)


print()
print("THRESHOLDS")
print("-" * 90)

print(f"Buyer Accel P75 : {ba75:+.6f}")
print(f"Buyer Accel P90 : {ba90:+.6f}")

print(f"Flow Accel  P75 : {fa75:+.6f}")
print(f"Flow Accel  P90 : {fa90:+.6f}")

print(f"Net Flow    P75 : {nf75:+.6f}")
print(f"Imbalance   P75 : {imb75:+.6f}")


# ============================================================
# SIGNAL TEST
# ============================================================

signals = {

    "BA > P75":
        lambda r:
            r["buyer_accel_fast"] >= ba75,

    "BA > P90":
        lambda r:
            r["buyer_accel_fast"] >= ba90,

    "FA > P75":
        lambda r:
            r["flow_accel_fast"] >= fa75,

    "FA > P90":
        lambda r:
            r["flow_accel_fast"] >= fa90,

    "NETFLOW > P75":
        lambda r:
            r["net_flow_30"] >= nf75,

    "IMBALANCE > P75":
        lambda r:
            r["imbalance_30"] >= imb75,

    "BA+FA P75":
        lambda r:
            (
                r["buyer_accel_fast"] >= ba75
                and r["flow_accel_fast"] >= fa75
            ),

    "BA+FA+FLOW":
        lambda r:
            (
                r["buyer_accel_fast"] >= ba75
                and r["flow_accel_fast"] >= fa75
                and r["net_flow_30"] >= nf75
            ),

    "FPA CANDIDATE":
        lambda r:
            (
                r["buyer_accel_fast"] >= ba75
                and r["flow_accel_fast"] >= fa75
                and r["net_flow_30"] > 0
                and r["imbalance_30"] > 0
                and (
                    r["price_change_30"] is None
                    or r["price_change_30"] < 5
                )
            )
}


print()
print("=" * 90)
print("SIGNAL RESULTS")
print("=" * 90)

for name, condition in signals.items():

    selected = [
        r for r in rows
        if condition(r)
    ]

    print()
    print(name)
    print("-" * 90)

    print(
        f"Signals: {len(selected):,} "
        f"({100*len(selected)/len(rows):.1f}% des snapshots)"
    )

    if len(selected) < 5:
        print("Trop peu d'observations.")
        continue

    for label, col in horizons:

        vals = [
            r[col]
            for r in selected
            if r[col] is not None
        ]

        if not vals:
            continue

        print(
            f"{label:>5} | "
            f"N={len(vals):>5} | "
            f"AVG={avg(vals):>+9.3f}% | "
            f"MED={median(vals):>+9.3f}% | "
            f"WIN={pct_positive(vals):>6.1f}%"
        )


# ============================================================
# TOP COMBINED SIGNALS
# ============================================================

scored = []

for r in rows:

    score = (
        r["buyer_accel_fast"] * 10
        + r["flow_accel_fast"]
        + max(r["imbalance_30"], 0)
    )

    scored.append(
        (
            score,
            r["token_mint"],
            r["return_10s"],
            r["return_30s"],
            r["return_60s"],
            r["return_300s"]
        )
    )

scored.sort(
    reverse=True,
    key=lambda x: x[0]
)

print()
print("=" * 90)
print("TOP 15 RAW SIGNALS")
print("=" * 90)

for x in scored[:15]:

    score, token, r10, r30, r60, r300 = x

    def fmt(v):
        return (
            f"{v:+.2f}%"
            if v is not None
            else "NA"
        )

    print(
        f"{token[:12]}... | "
        f"SCORE {score:+.4f} | "
        f"10s {fmt(r10)} | "
        f"30s {fmt(r30)} | "
        f"60s {fmt(r60)} | "
        f"300s {fmt(r300)}"
    )

print()
print("=" * 90)
print("DONE")
print("=" * 90)

db.close()
