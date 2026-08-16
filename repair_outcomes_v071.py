import sqlite3
import statistics

DB = "validation_v070.db"

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row

print()
print("=" * 90)
print("MEMECOIN LAB — OUTCOME REPAIR V0.7.1")
print("=" * 90)

# ============================================================
# IMPORTANT
#
# V0.7 utilisait une tolérance future de seulement 15 secondes.
# Avec notre sampler 25%, le prochain swap observé peut arriver
# bien après la cible.
#
# Ici on affiche plusieurs tolérances pour voir ce que permet
# réellement le dataset sans inventer de prix.
# ============================================================

HORIZONS = [10, 30, 60, 300]

# tolerance maximum après l'horizon demandé
TOLERANCES = {
    10: 60,
    30: 60,
    60: 90,
    300: 120,
}


def get_future_price(token, target, tolerance):

    row = db.execute("""
        SELECT
            timestamp,
            clean_price

        FROM swaps

        WHERE
            token_mint = ?
            AND price_valid = 1
            AND clean_price IS NOT NULL
            AND timestamp >= ?

        ORDER BY timestamp ASC
        LIMIT 1
    """, (
        token,
        target
    )).fetchone()

    if not row:
        return None, None

    delay = row["timestamp"] - target

    if delay > tolerance:
        return None, delay

    return row["clean_price"], delay


def calc_return(start, future):

    if (
        start is None
        or future is None
        or start <= 0
        or future <= 0
    ):
        return None

    return (
        future / start - 1
    ) * 100


signals = db.execute("""
    SELECT *
    FROM signals
    ORDER BY timestamp ASC
""").fetchall()

print(f"SIGNALS : {len(signals)}")

if not signals:
    print("Aucun signal à analyser.")
    raise SystemExit


# ============================================================
# RESET OUTCOMES
# ============================================================

db.execute("""
UPDATE signals
SET
    return_10s=NULL,
    return_30s=NULL,
    return_60s=NULL,
    return_300s=NULL,
    done_10=0,
    done_30=0,
    done_60=0,
    done_300=0
""")

db.commit()


# ============================================================
# REBUILD
# ============================================================

counts = {
    h: {
        "ok": 0,
        "missing": 0,
        "delays": []
    }
    for h in HORIZONS
}

for signal in signals:

    updates = {}

    for h in HORIZONS:

        target = (
            signal["timestamp"] + h
        )

        future, delay = get_future_price(
            signal["token_mint"],
            target,
            TOLERANCES[h]
        )

        result = calc_return(
            signal["price"],
            future
        )

        if result is None:

            counts[h]["missing"] += 1

        else:

            counts[h]["ok"] += 1

            counts[h]["delays"].append(
                delay
            )

        updates[h] = result

    db.execute("""
        UPDATE signals

        SET
            return_10s=?,
            return_30s=?,
            return_60s=?,
            return_300s=?,

            done_10=1,
            done_30=1,
            done_60=1,
            done_300=1

        WHERE id=?
    """, (
        updates[10],
        updates[30],
        updates[60],
        updates[300],
        signal["id"]
    ))

db.commit()


# ============================================================
# COVERAGE
# ============================================================

print()
print("=" * 90)
print("OUTCOME COVERAGE")
print("=" * 90)

for h in HORIZONS:

    c = counts[h]

    total = (
        c["ok"]
        + c["missing"]
    )

    coverage = (
        c["ok"] / total * 100
        if total
        else 0
    )

    if c["delays"]:

        median_delay = statistics.median(
            c["delays"]
        )

        max_delay = max(
            c["delays"]
        )

    else:

        median_delay = None
        max_delay = None

    print(
        f"{h:>3}s | "
        f"OK={c['ok']:>3} | "
        f"MISSING={c['missing']:>3} | "
        f"COVERAGE={coverage:5.1f}% | "
        f"MED_DELAY={median_delay} | "
        f"MAX_DELAY={max_delay}"
    )


# ============================================================
# RESULTS BY SIGNAL
# ============================================================

print()
print("=" * 90)
print("PRELIMINARY OOS RESULTS")
print("=" * 90)


def percentile(vals, p):

    vals = sorted(vals)

    if not vals:
        return None

    pos = (len(vals) - 1) * p

    low = int(pos)
    high = min(
        low + 1,
        len(vals) - 1
    )

    weight = pos - low

    return (
        vals[low] * (1 - weight)
        + vals[high] * weight
    )


signal_types = [
    "FA_P90",
    "FA_P95",
    "EXTREME_BUY",
    "FPA",
]


for signal_type in signal_types:

    rows = db.execute("""
        SELECT *
        FROM signals
        WHERE signal_type=?
        ORDER BY timestamp
    """, (
        signal_type,
    )).fetchall()

    print()
    print(signal_type)
    print("-" * 90)
    print(f"SIGNALS = {len(rows)}")

    for h in HORIZONS:

        col = f"return_{h}s"

        vals = [
            r[col]
            for r in rows
            if r[col] is not None
        ]

        if not vals:
            print(
                f"{h:>3}s | N=0"
            )
            continue

        avg = statistics.mean(vals)
        med = statistics.median(vals)

        win = (
            sum(x > 0 for x in vals)
            / len(vals)
            * 100
        )

        p10 = percentile(vals, .10)
        p90 = percentile(vals, .90)

        print(
            f"{h:>3}s | "
            f"N={len(vals):>3} | "
            f"AVG={avg:+8.2f}% | "
            f"MED={med:+8.2f}% | "
            f"WIN={win:5.1f}% | "
            f"P10={p10:+8.2f}% | "
            f"P90={p90:+8.2f}%"
        )


# ============================================================
# RAW SIGNAL DETAIL
# ============================================================

print()
print("=" * 90)
print("SIGNAL DETAIL")
print("=" * 90)

rows = db.execute("""
SELECT
    signal_type,
    token_mint,
    flow_accel_fast,
    net_flow_30,
    imbalance_30,
    return_10s,
    return_30s,
    return_60s,
    return_300s

FROM signals

ORDER BY timestamp ASC
""").fetchall()


def fmt(x):

    if x is None:
        return "NA"

    return f"{x:+.2f}%"


for r in rows:

    print(
        f"{r['signal_type']:12} | "
        f"{r['token_mint'][:12]}... | "
        f"FA={r['flow_accel_fast']:+.4f} | "
        f"NF={r['net_flow_30']:+.3f} | "
        f"10={fmt(r['return_10s'])} | "
        f"30={fmt(r['return_30s'])} | "
        f"60={fmt(r['return_60s'])} | "
        f"300={fmt(r['return_300s'])}"
    )


print()
print("=" * 90)
print("REPAIR DONE")
print("=" * 90)

db.close()
