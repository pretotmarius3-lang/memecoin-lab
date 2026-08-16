import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"

# ID figé de notre vraie validation V2
OOS_START_ID = 162

HORIZONS = [30, 60]

# Minimum d'observations pour afficher un croisement
MIN_N_DISCOVERY = 5
MIN_N_OOS = 2


# ============================================================
# HELPERS
# ============================================================

def percentile(values, p):
    values = sorted(
        x for x in values
        if x is not None
        and math.isfinite(x)
    )

    if not values:
        return None

    k = (len(values) - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return values[lo]

    return (
        values[lo] * (hi-k)
        + values[hi] * (k-lo)
    )


def stats(values):
    values = [
        x for x in values
        if x is not None
        and math.isfinite(x)
    ]

    if not values:
        return None

    return {
        "n": len(values),
        "avg": statistics.mean(values),
        "med": statistics.median(values),
        "win": 100 * sum(x > 0 for x in values) / len(values),
        "p10": percentile(values, .10),
        "worst": min(values),
        "best": max(values),
        "crash10": 100 * sum(x <= -10 for x in values) / len(values),
    }


def safe_ratio(a, b):
    if a is None or b is None:
        return None

    total = a + b

    if total <= 0:
        return None

    return a / total


def getv(row, name):
    try:
        return row[name]
    except Exception:
        return None


# ============================================================
# LOAD DATA
# ============================================================

def load():

    db = sqlite3.connect(
        DB,
        timeout=30
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")

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

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,
            d.fdv,
            d.buys_m5,
            d.sells_m5

        FROM events e

        LEFT JOIN first_dex d
        ON d.event_id = e.id

        WHERE e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()

    db.close()

    return rows


# ============================================================
# BUILD DYNAMIC THRESHOLDS
# ============================================================

def thresholds(rows):

    def vals(field):
        return [
            getv(r, field)
            for r in rows
            if getv(r, field) is not None
            and getv(r, field) > 0
        ]

    volume = vals("volume_m5")
    liq = vals("liquidity_usd")
    mcap = vals("market_cap")

    return {
        "vol50": percentile(volume, .50),
        "vol75": percentile(volume, .75),

        "liq25": percentile(liq, .25),
        "liq50": percentile(liq, .50),
        "liq75": percentile(liq, .75),

        "mc50": percentile(mcap, .50),
    }


# ============================================================
# SIGNAL DEFINITIONS
# ============================================================

def build_signals(t):

    vol50 = t["vol50"]
    vol75 = t["vol75"]

    liq25 = t["liq25"]
    liq50 = t["liq50"]
    liq75 = t["liq75"]

    mc50 = t["mc50"]

    def fa95(r):
        return getv(r, "fa95") == 1

    def new30_2(r):
        v = getv(r, "new_wallets30")
        return v is not None and v >= 2

    def new30_3(r):
        v = getv(r, "new_wallets30")
        return v is not None and v >= 3

    def new10_1(r):
        v = getv(r, "new_wallets10")
        return v is not None and v >= 1

    def vol_hi(r):
        v = getv(r, "volume_m5")
        return (
            vol50 is not None
            and v is not None
            and v >= vol50
        )

    def vol_top(r):
        v = getv(r, "volume_m5")
        return (
            vol75 is not None
            and v is not None
            and v >= vol75
        )

    def vol_frozen(r):
        v = getv(r, "volume_m5")
        return (
            v is not None
            and v >= 8837.925
        )

    def liq_mid(r):
        v = getv(r, "liquidity_usd")
        return (
            liq25 is not None
            and liq75 is not None
            and v is not None
            and liq25 <= v <= liq75
        )

    def liq_above_med(r):
        v = getv(r, "liquidity_usd")
        return (
            liq50 is not None
            and v is not None
            and v >= liq50
        )

    def mcap_hi(r):
        v = getv(r, "market_cap")
        return (
            mc50 is not None
            and v is not None
            and v >= mc50
        )

    def buy60(r):
        ratio = safe_ratio(
            getv(r, "buys_m5"),
            getv(r, "sells_m5")
        )

        return (
            ratio is not None
            and ratio >= .60
        )

    def buy70(r):
        ratio = safe_ratio(
            getv(r, "buys_m5"),
            getv(r, "sells_m5")
        )

        return (
            ratio is not None
            and ratio >= .70
        )

    def nf_positive(r):
        v = getv(r, "net_flow")
        return v is not None and v > 0

    def conc_low(r):
        v = getv(r, "concentration")
        return v is not None and v < .90

    def conc_very_low(r):
        v = getv(r, "concentration")
        return v is not None and v < .75

    signals = {
        "FA95":
            lambda r: fa95(r),

        "FA95 + NEW30>=2":
            lambda r:
                fa95(r)
                and new30_2(r),

        "FA95 + NEW30>=3":
            lambda r:
                fa95(r)
                and new30_3(r),

        "FA95 + NEW10>=1":
            lambda r:
                fa95(r)
                and new10_1(r),

        "FA95 + VOL>P50":
            lambda r:
                fa95(r)
                and vol_hi(r),

        "FA95 + VOL>P75":
            lambda r:
                fa95(r)
                and vol_top(r),

        # IMPORTANT:
        # This is the frozen V2 candidate.
        "V2 FROZEN":
            lambda r:
                fa95(r)
                and new30_2(r)
                and vol_frozen(r),

        "FA95 + NEW30>=2 + VOL>P50":
            lambda r:
                fa95(r)
                and new30_2(r)
                and vol_hi(r),

        "FA95 + NEW30>=3 + VOL>P50":
            lambda r:
                fa95(r)
                and new30_3(r)
                and vol_hi(r),

        "FA95 + BUY_RATIO>=60%":
            lambda r:
                fa95(r)
                and buy60(r),

        "FA95 + BUY_RATIO>=70%":
            lambda r:
                fa95(r)
                and buy70(r),

        "FA95 + NEW30>=2 + BUY60":
            lambda r:
                fa95(r)
                and new30_2(r)
                and buy60(r),

        "FA95 + LIQ P25-P75":
            lambda r:
                fa95(r)
                and liq_mid(r),

        "FA95 + LIQ>=P50":
            lambda r:
                fa95(r)
                and liq_above_med(r),

        "FA95 + NEW30>=2 + LIQ MID":
            lambda r:
                fa95(r)
                and new30_2(r)
                and liq_mid(r),

        "FA95 + VOL>P50 + LIQ MID":
            lambda r:
                fa95(r)
                and vol_hi(r)
                and liq_mid(r),

        "FA95 + NEW30>=2 + VOL>P50 + LIQ MID":
            lambda r:
                fa95(r)
                and new30_2(r)
                and vol_hi(r)
                and liq_mid(r),

        "FA95 + NF>0":
            lambda r:
                fa95(r)
                and nf_positive(r),

        "FA95 + NEW30>=2 + NF>0":
            lambda r:
                fa95(r)
                and new30_2(r)
                and nf_positive(r),

        "FA95 + CONC<0.90":
            lambda r:
                fa95(r)
                and conc_low(r),

        "FA95 + CONC<0.75":
            lambda r:
                fa95(r)
                and conc_very_low(r),

        "FA95 + MCAP>P50":
            lambda r:
                fa95(r)
                and mcap_hi(r),
    }

    return signals


# ============================================================
# SCORE
# ============================================================

def robustness(s30, s60, tokens):

    if not s30 or not s60:
        return -999

    # Intentionally penalise tiny samples.
    n_score = min(
        s60["n"],
        30
    ) / 30

    token_score = min(
        tokens,
        20
    ) / 20

    med_score = (
        max(-10, min(20, s30["med"]))
        +
        max(-10, min(20, s60["med"]))
    ) / 2

    win_score = (
        (s30["win"] - 50)
        +
        (s60["win"] - 50)
    ) / 2

    downside_penalty = (
        max(
            0,
            -s60["p10"]
        ) * .30
    )

    crash_penalty = (
        s60["crash10"] * .20
    )

    return (
        med_score
        + win_score * .35
        + n_score * 8
        + token_score * 5
        - downside_penalty
        - crash_penalty
    )


# ============================================================
# ANALYSIS
# ============================================================

def analyse(rows, signals, minimum_n):

    output = []

    for name, rule in signals.items():

        subset = []

        for r in rows:
            try:
                if rule(r):
                    subset.append(r)
            except Exception:
                pass

        r30 = [
            getv(r, "dex_return_30s")
            for r in subset
            if getv(r, "dex_return_30s") is not None
        ]

        r60 = [
            getv(r, "dex_return_60s")
            for r in subset
            if getv(r, "dex_return_60s") is not None
        ]

        s30 = stats(r30)
        s60 = stats(r60)

        tokens = len(set(
            getv(r, "token_mint")
            for r in subset
            if getv(r, "token_mint")
        ))

        if (
            s60 is None
            or s60["n"] < minimum_n
        ):
            continue

        score = robustness(
            s30,
            s60,
            tokens
        )

        output.append({
            "name": name,
            "n": s60["n"],
            "tokens": tokens,
            "s30": s30,
            "s60": s60,
            "score": score,
        })

    return sorted(
        output,
        key=lambda x: x["score"],
        reverse=True
    )


# ============================================================
# DISPLAY
# ============================================================

def table(title, results):

    print()
    print("=" * 150)
    print(title)
    print("=" * 150)

    print(
        f"{'SIGNAL':45} "
        f"{'N':>4} "
        f"{'TOK':>4} | "
        f"{'MED30':>8} "
        f"{'WIN30':>7} | "
        f"{'MED60':>8} "
        f"{'WIN60':>7} "
        f"{'AVG60':>8} "
        f"{'P10':>8} "
        f"{'CRASH':>7} | "
        f"{'SCORE':>7}"
    )

    print("-" * 150)

    for x in results:

        a = x["s30"]
        b = x["s60"]

        med30 = a["med"] if a else 0
        win30 = a["win"] if a else 0

        print(
            f"{x['name'][:45]:45} "
            f"{x['n']:>4} "
            f"{x['tokens']:>4} | "
            f"{med30:+7.2f}% "
            f"{win30:6.1f}% | "
            f"{b['med']:+7.2f}% "
            f"{b['win']:6.1f}% "
            f"{b['avg']:+7.2f}% "
            f"{b['p10']:+7.2f}% "
            f"{b['crash10']:6.1f}% | "
            f"{x['score']:+7.2f}"
        )


# ============================================================
# LOOP
# ============================================================

while True:

    try:

        rows = load()

        if not rows:
            print("Aucune donnée exploitable.")
            time.sleep(10)
            continue

        t = thresholds(rows)
        signals = build_signals(t)

        discovery = analyse(
            rows,
            signals,
            MIN_N_DISCOVERY
        )

        oos_rows = [
            r for r in rows
            if getv(r, "id") > OOS_START_ID
        ]

        oos = analyse(
            oos_rows,
            signals,
            MIN_N_OOS
        )

        os.system("clear")

        print("=" * 150)
        print("MEMECOIN LAB — CROSS SIGNAL EXPLORER V1.7")
        print("=" * 150)

        print(
            f"ALL USABLE EVENTS : {len(rows)}"
            f" | OOS EVENTS ID>{OOS_START_ID}: {len(oos_rows)}"
        )

        print()

        print(
            "DYNAMIC THRESHOLDS | "
            f"VOL P50={t['vol50']} | "
            f"VOL P75={t['vol75']} | "
            f"LIQ P25={t['liq25']} | "
            f"LIQ P50={t['liq50']} | "
            f"LIQ P75={t['liq75']}"
        )

        table(
            "DISCOVERY — HISTORICAL / HYPOTHESIS GENERATION ONLY",
            discovery
        )

        table(
            "OOS — EVENTS CREATED AFTER ID 162",
            oos
        )

        print()
        print("=" * 150)
        print("IMPORTANT")
        print("=" * 150)

        print(
            "DISCOVERY = cherche des hypotheses. "
            "Un gros score ici n'est PAS une validation."
        )

        print(
            "V2 FROZEN = FA95 + NEW30>=2 + VOLUME_M5>=8837.925."
        )

        print(
            "Ne change PAS V2 pendant la collecte OOS."
        )

        print()
        print(
            "Refresh automatique toutes les 15 secondes."
        )

        time.sleep(15)

    except KeyboardInterrupt:
        print("\nExplorer stopped.")
        break

    except Exception as e:
        print(
            "ERROR:",
            repr(e)
        )
        time.sleep(5)
