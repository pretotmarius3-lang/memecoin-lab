import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

N_PERIODS = 6


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def median(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def mean(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


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


def safe_div(a,b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a/b


def fmt(x, digits=4):
    if x is None:
        return "NA"

    return f"{x:+.{digits}f}"


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


def load():

    db = connect()

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

            ON d.event_id=x.event_id
            AND d.timestamp=x.first_time
        )

        SELECT
            e.id,
            e.timestamp,
            e.token_mint,
            e.dex_return_60s,

            e.fa,
            e.nf30,

            e.new_wallets10,
            e.new_wallets30,

            e.buy_volume30,
            e.sell_volume30,
            e.buy_concentration30,

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,
            d.buys_m5,
            d.sells_m5,

            s.mid_buy_count,
            s.mid_sell_count,
            s.recent_unique_buyers,

            s.early_swaps_per_sec,
            s.mid_swaps_per_sec,
            s.recent_swaps_per_sec,

            s.buy_concentration_trend,

            s.recent_price_return,
            s.mid_price_return,

            s.recent_sell_sol,
            s.recent_net_sol,
            s.recent_buy_share,

            s.late_chase_score,
            s.breadth_score

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        LEFT JOIN first_dex d
        ON d.event_id=e.id

        WHERE e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()

    db.close()

    return rows


def feature(r, name):

    if name in r.keys():
        return r[name]

    if name == "vol_liq":
        return safe_div(
            r["volume_m5"],
            r["liquidity_usd"]
        )

    if name == "buy_ratio_m5":

        b = r["buys_m5"]
        s = r["sells_m5"]

        if not valid(b) or not valid(s):
            return None

        return safe_div(
            b,
            b+s
        )

    if name == "mid_flow_balance":

        b = r["mid_buy_count"]
        s = r["mid_sell_count"]

        if not valid(b) or not valid(s):
            return None

        return b-s

    if name == "swap_velocity_mean":

        vals = [
            r["early_swaps_per_sec"],
            r["mid_swaps_per_sec"],
            r["recent_swaps_per_sec"],
        ]

        vals = [x for x in vals if valid(x)]

        return mean(vals) if vals else None

    if name == "price_flow_tension":

        p = r["recent_price_return"]
        f = r["recent_net_sol"]

        if not valid(p) or not valid(f):
            return None

        return f-p

    return None


FEATURES = [
    "fa",
    "nf30",

    "new_wallets10",
    "new_wallets30",

    "buy_volume30",
    "sell_volume30",
    "buy_concentration30",

    "volume_m5",
    "liquidity_usd",
    "market_cap",

    "vol_liq",
    "buy_ratio_m5",

    "mid_buy_count",
    "mid_sell_count",
    "mid_flow_balance",

    "recent_unique_buyers",

    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",
    "swap_velocity_mean",

    "buy_concentration_trend",

    "recent_price_return",
    "mid_price_return",

    "recent_sell_sol",
    "recent_net_sol",
    "recent_buy_share",

    "late_chase_score",
    "breadth_score",

    "price_flow_tension",
]


def split_periods(rows):

    periods = []

    n = len(rows)

    for i in range(N_PERIODS):

        start = int(
            n*i/N_PERIODS
        )

        end = int(
            n*(i+1)/N_PERIODS
        )

        part = rows[start:end]

        periods.append(
            (
                f"P{i+1}",
                part
            )
        )

    return periods


def dist_stats(rows, fname):

    vals = [
        feature(r,fname)
        for r in rows
        if valid(feature(r,fname))
    ]

    if not vals:
        return None

    return {
        "n": len(vals),
        "med": median(vals),
        "mean": mean(vals),
        "p25": percentile(vals,.25),
        "p75": percentile(vals,.75),
    }


def standardized_shift(old, new):

    if not old or not new:
        return None

    spread_old = (
        old["p75"] - old["p25"]
        if (
            old["p75"] is not None
            and old["p25"] is not None
        )
        else None
    )

    if (
        spread_old is None
        or spread_old == 0
    ):
        return None

    return (
        new["med"]
        - old["med"]
    ) / abs(spread_old)


def relation(rows, fname):

    pairs = []

    for r in rows:

        x = feature(r,fname)
        y = r["dex_return_60s"]

        if valid(x) and valid(y):
            pairs.append(
                (x,y)
            )

    if len(pairs) < 12:
        return None

    xs = [
        x for x,_ in pairs
    ]

    cut = median(xs)

    high = [
        y for x,y in pairs
        if x >= cut
    ]

    low = [
        y for x,y in pairs
        if x < cut
    ]

    if (
        len(high) < 4
        or len(low) < 4
    ):
        return None

    high_runner = (
        100*sum(y >= RUNNER for y in high)
        / len(high)
    )

    high_dump = (
        100*sum(y <= DUMP for y in high)
        / len(high)
    )

    low_runner = (
        100*sum(y >= RUNNER for y in low)
        / len(low)
    )

    low_dump = (
        100*sum(y <= DUMP for y in low)
        / len(low)
    )

    high_edge = (
        high_runner
        - high_dump
    )

    low_edge = (
        low_runner
        - low_dump
    )

    return {
        "cut": cut,

        "high_n": len(high),
        "low_n": len(low),

        "high_edge": high_edge,
        "low_edge": low_edge,

        "direction":
            high_edge
            - low_edge,

        "high_med":
            median(high),

        "low_med":
            median(low),

        "med_diff":
            median(high)
            - median(low),
    }


def outcome_summary(rows):

    vals = [
        r["dex_return_60s"]
        for r in rows
        if valid(r["dex_return_60s"])
    ]

    if not vals:
        return None

    runners = sum(
        x >= RUNNER
        for x in vals
    )

    dumps = sum(
        x <= DUMP
        for x in vals
    )

    return {
        "n": len(vals),

        "med": median(vals),
        "avg": mean(vals),

        "runner":
            100*runners/len(vals),

        "dump":
            100*dumps/len(vals),

        "edge":
            100*(runners-dumps)
            / len(vals),
    }


while True:

    try:

        rows = load()

        if len(rows) < 120:

            print(
                "Need more data:",
                len(rows)
            )

            time.sleep(20)
            continue

        periods = split_periods(
            rows
        )

        historical = []

        for _,part in periods[:5]:
            historical += part

        p6 = periods[5][1]

        hist_out = outcome_summary(
            historical
        )

        p6_out = outcome_summary(
            p6
        )

        shifts = []

        relation_changes = []

        for fname in FEATURES:

            a = dist_stats(
                historical,
                fname
            )

            b = dist_stats(
                p6,
                fname
            )

            shift = standardized_shift(
                a,
                b
            )

            if shift is not None:

                shifts.append({
                    "feature": fname,

                    "hist_med":
                        a["med"],

                    "p6_med":
                        b["med"],

                    "shift":
                        shift,

                    "hist_n":
                        a["n"],

                    "p6_n":
                        b["n"],
                })

            ra = relation(
                historical,
                fname
            )

            rb = relation(
                p6,
                fname
            )

            if (
                ra is not None
                and rb is not None
            ):

                rel_change = (
                    rb["direction"]
                    - ra["direction"]
                )

                flipped = (
                    (
                        ra["direction"] > 0
                        and rb["direction"] < 0
                    )
                    or
                    (
                        ra["direction"] < 0
                        and rb["direction"] > 0
                    )
                )

                relation_changes.append({
                    "feature":
                        fname,

                    "hist_dir":
                        ra["direction"],

                    "p6_dir":
                        rb["direction"],

                    "change":
                        rel_change,

                    "flipped":
                        flipped,

                    "hist_med_diff":
                        ra["med_diff"],

                    "p6_med_diff":
                        rb["med_diff"],
                })

        shifts.sort(
            key=lambda x:
                abs(x["shift"]),
            reverse=True
        )

        relation_changes.sort(
            key=lambda x:
                (
                    x["flipped"],
                    abs(x["change"])
                ),
            reverse=True
        )

        os.system("clear")

        print("="*145)
        print(
            "MEMECOIN LAB — "
            "V5.2 REGIME SHIFT DETECTOR"
        )
        print("="*145)

        print(
            f"TOTAL EVENTS : {len(rows)}"
        )

        print(
            f"HISTORY P1-P5: "
            f"{len(historical)}"
        )

        print(
            f"P6           : "
            f"{len(p6)}"
        )

        print()

        print(
            "OUTCOME REGIME"
        )

        print("-"*90)

        print(
            f"P1-P5 | "
            f"MED={hist_out['med']:+.2f}% | "
            f"AVG={hist_out['avg']:+.2f}% | "
            f"RUNNER={hist_out['runner']:.1f}% | "
            f"DUMP={hist_out['dump']:.1f}% | "
            f"EDGE={hist_out['edge']:+.1f}%"
        )

        print(
            f"P6    | "
            f"MED={p6_out['med']:+.2f}% | "
            f"AVG={p6_out['avg']:+.2f}% | "
            f"RUNNER={p6_out['runner']:.1f}% | "
            f"DUMP={p6_out['dump']:.1f}% | "
            f"EDGE={p6_out['edge']:+.1f}%"
        )

        print()
        print("="*145)
        print(
            "A) BIGGEST DISTRIBUTION SHIFTS — P6 VS P1-P5"
        )
        print("="*145)

        print(
            f"{'FEATURE':32}"
            f"{'HIST MED':>15}"
            f"{'P6 MED':>15}"
            f"{'SHIFT/IQR':>12}"
            f"{'H N':>7}"
            f"{'P6 N':>7}"
        )

        print("-"*100)

        for x in shifts[:25]:

            print(
                f"{x['feature']:32}"
                f"{fmt(x['hist_med']):>15}"
                f"{fmt(x['p6_med']):>15}"
                f"{x['shift']:+12.3f}"
                f"{x['hist_n']:7d}"
                f"{x['p6_n']:7d}"
            )

        print()
        print("="*145)
        print(
            "B) FEATURE → OUTCOME RELATION CHANGES"
        )
        print("="*145)

        print(
            "Direction = edge(HIGH feature) - edge(LOW feature)."
        )

        print(
            "Positive = HIGH values more runner-like."
        )

        print(
            "Negative = HIGH values more dump-like."
        )

        print()

        print(
            f"{'FEATURE':32}"
            f"{'P1-P5 DIR':>12}"
            f"{'P6 DIR':>12}"
            f"{'CHANGE':>12}"
            f"{'FLIP':>8}"
            f"{'H MEDΔ':>12}"
            f"{'P6 MEDΔ':>12}"
        )

        print("-"*110)

        for x in relation_changes[:25]:

            print(
                f"{x['feature']:32}"
                f"{x['hist_dir']:+11.1f}"
                f"{x['p6_dir']:+11.1f}"
                f"{x['change']:+11.1f}"
                f"{('YES' if x['flipped'] else 'NO'):>8}"
                f"{x['hist_med_diff']:+11.2f}%"
                f"{x['p6_med_diff']:+11.2f}%"
            )

        print()
        print("="*145)
        print(
            "C) STRONGEST TRUE SIGN FLIPS"
        )
        print("="*145)

        flips = [
            x for x in relation_changes
            if x["flipped"]
        ]

        if not flips:

            print(
                "No sign flips with sufficient sample."
            )

        else:

            for x in flips[:15]:

                print(
                    f"{x['feature']:32} | "
                    f"P1-P5={x['hist_dir']:+6.1f} pts "
                    f"→ P6={x['p6_dir']:+6.1f} pts "
                    f"| Δ={x['change']:+6.1f}"
                )

        print()
        print("="*145)
        print(
            "INTERPRETATION"
        )
        print("="*145)

        print(
            "1. Large SHIFT/IQR = the market itself changed."
        )

        print(
            "2. Sign flip with small distribution shift = "
            "same feature, different meaning."
        )

        print(
            "3. Sign flip + large distribution shift = "
            "strong regime-change candidate."
        )

        print(
            "4. Features stable in both distribution and relation "
            "are better candidates for universal models."
        )

        print(
            "5. Do NOT modify V4 Frozen from this analysis."
        )

        print()

        print(
            "Refresh every 30 seconds."
        )

        time.sleep(30)

    except KeyboardInterrupt:

        print(
            "\nV5.2 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)
