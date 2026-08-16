import sqlite3
import statistics
import math
import os
import time
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

N_PERIODS = 6
MIN_EVENTS_PERIOD = 5

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

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

    return (
        vals[lo] * (hi-k)
        + vals[hi] * (k-lo)
    )


def safe_div(a,b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a/b


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


# ------------------------------------------------------------
# Load
# ------------------------------------------------------------

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

            e.new_wallets10,
            e.new_wallets30,

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,

            s.mid_buy_count,
            s.mid_sell_count,
            s.recent_unique_buyers,

            s.early_swaps_per_sec,
            s.mid_swaps_per_sec,
            s.recent_swaps_per_sec,

            s.buy_concentration_trend,
            s.recent_price_return,
            s.recent_buy_share,
            s.recent_net_sol,
            s.late_chase_score,
            s.breadth_score

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        LEFT JOIN first_dex d
        ON d.event_id=e.id

        WHERE
            e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()

    db.close()
    return rows


# ------------------------------------------------------------
# Derived features
# ------------------------------------------------------------

def f(r, name):

    if name in r.keys():
        return r[name]

    if name == "vol_liq":
        return safe_div(
            r["volume_m5"],
            r["liquidity_usd"]
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

        vals = [
            x for x in vals
            if valid(x)
        ]

        return mean(vals) if vals else None

    return None


# ------------------------------------------------------------
# Frozen discovery thresholds
#
# Important:
# thresholds are calculated once from first 60%,
# then reused everywhere.
# ------------------------------------------------------------

def build_thresholds(rows):

    cutoff = int(
        len(rows) * 0.60
    )

    base = rows[:cutoff]

    names = [
        "recent_unique_buyers",
        "early_swaps_per_sec",
        "mid_swaps_per_sec",
        "recent_swaps_per_sec",
        "swap_velocity_mean",
        "vol_liq",
        "mid_buy_count",
        "mid_sell_count",
        "buy_concentration_trend",
        "recent_price_return",
        "recent_buy_share",
        "recent_net_sol",
        "late_chase_score",
        "breadth_score",
        "market_cap",
        "liquidity_usd",
    ]

    t = {}

    for name in names:

        vals = [
            f(r,name)
            for r in base
            if valid(f(r,name))
        ]

        if not vals:
            continue

        t[name] = {
            "p25": percentile(vals,.25),
            "p50": percentile(vals,.50),
            "p75": percentile(vals,.75),
        }

    return t


# ------------------------------------------------------------
# Candidate hypotheses
# ------------------------------------------------------------

def build_candidates(t):

    C = {}

    def low(name):
        return lambda r: (
            name in t
            and valid(f(r,name))
            and f(r,name) < t[name]["p50"]
        )

    def high(name):
        return lambda r: (
            name in t
            and valid(f(r,name))
            and f(r,name) >= t[name]["p50"]
        )

    C[
        "BUYERS_LOW + EARLY_FAST"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and high("early_swaps_per_sec")(r)
    )

    C[
        "BUYERS_LOW + MID_FAST"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and high("mid_swaps_per_sec")(r)
    )

    C[
        "BUYERS_LOW + RECENT_FAST"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and high("recent_swaps_per_sec")(r)
    )

    C[
        "BUYERS_LOW + SPEED_HIGH"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and high("swap_velocity_mean")(r)
    )

    C[
        "BUYERS_LOW + VOLLIQ_HIGH"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and high("vol_liq")(r)
    )

    C[
        "BUYERS_LOW + MID_BUYS_HIGH"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and high("mid_buy_count")(r)
    )

    C[
        "BUYERS_LOW + MID_SELLS_LOW"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and low("mid_sell_count")(r)
    )

    C[
        "BUYERS_LOW + LATECHASE_LOW"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and low("late_chase_score")(r)
    )

    C[
        "BUYERS_LOW + BUYSHARE_LOW"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and low("recent_buy_share")(r)
    )

    C[
        "BUYERS_LOW + PRICE_LOW"
    ] = lambda r: (
        low("recent_unique_buyers")(r)
        and low("recent_price_return")(r)
    )

    C[
        "SPEED_HIGH + BUYSHARE_LOW"
    ] = lambda r: (
        high("swap_velocity_mean")(r)
        and low("recent_buy_share")(r)
    )

    C[
        "VOLLIQ_HIGH + BUYERS_LOW"
    ] = lambda r: (
        high("vol_liq")(r)
        and low("recent_unique_buyers")(r)
    )

    C[
        "VOLLIQ_HIGH + PRICE_LOW"
    ] = lambda r: (
        high("vol_liq")(r)
        and low("recent_price_return")(r)
    )

    C[
        "MID_BUYS_HIGH + BUYERS_LOW"
    ] = lambda r: (
        high("mid_buy_count")(r)
        and low("recent_unique_buyers")(r)
    )

    C[
        "MID_SELLS_LOW + BUYERS_LOW"
    ] = lambda r: (
        low("mid_sell_count")(r)
        and low("recent_unique_buyers")(r)
    )

    C[
        "CONC_LOW + BUYERS_LOW"
    ] = lambda r: (
        low("buy_concentration_trend")(r)
        and low("recent_unique_buyers")(r)
    )

    return C


# ------------------------------------------------------------
# Stats
# ------------------------------------------------------------

def stats(rows):

    vals = [
        r["dex_return_60s"]
        for r in rows
        if valid(
            r["dex_return_60s"]
        )
    ]

    if not vals:
        return None

    runner = sum(
        x >= RUNNER
        for x in vals
    )

    dump = sum(
        x <= DUMP
        for x in vals
    )

    return {
        "n": len(vals),
        "tokens": len(set(
            r["token_mint"]
            for r in rows
        )),
        "med": median(vals),
        "avg": mean(vals),
        "runner": 100*runner/len(vals),
        "dump": 100*dump/len(vals),
        "edge": 100*(runner-dump)/len(vals),
    }


# ------------------------------------------------------------
# Token-balanced stats
#
# Each token contributes one median outcome.
# ------------------------------------------------------------

def token_balanced_stats(rows):

    groups = defaultdict(list)

    for r in rows:

        ret = r[
            "dex_return_60s"
        ]

        if valid(ret):
            groups[
                r["token_mint"]
            ].append(ret)

    if not groups:
        return None

    token_returns = [
        median(v)
        for v in groups.values()
    ]

    runner = sum(
        x >= RUNNER
        for x in token_returns
    )

    dump = sum(
        x <= DUMP
        for x in token_returns
    )

    return {
        "n": len(token_returns),
        "med": median(token_returns),
        "avg": mean(token_returns),
        "runner": 100*runner/len(token_returns),
        "dump": 100*dump/len(token_returns),
        "edge": 100*(runner-dump)/len(token_returns),
    }


# ------------------------------------------------------------
# Time periods
# ------------------------------------------------------------

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


# ------------------------------------------------------------
# Evaluate one candidate through time
# ------------------------------------------------------------

def evaluate_candidate(
    name,
    rule,
    periods
):

    result = []

    for pname,period in periods:

        subset = []

        for r in period:

            try:
                if rule(r):
                    subset.append(r)
            except Exception:
                pass

        s = stats(subset)
        tb = token_balanced_stats(
            subset
        )

        result.append(
            (
                pname,
                s,
                tb
            )
        )

    return result


# ------------------------------------------------------------
# Stability score
# ------------------------------------------------------------

def stability_score(result):

    usable = [
        (s,tb)
        for _,s,tb in result
        if (
            s is not None
            and s["n"]
            >= MIN_EVENTS_PERIOD
        )
    ]

    if not usable:
        return -999

    positive_periods = sum(
        s["edge"] > 0
        for s,_ in usable
    )

    negative_periods = sum(
        s["edge"] < 0
        for s,_ in usable
    )

    token_positive = sum(
        tb is not None
        and tb["edge"] > 0
        for _,tb in usable
    )

    med_edges = [
        s["edge"]
        for s,_ in usable
    ]

    med_token_edges = [
        tb["edge"]
        for _,tb in usable
        if tb is not None
    ]

    avg_n = mean([
        s["n"]
        for s,_ in usable
    ])

    return (
        positive_periods * 8
        - negative_periods * 8
        + token_positive * 6
        + median(med_edges)
        + (
            median(
                med_token_edges
            )
            if med_token_edges
            else 0
        )
        + min(avg_n,20)*0.4
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

while True:

    try:

        rows = load()

        if len(rows) < 120:

            print(
                f"Not enough data: "
                f"{len(rows)}"
            )

            time.sleep(20)
            continue

        thresholds = build_thresholds(
            rows
        )

        candidates = build_candidates(
            thresholds
        )

        periods = split_periods(
            rows
        )

        reports = []

        for name,rule in candidates.items():

            result = evaluate_candidate(
                name,
                rule,
                periods
            )

            score = stability_score(
                result
            )

            reports.append(
                (
                    score,
                    name,
                    result
                )
            )

        reports.sort(
            reverse=True
        )

        os.system("clear")

        print("="*170)
        print(
            "MEMECOIN LAB — "
            "V5.1 REGIME STABILITY MATRIX"
        )
        print("="*170)

        print(
            f"TOTAL EVENTS : {len(rows)}"
        )

        print(
            f"PERIODS      : {N_PERIODS}"
        )

        print(
            "Thresholds frozen from first 60% of data."
        )

        print(
            "TB = token-balanced."
        )

        print()

        print("="*170)
        print(
            "RANKING BY TEMPORAL + TOKEN STABILITY"
        )
        print("="*170)

        print(
            f"{'CANDIDATE':42} "
            f"{'SCORE':>8} | "
            f"{'P1':>14} "
            f"{'P2':>14} "
            f"{'P3':>14} "
            f"{'P4':>14} "
            f"{'P5':>14} "
            f"{'P6':>14}"
        )

        print("-"*170)

        for score,name,result in reports:

            cells = []

            for pname,s,tb in result:

                if (
                    s is None
                    or s["n"]
                    < MIN_EVENTS_PERIOD
                ):

                    cells.append(
                        "    NA"
                    )

                else:

                    cells.append(
                        f"{s['edge']:+5.1f}/"
                        f"{tb['edge']:+5.1f}"
                        if tb is not None
                        else
                        f"{s['edge']:+5.1f}/  NA"
                    )

            print(
                f"{name[:42]:42} "
                f"{score:8.2f} | "
                + " ".join(
                    f"{x:>14}"
                    for x in cells
                )
            )

        print()
        print(
            "Cell format = "
            "EVENT_EDGE / TOKEN_BALANCED_EDGE"
        )

        print()

        print("="*170)
        print(
            "TOP 8 CANDIDATES — PERIOD DETAIL"
        )
        print("="*170)

        for score,name,result in reports[:8]:

            print()
            print(
                f"{name} | "
                f"STABILITY SCORE={score:.2f}"
            )

            print("-"*125)

            print(
                f"{'PERIOD':7} "
                f"{'N':>5} "
                f"{'TOK':>5} "
                f"{'MED':>9} "
                f"{'RUN':>8} "
                f"{'DUMP':>8} "
                f"{'EDGE':>8} || "
                f"{'TB_N':>5} "
                f"{'TB_MED':>9} "
                f"{'TB_EDGE':>9}"
            )

            for pname,s,tb in result:

                if s is None:

                    print(
                        f"{pname:7} NO DATA"
                    )

                    continue

                print(
                    f"{pname:7} "
                    f"{s['n']:5d} "
                    f"{s['tokens']:5d} "
                    f"{s['med']:+8.2f}% "
                    f"{s['runner']:7.1f}% "
                    f"{s['dump']:7.1f}% "
                    f"{s['edge']:+7.1f}% || "
                    f"{(tb['n'] if tb else 0):5d} "
                    f"{(tb['med'] if tb else 0):+8.2f}% "
                    f"{(tb['edge'] if tb else 0):+8.1f}%"
                )

        print()
        print("="*170)
        print(
            "HOW TO READ"
        )
        print("="*170)

        print(
            "Good candidate:"
        )

        print(
            "• positive event edge in several periods"
        )

        print(
            "• positive token-balanced edge too"
        )

        print(
            "• does not depend on one single period"
        )

        print(
            "• has several unique tokens"
        )

        print()

        print(
            "Bad candidate:"
        )

        print(
            "• huge P6 result but negative P1-P5"
        )

        print(
            "• event edge positive but token-balanced negative"
        )

        print(
            "• tiny N"
        )

        print()

        print(
            "Do NOT modify V4 Frozen from this."
        )

        print(
            "V5.1 is discovery / robustness research only."
        )

        print(
            "Refresh every 30 seconds."
        )

        time.sleep(30)

    except KeyboardInterrupt:

        print(
            "\nV5.1 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)
