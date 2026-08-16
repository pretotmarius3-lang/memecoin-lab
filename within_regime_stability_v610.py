import sqlite3
import statistics
import math
import os
import time

import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

N_PERIODS = 6
K = 3

FEATURES_CLUSTER = [
    "fa",
    "nf30",
    "new_wallets10",
    "new_wallets30",
    "volume_m5",
    "liquidity_usd",
    "market_cap",
    "vol_liq",
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
]

FEATURES_TEST = [
    "fa",
    "volume_m5",
    "vol_liq",
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
]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def median(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


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

        return b - s

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


def build_matrix(rows):

    X = []

    for r in rows:
        vals = []

        for name in FEATURES_CLUSTER:
            v = f(r, name)

            vals.append(
                float(v)
                if valid(v)
                else np.nan
            )

        X.append(vals)

    return np.asarray(
        X,
        dtype=float
    )


def split_periods(rows, labels):

    n = len(rows)
    periods = []

    for i in range(N_PERIODS):

        start = int(
            n * i / N_PERIODS
        )

        end = int(
            n * (i+1) / N_PERIODS
        )

        periods.append((
            f"P{i+1}",
            rows[start:end],
            labels[start:end],
        ))

    return periods


def relation(rows, feature_name):

    pairs = []

    for r in rows:

        x = f(r, feature_name)
        y = r["dex_return_60s"]

        if valid(x) and valid(y):
            pairs.append(
                (x,y)
            )

    if len(pairs) < 8:
        return None

    cut = median([
        x for x,_ in pairs
    ])

    hi = [
        y for x,y in pairs
        if x >= cut
    ]

    lo = [
        y for x,y in pairs
        if x < cut
    ]

    if len(hi) < 3 or len(lo) < 3:
        return None

    def edge(vals):

        runners = sum(
            x >= RUNNER
            for x in vals
        )

        dumps = sum(
            x <= DUMP
            for x in vals
        )

        return (
            100
            * (runners-dumps)
            / len(vals)
        )

    return {
        "n": len(pairs),
        "cut": cut,

        "hi_edge":
            edge(hi),

        "lo_edge":
            edge(lo),

        "direction":
            edge(hi)-edge(lo),

        "med_diff":
            median(hi)-median(lo),
    }


def token_balanced_relation(
    rows,
    feature_name
):

    by_token = {}

    for r in rows:

        x = f(r, feature_name)
        y = r["dex_return_60s"]

        if not valid(x) or not valid(y):
            continue

        by_token.setdefault(
            r["token_mint"],
            []
        ).append(
            (x,y)
        )

    token_points = []

    for token, vals in by_token.items():

        token_x = median([
            x for x,_ in vals
        ])

        token_y = median([
            y for _,y in vals
        ])

        token_points.append(
            (token_x, token_y)
        )

    if len(token_points) < 8:
        return None

    cut = median([
        x for x,_ in token_points
    ])

    hi = [
        y for x,y in token_points
        if x >= cut
    ]

    lo = [
        y for x,y in token_points
        if x < cut
    ]

    if len(hi) < 3 or len(lo) < 3:
        return None

    def edge(vals):

        runners = sum(
            x >= RUNNER
            for x in vals
        )

        dumps = sum(
            x <= DUMP
            for x in vals
        )

        return (
            100
            * (runners-dumps)
            / len(vals)
        )

    return {
        "n": len(token_points),

        "direction":
            edge(hi)-edge(lo),

        "med_diff":
            median(hi)-median(lo),
    }


def regime_rows(
    period_rows,
    period_labels,
    regime_id
):

    return [
        r for r,l
        in zip(
            period_rows,
            period_labels
        )
        if l == regime_id
    ]


def stability_score(cells):

    usable = [
        x for x in cells
        if x["relation"] is not None
    ]

    if len(usable) < 3:
        return -999

    dirs = [
        x["relation"]["direction"]
        for x in usable
    ]

    tb_dirs = [
        x["tb"]["direction"]
        for x in usable
        if x["tb"] is not None
    ]

    pos = sum(
        d > 0
        for d in dirs
    )

    neg = sum(
        d < 0
        for d in dirs
    )

    if pos >= neg:
        dominant_sign = 1
    else:
        dominant_sign = -1

    same = sum(
        (
            d > 0
            and dominant_sign > 0
        )
        or
        (
            d < 0
            and dominant_sign < 0
        )
        for d in dirs
    )

    tb_same = sum(
        (
            d > 0
            and dominant_sign > 0
        )
        or
        (
            d < 0
            and dominant_sign < 0
        )
        for d in tb_dirs
    )

    magnitude = median([
        abs(d)
        for d in dirs
    ])

    return (
        same * 12
        + tb_same * 8
        + magnitude
        - (len(dirs)-same) * 10
    )


while True:

    try:

        rows = load()

        if len(rows) < 120:
            print(
                f"Need more data: {len(rows)}"
            )
            time.sleep(20)
            continue

        X = build_matrix(rows)

        pre = Pipeline([
            (
                "imputer",
                SimpleImputer(
                    strategy="median"
                )
            ),
            (
                "scale",
                StandardScaler()
            ),
        ])

        Xs = pre.fit_transform(X)

        model = KMeans(
            n_clusters=K,
            n_init=20,
            random_state=42,
        )

        labels = model.fit_predict(
            Xs
        )

        periods = split_periods(
            rows,
            labels
        )

        reports = []

        for regime_id in [0,1]:

            for fname in FEATURES_TEST:

                cells = []

                for pname,pr,pl in periods:

                    rr = regime_rows(
                        pr,
                        pl,
                        regime_id
                    )

                    rel = relation(
                        rr,
                        fname
                    )

                    tb = token_balanced_relation(
                        rr,
                        fname
                    )

                    cells.append({
                        "period":
                            pname,

                        "n":
                            len(rr),

                        "tokens":
                            len(set(
                                r["token_mint"]
                                for r in rr
                            )),

                        "relation":
                            rel,

                        "tb":
                            tb,
                    })

                score = stability_score(
                    cells
                )

                reports.append({
                    "regime":
                        regime_id,

                    "feature":
                        fname,

                    "cells":
                        cells,

                    "score":
                        score,
                })

        reports.sort(
            key=lambda x:
                x["score"],
            reverse=True
        )

        os.system("clear")

        print("="*175)
        print(
            "MEMECOIN LAB — "
            "V6.1 WITHIN-REGIME STABILITY LAB"
        )
        print("="*175)

        print(
            f"EVENTS  : {len(rows)}"
        )

        print(
            "KMEANS K=3 reconstructed "
            "without future outcomes."
        )

        print(
            "Analysis focuses on R0 and R1."
        )

        print()
        print("="*175)
        print(
            "TOP FEATURE → OUTCOME RELATIONS "
            "BY WITHIN-REGIME TEMPORAL STABILITY"
        )
        print("="*175)

        print(
            f"{'REG':>4} "
            f"{'FEATURE':30} "
            f"{'SCORE':>8} | "
            f"{'P1':>15} "
            f"{'P2':>15} "
            f"{'P3':>15} "
            f"{'P4':>15} "
            f"{'P5':>15} "
            f"{'P6':>15}"
        )

        print("-"*175)

        for x in reports[:30]:

            cells_txt = []

            for c in x["cells"]:

                rel = c["relation"]
                tb = c["tb"]

                if rel is None:
                    txt = "NA"
                else:
                    txt = (
                        f"{rel['direction']:+5.1f}"
                        "/"
                        + (
                            f"{tb['direction']:+5.1f}"
                            if tb is not None
                            else "  NA"
                        )
                    )

                cells_txt.append(
                    txt
                )

            print(
                f"R{x['regime']:<3} "
                f"{x['feature'][:30]:30} "
                f"{x['score']:8.2f} | "
                + " ".join(
                    f"{z:>15}"
                    for z in cells_txt
                )
            )

        print()
        print(
            "Cell = EVENT_DIRECTION / TOKEN_BALANCED_DIRECTION"
        )

        print(
            "Positive means HIGH feature values are more runner-like."
        )

        print()

        print("="*175)
        print(
            "TOP 10 — PERIOD DETAILS"
        )
        print("="*175)

        for x in reports[:10]:

            print()
            print(
                f"R{x['regime']} / "
                f"{x['feature']} "
                f"| STABILITY={x['score']:.2f}"
            )

            print("-"*125)

            print(
                f"{'PERIOD':7}"
                f"{'REG_N':>7}"
                f"{'TOK':>6}"
                f"{'DIR':>11}"
                f"{'MEDΔ':>11}"
                f"{'TB_N':>8}"
                f"{'TB_DIR':>11}"
                f"{'TB_MEDΔ':>12}"
            )

            for c in x["cells"]:

                rel = c["relation"]
                tb = c["tb"]

                if rel is None:

                    print(
                        f"{c['period']:7}"
                        f"{c['n']:7d}"
                        f"{c['tokens']:6d}"
                        f"{'NA':>11}"
                    )

                    continue

                print(
                    f"{c['period']:7}"
                    f"{c['n']:7d}"
                    f"{c['tokens']:6d}"
                    f"{rel['direction']:+10.1f}"
                    f"{rel['med_diff']:+10.2f}%"
                    f"{(tb['n'] if tb else 0):8d}"
                    f"{(tb['direction'] if tb else 0):+10.1f}"
                    f"{(tb['med_diff'] if tb else 0):+11.2f}%"
                )

        print()
        print("="*175)
        print("HOW TO INTERPRET")
        print("="*175)

        print(
            "Good feature:"
        )

        print(
            "• same sign in most periods inside SAME regime"
        )

        print(
            "• token-balanced sign agrees"
        )

        print(
            "• survives P6"
        )

        print(
            "• reasonable N / token count"
        )

        print()

        print(
            "If no feature survives:"
        )

        print(
            "→ R0/R1 are still too heterogeneous"
        )

        print(
            "→ hierarchical static models may still fail"
        )

        print(
            "→ next step should become online/adaptive"
        )

        print()

        print(
            "Do NOT modify V4 Frozen."
        )

        print(
            "Refresh every 60 seconds."
        )

        time.sleep(60)

    except KeyboardInterrupt:

        print(
            "\nV6.1 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(10)
