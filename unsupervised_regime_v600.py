import sqlite3
import math
import os
import time
import statistics

import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

K_VALUES = [2, 3, 4, 5]

FEATURES = [
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


def feature(r, name):

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

        for f in FEATURES:
            v = feature(r, f)
            vals.append(
                float(v)
                if valid(v)
                else np.nan
            )

        X.append(vals)

    return np.array(
        X,
        dtype=float
    )


def outcome_stats(rows):

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

        "tokens":
            len(set(
                r["token_mint"]
                for r in rows
            )),

        "med":
            median(vals),

        "avg":
            mean(vals),

        "runner":
            100 * runners / len(vals),

        "dump":
            100 * dumps / len(vals),

        "edge":
            100 * (runners - dumps) / len(vals),
    }


def cluster_feature_profile(
    rows,
    labels,
    cluster_id
):

    subset = [
        r for r, lab
        in zip(rows, labels)
        if lab == cluster_id
    ]

    profile = {}

    for f in FEATURES:

        vals = [
            feature(r, f)
            for r in subset
            if valid(feature(r, f))
        ]

        if vals:
            profile[f] = median(vals)

    return profile


def global_profile(rows):

    profile = {}

    for f in FEATURES:

        vals = [
            feature(r, f)
            for r in rows
            if valid(feature(r, f))
        ]

        if vals:
            profile[f] = median(vals)

    return profile


def relative_profile(
    cluster_profile,
    global_profile
):

    out = []

    for f in FEATURES:

        if (
            f not in cluster_profile
            or f not in global_profile
        ):
            continue

        c = cluster_profile[f]
        g = global_profile[f]

        if not valid(c) or not valid(g):
            continue

        diff = c - g

        # relative magnitude where meaningful
        if abs(g) > 1e-9:
            rel = diff / abs(g)
        else:
            rel = diff

        out.append(
            (
                abs(rel),
                f,
                c,
                g,
                rel
            )
        )

    return sorted(
        out,
        reverse=True
    )


def token_balanced_cluster_stats(
    rows,
    labels,
    cluster_id
):

    groups = {}

    for r, lab in zip(rows, labels):

        if lab != cluster_id:
            continue

        groups.setdefault(
            r["token_mint"],
            []
        ).append(
            r["dex_return_60s"]
        )

    if not groups:
        return None

    token_returns = [
        median(vals)
        for vals in groups.values()
    ]

    runners = sum(
        x >= RUNNER
        for x in token_returns
    )

    dumps = sum(
        x <= DUMP
        for x in token_returns
    )

    return {
        "tokens":
            len(token_returns),

        "med":
            median(token_returns),

        "runner":
            100 * runners / len(token_returns),

        "dump":
            100 * dumps / len(token_returns),

        "edge":
            100 * (runners - dumps) / len(token_returns),
    }


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

        candidates = []

        for k in K_VALUES:

            model = KMeans(
                n_clusters=k,
                n_init=20,
                random_state=42,
            )

            labels = model.fit_predict(
                Xs
            )

            sil = silhouette_score(
                Xs,
                labels
            )

            candidates.append(
                (
                    sil,
                    k,
                    model,
                    labels
                )
            )

        candidates.sort(
            reverse=True,
            key=lambda x:x[0]
        )

        best_sil, best_k, best_model, labels = candidates[0]

        gp = global_profile(rows)

        os.system("clear")

        print("=" * 145)
        print(
            "MEMECOIN LAB — "
            "V6 UNSUPERVISED REGIME DISCOVERY"
        )
        print("=" * 145)

        print(
            f"EVENTS : {len(rows)}"
        )

        print()
        print(
            "IMPORTANT: clustering never sees dex_return_60s."
        )

        print()

        print("=" * 145)
        print(
            "CLUSTER COUNT SELECTION"
        )
        print("=" * 145)

        for sil,k,_,_ in candidates:

            print(
                f"K={k} | "
                f"SILHOUETTE={sil:.4f}"
            )

        print()
        print(
            f"BEST K = {best_k}"
            f" | SILHOUETTE = {best_sil:.4f}"
        )

        print()
        print("=" * 145)
        print(
            "REGIME OUTCOMES — REVEALED AFTER CLUSTERING"
        )
        print("=" * 145)

        print(
            f"{'REGIME':10}"
            f"{'N':>6}"
            f"{'TOK':>6}"
            f"{'MED60':>10}"
            f"{'AVG60':>10}"
            f"{'RUNNER':>9}"
            f"{'DUMP':>9}"
            f"{'EDGE':>9}"
            f"{'TB_EDGE':>11}"
        )

        print("-" * 95)

        regime_stats = []

        for cid in range(best_k):

            subset = [
                r for r,lab
                in zip(rows,labels)
                if lab == cid
            ]

            s = outcome_stats(
                subset
            )

            tb = token_balanced_cluster_stats(
                rows,
                labels,
                cid
            )

            regime_stats.append(
                (
                    cid,
                    s,
                    tb
                )
            )

            print(
                f"R{cid:<9}"
                f"{s['n']:6d}"
                f"{s['tokens']:6d}"
                f"{s['med']:+9.2f}%"
                f"{s['avg']:+9.2f}%"
                f"{s['runner']:8.1f}%"
                f"{s['dump']:8.1f}%"
                f"{s['edge']:+8.1f}%"
                f"{tb['edge']:+10.1f}%"
            )

        print()
        print("=" * 145)
        print(
            "REGIME FEATURE PROFILES"
        )
        print("=" * 145)

        for cid,s,tb in regime_stats:

            cp = cluster_feature_profile(
                rows,
                labels,
                cid
            )

            rel = relative_profile(
                cp,
                gp
            )

            print()
            print(
                f"REGIME R{cid}"
                f" | N={s['n']}"
                f" | EDGE={s['edge']:+.1f}%"
                f" | TB_EDGE={tb['edge']:+.1f}%"
            )

            print("-" * 110)

            print(
                "Most distinctive median features:"
            )

            for _,f,c,g,relv in rel[:12]:

                print(
                    f"{f:30} "
                    f"R={c:+.4f} | "
                    f"GLOBAL={g:+.4f} | "
                    f"REL={relv:+.2f}"
                )

        # ----------------------------------------------------
        # TIME STABILITY OF REGIME MIX
        # ----------------------------------------------------

        print()
        print("=" * 145)
        print(
            "REGIME MIX THROUGH TIME"
        )
        print("=" * 145)

        n = len(rows)

        for p in range(6):

            start = int(
                n*p/6
            )

            end = int(
                n*(p+1)/6
            )

            part_labels = labels[
                start:end
            ]

            counts = []

            for cid in range(best_k):

                c = int(
                    np.sum(
                        part_labels == cid
                    )
                )

                pct = (
                    100*c/len(part_labels)
                    if len(part_labels)
                    else 0
                )

                counts.append(
                    f"R{cid}={pct:5.1f}%"
                )

            print(
                f"P{p+1}: "
                + " | ".join(counts)
            )

        # ----------------------------------------------------
        # OUTCOME RELATION INSIDE EACH REGIME
        # ----------------------------------------------------

        print()
        print("=" * 145)
        print(
            "FEATURE DIRECTION INSIDE REGIMES"
        )
        print("=" * 145)

        CHECK_FEATURES = [
            "fa",
            "volume_m5",
            "vol_liq",

            "mid_buy_count",
            "mid_sell_count",

            "recent_unique_buyers",

            "early_swaps_per_sec",
            "mid_swaps_per_sec",
            "recent_swaps_per_sec",

            "buy_concentration_trend",

            "recent_price_return",
            "recent_buy_share",
        ]

        for cid in range(best_k):

            subset = [
                r for r,lab
                in zip(rows,labels)
                if lab == cid
            ]

            print()
            print(
                f"REGIME R{cid}"
            )

            print("-"*105)

            for fname in CHECK_FEATURES:

                vals = [
                    feature(r,fname)
                    for r in subset
                    if valid(feature(r,fname))
                ]

                if len(vals) < 16:
                    continue

                cut = median(vals)

                hi = [
                    r["dex_return_60s"]
                    for r in subset
                    if (
                        valid(feature(r,fname))
                        and feature(r,fname) >= cut
                    )
                ]

                lo = [
                    r["dex_return_60s"]
                    for r in subset
                    if (
                        valid(feature(r,fname))
                        and feature(r,fname) < cut
                    )
                ]

                if (
                    len(hi) < 5
                    or len(lo) < 5
                ):
                    continue

                hi_run = (
                    100
                    * sum(x >= RUNNER for x in hi)
                    / len(hi)
                )

                hi_dump = (
                    100
                    * sum(x <= DUMP for x in hi)
                    / len(hi)
                )

                lo_run = (
                    100
                    * sum(x >= RUNNER for x in lo)
                    / len(lo)
                )

                lo_dump = (
                    100
                    * sum(x <= DUMP for x in lo)
                    / len(lo)
                )

                hi_edge = (
                    hi_run
                    - hi_dump
                )

                lo_edge = (
                    lo_run
                    - lo_dump
                )

                direction = (
                    hi_edge
                    - lo_edge
                )

                print(
                    f"{fname:30} "
                    f"HIGH-LOW EDGE={direction:+7.1f} pts"
                )

        print()
        print("=" * 145)
        print("INTERPRETATION")
        print("=" * 145)

        print(
            "1. Clusters are built without future returns."
        )

        print(
            "2. Interesting regime = materially different "
            "runner/dump behavior after outcomes are revealed."
        )

        print(
            "3. Token-balanced edge matters as much as event edge."
        )

        print(
            "4. If P6 contains much more of one toxic regime, "
            "that may explain V4 deterioration."
        )

        print(
            "5. Feature directions should be compared across regimes."
        )

        print(
            "6. Do NOT modify V4 Frozen from this analysis."
        )

        print()
        print(
            "Refresh every 60 seconds."
        )

        time.sleep(60)

    except KeyboardInterrupt:

        print(
            "\nV6 regime discovery stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(10)
