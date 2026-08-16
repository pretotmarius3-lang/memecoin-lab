#!/usr/bin/env python3

import hashlib
import itertools
import json
import math
import random
import sqlite3
import statistics
import time
from pathlib import Path


ROOT = Path.home() / "memecoin_lab"
MARKET_DB = ROOT / "validation_v090.db"
RESEARCH_DB = ROOT / "research_v4.db"

STAGES = [5, 10, 20, 30, 60]

TARGETS = [
    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
]

N_SPLITS = 50
N_PERMUTATIONS = 300


BASE_REQUIRED = {
    "token_mint",
    "stage_s",
    "future_ready",
    "coverage_status",

    "return_since_entry",
    "mfe_so_far",
    "mae_so_far",

    "swaps",
    "buys",
    "sells",
    "buy_ratio",

    "buy_sol",
    "sell_sol",
    "net_sol",

    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
}


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return sum(xs) / len(xs) if xs else None


def stdev(xs):
    xs = [x for x in xs if valid(x)]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


def median(xs):
    xs = [x for x in xs if valid(x)]

    if not xs:
        return None

    return statistics.median(xs)


def percentile(xs, q):
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    if len(xs) == 1:
        return xs[0]

    p = (len(xs) - 1) * q

    lo = math.floor(p)
    hi = math.ceil(p)

    if lo == hi:
        return xs[lo]

    w = p - lo

    return (
        xs[lo] * (1 - w)
        + xs[hi] * w
    )


def ranks(values):
    indexed = sorted(
        enumerate(values),
        key=lambda x: x[1],
    )

    out = [0.0] * len(values)

    i = 0

    while i < len(indexed):
        j = i

        while (
            j + 1 < len(indexed)
            and indexed[j + 1][1] == indexed[i][1]
        ):
            j += 1

        rank = (i + j + 2) / 2.0

        for k in range(i, j + 1):
            out[indexed[k][0]] = rank

        i = j + 1

    return out


def pearson(x, y):
    if len(x) < 3:
        return None

    mx = mean(x)
    my = mean(y)

    if mx is None or my is None:
        return None

    num = sum(
        (a - mx) * (b - my)
        for a, b in zip(x, y)
    )

    dx = math.sqrt(
        sum((a - mx) ** 2 for a in x)
    )

    dy = math.sqrt(
        sum((b - my) ** 2 for b in y)
    )

    if dx == 0 or dy == 0:
        return None

    return num / (dx * dy)


def spearman(x, y):
    pairs = [
        (a, b)
        for a, b in zip(x, y)
        if valid(a) and valid(b)
    ]

    if len(pairs) < 3:
        return None

    return pearson(
        ranks([a for a, _ in pairs]),
        ranks([b for _, b in pairs]),
    )


def open_market():
    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=30,
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")

    return db


def open_research():
    db = sqlite3.connect(
        RESEARCH_DB,
        timeout=30,
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")

    return db


def verify_schema():
    db = open_market()

    table = db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE
        type='table'
        AND name='lab_exp0121_stage_features'
    """).fetchone()

    if not table:
        raise SystemExit(
            "ERROR: lab_exp0121_stage_features missing"
        )

    columns = {
        r["name"]
        for r in db.execute(
            "PRAGMA table_info(lab_exp0121_stage_features)"
        ).fetchall()
    }

    db.close()

    missing = sorted(
        BASE_REQUIRED - columns
    )

    if missing:
        raise SystemExit(
            "ERROR: required columns missing: "
            + ", ".join(missing)
        )

    print("SCHEMA OK")


def deterministic_bucket(mint, seed):
    raw = hashlib.sha256(
        f"{seed}:{mint}".encode()
    ).digest()

    return (
        int.from_bytes(raw[:8], "big")
        % 100
    )


def safe_div(a, b):
    if not valid(a) or not valid(b):
        return None

    if abs(b) < 1e-12:
        return None

    return a / b


def delta(snapshot, feature, a, b):
    va = snapshot.get(a, {}).get(feature)
    vb = snapshot.get(b, {}).get(feature)

    if not valid(va) or not valid(vb):
        return None

    return vb - va


def velocity(snapshot, feature, a, b):
    d = delta(snapshot, feature, a, b)

    if d is None:
        return None

    return d / (b - a)


def acceleration(snapshot, feature, a, b, c):
    v1 = velocity(
        snapshot,
        feature,
        a,
        b,
    )

    v2 = velocity(
        snapshot,
        feature,
        b,
        c,
    )

    if v1 is None or v2 is None:
        return None

    midpoint_gap = (
        (c + b) / 2
        - (b + a) / 2
    )

    if midpoint_gap == 0:
        return None

    return (
        v2 - v1
    ) / midpoint_gap


def build_token_matrix():
    db = open_market()

    rows = db.execute("""
    SELECT
        token_mint,
        stage_s,

        return_since_entry,
        mfe_so_far,
        mae_so_far,

        swaps,
        buys,
        sells,
        buy_ratio,

        buy_sol,
        sell_sol,
        net_sol,

        future_hit10,
        future_hit20,
        future_hit30,
        future_hit50

    FROM lab_exp0121_stage_features

    WHERE
        stage_s IN (5,10,20,30,60)
        AND future_ready=1
        AND coverage_status='GOOD'

    ORDER BY
        token_mint,
        stage_s
    """).fetchall()

    db.close()

    tokens = {}

    for row in rows:
        mint = row["token_mint"]
        stage = int(row["stage_s"])

        if mint not in tokens:
            tokens[mint] = {
                "stages": {},
                "targets": {},
            }

        tokens[mint]["stages"][stage] = dict(row)

        for target in TARGETS:
            value = row[target]

            if value is not None:
                tokens[mint]["targets"][target] = int(value)

    return tokens


def derive_features(token):
    s = token["stages"]

    out = {}

    # --------------------------------------------------
    # RAW FLOW AT OBSERVATION HORIZONS
    # --------------------------------------------------

    for stage in STAGES:
        row = s.get(stage)

        if not row:
            continue

        for feature in (
            "buy_sol",
            "sell_sol",
            "net_sol",
            "buy_ratio",
            "swaps",
            "buys",
            "sells",
            "return_since_entry",
            "mfe_so_far",
            "mae_so_far",
        ):
            value = row.get(feature)

            if valid(value):
                out[f"{feature}_{stage}"] = value

    # --------------------------------------------------
    # FLOW VELOCITIES
    # --------------------------------------------------

    intervals = [
        (5, 10),
        (10, 20),
        (20, 30),
        (30, 60),
    ]

    for a, b in intervals:
        for feature in (
            "buy_sol",
            "sell_sol",
            "net_sol",
            "buys",
            "sells",
            "swaps",
            "return_since_entry",
        ):
            value = velocity(
                s,
                feature,
                a,
                b,
            )

            if value is not None:
                out[
                    f"vel_{feature}_{a}_{b}"
                ] = value

    # --------------------------------------------------
    # FLOW ACCELERATION
    # --------------------------------------------------

    triples = [
        (5, 10, 20),
        (10, 20, 30),
        (20, 30, 60),
    ]

    for a, b, c in triples:
        for feature in (
            "buy_sol",
            "sell_sol",
            "net_sol",
            "buys",
            "sells",
            "swaps",
        ):
            value = acceleration(
                s,
                feature,
                a,
                b,
                c,
            )

            if value is not None:
                out[
                    f"acc_{feature}_{a}_{b}_{c}"
                ] = value

    # --------------------------------------------------
    # IMBALANCE DYNAMICS
    # --------------------------------------------------

    for a, b in intervals:
        change = delta(
            s,
            "buy_ratio",
            a,
            b,
        )

        if change is not None:
            out[
                f"delta_buy_ratio_{a}_{b}"
            ] = change

    # --------------------------------------------------
    # FLOW PERSISTENCE
    #
    # Positive means same directional net flow is being
    # maintained across consecutive windows.
    # --------------------------------------------------

    for a, b, c in triples:
        v1 = velocity(
            s,
            "net_sol",
            a,
            b,
        )

        v2 = velocity(
            s,
            "net_sol",
            b,
            c,
        )

        if v1 is not None and v2 is not None:
            out[
                f"net_flow_persistence_{a}_{b}_{c}"
            ] = (
                1.0
                if v1 * v2 > 0
                else -1.0
            )

            out[
                f"net_flow_momentum_{a}_{b}_{c}"
            ] = v2 - v1

    # --------------------------------------------------
    # BUY / SELL PRESSURE DIFFERENTIAL
    # --------------------------------------------------

    for stage in STAGES:
        row = s.get(stage)

        if not row:
            continue

        buy = row.get("buy_sol")
        sell = row.get("sell_sol")

        if valid(buy) and valid(sell):
            total = buy + sell

            if total > 0:
                out[
                    f"sol_imbalance_{stage}"
                ] = (
                    buy - sell
                ) / total

    # --------------------------------------------------
    # PRICE RESPONSE / FLOW EFFICIENCY
    # --------------------------------------------------

    for stage in STAGES:
        row = s.get(stage)

        if not row:
            continue

        ret = row.get(
            "return_since_entry"
        )

        net = row.get(
            "net_sol"
        )

        total_flow = None

        if (
            valid(row.get("buy_sol"))
            and valid(row.get("sell_sol"))
        ):
            total_flow = (
                abs(row["buy_sol"])
                + abs(row["sell_sol"])
            )

        efficiency = safe_div(
            ret,
            total_flow,
        )

        if efficiency is not None:
            out[
                f"price_per_total_sol_{stage}"
            ] = efficiency

        net_eff = safe_div(
            ret,
            abs(net)
            if valid(net)
            else None,
        )

        if net_eff is not None:
            out[
                f"price_per_abs_net_sol_{stage}"
            ] = net_eff

    # --------------------------------------------------
    # MFE / MAE PER FLOW
    # --------------------------------------------------

    for stage in STAGES:
        row = s.get(stage)

        if not row:
            continue

        flow = None

        if (
            valid(row.get("buy_sol"))
            and valid(row.get("sell_sol"))
        ):
            flow = (
                abs(row["buy_sol"])
                + abs(row["sell_sol"])
            )

        if flow is None or flow <= 0:
            continue

        mfe = row.get("mfe_so_far")
        mae = row.get("mae_so_far")

        if valid(mfe):
            out[
                f"mfe_per_sol_{stage}"
            ] = mfe / flow

        if valid(mae):
            out[
                f"mae_per_sol_{stage}"
            ] = mae / flow

    return out


def create_dataset(tokens, target):
    rows = []

    for mint, token in tokens.items():
        if target not in token["targets"]:
            continue

        features = derive_features(
            token
        )

        if not features:
            continue

        row = {
            "token_mint": mint,
            "target":
                token["targets"][target],
        }

        row.update(features)

        rows.append(row)

    return rows


def candidate_feature_sets(all_feature_names):
    names = set(all_feature_names)

    groups = {}

    groups["FLOW_LEVEL"] = sorted(
        n for n in names
        if (
            n.startswith("buy_sol_")
            or n.startswith("sell_sol_")
            or n.startswith("net_sol_")
            or n.startswith("sol_imbalance_")
        )
    )

    groups["FLOW_VELOCITY"] = sorted(
        n for n in names
        if n.startswith("vel_")
        and (
            "sol" in n
            or "buys" in n
            or "sells" in n
        )
    )

    groups["FLOW_ACCELERATION"] = sorted(
        n for n in names
        if n.startswith("acc_")
    )

    groups["IMBALANCE"] = sorted(
        n for n in names
        if (
            n.startswith("delta_buy_ratio_")
            or n.startswith("sol_imbalance_")
        )
    )

    groups["PERSISTENCE"] = sorted(
        n for n in names
        if (
            n.startswith(
                "net_flow_persistence_"
            )
            or n.startswith(
                "net_flow_momentum_"
            )
        )
    )

    groups["EFFICIENCY"] = sorted(
        n for n in names
        if (
            n.startswith(
                "price_per_"
            )
            or n.startswith(
                "mfe_per_sol_"
            )
            or n.startswith(
                "mae_per_sol_"
            )
        )
    )

    groups["ACTIVITY_DYNAMICS"] = sorted(
        n for n in names
        if (
            n.startswith("vel_swaps_")
            or n.startswith("vel_buys_")
            or n.startswith("vel_sells_")
            or n.startswith("acc_swaps_")
            or n.startswith("acc_buys_")
            or n.startswith("acc_sells_")
        )
    )

    groups["FLOW_MOTION"] = sorted(
        set(
            groups["FLOW_VELOCITY"]
            + groups["FLOW_ACCELERATION"]
            + groups["PERSISTENCE"]
        )
    )

    groups["FLOW_PLUS_EFFICIENCY"] = sorted(
        set(
            groups["FLOW_LEVEL"]
            + groups["FLOW_VELOCITY"]
            + groups["EFFICIENCY"]
        )
    )

    groups["FULL_DYNAMIC"] = sorted(
        set(
            itertools.chain.from_iterable(
                groups.values()
            )
        )
    )

    # Remove empty or pathological huge duplicate sets.
    clean = {}

    seen = set()

    for name, features in groups.items():
        if len(features) < 2:
            continue

        key = tuple(features)

        if key in seen:
            continue

        seen.add(key)
        clean[name] = features

    return clean


def complete_rows(rows, features):
    out = []

    for row in rows:
        if all(
            feature in row
            and valid(row[feature])
            for feature in features
        ):
            out.append(row)

    return out


def fit_model(rows, features):
    y = [
        row["target"]
        for row in rows
    ]

    model = {}

    for feature in features:
        x = [
            row[feature]
            for row in rows
        ]

        m = mean(x) or 0.0
        sd = stdev(x)

        if sd is None or sd == 0:
            sd = 1.0

        rho = spearman(
            x,
            y,
        )

        direction = 0.0

        if rho is not None:
            direction = (
                1.0
                if rho >= 0
                else -1.0
            )

        model[feature] = {
            "mean": m,
            "std": sd,
            "direction": direction,
        }

    return model


def score_rows(rows, features, model):
    output = []

    for row in rows:
        parts = []

        for feature in features:
            info = model[feature]

            z = (
                row[feature]
                - info["mean"]
            ) / info["std"]

            parts.append(
                z * info["direction"]
            )

        x = dict(row)
        x["_score"] = mean(parts) or 0.0

        output.append(x)

    return output


def split_evaluate(rows, features, seed):
    train = []
    test = []

    for row in rows:
        bucket = deterministic_bucket(
            row["token_mint"],
            seed,
        )

        if bucket < 70:
            train.append(row)
        else:
            test.append(row)

    if (
        len(train) < 35
        or len(test) < 12
    ):
        return None

    if (
        sum(r["target"] for r in train) < 6
        or sum(r["target"] for r in test) < 3
    ):
        return None

    model = fit_model(
        train,
        features,
    )

    scored = score_rows(
        test,
        features,
        model,
    )

    rho = spearman(
        [r["_score"] for r in scored],
        [r["target"] for r in scored],
    )

    if rho is None:
        return None

    ordered = sorted(
        scored,
        key=lambda r: r["_score"],
    )

    qn = max(
        1,
        len(ordered) // 4,
    )

    low = ordered[:qn]
    high = ordered[-qn:]

    low_rate = (
        100.0
        * sum(r["target"] for r in low)
        / len(low)
    )

    high_rate = (
        100.0
        * sum(r["target"] for r in high)
        / len(high)
    )

    return {
        "rho": rho,
        "qdiff": high_rate - low_rate,
    }


def repeated_test(rows, features):
    results = []

    for seed in range(N_SPLITS):
        result = split_evaluate(
            rows,
            features,
            seed,
        )

        if result:
            results.append(result)

    rhos = [
        r["rho"]
        for r in results
    ]

    qdiffs = [
        r["qdiff"]
        for r in results
    ]

    return {
        "splits": len(results),
        "median_rho": median(rhos),
        "p10_rho": percentile(
            rhos,
            0.10,
        ),
        "p90_rho": percentile(
            rhos,
            0.90,
        ),
        "sign_rate": (
            sum(r > 0 for r in rhos)
            / len(rhos)
            if rhos
            else 0.0
        ),
        "median_qdiff":
            median(qdiffs),
    }


def permutation_test(
    rows,
    features,
    observed_median,
):
    if observed_median is None:
        return None

    rng = random.Random(
        20260816
    )

    labels = [
        row["target"]
        for row in rows
    ]

    null = []

    for _ in range(
        N_PERMUTATIONS
    ):
        permuted_labels = list(
            labels
        )

        rng.shuffle(
            permuted_labels
        )

        permuted_rows = []

        for row, label in zip(
            rows,
            permuted_labels,
        ):
            x = dict(row)
            x["target"] = label
            permuted_rows.append(x)

        # Reduced split count inside permutation.
        rhos = []

        for seed in range(10):
            result = split_evaluate(
                permuted_rows,
                features,
                seed,
            )

            if result:
                rhos.append(
                    result["rho"]
                )

        value = median(rhos)

        if value is not None:
            null.append(value)

    if not null:
        return None

    extreme = sum(
        x >= observed_median
        for x in null
    )

    return (
        extreme + 1
    ) / (
        len(null) + 1
    )


def classify(result):
    if (
        result["splits"] >= 30
        and result["median_rho"] is not None
        and result["median_rho"] >= 0.15
        and result["p10_rho"] is not None
        and result["p10_rho"] > 0
        and result["sign_rate"] >= 0.85
        and result["median_qdiff"] is not None
        and result["median_qdiff"] >= 10
        and result["permutation_p"] is not None
        and result["permutation_p"] < 0.05
    ):
        return "ROBUST"

    if (
        result["median_rho"] is not None
        and result["median_rho"] >= 0.10
        and result["sign_rate"] >= 0.70
    ):
        return "WEAK"

    return "REJECT"


def init_research():
    db = open_research()

    db.execute("""
    CREATE TABLE IF NOT EXISTS v4_flow_dynamics (
        experiment_id TEXT PRIMARY KEY,
        target TEXT NOT NULL,
        family TEXT NOT NULL,
        n_tokens INTEGER,
        n_features INTEGER,
        verdict TEXT NOT NULL,
        median_rho REAL,
        p10_rho REAL,
        sign_rate REAL,
        median_qdiff REAL,
        permutation_p REAL,
        result_json TEXT NOT NULL,
        created_at REAL NOT NULL
    )
    """)

    db.commit()
    db.close()


def main():
    print("=" * 155)
    print("MEMECOIN LAB — V4 FLOW DYNAMICS LAB")
    print("=" * 155)

    verify_schema()
    init_research()

    tokens = build_token_matrix()

    print(
        f"TOKENS LOADED : {len(tokens)}"
    )

    all_results = []

    for target in TARGETS:
        dataset = create_dataset(
            tokens,
            target,
        )

        if not dataset:
            continue

        feature_names = sorted(
            set(
                key
                for row in dataset
                for key in row.keys()
                if key not in (
                    "token_mint",
                    "target",
                )
            )
        )

        families = candidate_feature_sets(
            feature_names
        )

        for family, features in families.items():
            rows = complete_rows(
                dataset,
                features,
            )

            if len(rows) < 50:
                continue

            positives = sum(
                row["target"]
                for row in rows
            )

            negatives = (
                len(rows)
                - positives
            )

            if positives < 10 or negatives < 25:
                continue

            base = repeated_test(
                rows,
                features,
            )

            p = permutation_test(
                rows,
                features,
                base[
                    "median_rho"
                ],
            )

            result = {
                "target": target,
                "family": family,
                "n_tokens": len(rows),
                "n_features": len(features),
                "positives": positives,
                "negatives": negatives,

                **base,

                "permutation_p": p,
                "features": features,
            }

            result["verdict"] = classify(
                result
            )

            raw_id = json.dumps(
                {
                    "target": target,
                    "family": family,
                    "features": features,
                },
                sort_keys=True,
            )

            exp_id = (
                "FLOW_"
                + hashlib.sha256(
                    raw_id.encode()
                ).hexdigest()[:18]
            )

            db = open_research()

            db.execute("""
            INSERT OR REPLACE INTO v4_flow_dynamics (
                experiment_id,
                target,
                family,
                n_tokens,
                n_features,
                verdict,
                median_rho,
                p10_rho,
                sign_rate,
                median_qdiff,
                permutation_p,
                result_json,
                created_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                exp_id,
                target,
                family,
                len(rows),
                len(features),
                result["verdict"],
                result["median_rho"],
                result["p10_rho"],
                result["sign_rate"],
                result["median_qdiff"],
                p,
                json.dumps(
                    result,
                    separators=(",", ":"),
                ),
                time.time(),
            ))

            db.commit()
            db.close()

            all_results.append(
                result
            )

            print(
                f"{target:<12}"
                f" | {family:<22}"
                f" | N={len(rows):>3}"
                f" | F={len(features):>2}"
                f" | MED={result['median_rho'] or 0:6.3f}"
                f" | P10={result['p10_rho'] or 0:6.3f}"
                f" | SIGN={100*result['sign_rate']:5.1f}%"
                f" | QD={result['median_qdiff'] or 0:6.1f}pp"
                f" | P={p if p is not None else 1:6.4f}"
                f" | {result['verdict']}"
            )

    verdict_order = {
        "ROBUST": 2,
        "WEAK": 1,
        "REJECT": 0,
    }

    all_results.sort(
        key=lambda r: (
            verdict_order[
                r["verdict"]
            ],
            r["median_rho"]
            if r["median_rho"] is not None
            else -999,
        ),
        reverse=True,
    )

    print()
    print("=" * 155)
    print("FINAL FLOW DYNAMICS RANKING")
    print("=" * 155)

    for i, result in enumerate(
        all_results,
        start=1,
    ):
        print(
            f"#{i:02d} "
            f"{result['verdict']:<7}"
            f" | {result['target']:<12}"
            f" | {result['family']:<22}"
            f" | N={result['n_tokens']:>3}"
            f" | F={result['n_features']:>2}"
            f" | MED={result['median_rho'] or 0:6.3f}"
            f" | P10={result['p10_rho'] or 0:6.3f}"
            f" | SIGN={100*result['sign_rate']:5.1f}%"
            f" | QD={result['median_qdiff'] or 0:6.1f}pp"
            f" | P={result['permutation_p'] if result['permutation_p'] is not None else 1:6.4f}"
        )

    robust = sum(
        r["verdict"] == "ROBUST"
        for r in all_results
    )

    weak = sum(
        r["verdict"] == "WEAK"
        for r in all_results
    )

    rejected = sum(
        r["verdict"] == "REJECT"
        for r in all_results
    )

    print()
    print("=" * 155)
    print("SUMMARY")
    print("=" * 155)

    print(
        f"EXPERIMENTS : {len(all_results)}"
    )

    print(
        f"ROBUST      : {robust}"
    )

    print(
        f"WEAK        : {weak}"
    )

    print(
        f"REJECT      : {rejected}"
    )

    if robust > 0:
        print()
        print(
            "NEXT ACTION: FREEZE ROBUST FLOW MODEL(S)"
        )

        print(
            "Then prospective-only shadow validation."
        )

    else:
        print()
        print(
            "NEXT ACTION: STOP FLOW FAMILY."
        )

        print(
            "Open MIGRATION / SUCCESS / DEATH LAB."
        )


if __name__ == "__main__":
    main()
