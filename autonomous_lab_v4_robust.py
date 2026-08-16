#!/usr/bin/env python3

import hashlib
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

N_SPLITS = 50
N_PERMUTATIONS = 300

RES_FAMILIES = {
    "PRICE": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",
    ],

    "ACTIVITY": [
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
    ],

    "FLOW": [
        "buy_sol",
        "sell_sol",
        "net_sol",
    ],

    "PRICE_ACTIVITY": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
    ],

    "ALL": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
        "buy_sol",
        "sell_sol",
        "net_sol",
    ],
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


def median(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def percentile(xs, q):
    xs = sorted(x for x in xs if valid(x))

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


def stdev(xs):
    xs = [x for x in xs if valid(x)]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


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


def load_resurrection(spec):
    family = spec["family"]

    if family not in RES_FAMILIES:
        return [], []

    features = RES_FAMILIES[family]
    target = spec["target"]
    stage = int(spec["stage_s"])

    db = open_market()

    rows = [
        dict(r)
        for r in db.execute(
            f"""
            SELECT
                token_mint,
                {",".join(features)},
                {target} AS target
            FROM lab_exp0121_stage_features
            WHERE
                stage_s=?
                AND future_ready=1
                AND coverage_status='GOOD'
                AND {target} IS NOT NULL
            """,
            (stage,),
        ).fetchall()
    ]

    db.close()

    good = []

    for row in rows:
        if not all(valid(row[f]) for f in features):
            continue

        row["target"] = int(row["target"])
        good.append(row)

    return good, features


def deterministic_bucket(mint, seed):
    payload = f"{seed}:{mint}".encode()

    return (
        int.from_bytes(
            hashlib.sha256(payload).digest()[:8],
            "big",
        )
        % 100
    )


def fit_model(rows, features):
    y = [r["target"] for r in rows]

    model = {}

    for feature in features:
        xs = [r[feature] for r in rows]

        m = mean(xs) or 0.0
        sd = stdev(xs)

        if sd is None or sd == 0:
            sd = 1.0

        rho = spearman(xs, y)

        if rho is None:
            direction = 0.0
        elif rho >= 0:
            direction = 1.0
        else:
            direction = -1.0

        model[feature] = {
            "mean": m,
            "std": sd,
            "direction": direction,
        }

    return model


def score(rows, features, model):
    output = []

    for row in rows:
        parts = []

        for feature in features:
            info = model[feature]

            z = (
                row[feature] - info["mean"]
            ) / info["std"]

            parts.append(
                z * info["direction"]
            )

        x = dict(row)
        x["_score"] = mean(parts) or 0.0

        output.append(x)

    return output


def evaluate_test(train, test, features):
    if len(train) < 30 or len(test) < 10:
        return None

    train_pos = sum(r["target"] for r in train)
    test_pos = sum(r["target"] for r in test)

    if train_pos < 5 or test_pos < 2:
        return None

    model = fit_model(
        train,
        features,
    )

    scored = score(
        test,
        features,
        model,
    )

    rho = spearman(
        [r["_score"] for r in scored],
        [r["target"] for r in scored],
    )

    ordered = sorted(
        scored,
        key=lambda r: r["_score"],
    )

    qn = max(
        1,
        len(ordered) // 4,
    )

    q1 = ordered[:qn]
    q4 = ordered[-qn:]

    q1_rate = (
        100.0
        * sum(r["target"] for r in q1)
        / len(q1)
    )

    q4_rate = (
        100.0
        * sum(r["target"] for r in q4)
        / len(q4)
    )

    return {
        "rho": rho,
        "qdiff": q4_rate - q1_rate,
        "n": len(test),
        "positive": test_pos,
        "scores": scored,
    }


def repeated_splits(rows, features):
    results = []

    for seed in range(N_SPLITS):
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

        result = evaluate_test(
            train,
            test,
            features,
        )

        if (
            result
            and result["rho"] is not None
        ):
            results.append(result)

    return results


def extreme_score_stress(rows, features):
    model = fit_model(
        rows,
        features,
    )

    scored = score(
        rows,
        features,
        model,
    )

    ranked = sorted(
        scored,
        key=lambda r: abs(r["_score"]),
        reverse=True,
    )

    remove_n = max(
        1,
        int(len(ranked) * 0.05),
    )

    trimmed = ranked[remove_n:]

    return spearman(
        [r["_score"] for r in trimmed],
        [r["target"] for r in trimmed],
    )


def feature_ablation(rows, features):
    results = {}

    if len(features) <= 1:
        return results

    for removed in features:
        keep = [
            feature
            for feature in features
            if feature != removed
        ]

        splits = repeated_splits(
            rows,
            keep,
        )

        rhos = [
            result["rho"]
            for result in splits
            if result["rho"] is not None
        ]

        results[removed] = {
            "median_rho": median(rhos),
            "p10_rho": percentile(rhos, 0.10),
            "sign_rate": (
                sum(r > 0 for r in rhos) / len(rhos)
                if rhos
                else None
            ),
        }

    return results


def permutation_test(rows, features, observed_median):
    if observed_median is None:
        return None

    rng = random.Random(117117)

    target_values = [
        row["target"]
        for row in rows
    ]

    null_medians = []

    for permutation_id in range(
        N_PERMUTATIONS
    ):
        shuffled = list(target_values)
        rng.shuffle(shuffled)

        permuted = []

        for row, target in zip(
            rows,
            shuffled,
        ):
            x = dict(row)
            x["target"] = target
            permuted.append(x)

        # Fewer repeated splits per permutation to keep it fast.
        permutation_rhos = []

        for seed in range(8):
            train = []
            test = []

            for row in permuted:
                bucket = deterministic_bucket(
                    row["token_mint"],
                    seed,
                )

                if bucket < 70:
                    train.append(row)
                else:
                    test.append(row)

            result = evaluate_test(
                train,
                test,
                features,
            )

            if (
                result
                and result["rho"] is not None
            ):
                permutation_rhos.append(
                    result["rho"]
                )

        m = median(permutation_rhos)

        if m is not None:
            null_medians.append(m)

    if not null_medians:
        return None

    exceed = sum(
        value >= observed_median
        for value in null_medians
    )

    return (
        exceed + 1
    ) / (
        len(null_medians) + 1
    )


def classify(result):
    if (
        result["split_count"] >= 30
        and result["median_rho"] is not None
        and result["median_rho"] >= 0.12
        and result["p10_rho"] is not None
        and result["p10_rho"] > 0
        and result["sign_rate"] >= 0.80
        and result["median_qdiff"] is not None
        and result["median_qdiff"] >= 10.0
        and result["trimmed_rho"] is not None
        and result["trimmed_rho"] > 0
        and result["permutation_p"] is not None
        and result["permutation_p"] <= 0.05
    ):
        return "ROBUST"

    if (
        result["median_rho"] is not None
        and result["median_rho"] > 0
        and result["sign_rate"] >= 0.65
    ):
        return "WEAK"

    return "REJECT"


def main():
    research = open_research()

    research.execute("""
    CREATE TABLE IF NOT EXISTS v4_robustness (
        experiment_id TEXT PRIMARY KEY,
        verdict TEXT NOT NULL,
        result_json TEXT NOT NULL,
        created_at REAL NOT NULL
    )
    """)

    survivors = research.execute("""
    SELECT
        experiment_id,
        spec_json,
        discovery_rho,
        holdout_rho,
        qdiff_pp
    FROM v4_fast_experiments
    WHERE status='PASS_HOLDOUT'
    ORDER BY holdout_rho DESC
    """).fetchall()

    research.close()

    print("=" * 145)
    print("MEMECOIN LAB — V4 ROBUSTNESS ARENA")
    print("=" * 145)
    print(f"CANDIDATES : {len(survivors)}")
    print(f"SPLITS     : {N_SPLITS} / candidate")
    print(f"PERMUTATIONS: {N_PERMUTATIONS} / candidate")
    print()

    outputs = []

    for index, candidate in enumerate(
        survivors,
        start=1,
    ):
        spec = json.loads(
            candidate["spec_json"]
        )

        if spec["branch"] != "RESURRECTION":
            print(
                f"[{index}/{len(survivors)}] "
                f"{spec['branch']} skipped for dedicated branch test"
            )
            continue

        rows, features = load_resurrection(
            spec
        )

        splits = repeated_splits(
            rows,
            features,
        )

        rhos = [
            result["rho"]
            for result in splits
            if result["rho"] is not None
        ]

        qdiffs = [
            result["qdiff"]
            for result in splits
            if result["qdiff"] is not None
        ]

        result = {
            "spec": spec,
            "n_tokens": len(rows),

            "original_discovery_rho":
                candidate["discovery_rho"],

            "original_holdout_rho":
                candidate["holdout_rho"],

            "original_qdiff_pp":
                candidate["qdiff_pp"],

            "split_count": len(rhos),

            "median_rho": median(rhos),
            "p10_rho": percentile(rhos, 0.10),
            "p90_rho": percentile(rhos, 0.90),

            "sign_rate": (
                sum(rho > 0 for rho in rhos)
                / len(rhos)
                if rhos
                else 0.0
            ),

            "median_qdiff": median(qdiffs),

            "trimmed_rho":
                extreme_score_stress(
                    rows,
                    features,
                ),

            "ablations":
                feature_ablation(
                    rows,
                    features,
                ),
        }

        result["permutation_p"] = permutation_test(
            rows,
            features,
            result["median_rho"],
        )

        result["verdict"] = classify(
            result
        )

        outputs.append(result)

        research = open_research()

        research.execute("""
        INSERT OR REPLACE INTO v4_robustness (
            experiment_id,
            verdict,
            result_json,
            created_at
        )
        VALUES (?,?,?,?)
        """, (
            candidate["experiment_id"],
            result["verdict"],
            json.dumps(
                result,
                separators=(",", ":"),
            ),
            time.time(),
        ))

        research.commit()
        research.close()

        print(
            f"[{index:02d}/{len(survivors):02d}] "
            f"{spec['family']:<16}"
            f"T={spec['stage_s']:>2}s "
            f"{spec['target']:<12}"
            f" | N={len(rows):>3}"
            f" | MED={result['median_rho'] if result['median_rho'] is not None else 0:6.3f}"
            f" | P10={result['p10_rho'] if result['p10_rho'] is not None else 0:6.3f}"
            f" | SIGN={100*result['sign_rate']:5.1f}%"
            f" | QD={result['median_qdiff'] if result['median_qdiff'] is not None else 0:6.1f}pp"
            f" | P={result['permutation_p'] if result['permutation_p'] is not None else 1:6.4f}"
            f" | {result['verdict']}"
        )

    outputs.sort(
        key=lambda r: (
            r["verdict"] == "ROBUST",
            r["median_rho"]
            if r["median_rho"] is not None
            else -999,
        ),
        reverse=True,
    )

    robust = [
        r for r in outputs
        if r["verdict"] == "ROBUST"
    ]

    weak = [
        r for r in outputs
        if r["verdict"] == "WEAK"
    ]

    rejected = [
        r for r in outputs
        if r["verdict"] == "REJECT"
    ]

    print()
    print("=" * 145)
    print("FINAL ROBUSTNESS RANKING")
    print("=" * 145)

    for rank, result in enumerate(
        outputs,
        start=1,
    ):
        spec = result["spec"]

        print(
            f"#{rank:02d} "
            f"{result['verdict']:<7}"
            f" | {spec['family']:<16}"
            f" | T={spec['stage_s']:>2}s"
            f" | {spec['target']:<12}"
            f" | MED={result['median_rho'] or 0:6.3f}"
            f" | P10={result['p10_rho'] or 0:6.3f}"
            f" | SIGN={100*result['sign_rate']:5.1f}%"
            f" | P={result['permutation_p'] if result['permutation_p'] is not None else 1:6.4f}"
        )

    print()
    print("=" * 145)
    print("SUMMARY")
    print("=" * 145)

    print(
        f"ROBUST : {len(robust)}"
    )

    print(
        f"WEAK   : {len(weak)}"
    )

    print(
        f"REJECT : {len(rejected)}"
    )

    print()
    print(
        "IMPORTANT: historical robustness is NOT prospective validation."
    )

    print(
        "Next: freeze ROBUST candidates and test only on NEW incoming tokens."
    )


if __name__ == "__main__":
    main()
