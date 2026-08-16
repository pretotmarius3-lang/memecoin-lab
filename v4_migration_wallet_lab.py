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

SNAPSHOTS = [30, 60, 120]
HORIZONS = [300, 600, 900, 1800]

N_SPLITS = 50
N_PERMUTATIONS = 300


PRICE_FEATURES = [
    "return_pct",
    "range_pct",
    "max_run_pct",
    "max_drawdown_pct",
]

FLOW_FEATURES = [
    "swaps",
    "buys",
    "sells",
    "buy_ratio",
    "buy_sol",
    "sell_sol",
    "net_sol",
    "gross_sol",
    "avg_trade_sol",
    "median_trade_sol",
    "std_trade_sol",
    "max_trade_sol",
    "buy_sol_per_swap",
    "net_sol_per_swap",
    "first_half_net_sol",
    "second_half_net_sol",
    "net_flow_acceleration",
    "first_half_buy_ratio",
    "second_half_buy_ratio",
    "buy_ratio_change",
]

WALLET_FEATURES = [
    "unique_wallets",
    "unique_buyers",
    "unique_sellers",
    "buyer_seller_ratio",
    "wallet_top1_share",
    "wallet_top3_share",
    "wallet_top5_share",
    "wallet_hhi",
    "wallet_entropy",
    "repeat_wallet_ratio",
    "unique_signatures",
]

MICROSTRUCTURE_FEATURES = [
    "top1_trade_share",
    "top3_trade_share",
    "top5_trade_share",
    "trade_hhi",
    "trade_entropy",
    "price_move_per_gross_sol",
    "price_move_per_net_sol",
]

FAMILIES = {
    "PRICE": PRICE_FEATURES,
    "FLOW": FLOW_FEATURES,
    "WALLET": WALLET_FEATURES,
    "MICROSTRUCTURE": MICROSTRUCTURE_FEATURES,
    "PRICE_FLOW": sorted(set(PRICE_FEATURES + FLOW_FEATURES)),
    "FLOW_WALLET": sorted(set(FLOW_FEATURES + WALLET_FEATURES)),
    "FULL": sorted(set(
        PRICE_FEATURES
        + FLOW_FEATURES
        + WALLET_FEATURES
        + MICROSTRUCTURE_FEATURES
    )),
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


def stdev(xs):
    xs = [x for x in xs if valid(x)]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


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


def ranks(values):
    ordered = sorted(
        enumerate(values),
        key=lambda x: x[1]
    )

    out = [0.0] * len(values)

    i = 0

    while i < len(ordered):
        j = i

        while (
            j + 1 < len(ordered)
            and ordered[j + 1][1] == ordered[i][1]
        ):
            j += 1

        rank = (i + j + 2) / 2.0

        for k in range(i, j + 1):
            out[ordered[k][0]] = rank

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
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    return db


def deterministic_bucket(mint, seed):
    h = hashlib.sha256(
        f"{seed}:{mint}".encode()
    ).digest()

    return int.from_bytes(
        h[:8],
        "big"
    ) % 100


def winsorize_fit(rows, features):
    limits = {}

    for feature in features:
        vals = sorted(
            row[feature]
            for row in rows
            if valid(row.get(feature))
        )

        if not vals:
            continue

        limits[feature] = (
            percentile(vals, 0.01),
            percentile(vals, 0.99),
        )

    return limits


def clip(value, bounds):
    if not valid(value):
        return None

    lo, hi = bounds

    return min(
        hi,
        max(lo, value)
    )


def fit_model(rows, features):
    limits = winsorize_fit(
        rows,
        features
    )

    y = [
        row["target"]
        for row in rows
    ]

    model = {}

    for feature in features:
        if feature not in limits:
            continue

        xs = [
            clip(
                row[feature],
                limits[feature]
            )
            for row in rows
        ]

        if not all(valid(x) for x in xs):
            continue

        m = mean(xs) or 0.0
        sd = stdev(xs)

        if sd is None or sd == 0:
            sd = 1.0

        rho = spearman(
            xs,
            y
        )

        direction = 0.0

        if rho is not None:
            direction = (
                1.0
                if rho >= 0
                else -1.0
            )

        model[feature] = {
            "lo": limits[feature][0],
            "hi": limits[feature][1],
            "mean": m,
            "std": sd,
            "direction": direction,
            "feature_rho": rho,
        }

    return model


def score_rows(rows, model):
    output = []

    for row in rows:
        components = []

        for feature, info in model.items():
            value = row.get(feature)

            if not valid(value):
                continue

            value = min(
                info["hi"],
                max(
                    info["lo"],
                    value
                )
            )

            z = (
                value - info["mean"]
            ) / info["std"]

            components.append(
                z * info["direction"]
            )

        if not components:
            continue

        x = dict(row)
        x["_score"] = mean(components) or 0.0

        output.append(x)

    return output


def quartile_diff(rows):
    if len(rows) < 12:
        return None

    ordered = sorted(
        rows,
        key=lambda r: r["_score"]
    )

    q = max(
        1,
        len(ordered) // 4
    )

    low = ordered[:q]
    high = ordered[-q:]

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

    return high_rate - low_rate


def evaluate_split(rows, features, seed):
    train = []
    test = []

    for row in rows:
        bucket = deterministic_bucket(
            row["token_mint"],
            seed
        )

        if bucket < 70:
            train.append(row)
        else:
            test.append(row)

    train_pos = sum(
        r["target"]
        for r in train
    )

    test_pos = sum(
        r["target"]
        for r in test
    )

    if (
        len(train) < 100
        or len(test) < 35
        or train_pos < 8
        or test_pos < 3
    ):
        return None

    model = fit_model(
        train,
        features
    )

    if not model:
        return None

    scored = score_rows(
        test,
        model
    )

    if len(scored) < 30:
        return None

    rho = spearman(
        [r["_score"] for r in scored],
        [r["target"] for r in scored],
    )

    if rho is None:
        return None

    return {
        "rho": rho,
        "qdiff": quartile_diff(scored),
        "test_n": len(scored),
        "test_pos": sum(
            r["target"]
            for r in scored
        ),
    }


def repeated_test(rows, features):
    results = []

    for seed in range(N_SPLITS):
        result = evaluate_split(
            rows,
            features,
            seed
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
        if r["qdiff"] is not None
    ]

    return {
        "splits": len(results),
        "median_rho": median(rhos),
        "p10_rho": percentile(
            rhos,
            0.10
        ),
        "p90_rho": percentile(
            rhos,
            0.90
        ),
        "sign_rate": (
            sum(rho > 0 for rho in rhos)
            / len(rhos)
            if rhos
            else 0.0
        ),
        "median_qdiff": median(qdiffs),
    }


def permutation_test(
    rows,
    features,
    observed_median
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

    null_medians = []

    for _ in range(
        N_PERMUTATIONS
    ):
        permuted_labels = list(labels)
        rng.shuffle(permuted_labels)

        permuted = []

        for row, label in zip(
            rows,
            permuted_labels
        ):
            x = dict(row)
            x["target"] = label
            permuted.append(x)

        rhos = []

        for seed in range(8):
            result = evaluate_split(
                permuted,
                features,
                seed
            )

            if result:
                rhos.append(
                    result["rho"]
                )

        m = median(rhos)

        if m is not None:
            null_medians.append(m)

    if not null_medians:
        return None

    exceed = sum(
        x >= observed_median
        for x in null_medians
    )

    return (
        exceed + 1
    ) / (
        len(null_medians) + 1
    )


def migration_times():
    """
    Discover the real t101_migrations schema instead of assuming
    migration_time / block_time / detected_at exist.
    """

    db = open_market()

    # --------------------------------------------------------
    # REAL SCHEMA
    # --------------------------------------------------------

    info = db.execute(
        "PRAGMA table_info(t101_migrations)"
    ).fetchall()

    cols = {
        row["name"]
        for row in info
    }

    print()
    print("MIGRATION TABLE SCHEMA")
    print("-" * 90)
    print(", ".join(sorted(cols)))

    # --------------------------------------------------------
    # TOKEN COLUMN
    # --------------------------------------------------------

    token_candidates = [
        "token_mint",
        "mint",
        "address",
        "token",
    ]

    token_col = next(
        (
            c
            for c in token_candidates
            if c in cols
        ),
        None,
    )

    if token_col is None:
        db.close()

        raise RuntimeError(
            "Cannot identify token column "
            "in t101_migrations. "
            f"Columns={sorted(cols)}"
        )

    # --------------------------------------------------------
    # MIGRATION TIMESTAMP COLUMN
    # --------------------------------------------------------

    time_candidates = [
        "migration_ts",
        "migrated_at",
        "migration_at",
        "migration_timestamp",
        "block_time",
        "timestamp",
        "detected_at",
        "created_at",
        "ts",
        "time",
    ]

    time_col = next(
        (
            c
            for c in time_candidates
            if c in cols
        ),
        None,
    )

    if time_col is None:
        db.close()

        raise RuntimeError(
            "Cannot identify migration timestamp "
            "in t101_migrations. "
            f"Columns={sorted(cols)}"
        )

    # --------------------------------------------------------
    # OPTIONAL STATUS FILTER
    # --------------------------------------------------------

    status_col = next(
        (
            c
            for c in [
                "status",
                "state",
                "result",
            ]
            if c in cols
        ),
        None,
    )

    where = [
        f"{token_col} IS NOT NULL",
        f"{time_col} IS NOT NULL",
    ]

    # Do NOT blindly assume status='OK'.
    # Existing rows in t101_migrations are migration records;
    # status semantics can be inspected separately.

    sql = f"""
    SELECT
        {token_col} AS token_mint,
        MIN({time_col}) AS migration_ts
    FROM t101_migrations
    WHERE
        {" AND ".join(where)}
    GROUP BY
        {token_col}
    """

    rows = db.execute(sql).fetchall()

    migrations = {}

    for row in rows:
        mint = row["token_mint"]
        ts = row["migration_ts"]

        if mint is None or ts is None:
            continue

        # Normalize numeric timestamps.
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            continue

        # Milliseconds -> seconds if necessary.
        if ts > 10_000_000_000:
            ts /= 1000.0

        migrations[str(mint)] = ts

    print()
    print(
        "MIGRATION COLUMN MAPPING"
    )
    print("-" * 90)
    print(
        f"token     -> {token_col}"
    )
    print(
        f"timestamp -> {time_col}"
    )
    print(
        f"status    -> {status_col or 'NONE / NOT USED'}"
    )
    print(
        f"migrations loaded -> {len(migrations):,}"
    )

    if migrations:
        values = sorted(
            migrations.values()
        )

        print(
            f"earliest migration -> {values[0]:.0f}"
        )
        print(
            f"latest migration   -> {values[-1]:.0f}"
        )

    db.close()

    return migrations


def load_snapshot_rows(
    snapshot_s,
    horizon_s,
    migrations
):
    db = open_research()

    rows = [
        dict(r)
        for r in db.execute("""
        SELECT *
        FROM v4_onchain_snapshots
        WHERE snapshot_s=?
        """, (
            snapshot_s,
        )).fetchall()
    ]

    db.close()

    now = time.time()

    dataset = []

    for row in rows:
        mint = row["token_mint"]
        snapshot_ts = row["snapshot_ts"]

        if snapshot_ts is None:
            continue

        target_end = (
            snapshot_ts
            + horizon_s
        )

        # Censor future-unobserved tokens.
        if now < target_end:
            continue

        migration_ts = migrations.get(
            mint
        )

        # If it migrated before snapshot,
        # it is not a prediction candidate.
        if (
            migration_ts is not None
            and migration_ts <= snapshot_ts
        ):
            continue

        target = int(
            migration_ts is not None
            and snapshot_ts < migration_ts <= target_end
        )

        x = dict(row)
        x["target"] = target

        dataset.append(x)

    return dataset


def complete_rows(rows, features):
    return [
        row
        for row in rows
        if all(
            valid(row.get(feature))
            for feature in features
        )
    ]


def classify(result):
    if (
        result["splits"] >= 30
        and result["median_rho"] is not None
        and result["median_rho"] >= 0.15
        and result["p10_rho"] is not None
        and result["p10_rho"] > 0
        and result["sign_rate"] >= 0.85
        and result["median_qdiff"] is not None
        and result["median_qdiff"] >= 10.0
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


def feature_audit(rows, features):
    y = [
        row["target"]
        for row in rows
    ]

    result = []

    for feature in features:
        xs = [
            row[feature]
            for row in rows
        ]

        rho = spearman(
            xs,
            y
        )

        if rho is None:
            continue

        pos = [
            row[feature]
            for row in rows
            if row["target"] == 1
        ]

        neg = [
            row[feature]
            for row in rows
            if row["target"] == 0
        ]

        result.append({
            "feature": feature,
            "rho": rho,
            "pos_median": median(pos),
            "neg_median": median(neg),
        })

    result.sort(
        key=lambda x: abs(x["rho"]),
        reverse=True
    )

    return result


def init_output():
    db = open_research()

    db.execute("""
    CREATE TABLE IF NOT EXISTS v4_migration_wallet_results (
        experiment_id TEXT PRIMARY KEY,
        snapshot_s INTEGER NOT NULL,
        horizon_s INTEGER NOT NULL,
        family TEXT NOT NULL,

        n_tokens INTEGER NOT NULL,
        positives INTEGER NOT NULL,
        negatives INTEGER NOT NULL,

        n_features INTEGER NOT NULL,

        verdict TEXT NOT NULL,

        median_rho REAL,
        p10_rho REAL,
        p90_rho REAL,
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
    print("=" * 165)
    print("MEMECOIN LAB — V4 MIGRATION / WALLET STRUCTURE LAB")
    print("=" * 165)

    init_output()

    migrations = migration_times()

    print(
        f"KNOWN MIGRATIONS : {len(migrations)}"
    )

    all_results = []

    for snapshot_s in SNAPSHOTS:
        for horizon_s in HORIZONS:
            base = load_snapshot_rows(
                snapshot_s,
                horizon_s,
                migrations
            )

            if not base:
                continue

            print()
            print(
                f"SNAPSHOT={snapshot_s}s "
                f"HORIZON={horizon_s}s "
                f"BASE_N={len(base)}"
            )

            for family, features in FAMILIES.items():
                rows = complete_rows(
                    base,
                    features
                )

                if len(rows) < 150:
                    continue

                positives = sum(
                    row["target"]
                    for row in rows
                )

                negatives = (
                    len(rows)
                    - positives
                )

                # We do not pretend to model
                # tiny positive samples.
                if (
                    positives < 15
                    or negatives < 80
                ):
                    print(
                        f"{family:<16}"
                        f" N={len(rows):>4}"
                        f" MIG={positives:>3}"
                        f" -> COLLECT_MORE"
                    )
                    continue

                base_test = repeated_test(
                    rows,
                    features
                )

                p = permutation_test(
                    rows,
                    features,
                    base_test[
                        "median_rho"
                    ]
                )

                result = {
                    "snapshot_s":
                        snapshot_s,

                    "horizon_s":
                        horizon_s,

                    "family":
                        family,

                    "n_tokens":
                        len(rows),

                    "positives":
                        positives,

                    "negatives":
                        negatives,

                    "n_features":
                        len(features),

                    **base_test,

                    "permutation_p":
                        p,

                    "feature_audit":
                        feature_audit(
                            rows,
                            features
                        )[:12],
                }

                result["verdict"] = classify(
                    result
                )

                raw = json.dumps({
                    "snapshot_s":
                        snapshot_s,

                    "horizon_s":
                        horizon_s,

                    "family":
                        family,
                }, sort_keys=True)

                experiment_id = (
                    "MIG_"
                    + hashlib.sha256(
                        raw.encode()
                    ).hexdigest()[:18]
                )

                db = open_research()

                db.execute("""
                INSERT OR REPLACE INTO v4_migration_wallet_results (
                    experiment_id,
                    snapshot_s,
                    horizon_s,
                    family,

                    n_tokens,
                    positives,
                    negatives,

                    n_features,

                    verdict,

                    median_rho,
                    p10_rho,
                    p90_rho,
                    sign_rate,
                    median_qdiff,
                    permutation_p,

                    result_json,
                    created_at
                )

                VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
                """, (
                    experiment_id,
                    snapshot_s,
                    horizon_s,
                    family,

                    len(rows),
                    positives,
                    negatives,

                    len(features),

                    result["verdict"],

                    result["median_rho"],
                    result["p10_rho"],
                    result["p90_rho"],
                    result["sign_rate"],
                    result["median_qdiff"],
                    p,

                    json.dumps(
                        result,
                        separators=(",", ":")
                    ),

                    time.time(),
                ))

                db.commit()
                db.close()

                all_results.append(
                    result
                )

                print(
                    f"{family:<16}"
                    f" | N={len(rows):>4}"
                    f" | MIG={positives:>3}"
                    f" | MED={result['median_rho'] or 0:6.3f}"
                    f" | P10={result['p10_rho'] or 0:6.3f}"
                    f" | SIGN={100*result['sign_rate']:5.1f}%"
                    f" | QD={result['median_qdiff'] or 0:6.1f}pp"
                    f" | P={p if p is not None else 1:6.4f}"
                    f" | {result['verdict']}"
                )

    verdict_rank = {
        "ROBUST": 2,
        "WEAK": 1,
        "REJECT": 0,
    }

    all_results.sort(
        key=lambda r: (
            verdict_rank[
                r["verdict"]
            ],
            r["median_rho"]
            if r["median_rho"] is not None
            else -999,
        ),
        reverse=True,
    )

    print()
    print("=" * 165)
    print("FINAL MIGRATION RANKING")
    print("=" * 165)

    for index, result in enumerate(
        all_results,
        start=1
    ):
        print(
            f"#{index:02d} "
            f"{result['verdict']:<7}"
            f" | T={result['snapshot_s']:>3}s"
            f" | H={result['horizon_s']:>4}s"
            f" | {result['family']:<16}"
            f" | N={result['n_tokens']:>4}"
            f" | MIG={result['positives']:>3}"
            f" | MED={result['median_rho'] or 0:6.3f}"
            f" | P10={result['p10_rho'] or 0:6.3f}"
            f" | SIGN={100*result['sign_rate']:5.1f}%"
            f" | QD={result['median_qdiff'] or 0:6.1f}pp"
            f" | P={result['permutation_p'] if result['permutation_p'] is not None else 1:6.4f}"
        )

        if index <= 5:
            print("     TOP FEATURES:")

            for feature in result[
                "feature_audit"
            ][:8]:
                print(
                    f"       {feature['feature']:<28}"
                    f" rho={feature['rho']:>7.3f}"
                    f" | MIG med={feature['pos_median']}"
                    f" | NON med={feature['neg_median']}"
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
    print("=" * 165)
    print("SUMMARY")
    print("=" * 165)

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

    if robust:
        print()
        print(
            "NEXT: freeze migration candidate(s) "
            "and build wallet-cluster holdout."
        )

    else:
        print()
        print(
            "NEXT: proceed to SUCCESS / DEATH "
            "and TWINS even if migration itself is weak."
        )


if __name__ == "__main__":
    main()
