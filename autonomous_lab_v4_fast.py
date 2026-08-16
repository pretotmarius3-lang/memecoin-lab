#!/usr/bin/env python3

import concurrent.futures
import hashlib
import json
import math
import os
import random
import signal
import sqlite3
import statistics
import time
from pathlib import Path


ROOT = Path.home() / "memecoin_lab"
MARKET_DB = ROOT / "validation_v090.db"
RESEARCH_DB = ROOT / "research_v4.db"

CPU = os.cpu_count() or 4
MAX_WORKERS = min(12, max(6, CPU // 2))

REFRESH = 2
shutdown_requested = False


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

RES_STAGES = [5, 10, 20, 30, 60]
RES_TARGETS = [
    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
]

MIG_STAGES = [30, 60, 90, 120, 180, 300]
MIG_HORIZONS = [300, 600, 900, 1800]


def stop(sig, frame):
    global shutdown_requested
    shutdown_requested = True


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


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


def ranks(values):
    indexed = sorted(enumerate(values), key=lambda x: x[1])
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

    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    dx = math.sqrt(sum((a - mx) ** 2 for a in x))
    dy = math.sqrt(sum((b - my) ** 2 for b in y))

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

    xx = [a for a, _ in pairs]
    yy = [b for _, b in pairs]

    return pearson(ranks(xx), ranks(yy))


def deterministic_holdout(mint):
    h = hashlib.sha256(
        mint.encode()
    ).digest()

    # 75% discovery / 25% frozen holdout
    return (
        int.from_bytes(h[:4], "big") % 100
    ) >= 75


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


def init_db():
    db = open_research()

    db.executescript("""
    CREATE TABLE IF NOT EXISTS v4_fast_experiments (
        experiment_id TEXT PRIMARY KEY,
        branch TEXT NOT NULL,
        spec_json TEXT NOT NULL,
        status TEXT NOT NULL,
        discovery_n INTEGER,
        holdout_n INTEGER,
        discovery_rho REAL,
        holdout_rho REAL,
        qdiff_pp REAL,
        positive_holdout INTEGER,
        result_json TEXT,
        created_at REAL NOT NULL,
        finished_at REAL
    );

    CREATE TABLE IF NOT EXISTS v4_fast_runs (
        run_id TEXT PRIMARY KEY,
        started_at REAL NOT NULL,
        finished_at REAL,
        experiments INTEGER NOT NULL DEFAULT 0,
        passed INTEGER NOT NULL DEFAULT 0
    );
    """)

    db.commit()
    db.close()


def fingerprint(spec):
    payload = json.dumps(
        spec,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(
        payload.encode()
    ).hexdigest()[:18]


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

        r = dict(row)
        r["_score"] = mean(parts) or 0.0
        output.append(r)

    return output


def quartile_difference(rows):
    if len(rows) < 20:
        return None

    ordered = sorted(
        rows,
        key=lambda r: r["_score"]
    )

    q = max(1, len(ordered) // 4)

    bottom = ordered[:q]
    top = ordered[-q:]

    low_rate = (
        100.0
        * sum(r["target"] for r in bottom)
        / len(bottom)
    )

    high_rate = (
        100.0
        * sum(r["target"] for r in top)
        / len(top)
    )

    return high_rate - low_rate


def evaluate_dataset(rows, features):
    discovery = []
    holdout = []

    for r in rows:
        if deterministic_holdout(r["token_mint"]):
            holdout.append(r)
        else:
            discovery.append(r)

    if len(discovery) < 40 or len(holdout) < 15:
        return {
            "decision": "COLLECT_MORE",
            "discovery_n": len(discovery),
            "holdout_n": len(holdout),
        }

    discovery_pos = sum(
        r["target"]
        for r in discovery
    )

    holdout_pos = sum(
        r["target"]
        for r in holdout
    )

    if (
        discovery_pos < 8
        or holdout_pos < 3
    ):
        return {
            "decision": "COLLECT_MORE",
            "discovery_n": len(discovery),
            "holdout_n": len(holdout),
            "positive_holdout": holdout_pos,
        }

    model = fit_model(
        discovery,
        features,
    )

    d_scored = score_rows(
        discovery,
        features,
        model,
    )

    h_scored = score_rows(
        holdout,
        features,
        model,
    )

    d_rho = spearman(
        [r["_score"] for r in d_scored],
        [r["target"] for r in d_scored],
    )

    h_rho = spearman(
        [r["_score"] for r in h_scored],
        [r["target"] for r in h_scored],
    )

    qdiff = quartile_difference(
        h_scored
    )

    decision = "REJECT"

    if (
        d_rho is not None
        and h_rho is not None
        and qdiff is not None
        and d_rho >= 0.10
        and h_rho >= 0.10
        and qdiff >= 10.0
    ):
        decision = "PASS_HOLDOUT"

    return {
        "decision": decision,
        "discovery_n": len(discovery),
        "holdout_n": len(holdout),
        "positive_holdout": holdout_pos,
        "discovery_rho": d_rho,
        "holdout_rho": h_rho,
        "qdiff_pp": qdiff,
        "model": model,
    }


def load_resurrection(spec):
    features = RES_FAMILIES[
        spec["family"]
    ]

    target = spec["target"]
    stage = spec["stage_s"]

    db = open_market()

    cols = ",".join(features)

    sql = f"""
    SELECT
        token_mint,
        {cols},
        {target} AS target
    FROM lab_exp0121_stage_features
    WHERE
        stage_s=?
        AND future_ready=1
        AND coverage_status='GOOD'
        AND {target} IS NOT NULL
    """

    rows = [
        dict(r)
        for r in db.execute(
            sql,
            (stage,)
        ).fetchall()
    ]

    db.close()

    good = []

    for r in rows:
        if not all(
            valid(r[f])
            for f in features
        ):
            continue

        r["target"] = int(r["target"])
        good.append(r)

    return good, features


def load_migration(spec):
    stage = spec["stage_s"]
    horizon = spec["horizon_s"]

    db = open_market()

    token_rows = db.execute("""
    SELECT
        token_mint,
        MIN(timestamp) AS first_ts
    FROM t116_pump_swaps
    GROUP BY token_mint
    """).fetchall()

    migrations = {
        r["token_mint"]: r["migration_ts"]
        for r in db.execute("""
        SELECT
            token_mint,
            MIN(
                COALESCE(
                    block_time,
                    detected_at
                )
            ) AS migration_ts
        FROM t101_migrations
        WHERE
            status='OK'
            AND token_mint IS NOT NULL
        GROUP BY token_mint
        """).fetchall()
    }

    now = time.time()
    rows = []

    features = [
        "return_pct",
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
        "buy_sol",
        "sell_sol",
        "net_sol",
        "range_pct",
    ]

    for token in token_rows:
        mint = token["token_mint"]
        first_ts = token["first_ts"]

        if first_ts is None:
            continue

        cutoff = first_ts + stage
        target_end = cutoff + horizon

        if now < target_end:
            continue

        migration_ts = migrations.get(
            mint
        )

        # Already migrated before observation = excluded.
        if (
            migration_ts is not None
            and migration_ts <= cutoff
        ):
            continue

        swaps = db.execute("""
        SELECT
            side,
            sol_delta,
            raw_price_sol AS price
        FROM t116_pump_swaps
        WHERE
            token_mint=?
            AND timestamp>=?
            AND timestamp<=?
        ORDER BY timestamp ASC
        """, (
            mint,
            first_ts,
            cutoff,
        )).fetchall()

        prices = [
            r["price"]
            for r in swaps
            if valid(r["price"])
            and r["price"] > 0
        ]

        if len(prices) < 2:
            continue

        buys = sum(
            r["side"] == "BUY"
            for r in swaps
        )

        sells = sum(
            r["side"] == "SELL"
            for r in swaps
        )

        buy_sol = sum(
            abs(r["sol_delta"])
            for r in swaps
            if (
                r["side"] == "BUY"
                and valid(r["sol_delta"])
            )
        )

        sell_sol = sum(
            abs(r["sol_delta"])
            for r in swaps
            if (
                r["side"] == "SELL"
                and valid(r["sol_delta"])
            )
        )

        n = len(swaps)

        target = int(
            migration_ts is not None
            and cutoff < migration_ts <= target_end
        )

        rows.append({
            "token_mint": mint,
            "return_pct":
                100.0 * (
                    prices[-1] / prices[0] - 1
                ),
            "swaps": n,
            "buys": buys,
            "sells": sells,
            "buy_ratio":
                buys / n
                if n else 0.0,
            "buy_sol": buy_sol,
            "sell_sol": sell_sol,
            "net_sol":
                buy_sol - sell_sol,
            "range_pct":
                100.0 * (
                    max(prices) / min(prices) - 1
                ),
            "target": target,
        })

    db.close()

    return rows, features


def worker(spec):
    started = time.time()

    try:
        if spec["branch"] == "RESURRECTION":
            rows, features = load_resurrection(
                spec
            )

        elif spec["branch"] == "MIGRATION":
            rows, features = load_migration(
                spec
            )

        else:
            raise RuntimeError(
                "unknown branch"
            )

        result = evaluate_dataset(
            rows,
            features,
        )

        result["elapsed_s"] = (
            time.time() - started
        )

        result["branch"] = spec["branch"]

        return {
            "ok": True,
            "spec": spec,
            "result": result,
        }

    except Exception as exc:
        return {
            "ok": False,
            "spec": spec,
            "error": repr(exc),
        }


def build_population():
    specs = []

    for stage in RES_STAGES:
        for target in RES_TARGETS:
            for family in RES_FAMILIES:
                specs.append({
                    "branch": "RESURRECTION",
                    "stage_s": stage,
                    "target": target,
                    "family": family,
                })

    for stage in MIG_STAGES:
        for horizon in MIG_HORIZONS:
            specs.append({
                "branch": "MIGRATION",
                "stage_s": stage,
                "horizon_s": horizon,
                "family": "PRE_MIGRATION",
            })

    return specs


def display(
    total,
    done,
    passed,
    failed,
    started,
):
    os.system("clear")

    elapsed = max(
        0.001,
        time.time() - started
    )

    rate = (
        60.0 * done / elapsed
    )

    db = open_research()

    top = db.execute("""
    SELECT
        branch,
        spec_json,
        discovery_rho,
        holdout_rho,
        qdiff_pp,
        holdout_n
    FROM v4_fast_experiments
    WHERE status='PASS_HOLDOUT'
    ORDER BY
        holdout_rho DESC,
        qdiff_pp DESC
    LIMIT 12
    """).fetchall()

    db.close()

    print("=" * 135)
    print("MEMECOIN LAB — V4 FAST TRACK RESEARCH")
    print("=" * 135)

    print(
        f"WORKERS       : {MAX_WORKERS}"
    )

    print(
        f"EXPERIMENTS   : {done}/{total}"
    )

    print(
        f"PASS HOLDOUT  : {passed}"
    )

    print(
        f"ERRORS        : {failed}"
    )

    print(
        f"RATE          : {rate:.1f} exp/min"
    )

    print(
        f"ELAPSED       : {elapsed/60:.1f} min"
    )

    print()
    print("=" * 135)
    print("TOP STRICT HOLDOUT SURVIVORS")
    print("=" * 135)

    if not top:
        print("No survivor yet.")

    for row in top:
        spec = json.loads(
            row["spec_json"]
        )

        print(
            f"{row['branch']:<13}"
            f" | {str(spec):<70}"
            f" | D_RHO={row['discovery_rho'] or 0:6.3f}"
            f" | H_RHO={row['holdout_rho'] or 0:6.3f}"
            f" | QDIFF={row['qdiff_pp'] or 0:6.1f}pp"
            f" | H_N={row['holdout_n']}"
        )

    print()
    print(
        "TRAIN/HOLDOUT SPLIT IS DETERMINISTIC BY TOKEN."
    )

    print(
        "NO LIVE TRADING. NO THRESHOLD OPTIMIZATION."
    )


def main():
    init_db()

    specs = build_population()

    run_id = (
        "RUN_"
        + str(int(time.time()))
    )

    db = open_research()

    db.execute("""
    INSERT INTO v4_fast_runs (
        run_id,
        started_at,
        experiments,
        passed
    )
    VALUES (?,?,?,0)
    """, (
        run_id,
        time.time(),
        len(specs),
    ))

    db.commit()
    db.close()

    started = time.time()

    done = 0
    passed = 0
    failed = 0

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=MAX_WORKERS
    ) as pool:

        futures = {
            pool.submit(worker, spec): spec
            for spec in specs
        }

        for future in concurrent.futures.as_completed(
            futures
        ):
            if shutdown_requested:
                break

            payload = future.result()

            spec = payload["spec"]
            exp_id = (
                "V4F_"
                + fingerprint(spec)
            )

            db = open_research()

            if payload["ok"]:
                result = payload["result"]

                status = result[
                    "decision"
                ]

                if status == "PASS_HOLDOUT":
                    passed += 1

                db.execute("""
                INSERT OR REPLACE INTO v4_fast_experiments (
                    experiment_id,
                    branch,
                    spec_json,
                    status,
                    discovery_n,
                    holdout_n,
                    discovery_rho,
                    holdout_rho,
                    qdiff_pp,
                    positive_holdout,
                    result_json,
                    created_at,
                    finished_at
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    exp_id,
                    spec["branch"],
                    json.dumps(
                        spec,
                        sort_keys=True
                    ),
                    status,
                    result.get(
                        "discovery_n"
                    ),
                    result.get(
                        "holdout_n"
                    ),
                    result.get(
                        "discovery_rho"
                    ),
                    result.get(
                        "holdout_rho"
                    ),
                    result.get(
                        "qdiff_pp"
                    ),
                    result.get(
                        "positive_holdout"
                    ),
                    json.dumps(
                        result,
                        separators=(",", ":")
                    ),
                    started,
                    time.time(),
                ))

            else:
                failed += 1

                db.execute("""
                INSERT OR REPLACE INTO v4_fast_experiments (
                    experiment_id,
                    branch,
                    spec_json,
                    status,
                    result_json,
                    created_at,
                    finished_at
                )
                VALUES (?,?,?,?,?,?,?)
                """, (
                    exp_id,
                    spec["branch"],
                    json.dumps(
                        spec,
                        sort_keys=True
                    ),
                    "ERROR",
                    json.dumps({
                        "error":
                            payload["error"]
                    }),
                    started,
                    time.time(),
                ))

            db.commit()
            db.close()

            done += 1

            display(
                len(specs),
                done,
                passed,
                failed,
                started,
            )

    db = open_research()

    db.execute("""
    UPDATE v4_fast_runs
    SET
        finished_at=?,
        passed=?
    WHERE run_id=?
    """, (
        time.time(),
        passed,
        run_id,
    ))

    db.commit()
    db.close()

    display(
        len(specs),
        done,
        passed,
        failed,
        started,
    )

    print()
    print("=" * 135)
    print("FAST TRACK COMPLETE")
    print("=" * 135)

    print(
        f"STRICT HOLDOUT SURVIVORS: {passed}"
    )

    print(
        "Next step = robustness attack ONLY on survivors."
    )


if __name__ == "__main__":
    main()
