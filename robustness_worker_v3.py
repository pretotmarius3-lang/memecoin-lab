#!/usr/bin/env python3

import json
import math
import random
import sqlite3
import statistics
import sys
import time
from pathlib import Path

import research_client as rc


ROOT = Path.home() / "memecoin_lab"
MARKET_DB = ROOT / "validation_v090.db"

BOOT = 120

MIN_N = 60
MIN_POS = 12
MIN_NEG = 25

PASS_MIN_SPLITS = 3
PASS_MIN_SCORE = 0.10


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return sum(xs)/len(xs) if xs else None


def ranks(values):

    indexed = sorted(
        enumerate(values),
        key=lambda x:x[1]
    )

    out = [0.0]*len(values)

    i = 0

    while i < len(indexed):

        j = i

        while (
            j + 1 < len(indexed)
            and indexed[j+1][1] == indexed[i][1]
        ):
            j += 1

        rank = (i+j+2)/2.0

        for k in range(i,j+1):
            out[indexed[k][0]] = rank

        i = j+1

    return out


def pearson(x,y):

    if len(x) < 3:
        return None

    mx = mean(x)
    my = mean(y)

    if mx is None or my is None:
        return None

    num = sum(
        (a-mx)*(b-my)
        for a,b in zip(x,y)
    )

    dx = math.sqrt(sum((a-mx)**2 for a in x))
    dy = math.sqrt(sum((b-my)**2 for b in y))

    if dx == 0 or dy == 0:
        return None

    return num/(dx*dy)


def spearman(x,y):

    pairs = [
        (a,b)
        for a,b in zip(x,y)
        if valid(a) and valid(b)
    ]

    if len(pairs) < 3:
        return None

    return pearson(
        ranks([a for a,_ in pairs]),
        ranks([b for _,b in pairs]),
    )


def market():

    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=30,
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")

    return db


def load_resurrection(spec):

    family_map = {
        "price": [
            "return_since_entry",
            "mfe_so_far",
            "mae_so_far",
            "new_low",
            "reclaim_entry",
        ],

        "activity": [
            "swaps",
            "buys",
            "sells",
            "buy_ratio",
        ],

        "flow": [
            "buy_sol",
            "sell_sol",
            "net_sol",
        ],

        "price_activity": [
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

        "all": [
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

    family = spec.get("family")

    if family not in family_map:
        return [], []

    features = family_map[family]

    target = spec["target"]
    stage = int(spec["stage_s"])

    db = market()

    rows = [
        dict(r)
        for r in db.execute(f"""
        SELECT
            token_mint,
            {",".join(features)},
            {target} AS target,
            future_max300,
            future_min300,
            future_end300
        FROM lab_exp0121_stage_features
        WHERE
            stage_s=?
            AND future_ready=1
            AND coverage_status='GOOD'
            AND {target} IS NOT NULL
        """,(stage,)).fetchall()
    ]

    db.close()

    good = []

    for row in rows:

        if not all(valid(row[f]) for f in features):
            continue

        good.append(row)

    return good, features


def fit_score(rows,features):

    y = [r["target"] for r in rows]

    directions = {}

    stats = {}

    for f in features:

        xs = [r[f] for r in rows]

        m = mean(xs)

        sd = statistics.stdev(xs) if len(xs) > 1 else 1.0

        if sd == 0:
            sd = 1.0

        rho = spearman(xs,y)

        direction = 0.0

        if rho is not None:
            direction = 1.0 if rho > 0 else -1.0

        stats[f] = (
            m or 0.0,
            sd
        )

        directions[f] = direction

    scores = []

    for r in rows:

        parts = []

        for f in features:

            m,sd = stats[f]

            z = (
                r[f]-m
            )/sd

            parts.append(
                z*directions[f]
            )

        scores.append(
            mean(parts) or 0.0
        )

    return scores


def eval_subset(rows,features):

    if len(rows) < MIN_N:
        return None

    y = [r["target"] for r in rows]

    pos = int(sum(y))
    neg = len(rows)-pos

    if pos < MIN_POS or neg < MIN_NEG:
        return None

    scores = fit_score(
        rows,
        features
    )

    return spearman(
        scores,
        y
    )


def robustness_resurrection(spec):

    rows, features = load_resurrection(spec)

    if len(rows) < MIN_N:
        return {
            "decision":"COLLECT_MORE",
            "reason":"not enough strict rows",
            "n":len(rows),
        }

    full = eval_subset(
        rows,
        features
    )

    if full is None:
        return {
            "decision":"COLLECT_MORE",
            "reason":"insufficient positive/negative balance",
            "n":len(rows),
        }

    tests = {}

    # --------------------------------------------------
    # 1. RANDOM HALF SPLITS
    # --------------------------------------------------

    rng = random.Random(
        abs(hash(
            json.dumps(spec,sort_keys=True)
        ))%(2**31)
    )

    split_scores = []

    for i in range(5):

        shuffled = list(rows)
        rng.shuffle(shuffled)

        half = len(shuffled)//2

        for part in (
            shuffled[:half],
            shuffled[half:],
        ):

            rho = eval_subset(
                part,
                features
            )

            if rho is not None:
                split_scores.append(rho)

    tests["half_splits"] = split_scores

    # --------------------------------------------------
    # 2. REMOVE BEST OUTCOME TOKENS
    # --------------------------------------------------

    ranked = sorted(
        rows,
        key=lambda r:
            (
                r.get("future_max300")
                if valid(r.get("future_max300"))
                else -1e99
            ),
        reverse=True,
    )

    cut = max(
        1,
        int(len(ranked)*0.05)
    )

    without_best = ranked[cut:]

    tests["remove_best_5pct"] = eval_subset(
        without_best,
        features
    )

    # --------------------------------------------------
    # 3. REMOVE WORST OUTCOME TOKENS
    # --------------------------------------------------

    without_worst = ranked[:-cut]

    tests["remove_worst_5pct"] = eval_subset(
        without_worst,
        features
    )

    # --------------------------------------------------
    # 4. FEATURE ABLATION
    # --------------------------------------------------

    ablations = {}

    if len(features) > 1:

        for f in features:

            keep = [
                x for x in features
                if x != f
            ]

            rho = eval_subset(
                rows,
                keep
            )

            ablations[f] = rho

    tests["ablations"] = ablations

    # --------------------------------------------------
    # 5. bootstrap stability
    # --------------------------------------------------

    boots = []

    for _ in range(BOOT):

        sample = [
            rows[
                rng.randrange(len(rows))
            ]
            for __ in range(len(rows))
        ]

        rho = eval_subset(
            sample,
            features
        )

        if rho is not None:
            boots.append(rho)

    boot_med = (
        statistics.median(boots)
        if boots
        else None
    )

    boot_low = None

    if boots:

        b = sorted(boots)

        idx = int(
            0.025*(len(b)-1)
        )

        boot_low = b[idx]

    # --------------------------------------------------
    # survival test count
    # --------------------------------------------------

    survival_scores = []

    if full is not None:
        survival_scores.append(full)

    for v in split_scores:
        if v is not None:
            survival_scores.append(v)

    for key in (
        "remove_best_5pct",
        "remove_worst_5pct",
    ):

        v = tests[key]

        if v is not None:
            survival_scores.append(v)

    for v in ablations.values():

        if v is not None:
            survival_scores.append(v)

    positive_survivals = sum(
        v >= PASS_MIN_SCORE
        for v in survival_scores
    )

    if (
        full >= PASS_MIN_SCORE
        and boot_med is not None
        and boot_med >= PASS_MIN_SCORE
        and boot_low is not None
        and boot_low > 0
        and positive_survivals >= PASS_MIN_SPLITS
    ):

        decision = "PASS_ROBUSTNESS"

    else:

        decision = "REJECT_ROBUSTNESS"

    return {
        "decision":decision,
        "n":len(rows),

        "full_rho":full,

        "bootstrap_median":boot_med,
        "bootstrap_low":boot_low,

        "positive_survivals":positive_survivals,
        "tests":tests,
    }


def main():

    if len(sys.argv) != 2:
        raise SystemExit(
            "robustness_worker_v3.py EXPERIMENT_ID"
        )

    exp_id = sys.argv[1]

    db = rc.readonly()

    row = db.execute("""
    SELECT *
    FROM experiments
    WHERE experiment_id=?
    """,(exp_id,)).fetchone()

    db.close()

    if not row:
        raise RuntimeError(
            f"experiment not found: {exp_id}"
        )

    spec = json.loads(
        row["spec_json"]
    )

    branch = spec.get("branch")

    started = time.time()

    if branch == "RESURRECTION":

        result = robustness_resurrection(
            spec
        )

    else:

        result = {
            "decision":"WAIT_UNSUPPORTED_BRANCH",
            "branch":branch,
        }

    result["compute_s"] = (
        time.time()-started
    )

    decision = result[
        "decision"
    ]

    if decision == "PASS_ROBUSTNESS":
        status = "ROBUSTNESS_PASSED"

    elif decision == "COLLECT_MORE":
        status = "COLLECT_MORE"

    elif decision == "WAIT_UNSUPPORTED_BRANCH":
        status = "ROBUSTNESS_WAIT"

    else:
        status = "REJECTED_ROBUSTNESS"

    rc.execute("""
    UPDATE experiments
    SET
        status=?,
        robustness_score=?,
        conclusion=?,
        updated_at=?
    WHERE experiment_id=?
    """,(
        status,
        result.get("full_rho"),
        decision,
        time.time(),
        exp_id,
    ))

    rc.execute("""
    INSERT OR REPLACE INTO robustness_results_v3 (
        experiment_id,
        branch,
        decision,
        result_json,
        created_at,
        updated_at
    )
    VALUES (?,?,?,?,?,?)
    """,(
        exp_id,
        branch,
        decision,
        json.dumps(
            result,
            separators=(",",":")
        ),
        time.time(),
        time.time(),
    ))

    print(
        json.dumps({
            "experiment":exp_id,
            "decision":decision,
            "rho":result.get("full_rho"),
        })
    )


if __name__ == "__main__":
    main()
