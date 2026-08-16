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

BOOTSTRAPS = 150

MIN_N = 80
MIN_POS = 15
MIN_NEG = 30

PASS_RHO = 0.15
PASS_BOOT_LOW = 0.00
PASS_QDIFF = 10.0


def market():
    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=30,
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    return db


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(x):
    x = [v for v in x if valid(v)]
    return sum(x)/len(x) if x else None


def stdev(x):
    x = [v for v in x if valid(v)]
    if len(x) < 2:
        return None
    return statistics.stdev(x)


def median(x):
    x = [v for v in x if valid(v)]
    return statistics.median(x) if x else None


def percentile(x, q):
    x = sorted(v for v in x if valid(v))
    if not x:
        return None
    if len(x) == 1:
        return x[0]
    p = (len(x)-1)*q
    a = math.floor(p)
    b = math.ceil(p)
    if a == b:
        return x[a]
    w = p-a
    return x[a]*(1-w)+x[b]*w


def ranks(x):
    z = sorted(enumerate(x), key=lambda a:a[1])
    out = [0.0]*len(x)
    i = 0
    while i < len(z):
        j = i
        while j+1 < len(z) and z[j+1][1] == z[i][1]:
            j += 1
        r = (i+j+2)/2.0
        for k in range(i,j+1):
            out[z[k][0]] = r
        i = j+1
    return out


def pearson(x,y):
    if len(x) < 3:
        return None
    mx, my = mean(x), mean(y)
    if mx is None or my is None:
        return None
    num = sum((a-mx)*(b-my) for a,b in zip(x,y))
    dx = math.sqrt(sum((a-mx)**2 for a in x))
    dy = math.sqrt(sum((b-my)**2 for b in y))
    if dx == 0 or dy == 0:
        return None
    return num/(dx*dy)


def spearman(x,y):
    p = [(a,b) for a,b in zip(x,y) if valid(a) and valid(b)]
    if len(p) < 3:
        return None
    return pearson(
        ranks([a for a,_ in p]),
        ranks([b for _,b in p]),
    )


def table_exists(db, name):
    return db.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name=?
    """,(name,)).fetchone() is not None


def cols(db, name):
    return {
        r[1]
        for r in db.execute(f"PRAGMA table_info({name})").fetchall()
    }


# =====================================================================
# RESURRECTION DATASET
# =====================================================================

RES_FEATURES = {
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


def resurrection_dataset(spec):

    db = market()

    if not table_exists(db, "lab_exp0121_stage_features"):
        db.close()
        return []

    stage = int(spec["stage_s"])
    target = spec["target"]
    features = RES_FEATURES[spec["family"]]

    allowed_targets = {
        "future_hit10",
        "future_hit20",
        "future_hit30",
        "future_hit50",
    }

    if target not in allowed_targets:
        db.close()
        return []

    sql = f"""
    SELECT
        token_mint,
        {",".join(features)},
        {target} AS target,
        future_max300 AS max_outcome,
        future_min300 AS min_outcome,
        future_end300 AS end_outcome

    FROM lab_exp0121_stage_features

    WHERE
        stage_s=?
        AND future_ready=1
        AND coverage_status='GOOD'
        AND {target} IS NOT NULL
        AND future_max300 IS NOT NULL
        AND future_min300 IS NOT NULL
        AND future_end300 IS NOT NULL
    """

    rows = [
        dict(r)
        for r in db.execute(sql,(stage,)).fetchall()
    ]

    db.close()

    out = []

    for r in rows:
        if not all(valid(r[f]) for f in features):
            continue
        out.append(r)

    return out, features


# =====================================================================
# STRICT MIGRATION DATASET
#
# Observation is frozen at token age X seconds.
# Target is migration AFTER observation but within future horizon.
# Non-migrated tokens must be old enough to avoid censoring.
# =====================================================================

MIG_FEATURES = [
    "return_pct",
    "swaps",
    "buys",
    "sells",
    "buy_ratio",
    "buy_sol",
    "sell_sol",
    "net_sol",
    "price_range_pct",
]


def migration_dataset(spec):

    db = market()

    if not table_exists(db, "t116_pump_swaps"):
        db.close()
        return [], MIG_FEATURES

    if not table_exists(db, "t101_migrations"):
        db.close()
        return [], MIG_FEATURES

    stage_s = int(spec["stage_s"])
    horizon_s = int(spec["horizon_s"])

    # Mature token universe.
    now = time.time()

    raw = db.execute("""
    SELECT
        token_mint,
        MIN(timestamp) AS first_ts,
        MAX(timestamp) AS last_ts
    FROM t116_pump_swaps
    GROUP BY token_mint
    """).fetchall()

    mig = {
        r["token_mint"]: r["migration_ts"]
        for r in db.execute("""
        SELECT
            token_mint,
            MIN(COALESCE(block_time, detected_at)) AS migration_ts
        FROM t101_migrations
        WHERE
            status='OK'
            AND token_mint IS NOT NULL
        GROUP BY token_mint
        """).fetchall()
    }

    out = []

    for token_row in raw:

        mint = token_row["token_mint"]
        first_ts = token_row["first_ts"]

        if first_ts is None:
            continue

        cutoff = first_ts + stage_s
        end_target = cutoff + horizon_s

        # Strict maturity / censor control.
        if now < end_target:
            continue

        m_ts = mig.get(mint)

        # Migration before the observation cutoff would leak outcome.
        if m_ts is not None and m_ts <= cutoff:
            continue

        swaps = db.execute("""
        SELECT
            timestamp,
            side,
            sol_delta,
            raw_price_sol AS raw_price
        FROM t116_pump_swaps
        WHERE
            token_mint=?
            AND timestamp >= ?
            AND timestamp <= ?
        ORDER BY timestamp ASC
        """,(mint,first_ts,cutoff)).fetchall()

        if len(swaps) < 2:
            continue

        prices = [
            r["raw_price"]
            for r in swaps
            if valid(r["raw_price"]) and r["raw_price"] > 0
        ]

        if len(prices) < 2:
            continue

        buys = sum(r["side"] == "BUY" for r in swaps)
        sells = sum(r["side"] == "SELL" for r in swaps)
        n = len(swaps)

        buy_sol = sum(
            abs(r["sol_delta"])
            for r in swaps
            if r["side"] == "BUY" and valid(r["sol_delta"])
        )

        sell_sol = sum(
            abs(r["sol_delta"])
            for r in swaps
            if r["side"] == "SELL" and valid(r["sol_delta"])
        )

        first_price = prices[0]
        last_price = prices[-1]

        return_pct = 100.0*(last_price/first_price-1.0)

        price_range_pct = 100.0*(
            max(prices)/min(prices)-1.0
        )

        target = int(
            m_ts is not None
            and cutoff < m_ts <= end_target
        )

        out.append({
            "token_mint": mint,

            "return_pct": return_pct,
            "swaps": n,
            "buys": buys,
            "sells": sells,
            "buy_ratio": buys/n if n else 0.0,

            "buy_sol": buy_sol,
            "sell_sol": sell_sol,
            "net_sol": buy_sol-sell_sol,

            "price_range_pct": price_range_pct,

            "target": target,

            # migration classifier has no price-outcome target yet.
            "max_outcome": float(target),
            "min_outcome": float(target),
            "end_outcome": float(target),
        })

    db.close()

    return out, MIG_FEATURES


# =====================================================================
# MODEL
# =====================================================================

def fit(rows, features):

    y = [r["target"] for r in rows]

    fitted = {}
    feature_rho = {}

    for f in features:

        x = [r[f] for r in rows]

        m = mean(x) or 0.0
        sd = stdev(x)

        if sd is None or sd == 0:
            sd = 1.0

        rho = spearman(x,y)

        feature_rho[f] = rho

        if rho is None or rho == 0:
            direction = 0.0
        elif rho > 0:
            direction = 1.0
        else:
            direction = -1.0

        fitted[f] = {
            "mean":m,
            "std":sd,
            "direction":direction,
        }

    return fitted, feature_rho


def score_rows(rows, features, fitted):

    result = []

    for r in rows:

        parts = []

        for f in features:

            z = (
                r[f] - fitted[f]["mean"]
            ) / fitted[f]["std"]

            parts.append(
                z * fitted[f]["direction"]
            )

        x = dict(r)
        x["_score"] = mean(parts) or 0.0
        result.append(x)

    return result


def evaluate(rows, features, seed_text):

    y = [r["target"] for r in rows]

    fitted, feature_rho = fit(rows,features)
    scored = score_rows(rows,features,fitted)

    scores = [r["_score"] for r in scored]

    rho = spearman(scores,y)

    max_rho = spearman(
        scores,
        [r["max_outcome"] for r in scored]
    )

    min_rho = spearman(
        scores,
        [r["min_outcome"] for r in scored]
    )

    end_rho = spearman(
        scores,
        [r["end_outcome"] for r in scored]
    )

    ordered = sorted(scored,key=lambda r:r["_score"])

    quartiles = []

    for i in range(4):

        a = int(len(ordered)*i/4)
        b = int(len(ordered)*(i+1)/4)

        part = ordered[a:b]

        if not part:
            quartiles.append(None)
            continue

        quartiles.append({
            "n":len(part),
            "hit_rate":100.0*sum(r["target"] for r in part)/len(part),
        })

    qdiff = None

    if quartiles[0] and quartiles[3]:
        qdiff = (
            quartiles[3]["hit_rate"]
            - quartiles[0]["hit_rate"]
        )

    rng = random.Random(
        abs(hash(seed_text))%(2**31)
    )

    boots = []

    for _ in range(BOOTSTRAPS):

        sample = [
            rows[rng.randrange(len(rows))]
            for __ in range(len(rows))
        ]

        bf,_ = fit(sample,features)
        bs = score_rows(sample,features,bf)

        brho = spearman(
            [r["_score"] for r in bs],
            [r["target"] for r in bs],
        )

        if brho is not None:
            boots.append(brho)

    boot_med = median(boots)
    boot_low = percentile(boots,0.025)
    boot_high = percentile(boots,0.975)

    n = len(rows)
    pos = int(sum(y))
    neg = n-pos

    if (
        n < MIN_N
        or pos < MIN_POS
        or neg < MIN_NEG
    ):
        decision = "COLLECT_MORE"

    elif (
        rho is not None
        and rho >= PASS_RHO
        and boot_low is not None
        and boot_low > PASS_BOOT_LOW
        and qdiff is not None
        and qdiff >= PASS_QDIFF
    ):
        decision = "PASS_DISCOVERY"

    else:
        decision = "REJECT_DISCOVERY"

    return {
        "n":n,
        "positives":pos,
        "negatives":neg,

        "target_rho":rho,
        "max_rho":max_rho,
        "min_rho":min_rho,
        "end_rho":end_rho,

        "bootstrap_median":boot_med,
        "bootstrap_low":boot_low,
        "bootstrap_high":boot_high,

        "q4_minus_q1_pp":qdiff,
        "quartiles":quartiles,

        "feature_rhos":feature_rho,
        "fitted_spec":fitted,

        "decision":decision,
    }


def main():

    if len(sys.argv) != 2:
        raise SystemExit("research_worker_v3.py JOB_ID")

    job_id = sys.argv[1]

    rdb = rc.readonly()

    job = rdb.execute("""
    SELECT *
    FROM jobs
    WHERE job_id=?
    """,(job_id,)).fetchone()

    rdb.close()

    if not job:
        raise RuntimeError(f"Unknown job {job_id}")

    spec = json.loads(job["payload_json"])

    exp_id = job["experiment_id"]

    now = time.time()

    rc.execute("""
    UPDATE jobs
    SET
        status='RUNNING',
        started_at=?,
        attempts=attempts+1,
        updated_at=?
    WHERE job_id=?
    """,(now,now,job_id))

    rc.execute("""
    UPDATE experiments
    SET
        status='DISCOVERY',
        updated_at=?
    WHERE experiment_id=?
    """,(now,exp_id))

    started = time.time()

    try:

        branch = spec["branch"]

        if branch == "RESURRECTION":
            rows, features = resurrection_dataset(spec)

        elif branch == "MIGRATION":
            rows, features = migration_dataset(spec)

        else:
            raise RuntimeError(
                f"Unsupported V3 branch: {branch}"
            )

        if len(rows) < 20:

            result = {
                "n":len(rows),
                "positives":None,
                "negatives":None,
                "decision":"WAIT_DATA",
                "reason":"insufficient strict rows",
            }

        else:

            result = evaluate(
                rows,
                features,
                json.dumps(spec,sort_keys=True),
            )

        result["compute_s"] = time.time()-started
        result["branch"] = branch

        status = result["decision"]

        if status == "PASS_DISCOVERY":
            exp_status = "DISCOVERY_PASSED"

        elif status in ("WAIT_DATA","COLLECT_MORE"):
            exp_status = "COLLECT_MORE"

        else:
            exp_status = "REJECTED_DISCOVERY"

        now = time.time()

        rc.execute("""
        UPDATE jobs
        SET
            status='DONE',
            finished_at=?,
            result_json=?,
            updated_at=?
        WHERE job_id=?
        """,(
            now,
            json.dumps(result,separators=(",",":")),
            now,
            job_id,
        ))

        rc.execute("""
        UPDATE experiments
        SET
            status=?,
            discovery_n=?,
            positive_n=?,
            negative_n=?,
            discovery_score=?,
            conclusion=?,
            updated_at=?
        WHERE experiment_id=?
        """,(
            exp_status,
            result.get("n"),
            result.get("positives"),
            result.get("negatives"),
            result.get("target_rho"),
            result["decision"],
            now,
            exp_id,
        ))

        print(json.dumps({
            "experiment":exp_id,
            "branch":branch,
            "decision":result["decision"],
            "n":result.get("n"),
            "rho":result.get("target_rho"),
        }))

    except Exception as e:

        now = time.time()

        rc.execute("""
        UPDATE jobs
        SET
            status='FAILED',
            error=?,
            finished_at=?,
            updated_at=?
        WHERE job_id=?
        """,(repr(e),now,now,job_id))

        rc.execute("""
        UPDATE experiments
        SET
            status='ERROR',
            conclusion=?,
            updated_at=?
        WHERE experiment_id=?
        """,(repr(e),now,exp_id))

        raise


if __name__ == "__main__":
    main()
