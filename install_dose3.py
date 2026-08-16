#!/usr/bin/env python3

from pathlib import Path
import py_compile
import re

ROOT = Path.home() / "memecoin_lab"

for fn in [
    "research_client.py",
    "research_writer.py",
    "autonomous_lab_v2.py",
]:
    if not (ROOT / fn).exists():
        raise SystemExit(f"Missing prerequisite: {fn}")


# =====================================================================
# RESEARCH WORKER V3
# =====================================================================

WORKER = r'''
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
            raw_price
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
'''


# =====================================================================
# RESEARCH DIRECTOR V3
# =====================================================================

DIRECTOR = r'''
#!/usr/bin/env python3

import hashlib
import json
import random
import time

import research_client as rc


ACTIVE_POPULATION_TARGET = 160
QUEUE_TARGET = 100

BRANCH_BUDGETS = {
    "RESURRECTION":0.40,
    "MIGRATION":0.45,
    "WILD":0.15,
}

RES_STAGES = [5,10,15,20,30,45,60,90]

RES_TARGETS = [
    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
]

RES_FAMILIES = [
    "price",
    "activity",
    "flow",
    "price_activity",
    "all",
]

MIG_STAGES = [
    15,
    30,
    45,
    60,
    90,
    120,
    180,
    300,
]

MIG_HORIZONS = [
    300,
    600,
    900,
    1800,
]


def canonical(x):
    return json.dumps(
        x,
        sort_keys=True,
        separators=(",",":")
    )


def spec_hash(spec):
    return hashlib.sha256(
        canonical(spec).encode()
    ).hexdigest()


def ensure_schema():

    rc.execute("""
    CREATE TABLE IF NOT EXISTS director_specs (
        spec_hash TEXT PRIMARY KEY,
        spec_json TEXT NOT NULL,
        branch TEXT NOT NULL,
        created_at REAL NOT NULL
    )
    """)

    rc.execute("""
    CREATE TABLE IF NOT EXISTS branch_state (
        branch TEXT PRIMARY KEY,
        budget REAL NOT NULL,
        generated INTEGER NOT NULL DEFAULT 0,
        passed INTEGER NOT NULL DEFAULT 0,
        rejected INTEGER NOT NULL DEFAULT 0,
        collect_more INTEGER NOT NULL DEFAULT 0,
        saturation REAL NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL
    )
    """)

    rc.execute("""
    CREATE TABLE IF NOT EXISTS director_processed (
        experiment_id TEXT PRIMARY KEY,
        processed_at REAL NOT NULL
    )
    """)


def exists(spec):

    h = spec_hash(spec)

    db = rc.readonly()

    row = db.execute("""
    SELECT 1
    FROM director_specs
    WHERE spec_hash=?
    """,(h,)).fetchone()

    db.close()

    return row is not None


def create(spec, priority=1.0, rationale="autonomous"):

    if exists(spec):
        return False

    h = spec_hash(spec)

    hypothesis_id = "HV3_" + h[:18]
    experiment_id = "EV3_" + h[:18]
    job_id = "JV3_" + h[:18]

    now = time.time()

    branch = spec["branch"]

    rc.execute("""
    INSERT INTO director_specs
    VALUES (?,?,?,?)
    """,(h,canonical(spec),branch,now))

    rc.execute("""
    INSERT INTO hypotheses (
        hypothesis_id,
        created_at,
        parent_hypothesis_id,
        branch,
        species,
        statement,
        rationale,
        novelty_score,
        information_gain_score,
        priority,
        status,
        updated_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """,(
        hypothesis_id,
        now,
        spec.get("parent_hypothesis"),
        branch,
        spec.get("species","DIRECTOR_V3"),
        spec.get("statement",canonical(spec)),
        rationale,
        1.0,
        1.0,
        priority,
        "EXPERIMENT_CREATED",
        now,
    ))

    rc.execute("""
    INSERT INTO experiments (
        experiment_id,
        hypothesis_id,
        created_at,
        branch,
        stage,
        status,
        spec_json,
        updated_at
    )
    VALUES (?,?,?,?,?,?,?,?)
    """,(
        experiment_id,
        hypothesis_id,
        now,
        branch,
        "DISCOVERY",
        "QUEUED",
        canonical(spec),
        now,
    ))

    rc.execute("""
    INSERT INTO jobs (
        job_id,
        experiment_id,
        job_type,
        priority,
        status,
        created_at,
        max_attempts,
        payload_json,
        updated_at
    )
    VALUES (?,?,?,?,?,?,?,?,?)
    """,(
        job_id,
        experiment_id,
        "DISCOVERY_V3",
        priority,
        "QUEUED",
        now,
        3,
        canonical(spec),
        now,
    ))

    return True


def random_resurrection():

    stage = random.choice(RES_STAGES)
    target = random.choice(RES_TARGETS)
    family = random.choice(RES_FAMILIES)

    return {
        "version":3,
        "branch":"RESURRECTION",
        "species":"REVERSAL_SCIENTIST",
        "stage_s":stage,
        "target":target,
        "family":family,
        "mutation":random.randint(0,10**9),
        "statement":(
            f"{family} at {stage}s predicts "
            f"{target} under strict executable entry."
        ),
    }


def random_migration():

    stage = random.choice(MIG_STAGES)
    horizon = random.choice(MIG_HORIZONS)

    return {
        "version":3,
        "branch":"MIGRATION",
        "species":"MIGRATION_SCIENTIST",
        "stage_s":stage,
        "horizon_s":horizon,
        "target":"MIGRATION_AFTER_STAGE",
        "family":"PRE_MIGRATION_FLOW_PATH",
        "mutation":random.randint(0,10**9),
        "statement":(
            f"Strict information available by token age "
            f"{stage}s predicts migration during next "
            f"{horizon}s."
        ),
    }


def active_counts():

    db = rc.readonly()

    rows = db.execute("""
    SELECT
        branch,
        COUNT(*) AS n
    FROM experiments
    WHERE status IN (
        'QUEUED',
        'DISCOVERY',
        'COLLECT_MORE',
        'DISCOVERY_PASSED',
        'ROBUSTNESS_QUEUED',
        'HOLDOUT'
    )
    GROUP BY branch
    """).fetchall()

    total = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE status='QUEUED'
    """).fetchone()[0]

    db.close()

    return {
        r["branch"]:r["n"]
        for r in rows
    }, total


def refill():

    active, queued = active_counts()

    attempts = 0

    while queued < QUEUE_TARGET and attempts < 1000:

        attempts += 1

        branch = random.choices(
            ["RESURRECTION","MIGRATION"],
            weights=[
                BRANCH_BUDGETS["RESURRECTION"],
                BRANCH_BUDGETS["MIGRATION"],
            ],
            k=1
        )[0]

        if branch == "RESURRECTION":
            spec = random_resurrection()
        else:
            spec = random_migration()

        priority = (
            1.15
            if branch == "MIGRATION"
            else 1.0
        )

        if create(
            spec,
            priority=priority,
            rationale="V3 perpetual director refill",
        ):
            queued += 1


def process_results():

    db = rc.readonly()

    rows = db.execute("""
    SELECT
        e.experiment_id,
        e.hypothesis_id,
        e.status,
        e.spec_json,
        j.result_json

    FROM experiments e

    JOIN jobs j
      ON j.experiment_id=e.experiment_id

    LEFT JOIN director_processed p
      ON p.experiment_id=e.experiment_id

    WHERE
        j.status='DONE'
        AND p.experiment_id IS NULL
        AND e.experiment_id LIKE 'EV3_%'

    LIMIT 100
    """).fetchall()

    db.close()

    for row in rows:

        result = json.loads(
            row["result_json"] or "{}"
        )

        spec = json.loads(
            row["spec_json"]
        )

        decision = result.get("decision")

        # PASS → robustness queue marker.
        if decision == "PASS_DISCOVERY":

            rc.execute("""
            UPDATE experiments
            SET
                status='ROBUSTNESS_QUEUED',
                robustness_score=NULL,
                updated_at=?
            WHERE experiment_id=?
            """,(time.time(),row["experiment_id"]))

            # Local mutation around successful stage.
            if spec["branch"] == "MIGRATION":

                for delta in (-15,15):

                    child = dict(spec)

                    child["stage_s"] = max(
                        10,
                        int(spec["stage_s"]) + delta
                    )

                    child["parent_hypothesis"] = row["hypothesis_id"]
                    child["mutation"] = random.randint(0,10**9)

                    create(
                        child,
                        priority=1.35,
                        rationale="Local mutation from migration discovery pass",
                    )

            elif spec["branch"] == "RESURRECTION":

                for delta in (-5,5):

                    child = dict(spec)

                    child["stage_s"] = max(
                        5,
                        int(spec["stage_s"]) + delta
                    )

                    child["parent_hypothesis"] = row["hypothesis_id"]
                    child["mutation"] = random.randint(0,10**9)

                    create(
                        child,
                        priority=1.30,
                        rationale="Local timing mutation from discovery pass",
                    )


        # FAILURE → one deliberate contradiction.
        elif decision == "REJECT_DISCOVERY":

            if random.random() < 0.20:

                child = dict(spec)

                child["mutation"] = random.randint(0,10**9)
                child["parent_hypothesis"] = row["hypothesis_id"]

                if spec["branch"] == "RESURRECTION":

                    child["family"] = random.choice(
                        RES_FAMILIES
                    )

                elif spec["branch"] == "MIGRATION":

                    child["horizon_s"] = random.choice(
                        MIG_HORIZONS
                    )

                create(
                    child,
                    priority=0.65,
                    rationale="Contradiction probe from rejected discovery",
                )


        rc.execute("""
        INSERT OR IGNORE INTO director_processed
        VALUES (?,?)
        """,(row["experiment_id"],time.time()))


def update_branch_state():

    db = rc.readonly()

    for branch, budget in BRANCH_BUDGETS.items():

        generated = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE branch=?
        """,(branch,)).fetchone()[0]

        passed = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE
            branch=?
            AND status IN (
                'DISCOVERY_PASSED',
                'ROBUSTNESS_QUEUED',
                'FROZEN',
                'HOLDOUT'
            )
        """,(branch,)).fetchone()[0]

        rejected = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE
            branch=?
            AND status='REJECTED_DISCOVERY'
        """,(branch,)).fetchone()[0]

        collect = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE
            branch=?
            AND status='COLLECT_MORE'
        """,(branch,)).fetchone()[0]

        saturation = (
            rejected/generated
            if generated
            else 0.0
        )

        rc.execute("""
        INSERT INTO branch_state (
            branch,
            budget,
            generated,
            passed,
            rejected,
            collect_more,
            saturation,
            updated_at
        )
        VALUES (?,?,?,?,?,?,?,?)

        ON CONFLICT(branch)
        DO UPDATE SET
            budget=excluded.budget,
            generated=excluded.generated,
            passed=excluded.passed,
            rejected=excluded.rejected,
            collect_more=excluded.collect_more,
            saturation=excluded.saturation,
            updated_at=excluded.updated_at
        """,(
            branch,
            budget,
            generated,
            passed,
            rejected,
            collect,
            saturation,
            time.time(),
        ))

    db.close()


def main():

    ensure_schema()

    while True:

        process_results()

        update_branch_state()

        refill()

        time.sleep(3)


if __name__ == "__main__":
    main()
'''


# =====================================================================
# FACTORY V3
# =====================================================================

FACTORY = r'''
#!/usr/bin/env python3

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import research_client as rc

ROOT = Path.home() / "memecoin_lab"

PYTHON = sys.executable

WORKER = ROOT / "research_worker_v3.py"
DIRECTOR = ROOT / "research_director_v3.py"

MAX_WORKERS = int(
    os.environ.get(
        "MEMECOIN_RESEARCH_WORKERS",
        min(8,max(4,(os.cpu_count() or 4)//2))
    )
)

REFRESH = 2

shutdown = False
workers = {}

director_proc = None
director_log = None


def start_director():

    global director_proc, director_log

    logs = ROOT / "autonomous_lab_v2_logs"
    logs.mkdir(exist_ok=True)

    director_log = open(
        logs / "research_director_v3.log",
        "a",
        buffering=1,
    )

    director_proc = subprocess.Popen(
        [PYTHON,str(DIRECTOR)],
        cwd=str(ROOT),
        stdout=director_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def queue():

    db = rc.readonly()

    rows = db.execute("""
    SELECT *
    FROM jobs
    WHERE
        status='QUEUED'
        AND job_type='DISCOVERY_V3'
    ORDER BY
        priority DESC,
        created_at ASC
    LIMIT ?
    """,(MAX_WORKERS-len(workers),)).fetchall()

    out = [dict(r) for r in rows]

    db.close()

    return out


def launch(job):

    jid = job["job_id"]

    if jid in workers:
        return

    rc.execute("""
    UPDATE jobs
    SET
        status='DISPATCHED',
        worker_id='V3_LOCAL',
        updated_at=?
    WHERE
        job_id=?
        AND status='QUEUED'
    """,(time.time(),jid))

    logdir = (
        ROOT
        / "autonomous_lab_v2_logs"
        / "workers_v3"
    )

    logdir.mkdir(
        parents=True,
        exist_ok=True
    )

    fh = open(
        logdir / f"{jid}.log",
        "a",
        buffering=1,
    )

    proc = subprocess.Popen(
        [PYTHON,str(WORKER),jid],
        cwd=str(ROOT),
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    workers[jid] = {
        "proc":proc,
        "log":fh,
        "started":time.time(),
    }


def reap():

    for jid, info in list(workers.items()):

        code = info["proc"].poll()

        if code is None:
            continue

        try:
            info["log"].close()
        except Exception:
            pass

        workers.pop(jid,None)


def recover():

    rc.execute("""
    UPDATE jobs
    SET
        status='QUEUED',
        worker_id=NULL,
        updated_at=?
    WHERE
        status='DISPATCHED'
        AND job_type='DISCOVERY_V3'
        AND updated_at < ?
    """,(time.time(),time.time()-60))


def counts():

    db = rc.readonly()

    row = db.execute("""
    SELECT

        (
            SELECT COUNT(*)
            FROM hypotheses
            WHERE hypothesis_id LIKE 'HV3_%'
        ) AS hyp,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE experiment_id LIKE 'EV3_%'
        ) AS exp,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE
                job_type='DISCOVERY_V3'
                AND status='QUEUED'
        ) AS queued,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE
                job_type='DISCOVERY_V3'
                AND status='DONE'
        ) AS done,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE
                experiment_id LIKE 'EV3_%'
                AND status='ROBUSTNESS_QUEUED'
        ) AS robustness,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE
                experiment_id LIKE 'EV3_%'
                AND status='REJECTED_DISCOVERY'
        ) AS rejected
    """).fetchone()

    branches = db.execute("""
    SELECT *
    FROM branch_state
    ORDER BY branch
    """).fetchall()

    db.close()

    return dict(row), [dict(r) for r in branches]


def throughput():

    db = rc.readonly()

    now = time.time()

    h = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE
        job_type='DISCOVERY_V3'
        AND status='DONE'
        AND finished_at >= ?
    """,(now-3600,)).fetchone()[0]

    d = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE
        job_type='DISCOVERY_V3'
        AND status='DONE'
        AND finished_at >= ?
    """,(now-86400,)).fetchone()[0]

    db.close()

    return h,d


def show():

    os.system("clear")

    c, branches = counts()
    hour, day = throughput()

    print("="*170)
    print("MEMECOIN LAB — RESEARCH DIRECTOR V3 — PERPETUAL RESEARCH")
    print("="*170)

    print(
        f"WORKERS              : {len(workers)}/{MAX_WORKERS}"
    )

    print(
        f"V3 HYPOTHESES        : {c['hyp']}"
    )

    print(
        f"V3 EXPERIMENTS       : {c['exp']}"
    )

    print(
        f"QUEUE                 : {c['queued']}"
    )

    print(
        f"DONE                  : {c['done']}"
    )

    print(
        f"ROBUSTNESS QUEUED     : {c['robustness']}"
    )

    print(
        f"REJECTED              : {c['rejected']}"
    )

    print()

    print(
        f"EXPERIMENTS / HOUR    : {hour}"
    )

    print(
        f"EXPERIMENTS / 24H     : {day}"
    )

    print()

    print("="*170)
    print("BRANCH ALLOCATION")
    print("="*170)

    for b in branches:

        print(
            f"{b['branch']:<18}"
            f" | BUDGET={100*b['budget']:5.1f}%"
            f" | GEN={b['generated']:5d}"
            f" | PASS={b['passed']:4d}"
            f" | REJECT={b['rejected']:5d}"
            f" | COLLECT={b['collect_more']:4d}"
            f" | SAT={100*b['saturation']:5.1f}%"
        )

    print()

    print("="*170)
    print("ACTIVE WORKERS")
    print("="*170)

    for jid, info in workers.items():

        print(
            f"🟢 {jid:<23}"
            f" PID={info['proc'].pid:<7}"
            f" AGE={time.time()-info['started']:6.1f}s"
        )

    print()

    print("="*170)
    print("MODE")
    print("="*170)

    print(
        "PERPETUAL DISCOVERY / NO GLOBAL HYPOTHESIS LIMIT"
    )

    print(
        "DISCOVERY PASS → ROBUSTNESS QUEUE"
    )

    print(
        "LIVE MONEY DISABLED"
    )

    print(
        "HOLDOUT SELF-MUTATION FORBIDDEN"
    )

    print()

    print(
        f"Refresh every {REFRESH}s"
    )


def stop(sig, frame):

    global shutdown
    shutdown = True


signal.signal(signal.SIGINT,stop)
signal.signal(signal.SIGTERM,stop)


def main():

    start_director()

    recover()

    while not shutdown:

        reap()

        recover()

        for job in queue():
            launch(job)

        show()

        time.sleep(REFRESH)

    for jid,info in list(workers.items()):

        try:
            os.killpg(
                os.getpgid(info["proc"].pid),
                signal.SIGINT
            )
        except Exception:
            pass

    if director_proc:

        try:
            os.killpg(
                os.getpgid(director_proc.pid),
                signal.SIGINT
            )
        except Exception:
            pass

    print("V3 stopped.")


if __name__ == "__main__":
    main()
'''


# =====================================================================
# WRITE FILES
# =====================================================================

files = {
    "research_worker_v3.py":WORKER,
    "research_director_v3.py":DIRECTOR,
    "research_factory_v3.py":FACTORY,
}

for name, content in files.items():

    path = ROOT / name

    path.write_text(
        content.lstrip()
    )

    py_compile.compile(
        str(path),
        doraise=True
    )

    print(f"✅ {name}")


# =====================================================================
# PATCH SUPERVISOR V2:
# replace V2 factory with V3 factory.
# =====================================================================

p = ROOT / "autonomous_lab_v2.py"

s = p.read_text()

backup = ROOT / "autonomous_lab_v2.before_dose3.py"

if not backup.exists():
    backup.write_text(s)


# First replace the script if RESEARCH_FACTORY already registered.
s = s.replace(
    '"script":\n            "research_factory_v2.py"',
    '"script":\n            "research_factory_v3.py"'
)

# If there is no factory entry, append one after T117.
if '"research_factory_v3.py"' not in s:

    anchor = '''    "T117_OUTCOMES": {
        "script":
            "t117_premigration_outcome_linker.py",

        "kind":
            "DERIVED",
    },
'''

    addition = anchor + '''

    "RESEARCH_FACTORY": {
        "script":
            "research_factory_v3.py",

        "kind":
            "RESEARCH",
    },
'''

    if anchor not in s:
        raise SystemExit(
            "Could not locate supervisor process anchor"
        )

    s = s.replace(
        anchor,
        addition,
        1
    )


p.write_text(s)

py_compile.compile(
    str(p),
    doraise=True
)

print("✅ autonomous_lab_v2.py patched for Dose 3")
print("✅ DOSE 3 INSTALLATION COMPLETE")
