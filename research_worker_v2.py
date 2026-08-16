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

MARKET_DB = (
    ROOT
    / "validation_v090.db"
)

MIN_N = 80
MIN_POS = 15
MIN_NEG = 30

BOOTSTRAPS = 120

RHO_GATE = 0.15
BOOT_LOW_GATE = 0.0
QUARTILE_GATE_PP = 10.0


# ============================================================
# BASIC MATH
# ============================================================

def valid(x):

    return (
        x is not None
        and isinstance(
            x,
            (int, float)
        )
        and math.isfinite(x)
    )


def mean(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    return (
        sum(xs) / len(xs)
        if xs
        else None
    )


def stdev(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


def median(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    return (
        statistics.median(xs)
        if xs
        else None
    )


def percentile(
    xs,
    q
):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    if len(xs) == 1:
        return xs[0]

    pos = (
        len(xs) - 1
    ) * q

    lo = math.floor(pos)
    hi = math.ceil(pos)

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return (
        xs[lo] * (1-w)
        + xs[hi] * w
    )


def ranks(values):

    indexed = sorted(
        enumerate(values),
        key=lambda x:
            x[1]
    )

    output = [
        0.0
    ] * len(values)

    i = 0

    while i < len(indexed):

        j = i

        while (
            j + 1
            < len(indexed)
            and indexed[j+1][1]
            == indexed[i][1]
        ):

            j += 1

        rank = (
            i + j + 2
        ) / 2.0

        for k in range(
            i,
            j + 1
        ):

            output[
                indexed[k][0]
            ] = rank

        i = j + 1

    return output


def pearson(
    x,
    y
):

    if len(x) < 3:
        return None

    mx = mean(x)
    my = mean(y)

    if (
        mx is None
        or my is None
    ):
        return None

    numerator = sum(
        (a-mx) * (b-my)

        for a, b in zip(
            x,
            y
        )
    )

    dx = math.sqrt(
        sum(
            (a-mx) ** 2
            for a in x
        )
    )

    dy = math.sqrt(
        sum(
            (b-my) ** 2
            for b in y
        )
    )

    if (
        dx == 0
        or dy == 0
    ):
        return None

    return (
        numerator
        / (
            dx * dy
        )
    )


def spearman(
    x,
    y
):

    pairs = [
        (a, b)

        for a, b in zip(
            x,
            y
        )

        if (
            valid(a)
            and valid(b)
        )
    ]

    if len(pairs) < 3:
        return None

    xx = [
        a for a, _
        in pairs
    ]

    yy = [
        b for _, b
        in pairs
    ]

    return pearson(
        ranks(xx),
        ranks(yy)
    )


# ============================================================
# DB
# ============================================================

def market():

    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=20,
    )

    db.row_factory = sqlite3.Row

    db.execute(
        "PRAGMA busy_timeout=20000"
    )

    return db


def research():

    return rc.readonly()


# ============================================================
# JOB
# ============================================================

def get_job(job_id):

    db = research()

    row = db.execute("""
    SELECT *
    FROM jobs
    WHERE job_id=?
    """, (
        job_id,
    )).fetchone()

    db.close()

    return row


# ============================================================
# DATA
# ============================================================

ALLOWED_TARGETS = {
    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
}


ALLOWED_FEATURES = {
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
}


def load_rows(spec):

    target = spec[
        "target"
    ]

    features = spec[
        "features"
    ]

    stage = int(
        spec[
            "stage_s"
        ]
    )


    if target not in ALLOWED_TARGETS:

        raise RuntimeError(
            "Target not allowed"
        )


    for f in features:

        if f not in ALLOWED_FEATURES:

            raise RuntimeError(
                f"Feature not allowed: {f}"
            )


    cols = (
        features
        + [
            target,

            "future_max300",
            "future_min300",
            "future_end300",
        ]
    )


    sql = f"""
    SELECT
        token_mint,
        {",".join(cols)}

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


    db = market()

    try:

        rows = [
            dict(r)

            for r in db.execute(
                sql,
                (
                    stage,
                )
            ).fetchall()
        ]

    except sqlite3.Error:

        rows = []

    finally:

        db.close()


    good = []


    for r in rows:

        if not valid(
            r[
                target
            ]
        ):
            continue

        if not all(
            valid(
                r[f]
            )
            for f in features
        ):
            continue

        good.append(r)


    return good


# ============================================================
# MODEL
# ============================================================

def fit_spec(
    rows,
    features,
    target
):

    y = [
        r[target]
        for r in rows
    ]

    spec = {}

    feature_rhos = {}


    for feature in features:

        x = [
            r[feature]
            for r in rows
        ]

        m = mean(x)
        sd = stdev(x)

        rho = spearman(
            x,
            y
        )

        feature_rhos[
            feature
        ] = rho


        if (
            rho is None
            or rho == 0
        ):

            direction = 0.0

        elif rho > 0:

            direction = 1.0

        else:

            direction = -1.0


        spec[
            feature
        ] = {
            "mean":
                m or 0.0,

            "std":
                (
                    sd
                    if (
                        sd is not None
                        and sd > 0
                    )
                    else 1.0
                ),

            "direction":
                direction,
        }


    return (
        spec,
        feature_rhos
    )


def score_rows(
    rows,
    features,
    fitted
):

    output = []


    for row in rows:

        parts = []


        for feature in features:

            info = fitted[
                feature
            ]

            z = (
                row[
                    feature
                ]
                - info[
                    "mean"
                ]
            ) / info[
                "std"
            ]

            parts.append(
                z
                * info[
                    "direction"
                ]
            )


        score = (
            mean(parts)
            or 0.0
        )


        new = dict(row)

        new[
            "_score"
        ] = score

        output.append(
            new
        )


    return output


# ============================================================
# EVALUATE
# ============================================================

def evaluate(
    rows,
    spec
):

    target = spec[
        "target"
    ]

    features = spec[
        "features"
    ]


    fitted, feature_rhos = (
        fit_spec(
            rows,
            features,
            target
        )
    )


    scored = score_rows(
        rows,
        features,
        fitted
    )


    scores = [
        r["_score"]
        for r in scored
    ]

    y = [
        r[target]
        for r in scored
    ]


    target_rho = spearman(
        scores,
        y
    )


    max_rho = spearman(
        scores,
        [
            r[
                "future_max300"
            ]
            for r in scored
        ]
    )


    min_rho = spearman(
        scores,
        [
            r[
                "future_min300"
            ]
            for r in scored
        ]
    )


    end_rho = spearman(
        scores,
        [
            r[
                "future_end300"
            ]
            for r in scored
        ]
    )


    # --------------------------------------------
    # QUARTILES
    # --------------------------------------------

    ordered = sorted(
        scored,
        key=lambda r:
            r["_score"]
    )


    quartiles = []


    for i in range(4):

        a = int(
            len(ordered)
            * i
            / 4
        )

        b = int(
            len(ordered)
            * (
                i + 1
            )
            / 4
        )


        part = ordered[
            a:b
        ]


        if not part:

            quartiles.append(
                None
            )

            continue


        hit_rate = (
            100.0
            * sum(
                r[target] == 1
                for r in part
            )
            / len(part)
        )


        quartiles.append({
            "n":
                len(part),

            "hit_rate":
                hit_rate,

            "max_med":
                median(
                    [
                        r[
                            "future_max300"
                        ]
                        for r in part
                    ]
                ),

            "min_med":
                median(
                    [
                        r[
                            "future_min300"
                        ]
                        for r in part
                    ]
                ),
        })


    q1 = quartiles[0]
    q4 = quartiles[3]


    qdiff = (
        q4[
            "hit_rate"
        ]
        - q1[
            "hit_rate"
        ]
        if (
            q1
            and q4
        )
        else None
    )


    # --------------------------------------------
    # BOOTSTRAP
    # --------------------------------------------

    seed = int(
        abs(
            hash(
                json.dumps(
                    spec,
                    sort_keys=True
                )
            )
        )
        % (
            2**31
        )
    )


    rng = random.Random(
        seed
    )


    bootstrap_rhos = []


    for _ in range(
        BOOTSTRAPS
    ):

        sample = [
            rows[
                rng.randrange(
                    len(rows)
                )
            ]

            for __ in range(
                len(rows)
            )
        ]


        bfit, _ = fit_spec(
            sample,
            features,
            target
        )


        bscored = score_rows(
            sample,
            features,
            bfit
        )


        rho = spearman(
            [
                r["_score"]
                for r in bscored
            ],

            [
                r[target]
                for r in bscored
            ]
        )


        if rho is not None:

            bootstrap_rhos.append(
                rho
            )


    boot_med = median(
        bootstrap_rhos
    )

    boot_low = percentile(
        bootstrap_rhos,
        0.025
    )

    boot_high = percentile(
        bootstrap_rhos,
        0.975
    )


    n = len(rows)

    positives = int(
        sum(y)
    )

    negatives = (
        n
        - positives
    )


    if (
        n < MIN_N
        or positives < MIN_POS
        or negatives < MIN_NEG
    ):

        decision = (
            "COLLECT_MORE"
        )


    elif (
        target_rho is not None
        and target_rho >= RHO_GATE

        and boot_low is not None
        and boot_low > BOOT_LOW_GATE

        and qdiff is not None
        and qdiff >= QUARTILE_GATE_PP
    ):

        decision = (
            "PASS_DISCOVERY"
        )


    else:

        decision = (
            "REJECT_DISCOVERY"
        )


    return {
        "n":
            n,

        "positives":
            positives,

        "negatives":
            negatives,

        "target_rho":
            target_rho,

        "max_rho":
            max_rho,

        "min_rho":
            min_rho,

        "end_rho":
            end_rho,

        "bootstrap_median":
            boot_med,

        "bootstrap_low":
            boot_low,

        "bootstrap_high":
            boot_high,

        "quartiles":
            quartiles,

        "q4_minus_q1_pp":
            qdiff,

        "feature_rhos":
            feature_rhos,

        "fitted_spec":
            fitted,

        "decision":
            decision,
    }


# ============================================================
# MAIN
# ============================================================

def main():

    if len(sys.argv) != 2:

        raise SystemExit(
            "usage: research_worker_v2.py JOB_ID"
        )


    job_id = sys.argv[1]


    job = get_job(
        job_id
    )


    if not job:

        raise RuntimeError(
            f"Unknown job: {job_id}"
        )


    payload = json.loads(
        job[
            "payload_json"
        ]
    )


    experiment_id = job[
        "experiment_id"
    ]


    rc.execute("""
    UPDATE jobs

    SET
        status='RUNNING',
        started_at=?,
        attempts=attempts+1,
        updated_at=?

    WHERE job_id=?
    """, (
        time.time(),
        time.time(),
        job_id,
    ))


    rc.execute("""
    UPDATE experiments

    SET
        status='DISCOVERY',
        updated_at=?

    WHERE experiment_id=?
    """, (
        time.time(),
        experiment_id,
    ))


    start = time.time()


    try:

        rows = load_rows(
            payload
        )


        if len(rows) < 20:

            result = {
                "n":
                    len(rows),

                "decision":
                    "WAIT_DATA",

                "reason":
                    "Insufficient executable GOOD rows",
            }


        else:

            result = evaluate(
                rows,
                payload
            )


        elapsed = (
            time.time()
            - start
        )


        result[
            "compute_s"
        ] = elapsed


        rc.execute("""
        UPDATE jobs

        SET
            status='DONE',
            finished_at=?,
            result_json=?,
            updated_at=?

        WHERE job_id=?
        """, (
            time.time(),
            json.dumps(
                result,
                separators=(
                    ",",
                    ":"
                )
            ),
            time.time(),
            job_id,
        ))


        decision = result.get(
            "decision"
        )


        if decision == "PASS_DISCOVERY":

            exp_status = (
                "DISCOVERY_PASSED"
            )


        elif decision in (
            "COLLECT_MORE",
            "WAIT_DATA",
        ):

            exp_status = (
                "COLLECT_MORE"
            )


        else:

            exp_status = (
                "REJECTED_DISCOVERY"
            )


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
        """, (

            exp_status,

            result.get(
                "n"
            ),

            result.get(
                "positives"
            ),

            result.get(
                "negatives"
            ),

            result.get(
                "target_rho"
            ),

            decision,

            time.time(),

            experiment_id,
        ))


        print(
            json.dumps(
                {
                    "job":
                        job_id,

                    "experiment":
                        experiment_id,

                    "decision":
                        decision,

                    "n":
                        result.get(
                            "n"
                        ),

                    "rho":
                        result.get(
                            "target_rho"
                        ),
                }
            )
        )


    except Exception as e:

        rc.execute("""
        UPDATE jobs

        SET
            status='FAILED',
            error=?,
            finished_at=?,
            updated_at=?

        WHERE job_id=?
        """, (

            repr(e),

            time.time(),
            time.time(),

            job_id,
        ))


        rc.execute("""
        UPDATE experiments

        SET
            status='ERROR',
            conclusion=?,
            updated_at=?

        WHERE experiment_id=?
        """, (

            repr(e),

            time.time(),

            experiment_id,
        ))


        raise


if __name__ == "__main__":

    main()
