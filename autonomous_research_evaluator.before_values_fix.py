#!/usr/bin/env python3

import sqlite3
import os
import time
import math
import random
import statistics

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

EXP_ID = "EXP_0121"
SOURCE = "lab_exp0121_stage_features"
EVAL_TABLE = "lab_exp0121_evaluations"

REFRESH = 30

STAGES = [5, 10, 20, 30, 60]

MIN_GOOD = 250
MIN_POS = 50
MIN_NEG = 100

BOOTSTRAPS = 400

TARGET_RHO_GATE = 0.15
LOTO_GATE = 0.12
Q4_Q1_GATE_PP = 10.0


FAMILIES = {

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

    "PRICE+ACTIVITY": [
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

    "PRICE+ACTIVITY+FLOW": [
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


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLES
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {EVAL_TABLE} (

    stage_s INTEGER NOT NULL,
    family TEXT NOT NULL,

    evaluated_at REAL NOT NULL,

    n INTEGER NOT NULL,
    positives INTEGER NOT NULL,
    negatives INTEGER NOT NULL,

    target_rho REAL,
    max_rho REAL,
    min_rho REAL,
    end_rho REAL,

    loto_median REAL,
    loto_p10 REAL,
    loto_p90 REAL,
    loto_same_sign INTEGER,
    loto_total INTEGER,

    bootstrap_median REAL,
    bootstrap_p025 REAL,
    bootstrap_p975 REAL,

    q1_hit_rate REAL,
    q2_hit_rate REAL,
    q3_hit_rate REAL,
    q4_hit_rate REAL,

    q1_max_med REAL,
    q4_max_med REAL,

    q1_min_med REAL,
    q4_min_med REAL,

    q4_minus_q1_pp REAL,

    decision TEXT,

    PRIMARY KEY (
        stage_s,
        family
    )
)
""")

db.commit()


# ============================================================
# HELPERS
# ============================================================

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

    return (
        statistics.median(xs)
        if xs
        else None
    )


def stdev(xs):
    xs = [x for x in xs if valid(x)]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


def percentile(xs, q):
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    if len(xs) == 1:
        return xs[0]

    pos = (len(xs) - 1) * q

    lo = math.floor(pos)
    hi = math.ceil(pos)

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return (
        xs[lo] * (1 - w)
        + xs[hi] * w
    )


def fmt(x, n=3):
    if x is None:
        return "NA"

    return f"{x:.{n}f}"


def ranks(values):
    indexed = sorted(
        enumerate(values),
        key=lambda x: x[1]
    )

    out = [0.0] * len(values)

    i = 0

    while i < len(indexed):

        j = i

        while (
            j + 1 < len(indexed)
            and indexed[j + 1][1]
                == indexed[i][1]
        ):
            j += 1

        rank = (
            i + j + 2
        ) / 2.0

        for k in range(i, j + 1):
            out[
                indexed[k][0]
            ] = rank

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
        sum(
            (a - mx) ** 2
            for a in x
        )
    )

    dy = math.sqrt(
        sum(
            (b - my) ** 2
            for b in y
        )
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

    xx = [
        a for a, _ in pairs
    ]

    yy = [
        b for _, b in pairs
    ]

    return pearson(
        ranks(xx),
        ranks(yy)
    )


def set_meta(key, value):
    db.execute("""
    INSERT INTO lab_meta (
        key,
        value
    )
    VALUES (?,?)

    ON CONFLICT(key)
    DO UPDATE SET
        value=excluded.value
    """, (
        key,
        str(value)
    ))

    db.commit()


# ============================================================
# DATA
# ============================================================

def load_stage(stage):
    return [
        dict(r)

        for r in db.execute(f"""
        SELECT *
        FROM {SOURCE}

        WHERE
            stage_s=?
            AND future_ready=1
            AND coverage_status='GOOD'
            AND future_hit20 IS NOT NULL
            AND future_max300 IS NOT NULL
            AND future_min300 IS NOT NULL
            AND future_end300 IS NOT NULL
        """, (
            stage,
        )).fetchall()
    ]


# ============================================================
# FIT FAMILY
#
# Discovery-only directional score:
#
# 1. standardize each feature
# 2. determine direction from feature↔target rho
# 3. equal-weight signed Z scores
#
# This is exploratory fitting.
# Any winner must later be FROZEN and validated prospectively.
# ============================================================

def fit_spec(
    rows,
    features
):
    spec = {}

    target = [
        r["future_hit20"]
        for r in rows
    ]

    for f in features:

        vals = [
            r[f]
            for r in rows
        ]

        if not all(
            valid(x)
            for x in vals
        ):
            return None

        m = mean(vals)
        sd = stdev(vals)

        if (
            m is None
            or sd is None
            or sd == 0
        ):
            direction = 0.0

        else:

            rho = spearman(
                vals,
                target
            )

            if rho is None:
                direction = 0.0

            elif rho > 0:
                direction = 1.0

            elif rho < 0:
                direction = -1.0

            else:
                direction = 0.0

        spec[f] = {
            "mean": m or 0.0,
            "std": sd or 1.0,
            "direction": direction,
        }

    return spec


def score_row(
    row,
    features,
    spec
):
    components = []

    for f in features:

        x = row[f]

        if not valid(x):
            return None

        s = spec[f]

        z = (
            (x - s["mean"])
            / s["std"]
            if s["std"] != 0
            else 0.0
        )

        components.append(
            z * s["direction"]
        )

    return mean(
        components
    )


def score_rows(
    rows,
    features,
    spec
):
    out = []

    for r in rows:

        score = score_row(
            r,
            features,
            spec
        )

        if score is None:
            continue

        d = dict(r)
        d["_score"] = score

        out.append(d)

    return out


# ============================================================
# METRICS
# ============================================================

def metrics(scored):

    if len(scored) < 10:
        return None

    s = [
        r["_score"]
        for r in scored
    ]

    target = [
        r["future_hit20"]
        for r in scored
    ]

    max300 = [
        r["future_max300"]
        for r in scored
    ]

    min300 = [
        r["future_min300"]
        for r in scored
    ]

    end300 = [
        r["future_end300"]
        for r in scored
    ]

    return {
        "target_rho":
            spearman(
                s,
                target
            ),

        "max_rho":
            spearman(
                s,
                max300
            ),

        "min_rho":
            spearman(
                s,
                min300
            ),

        "end_rho":
            spearman(
                s,
                end300
            ),

        "n":
            len(scored),

        "pos":
            sum(target),

        "neg":
            len(target)
            - sum(target),
    }


# ============================================================
# TRUE LOTO REFIT
# ============================================================

def loto(
    rows,
    features
):
    full_spec = fit_spec(
        rows,
        features
    )

    if not full_spec:
        return None

    full_scores = score_rows(
        rows,
        features,
        full_spec
    )

    full_m = metrics(
        full_scores
    )

    if (
        full_m is None
        or full_m[
            "target_rho"
        ] is None
    ):
        return None

    full_rho = full_m[
        "target_rho"
    ]

    rhos = []
    signs = []

    for i in range(
        len(rows)
    ):

        train = (
            rows[:i]
            + rows[i + 1:]
        )

        spec = fit_spec(
            train,
            features
        )

        if not spec:
            continue

        # Score the full remaining training cohort using
        # the refit spec.
        scored = score_rows(
            train,
            features,
            spec
        )

        m = metrics(
            scored
        )

        if (
            m is None
            or m[
                "target_rho"
            ] is None
        ):
            continue

        rho = m[
            "target_rho"
        ]

        rhos.append(
            rho
        )

        signs.append(
            int(
                rho == 0
                or full_rho == 0
                or (
                    rho > 0
                    and full_rho > 0
                )
                or (
                    rho < 0
                    and full_rho < 0
                )
            )
        )

    return {
        "full_spec":
            full_spec,

        "full_scores":
            full_scores,

        "full_metrics":
            full_m,

        "median":
            median(rhos),

        "p10":
            percentile(
                rhos,
                0.10
            ),

        "p90":
            percentile(
                rhos,
                0.90
            ),

        "same_sign":
            sum(signs),

        "sign_total":
            len(signs),
    }


# ============================================================
# BOOTSTRAP REFIT
# ============================================================

def bootstrap(
    rows,
    features,
    stage,
    family
):
    if len(rows) < 20:
        return None

    rng = random.Random(
        (
            stage * 10000
            + sum(
                ord(c)
                for c in family
            )
        )
    )

    values = []

    n = len(rows)

    for _ in range(
        BOOTSTRAPS
    ):

        sample = [
            rows[
                rng.randrange(n)
            ]
            for _ in range(n)
        ]

        spec = fit_spec(
            sample,
            features
        )

        if not spec:
            continue

        scored = score_rows(
            sample,
            features,
            spec
        )

        m = metrics(
            scored
        )

        if (
            m is not None
            and m[
                "target_rho"
            ] is not None
        ):

            values.append(
                m["target_rho"]
            )

    if not values:
        return None

    return {
        "median":
            median(values),

        "p025":
            percentile(
                values,
                0.025
            ),

        "p975":
            percentile(
                values,
                0.975
            ),
    }


# ============================================================
# QUARTILES
# ============================================================

def quartiles(scored):

    ordered = sorted(
        scored,
        key=lambda r:
            r["_score"]
    )

    n = len(ordered)

    if n < 20:
        return None

    out = []

    for i in range(4):

        a = int(
            n * i / 4
        )

        b = int(
            n * (
                i + 1
            ) / 4
        )

        part = ordered[
            a:b
        ]

        if not part:
            continue

        hit_rate = (
            100.0
            * sum(
                r[
                    "future_hit20"
                ] == 1

                for r in part
            )
            / len(part)
        )

        out.append({
            "n":
                len(part),

            "hit":
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

    return (
        out
        if len(out) == 4
        else None
    )


# ============================================================
# DECISION
# ============================================================

def decision_for(
    n,
    pos,
    neg,
    target_rho,
    loto_med,
    boot_low,
    qdiff
):

    if (
        n < MIN_GOOD
        or pos < MIN_POS
        or neg < MIN_NEG
    ):
        return "COLLECT_MORE"

    if (
        target_rho is None
        or loto_med is None
        or boot_low is None
        or qdiff is None
    ):
        return "REJECT"

    if (
        target_rho >= TARGET_RHO_GATE
        and loto_med >= LOTO_GATE
        and boot_low > 0
        and qdiff >= Q4_Q1_GATE_PP
    ):
        return "FREEZE_CANDIDATE"

    return "REJECT"


# ============================================================
# EVALUATE
# ============================================================

def evaluate_all():

    best_candidate = None

    for stage in STAGES:

        rows = load_stage(
            stage
        )

        for family, features in FAMILIES.items():

            if len(rows) < 20:
                continue

            result = loto(
                rows,
                features
            )

            if not result:
                continue

            scored = result[
                "full_scores"
            ]

            m = result[
                "full_metrics"
            ]

            boot = bootstrap(
                rows,
                features,
                stage,
                family
            )

            qs = quartiles(
                scored
            )

            if qs:

                q1 = qs[0]
                q2 = qs[1]
                q3 = qs[2]
                q4 = qs[3]

                qdiff = (
                    q4["hit"]
                    - q1["hit"]
                )

            else:

                q1 = q2 = q3 = q4 = {
                    "hit": None,
                    "max_med": None,
                    "min_med": None,
                }

                qdiff = None


            decision = decision_for(
                m["n"],
                m["pos"],
                m["neg"],

                m[
                    "target_rho"
                ],

                result[
                    "median"
                ],

                (
                    boot[
                        "p025"
                    ]
                    if boot
                    else None
                ),

                qdiff,
            )


            db.execute(f"""
            INSERT INTO {EVAL_TABLE} (

                stage_s,
                family,

                evaluated_at,

                n,
                positives,
                negatives,

                target_rho,
                max_rho,
                min_rho,
                end_rho,

                loto_median,
                loto_p10,
                loto_p90,
                loto_same_sign,
                loto_total,

                bootstrap_median,
                bootstrap_p025,
                bootstrap_p975,

                q1_hit_rate,
                q2_hit_rate,
                q3_hit_rate,
                q4_hit_rate,

                q1_max_med,
                q4_max_med,

                q1_min_med,
                q4_min_med,

                q4_minus_q1_pp,

                decision
            )

            VALUES (
                ?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,
                ?,?
            )

            ON CONFLICT(
                stage_s,
                family
            )

            DO UPDATE SET

                evaluated_at=
                    excluded.evaluated_at,

                n=
                    excluded.n,

                positives=
                    excluded.positives,

                negatives=
                    excluded.negatives,

                target_rho=
                    excluded.target_rho,

                max_rho=
                    excluded.max_rho,

                min_rho=
                    excluded.min_rho,

                end_rho=
                    excluded.end_rho,

                loto_median=
                    excluded.loto_median,

                loto_p10=
                    excluded.loto_p10,

                loto_p90=
                    excluded.loto_p90,

                loto_same_sign=
                    excluded.loto_same_sign,

                loto_total=
                    excluded.loto_total,

                bootstrap_median=
                    excluded.bootstrap_median,

                bootstrap_p025=
                    excluded.bootstrap_p025,

                bootstrap_p975=
                    excluded.bootstrap_p975,

                q1_hit_rate=
                    excluded.q1_hit_rate,

                q2_hit_rate=
                    excluded.q2_hit_rate,

                q3_hit_rate=
                    excluded.q3_hit_rate,

                q4_hit_rate=
                    excluded.q4_hit_rate,

                q1_max_med=
                    excluded.q1_max_med,

                q4_max_med=
                    excluded.q4_max_med,

                q1_min_med=
                    excluded.q1_min_med,

                q4_min_med=
                    excluded.q4_min_med,

                q4_minus_q1_pp=
                    excluded.q4_minus_q1_pp,

                decision=
                    excluded.decision
            """, (

                stage,
                family,

                time.time(),

                m["n"],
                m["pos"],
                m["neg"],

                m[
                    "target_rho"
                ],

                m[
                    "max_rho"
                ],

                m[
                    "min_rho"
                ],

                m[
                    "end_rho"
                ],

                result[
                    "median"
                ],

                result[
                    "p10"
                ],

                result[
                    "p90"
                ],

                result[
                    "same_sign"
                ],

                result[
                    "sign_total"
                ],

                (
                    boot[
                        "median"
                    ]
                    if boot
                    else None
                ),

                (
                    boot[
                        "p025"
                    ]
                    if boot
                    else None
                ),

                (
                    boot[
                        "p975"
                    ]
                    if boot
                    else None
                ),

                q1[
                    "hit"
                ],

                q2[
                    "hit"
                ],

                q3[
                    "hit"
                ],

                q4[
                    "hit"
                ],

                q1[
                    "max_med"
                ],

                q4[
                    "max_med"
                ],

                q1[
                    "min_med"
                ],

                q4[
                    "min_med"
                ],

                qdiff,

                decision,
            ))


            if (
                decision
                == "FREEZE_CANDIDATE"
            ):

                candidate_score = (
                    m[
                        "target_rho"
                    ]
                    + result[
                        "median"
                    ]
                    + max(
                        0.0,
                        qdiff / 100.0
                    )
                )

                if (
                    best_candidate is None
                    or candidate_score
                    > best_candidate[
                        "candidate_score"
                    ]
                ):

                    best_candidate = {
                        "stage":
                            stage,

                        "family":
                            family,

                        "score":
                            m[
                                "target_rho"
                            ],

                        "loto":
                            result[
                                "median"
                            ],

                        "qdiff":
                            qdiff,

                        "n":
                            m[
                                "n"
                            ],

                        "candidate_score":
                            candidate_score,
                    }


    db.commit()

    return best_candidate


# ============================================================
# AUTONOMOUS STATE TRANSITION
# ============================================================

def update_lab_state(
    candidate
):

    max_good = db.execute(f"""
    SELECT
        MAX(n)
    FROM {EVAL_TABLE}
    """).fetchone()[0]

    max_good = (
        max_good
        or 0
    )


    if candidate:

        set_meta(
            "research_next_action",
            (
                "FREEZE_EXP0121_CANDIDATE:"
                f"stage={candidate['stage']}s,"
                f"family={candidate['family']}"
            )
        )

        db.execute("""
        UPDATE lab_experiments

        SET
            status='DISCOVERY_PASSED',

            discovery_n=?,

            conclusion=?,

            last_update_at=?

        WHERE experiment_id=?
        """, (

            candidate[
                "n"
            ],

            (
                f"Autonomous discovery candidate: "
                f"stage={candidate['stage']}s; "
                f"family={candidate['family']}; "
                f"rho={candidate['score']:.3f}; "
                f"LOTO={candidate['loto']:.3f}; "
                f"Q4-Q1={candidate['qdiff']:.1f}pp"
            ),

            time.time(),

            EXP_ID,
        ))

        db.commit()

        return


    if max_good < MIN_GOOD:

        set_meta(
            "research_next_action",
            (
                f"COLLECT_MORE:"
                f"best_good_n={max_good}/"
                f"{MIN_GOOD}"
            )
        )

        db.execute("""
        UPDATE lab_experiments

        SET
            status='DISCOVERY',
            discovery_n=?,
            last_update_at=?

        WHERE experiment_id=?
        """, (
            max_good,
            time.time(),
            EXP_ID,
        ))

        db.commit()

        return


    any_noncollect = db.execute(f"""
    SELECT COUNT(*)
    FROM {EVAL_TABLE}

    WHERE
        n >= ?
        AND positives >= ?
        AND negatives >= ?
    """, (
        MIN_GOOD,
        MIN_POS,
        MIN_NEG,
    )).fetchone()[0]


    if any_noncollect > 0:

        set_meta(
            "research_next_action",
            "REJECT_EXP0121_OR_GENERATE_VARIANT"
        )

        db.execute("""
        UPDATE lab_experiments

        SET
            status='DISCOVERY_REJECTED',

            conclusion=(
                'No stage/family passed autonomous '
                'discovery gates.'
            ),

            last_update_at=?

        WHERE experiment_id=?
        """, (
            time.time(),
            EXP_ID,
        ))

        db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")

    rows = db.execute(f"""
    SELECT *

    FROM {EVAL_TABLE}

    ORDER BY
        stage_s ASC,
        target_rho DESC
    """).fetchall()


    print("=" * 190)

    print(
        "MEMECOIN LAB — AUTONOMOUS RESEARCH EVALUATOR"
    )

    print("=" * 190)

    print(
        f"EXPERIMENT        : {EXP_ID}"
    )

    print(
        f"MIN GOOD          : {MIN_GOOD}"
    )

    print(
        f"MIN POS/NEG       : "
        f"{MIN_POS}/{MIN_NEG}"
    )

    print(
        "PROMOTION GATES   : "
        f"rho>={TARGET_RHO_GATE}, "
        f"LOTO>={LOTO_GATE}, "
        "bootstrap low>0, "
        f"Q4-Q1>={Q4_Q1_GATE_PP:.0f}pp"
    )


    print()
    print("=" * 190)
    print("STAGE / FAMILY AUDIT")
    print("=" * 190)

    print(
        f"{'STAGE':<7}"
        f"{'FAMILY':<26}"
        f"{'N':>7}"
        f"{'+20':>7}"
        f"{'RHO':>9}"
        f"{'LOTO':>9}"
        f"{'B-LOW':>9}"
        f"{'Q1':>9}"
        f"{'Q4':>9}"
        f"{'ΔQ':>9}"
        f"{'DECISION':>20}"
    )


    for r in rows:

        print(
            f"{r['stage_s']:<7}"
            f"{r['family']:<26}"
            f"{r['n']:>7}"
            f"{r['positives']:>7}"
            f"{fmt(r['target_rho']):>9}"
            f"{fmt(r['loto_median']):>9}"
            f"{fmt(r['bootstrap_p025']):>9}"
            f"{fmt(r['q1_hit_rate'],1):>8}%"
            f"{fmt(r['q4_hit_rate'],1):>8}%"
            f"{fmt(r['q4_minus_q1_pp'],1):>8}"
            f"{r['decision']:>20}"
        )


    print()
    print("=" * 190)
    print("AUTONOMOUS DECISION")
    print("=" * 190)

    meta = db.execute("""
    SELECT value
    FROM lab_meta
    WHERE key='research_next_action'
    """).fetchone()

    print(
        meta["value"]
        if meta
        else "NONE"
    )

    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops evaluator only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        candidate = evaluate_all()

        update_lab_state(
            candidate
        )

        show()

        time.sleep(
            REFRESH
        )

except KeyboardInterrupt:

    print()
    print(
        "Autonomous evaluator stopped safely."
    )

finally:

    db.close()
