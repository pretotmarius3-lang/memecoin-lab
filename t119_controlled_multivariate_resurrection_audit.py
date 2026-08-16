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

DUMP_EVENTS = "t116_premigration_dump_events"
DUMP_OUT = "t117_dump_outcomes"

REFRESH = 30

PATH_MAX_RETURN_LIMIT = 1000.0
PATH_MIN_RETURN_LIMIT = -99.99

BOOTSTRAPS = 1000
RANDOM_SEED = 119

random.seed(RANDOM_SEED)

# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


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
    xs = [
        x for x in xs
        if valid(x)
    ]

    return (
        sum(xs) / len(xs)
        if xs
        else None
    )


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


def stdev(xs):
    xs = [
        x for x in xs
        if valid(x)
    ]

    if len(xs) < 2:
        return None

    return statistics.stdev(xs)


def fmt(x, n=3):
    if x is None:
        return "NA"

    return f"{x:.{n}f}"


def percentile(xs, q):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    if len(xs) == 1:
        return xs[0]

    pos = (
        (len(xs) - 1)
        * q
    )

    lo = math.floor(pos)
    hi = math.ceil(pos)

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return (
        xs[lo] * (1 - w)
        + xs[hi] * w
    )


def ranks(values):

    indexed = sorted(
        enumerate(values),
        key=lambda x: x[1]
    )

    result = [0.0] * len(values)

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

            result[
                indexed[k][0]
            ] = rank

        i = j + 1

    return result


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
        if valid(a)
        and valid(b)
    ]

    if len(pairs) < 3:
        return None

    xx = [
        a
        for a, _ in pairs
    ]

    yy = [
        b
        for _, b in pairs
    ]

    return pearson(
        ranks(xx),
        ranks(yy)
    )


def zscore(xs):

    m = mean(xs)
    sd = stdev(xs)

    if (
        m is None
        or sd is None
        or sd == 0
    ):
        return [
            0.0
            for _ in xs
        ]

    return [
        (x - m) / sd
        for x in xs
    ]


# ============================================================
# DATASET
# ============================================================

def load_dataset():

    rows = db.execute(f"""
    WITH ranked AS (

        SELECT
            e.*,

            ROW_NUMBER() OVER (
                PARTITION BY e.token_mint

                ORDER BY
                    e.trigger_timestamp ASC,
                    e.dump_level ASC,
                    e.id ASC
            ) AS rn

        FROM {DUMP_EVENTS} e
    )

    SELECT
        e.*,

        o.path_done_300s,
        o.path_snapshots_300s,

        o.path_max_return_300s,
        o.path_min_return_300s,
        o.path_end_return_300s,

        o.rebound20_300,
        o.rebound50_300,
        o.reclaim_old_peak_300

    FROM ranked e

    JOIN {DUMP_OUT} o
      ON o.t116_dump_event_id=e.id

    WHERE e.rn=1
    """).fetchall()

    out = []

    for r in rows:

        if (
            r["path_done_300s"] != 1
            or r["path_snapshots_300s"] is None
            or r["path_snapshots_300s"] < 1
        ):
            continue

        mx = r[
            "path_max_return_300s"
        ]

        mn = r[
            "path_min_return_300s"
        ]

        end = r[
            "path_end_return_300s"
        ]

        if not (
            valid(mx)
            and valid(mn)
            and valid(end)
        ):
            continue

        if (
            mx > PATH_MAX_RETURN_LIMIT
            or mn < PATH_MIN_RETURN_LIMIT
            or end < PATH_MIN_RETURN_LIMIT
        ):
            continue

        d = dict(r)

        d["target"] = int(
            r["rebound20_300"] == 1
        )

        out.append(d)

    return out


# ============================================================
# FEATURE FAMILIES
# ============================================================

FAMILIES = {

    "STRUCTURE": [
        "run_from_first_pct",
        "drawdown_pct",
    ],

    "STRUCTURE+ACTIVITY": [
        "run_from_first_pct",
        "drawdown_pct",

        "swaps_30s",
        "swaps_60s",
    ],

    "STRUCTURE+ACTIVITY+BUY": [
        "run_from_first_pct",
        "drawdown_pct",

        "swaps_30s",
        "swaps_60s",

        "buys_30s",
        "buys_60s",
        "buys_total",
    ],

    "FULL_FLOW": [
        "run_from_first_pct",
        "drawdown_pct",

        "swaps_30s",
        "swaps_60s",

        "buys_30s",
        "buys_60s",
        "buys_total",

        "net_sol_30s",
        "net_sol_60s",
        "net_sol_total",
    ],
}


# ============================================================
# FAMILY SCORE
# ============================================================

def family_score(
    rows,
    features
):

    usable = []

    for r in rows:

        if all(
            valid(r.get(f))
            for f in features
        ):
            usable.append(r)

    if len(usable) < 10:
        return []

    standardized = {}

    for f in features:

        vals = [
            r[f]
            for r in usable
        ]

        standardized[f] = zscore(
            vals
        )

    scores = []

    for i, r in enumerate(usable):

        feature_zs = [
            standardized[f][i]
            for f in features
        ]

        score = mean(
            feature_zs
        )

        scores.append({
            "token_mint":
                r["token_mint"],

            "score":
                score,

            "target":
                r["target"],

            "max300":
                r["path_max_return_300s"],

            "min300":
                r["path_min_return_300s"],

            "end300":
                r["path_end_return_300s"],
        })

    return scores


# ============================================================
# MODEL METRICS
# ============================================================

def metrics(scores):

    if len(scores) < 10:
        return None

    s = [
        r["score"]
        for r in scores
    ]

    target = [
        r["target"]
        for r in scores
    ]

    mx = [
        r["max300"]
        for r in scores
    ]

    mn = [
        r["min300"]
        for r in scores
    ]

    end = [
        r["end300"]
        for r in scores
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
                mx
            ),

        "min_rho":
            spearman(
                s,
                mn
            ),

        "end_rho":
            spearman(
                s,
                end
            ),

        "n":
            len(scores),

        "positives":
            sum(target),

        "negatives":
            len(target)
            - sum(target),
    }


# ============================================================
# LEAVE ONE TOKEN OUT
# ============================================================

def loto(scores):

    full = metrics(
        scores
    )

    if full is None:
        return None

    vals = []

    signs = []

    full_rho = full[
        "target_rho"
    ]

    for i in range(
        len(scores)
    ):

        sub = (
            scores[:i]
            + scores[i + 1:]
        )

        m = metrics(
            sub
        )

        if (
            m is None
            or m["target_rho"] is None
        ):
            continue

        rho = m[
            "target_rho"
        ]

        vals.append(
            rho
        )

        if full_rho is not None:

            same = (
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

            signs.append(
                int(same)
            )

    return {

        "full":
            full,

        "loto_median":
            median(vals),

        "loto_p10":
            percentile(
                vals,
                0.10
            ),

        "loto_p90":
            percentile(
                vals,
                0.90
            ),

        "same_sign":
            sum(signs),

        "sign_total":
            len(signs),
    }


# ============================================================
# BOOTSTRAP
# ============================================================

def bootstrap_target_rho(
    scores,
    iterations=BOOTSTRAPS
):

    n = len(scores)

    if n < 10:
        return None

    vals = []

    for _ in range(
        iterations
    ):

        sample = [
            scores[
                random.randrange(n)
            ]
            for _ in range(n)
        ]

        m = metrics(
            sample
        )

        if (
            m is not None
            and m[
                "target_rho"
            ] is not None
        ):
            vals.append(
                m["target_rho"]
            )

    if not vals:
        return None

    return {
        "median":
            median(vals),

        "p025":
            percentile(
                vals,
                0.025
            ),

        "p975":
            percentile(
                vals,
                0.975
            ),

        "n":
            len(vals),
    }


# ============================================================
# INCREMENTAL RESIDUAL TEST
# ============================================================

def residualize(
    x,
    controls
):

    # Simple sequential linear residualization.
    # Research diagnostic only.

    residual = list(x)

    for control in controls:

        pairs = [
            (a, b)
            for a, b in zip(
                residual,
                control
            )
            if valid(a)
            and valid(b)
        ]

        if len(pairs) < 3:
            continue

        rx = [
            a
            for a, _ in pairs
        ]

        cx = [
            b
            for _, b in pairs
        ]

        mc = mean(cx)
        mr = mean(rx)

        var_c = sum(
            (c - mc) ** 2
            for c in cx
        )

        if var_c == 0:
            continue

        beta = (
            sum(
                (c - mc)
                * (r - mr)

                for r, c
                in zip(rx, cx)
            )
            / var_c
        )

        new_resid = []

        for r, c in zip(
            residual,
            control
        ):

            new_resid.append(
                r
                - beta
                * (
                    c - mc
                )
            )

        residual = new_resid

    return residual


def controlled_buy_test(rows):

    required = [
        "buys_30s",
        "swaps_30s",
        "run_from_first_pct",
        "drawdown_pct",
        "target",
    ]

    usable = [
        r
        for r in rows
        if all(
            valid(r.get(x))
            for x in required
        )
    ]

    if len(usable) < 20:
        return None

    buy = [
        r[
            "buys_30s"
        ]
        for r in usable
    ]

    activity = [
        r[
            "swaps_30s"
        ]
        for r in usable
    ]

    run = [
        r[
            "run_from_first_pct"
        ]
        for r in usable
    ]

    dd = [
        r[
            "drawdown_pct"
        ]
        for r in usable
    ]

    target = [
        r[
            "target"
        ]
        for r in usable
    ]

    residual_buy = residualize(
        buy,
        [
            activity,
            run,
            dd,
        ]
    )

    return {

        "raw":
            spearman(
                buy,
                target
            ),

        "controlled":
            spearman(
                residual_buy,
                target
            ),

        "n":
            len(usable),
    }


# ============================================================
# BUCKET AUDIT
# ============================================================

def bucket_audit(scores):

    ordered = sorted(
        scores,
        key=lambda r:
            r["score"]
    )

    n = len(ordered)

    if n < 20:
        return []

    buckets = []

    for i in range(4):

        a = int(
            n * i / 4
        )

        b = int(
            n * (i + 1) / 4
        )

        part = ordered[a:b]

        if not part:
            continue

        positives = sum(
            r["target"]
            for r in part
        )

        buckets.append({

            "quartile":
                i + 1,

            "n":
                len(part),

            "hit_rate":
                positives
                / len(part),

            "max_med":
                median(
                    [
                        r["max300"]
                        for r in part
                    ]
                ),

            "min_med":
                median(
                    [
                        r["min300"]
                        for r in part
                    ]
                ),

            "end_med":
                median(
                    [
                        r["end300"]
                        for r in part
                    ]
                ),
        })

    return buckets


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")

    rows = load_dataset()

    positives = sum(
        r["target"]
        for r in rows
    )

    negatives = (
        len(rows)
        - positives
    )

    print("=" * 190)

    print(
        "MEMECOIN LAB — T119 CONTROLLED MULTIVARIATE "
        "PRE-MIGRATION RESURRECTION AUDIT"
    )

    print("=" * 190)

    print(
        f"ROWS              : {len(rows)}"
    )

    print(
        f"UNIQUE TOKENS     : {len(rows)}"
    )

    print(
        f"REBOUND +20       : {positives}"
    )

    print(
        f"NO +20            : {negatives}"
    )

    print()

    print(
        "TARGET            : +20% WITHIN 300s AFTER FIRST DUMP"
    )

    print(
        "PATH FILTER       : GOOD_PATH ONLY"
    )

    print(
        "MODEL TYPE        : STANDARDIZED FAMILY SCORES"
    )

    print(
        "ROBUSTNESS        : LEAVE-ONE-TOKEN-OUT + BOOTSTRAP"
    )

    print(
        "THRESHOLD SEARCH  : NONE"
    )

    print(
        "ENTRY RULE        : NONE"
    )


    # ========================================================
    # FAMILY COMPARISON
    # ========================================================

    print()
    print("=" * 190)
    print("HIERARCHICAL FAMILY COMPARISON")
    print("=" * 190)

    print(
        f"{'MODEL':<32}"
        f"{'TARGET RHO':>12}"
        f"{'LOTO MED':>12}"
        f"{'SIGN':>12}"
        f"{'MAX RHO':>11}"
        f"{'MIN RHO':>11}"
        f"{'END RHO':>11}"
        f"{'N':>7}"
    )

    results = {}

    for name, features in FAMILIES.items():

        scores = family_score(
            rows,
            features
        )

        result = loto(
            scores
        )

        if result is None:
            continue

        m = result[
            "full"
        ]

        results[
            name
        ] = {
            "scores":
                scores,

            "result":
                result,
        }

        print(
            f"{name:<32}"
            f"{fmt(m['target_rho']):>12}"
            f"{fmt(result['loto_median']):>12}"
            f"{result['same_sign']:>5}/"
            f"{result['sign_total']:<6}"
            f"{fmt(m['max_rho']):>11}"
            f"{fmt(m['min_rho']):>11}"
            f"{fmt(m['end_rho']):>11}"
            f"{m['n']:>7}"
        )


    # ========================================================
    # INCREMENTAL VALUE
    # ========================================================

    print()
    print("=" * 190)
    print("INCREMENTAL FAMILY VALUE")
    print("=" * 190)

    names = list(
        FAMILIES.keys()
    )

    previous = None

    for name in names:

        item = results.get(
            name
        )

        if not item:
            continue

        m = item[
            "result"
        ][
            "full"
        ]

        print()
        print(name)

        print(
            "  FEATURES : "
            + ", ".join(
                FAMILIES[name]
            )
        )

        print(
            f"  TARGET   : "
            f"full={fmt(m['target_rho'])} "
            f"| LOTO={fmt(item['result']['loto_median'])} "
            f"| sign="
            f"{item['result']['same_sign']}/"
            f"{item['result']['sign_total']}"
        )

        print(
            f"  MAX300   : "
            f"{fmt(m['max_rho'])}"
        )

        print(
            f"  MIN300   : "
            f"{fmt(m['min_rho'])}"
        )

        print(
            f"  END300   : "
            f"{fmt(m['end_rho'])}"
        )

        if previous is not None:

            prev = results[
                previous
            ][
                "result"
            ][
                "full"
            ]

            if (
                m["target_rho"] is not None
                and prev["target_rho"] is not None
            ):

                print(
                    f"  Δ TARGET : "
                    f"{m['target_rho'] - prev['target_rho']:+.3f}"
                )

        previous = name


    # ========================================================
    # CONTROLLED BUY TEST
    # ========================================================

    print()
    print("=" * 190)
    print("CONTROLLED BUY-PRESSURE TEST")
    print("=" * 190)

    controlled = controlled_buy_test(
        rows
    )

    if controlled:

        print(
            "Question:"
        )

        print(
            "Does buys_30s retain information after controlling "
            "for swaps_30s + prior run + dump depth?"
        )

        print()

        print(
            f"RAW buys_30s rho        : "
            f"{fmt(controlled['raw'])}"
        )

        print(
            f"CONTROLLED rho           : "
            f"{fmt(controlled['controlled'])}"
        )

        print(
            f"N                        : "
            f"{controlled['n']}"
        )

        if (
            controlled[
                "controlled"
            ] is not None
            and controlled[
                "controlled"
            ] > 0.10
        ):

            print(
                "🟢 BUY PRESSURE RETAINS POSITIVE "
                "INCREMENTAL INFORMATION"
            )

        else:

            print(
                "🟡 BUY PRESSURE MAY MOSTLY REFLECT "
                "GENERAL ACTIVITY"
            )


    # ========================================================
    # BOOTSTRAP
    # ========================================================

    print()
    print("=" * 190)
    print("BOOTSTRAP STABILITY — TARGET RHO")
    print("=" * 190)

    for name, item in results.items():

        b = bootstrap_target_rho(
            item["scores"]
        )

        if not b:
            continue

        print(
            f"{name:<32} "
            f"| MED={fmt(b['median'])} "
            f"| 95%=["
            f"{fmt(b['p025'])}, "
            f"{fmt(b['p975'])}"
            f"] "
            f"| B={b['n']}"
        )


    # ========================================================
    # QUARTILES
    # ========================================================

    print()
    print("=" * 190)
    print("SCORE QUARTILES — DESCRIPTIVE ONLY")
    print("=" * 190)

    best_name = None
    best_rho = None

    for name, item in results.items():

        rho = item[
            "result"
        ][
            "full"
        ][
            "target_rho"
        ]

        if rho is None:
            continue

        if (
            best_rho is None
            or rho > best_rho
        ):

            best_rho = rho
            best_name = name

    if best_name:

        print(
            f"DISPLAY MODEL : {best_name}"
        )

        print()

        print(
            f"{'Q':<5}"
            f"{'N':>8}"
            f"{'+20 RATE':>14}"
            f"{'MAX MED':>12}"
            f"{'MIN MED':>12}"
            f"{'END MED':>12}"
        )

        for b in bucket_audit(
            results[
                best_name
            ][
                "scores"
            ]
        ):

            print(
                f"Q{b['quartile']:<4}"
                f"{b['n']:>8}"
                f"{100*b['hit_rate']:>13.1f}%"
                f"{fmt(b['max_med'],1):>12}"
                f"{fmt(b['min_med'],1):>12}"
                f"{fmt(b['end_med'],1):>12}"
            )


    # ========================================================
    # READINESS
    # ========================================================

    print()
    print("=" * 190)
    print("INTERPRETATION / READINESS")
    print("=" * 190)

    print(
        f"GOOD UNIQUE TOKENS : {len(rows)}"
    )

    print(
        f"+20 POSITIVES      : {positives}"
    )

    print(
        f"NEGATIVES          : {negatives}"
    )

    print()

    if best_name:

        best = results[
            best_name
        ]

        print(
            f"BEST FAMILY        : {best_name}"
        )

        print(
            f"TARGET RHO         : "
            f"{fmt(best['result']['full']['target_rho'])}"
        )

        print(
            f"LOTO MEDIAN        : "
            f"{fmt(best['result']['loto_median'])}"
        )

    print()

    if len(rows) >= 300:

        print(
            "🟢 SAMPLE SIZE TARGET REACHED"
        )

    else:

        print(
            f"🔵 COLLECTING — "
            f"{300-len(rows)} MORE GOOD TOKENS "
            f"TO REACH 300"
        )

    print()

    print(
        "IMPORTANT:"
    )

    print(
        "T119 does not define an entry threshold."
    )

    print(
        "T119 does not authorize live trading."
    )

    print(
        "A stronger family becomes a candidate for a "
        "future frozen prospective test."
    )

    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T119 only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        show()

        time.sleep(
            REFRESH
        )

except KeyboardInterrupt:

    print()
    print(
        "T119 stopped safely."
    )

finally:

    db.close()
