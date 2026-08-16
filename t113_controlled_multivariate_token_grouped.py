#!/usr/bin/env python3

import sqlite3
import os
import math
import statistics

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

SOURCE = "t110c_strict_stage_forward"

STAGES = [30, 60]

OUTCOMES = [
    "max_return_300",
    "min_return_300",
    "end_return_300",
]

FAMILIES = {

    "STRUCTURE": [
        "market_cap_at_dump",
        "liquidity_at_dump",
    ],

    "ACTIVITY": [
        "volume_m5_at_dump",
        "pre60_swaps",
    ],

    "BUY_PRESSURE": [
        "pre60_buys",
        "pre60_buy_sol",
    ],

    "CONFIRMATION": [
        "post_stage_buys",
        "post_stage_buy_sol",
        "pre_stage_mae",
    ],
}

MODELS = {

    "STRUCTURE":
        FAMILIES["STRUCTURE"],

    "STRUCTURE+ACTIVITY":
        FAMILIES["STRUCTURE"]
        + FAMILIES["ACTIVITY"],

    "STRUCTURE+ACTIVITY+BUY":
        FAMILIES["STRUCTURE"]
        + FAMILIES["ACTIVITY"]
        + FAMILIES["BUY_PRESSURE"],

    "FULL_CONFIRMATION":
        FAMILIES["STRUCTURE"]
        + FAMILIES["ACTIVITY"]
        + FAMILIES["BUY_PRESSURE"]
        + FAMILIES["CONFIRMATION"],
}


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

    if not xs:
        return None

    return sum(xs) / len(xs)


def median(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    if not xs:
        return None

    return statistics.median(xs)


def std(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    if len(xs) < 2:
        return None

    m = mean(xs)

    return math.sqrt(
        sum(
            (x - m) ** 2
            for x in xs
        )
        / len(xs)
    )


def fmt(x, n=3):

    if x is None:
        return "NA"

    return f"{x:.{n}f}"


def rank(values):

    indexed = sorted(
        enumerate(values),
        key=lambda x: x[1]
    )

    ranks = [0.0] * len(values)

    i = 0

    while i < len(indexed):

        j = i + 1

        while (
            j < len(indexed)
            and indexed[j][1] == indexed[i][1]
        ):
            j += 1

        avg = (
            i + 1 + j
        ) / 2.0

        for k in range(i, j):
            ranks[
                indexed[k][0]
            ] = avg

        i = j

    return ranks


def pearson(x, y):

    pairs = [
        (a, b)
        for a, b in zip(x, y)
        if valid(a)
        and valid(b)
    ]

    if len(pairs) < 3:
        return None

    xs = [
        a for a, _ in pairs
    ]

    ys = [
        b for _, b in pairs
    ]

    mx = mean(xs)
    my = mean(ys)

    num = sum(
        (a - mx) * (b - my)
        for a, b in pairs
    )

    dx = math.sqrt(
        sum(
            (a - mx) ** 2
            for a in xs
        )
    )

    dy = math.sqrt(
        sum(
            (b - my) ** 2
            for b in ys
        )
    )

    if dx == 0 or dy == 0:
        return None

    return num / (
        dx * dy
    )


def spearman(x, y):

    pairs = [
        (a, b)
        for a, b in zip(x, y)
        if valid(a)
        and valid(b)
    ]

    if len(pairs) < 3:
        return None

    xs = [
        a for a, _ in pairs
    ]

    ys = [
        b for _, b in pairs
    ]

    return pearson(
        rank(xs),
        rank(ys)
    )


# ============================================================
# STANDARDIZATION
# ============================================================

def fit_scaler(rows, features):

    stats = {}

    for f in features:

        vals = [
            r[f]
            for r in rows
            if valid(
                r[f]
            )
        ]

        m = mean(vals)
        s = std(vals)

        stats[f] = (
            m,
            s
        )

    return stats


def z(value, stat):

    if not valid(value):
        return None

    m, s = stat

    if (
        m is None
        or s is None
        or s == 0
    ):
        return 0.0

    return (
        value - m
    ) / s


# ============================================================
# FAMILY SCORE
# ============================================================

def family_score(
    row,
    features,
    scaler
):

    vals = []

    for f in features:

        v = z(
            row[f],
            scaler[f]
        )

        if v is not None:
            vals.append(v)

    if not vals:
        return None

    return mean(vals)


# ============================================================
# TOKEN LEVEL AGGREGATION
# ============================================================

def token_aggregate(
    rows,
    model_features,
    outcome
):

    scaler = fit_scaler(
        rows,
        model_features
    )

    by_token = {}

    for r in rows:

        token = r[
            "token_mint"
        ]

        score = family_score(
            r,
            model_features,
            scaler
        )

        y = r[
            outcome
        ]

        if (
            score is None
            or not valid(y)
        ):
            continue

        by_token.setdefault(
            token,
            {
                "scores": [],
                "outcomes": [],
            }
        )

        by_token[token][
            "scores"
        ].append(score)

        by_token[token][
            "outcomes"
        ].append(y)


    token_scores = []
    token_outcomes = []

    for token, d in by_token.items():

        token_scores.append(
            mean(
                d["scores"]
            )
        )

        token_outcomes.append(
            mean(
                d["outcomes"]
            )
        )


    return (
        token_scores,
        token_outcomes
    )


# ============================================================
# LEAVE ONE TOKEN OUT
# ============================================================

def loto_model(
    rows,
    features,
    outcome
):

    tokens = sorted(
        {
            r["token_mint"]
            for r in rows
        }
    )


    # Full token-level relationship
    x_full, y_full = token_aggregate(
        rows,
        features,
        outcome
    )

    full_rho = spearman(
        x_full,
        y_full
    )


    loto = []


    for removed in tokens:

        reduced = [
            r
            for r in rows
            if r["token_mint"]
            != removed
        ]


        x, y = token_aggregate(
            reduced,
            features,
            outcome
        )

        rho = spearman(
            x,
            y
        )

        loto.append(
            (
                removed,
                rho
            )
        )


    vals = [
        rho
        for _, rho in loto
        if rho is not None
    ]


    same_sign = 0
    valid_n = 0


    if full_rho is not None:

        for _, rho in loto:

            if rho is None:
                continue

            valid_n += 1

            if (
                rho > 0
                and full_rho > 0
            ) or (
                rho < 0
                and full_rho < 0
            ):

                same_sign += 1


    most_influential = None
    worst_delta = None


    if full_rho is not None:

        influence = []

        for token, rho in loto:

            if rho is None:
                continue

            influence.append(
                (
                    abs(
                        rho
                        - full_rho
                    ),
                    token,
                    rho
                )
            )


        if influence:

            influence.sort(
                reverse=True
            )

            worst_delta = (
                influence[0][0]
            )

            most_influential = (
                influence[0][1]
            )


    return {
        "full":
            full_rho,

        "median":
            median(vals),

        "mean":
            mean(vals),

        "min":
            min(vals)
            if vals
            else None,

        "max":
            max(vals)
            if vals
            else None,

        "same_sign":
            same_sign,

        "valid":
            valid_n,

        "worst_delta":
            worst_delta,

        "most_influential":
            most_influential,
    }


# ============================================================
# DATA
# ============================================================

rows = db.execute(f"""
SELECT *
FROM {SOURCE}

WHERE
    mature_300=1

    AND actual_stage_delay_s <= 35

    AND max_gap_after_entry_300 <= 60

ORDER BY
    requested_stage_seconds,
    token_mint,
    trigger_timestamp
""").fetchall()


print()
print("=" * 190)
print(
    "MEMECOIN LAB — T113 CONTROLLED MULTIVARIATE / TOKEN-GROUPED AUDIT"
)
print("=" * 190)

print(
    f"STRICT ROWS       : {len(rows)}"
)

print(
    f"UNIQUE TOKENS     : "
    f"{len(set(r['token_mint'] for r in rows))}"
)

print()

print(
    "MODEL TYPE        : SIMPLE STANDARDIZED FAMILY SCORES"
)

print(
    "UNIT              : TOKEN-LEVEL AGGREGATION"
)

print(
    "ROBUSTNESS        : LEAVE-ONE-TOKEN-OUT"
)

print(
    "THRESHOLD SEARCH  : NONE"
)

print(
    "ENTRY RULE        : NONE"
)


# ============================================================
# STAGE LOOP
# ============================================================

for stage in STAGES:

    stage_rows = [
        r
        for r in rows
        if r[
            "requested_stage_seconds"
        ] == stage
    ]


    tokens = {
        r["token_mint"]
        for r in stage_rows
    }


    print()
    print("=" * 190)

    print(
        f"STAGE {stage}s "
        f"| ROWS={len(stage_rows)} "
        f"| TOKENS={len(tokens)}"
    )

    print("=" * 190)


    results = []


    for model_name, features in MODELS.items():

        outcome_results = {}

        for outcome in OUTCOMES:

            outcome_results[
                outcome
            ] = loto_model(
                stage_rows,
                features,
                outcome
            )


        mx = outcome_results[
            "max_return_300"
        ]

        en = outcome_results[
            "end_return_300"
        ]

        mn = outcome_results[
            "min_return_300"
        ]


        score_parts = []


        for item in (
            mx,
            en,
        ):

            if (
                item["valid"]
                and item["median"]
                is not None
            ):

                sign_rate = (
                    item["same_sign"]
                    / item["valid"]
                )

                score_parts.append(
                    sign_rate
                )

                score_parts.append(
                    min(
                        abs(
                            item["median"]
                        ),
                        1.0
                    )
                )


        robustness = (
            mean(
                score_parts
            )
            if score_parts
            else None
        )


        results.append({
            "model":
                model_name,

            "features":
                features,

            "outcomes":
                outcome_results,

            "score":
                robustness,
        })


    results.sort(
        key=lambda x:
            x["score"]
            if x["score"] is not None
            else -1,

        reverse=True
    )


    print()
    print(
        "HIERARCHICAL MODEL COMPARISON"
    )

    print("-" * 190)

    print(
        f"{'MODEL':<32}"
        f"{'MAX RHO':>10}"
        f"{'MAX LOTO':>11}"
        f"{'MAX SIGN':>11}"
        f"{'END RHO':>10}"
        f"{'END LOTO':>11}"
        f"{'END SIGN':>11}"
        f"{'MIN RHO':>10}"
        f"{'SCORE':>9}"
    )

    print("-" * 190)


    for r in results:

        mx = r[
            "outcomes"
        ][
            "max_return_300"
        ]

        en = r[
            "outcomes"
        ][
            "end_return_300"
        ]

        mn = r[
            "outcomes"
        ][
            "min_return_300"
        ]


        print(
            f"{r['model']:<32}"
            f"{fmt(mx['full']):>10}"
            f"{fmt(mx['median']):>11}"
            f"{mx['same_sign']}/{mx['valid']:>8}"
            f"{fmt(en['full']):>10}"
            f"{fmt(en['median']):>11}"
            f"{en['same_sign']}/{en['valid']:>8}"
            f"{fmt(mn['full']):>10}"
            f"{fmt(r['score']):>9}"
        )


    # ========================================================
    # INCREMENTAL VALUE
    # ========================================================

    print()
    print("=" * 190)
    print(
        "INCREMENTAL FAMILY VALUE"
    )
    print("=" * 190)


    ordered_models = [
        "STRUCTURE",
        "STRUCTURE+ACTIVITY",
        "STRUCTURE+ACTIVITY+BUY",
        "FULL_CONFIRMATION",
    ]


    previous = None


    for name in ordered_models:

        current = next(
            r
            for r in results
            if r[
                "model"
            ] == name
        )


        mx = current[
            "outcomes"
        ][
            "max_return_300"
        ]

        en = current[
            "outcomes"
        ][
            "end_return_300"
        ]


        print()
        print(
            name
        )

        print(
            "  FEATURES : "
            + ", ".join(
                current["features"]
            )
        )

        print(
            f"  MAX      : full={fmt(mx['full'])} "
            f"| LOTO median={fmt(mx['median'])} "
            f"| sign={mx['same_sign']}/{mx['valid']}"
        )

        print(
            f"  END      : full={fmt(en['full'])} "
            f"| LOTO median={fmt(en['median'])} "
            f"| sign={en['same_sign']}/{en['valid']}"
        )


        if previous:

            prev_mx = previous[
                "outcomes"
            ][
                "max_return_300"
            ]

            prev_en = previous[
                "outcomes"
            ][
                "end_return_300"
            ]


            delta_max = (
                mx["median"]
                - prev_mx["median"]

                if (
                    mx["median"] is not None
                    and prev_mx["median"] is not None
                )

                else None
            )


            delta_end = (
                en["median"]
                - prev_en["median"]

                if (
                    en["median"] is not None
                    and prev_en["median"] is not None
                )

                else None
            )


            print(
                f"  Δ vs previous "
                f"| MAX={fmt(delta_max)} "
                f"| END={fmt(delta_end)}"
            )


        previous = current


    # ========================================================
    # MOST INFLUENTIAL TOKENS
    # ========================================================

    print()
    print("=" * 190)

    print(
        "MOST INFLUENTIAL TOKENS"
    )

    print("=" * 190)


    for r in results:

        mx = r[
            "outcomes"
        ][
            "max_return_300"
        ]


        print(
            f"{r['model']:<32} "
            f"| token="
            f"{(mx['most_influential'] or 'NA')[:24]:24} "
            f"| Δrho="
            f"{fmt(mx['worst_delta'])}"
        )


# ============================================================
# CROSS-STAGE MODEL STABILITY
# ============================================================

print()
print("=" * 190)
print(
    "CROSS-STAGE MODEL STABILITY"
)
print("=" * 190)


for model_name, features in MODELS.items():

    values = {}


    for stage in STAGES:

        stage_rows = [
            r
            for r in rows
            if r[
                "requested_stage_seconds"
            ] == stage
        ]


        result = loto_model(
            stage_rows,
            features,
            "max_return_300"
        )


        values[
            stage
        ] = result


    r30 = values[30]
    r60 = values[60]


    same_sign = (
        r30["full"] is not None
        and r60["full"] is not None
        and (
            (
                r30["full"] > 0
                and r60["full"] > 0
            )
            or
            (
                r30["full"] < 0
                and r60["full"] < 0
            )
        )
    )


    print(
        f"{model_name:<32} "
        f"| 30 FULL={fmt(r30['full'])} "
        f"LOTO={fmt(r30['median'])} "
        f"| 60 FULL={fmt(r60['full'])} "
        f"LOTO={fmt(r60['median'])} "
        f"| SAME SIGN={same_sign}"
    )


print()
print("=" * 190)
print(
    "INTERPRETATION"
)
print("=" * 190)

print("""
T113 asks whether adding feature families improves token-level
information after accounting for simpler structural variables.

What we want:

STRUCTURE
    useful baseline

+ ACTIVITY
    improves robust LOTO relationship

+ BUY PRESSURE
    improves again

+ CONFIRMATION
    improves again or meaningfully reduces downside

What we do NOT want:

adding more variables improves the full sample but collapses LOTO.

With only ~11 tokens, even a strong result is still discovery evidence,
not strategy validation.

If one compact family survives:
next = T114 frozen candidate specification + prospective holdout.
""")

db.close()
