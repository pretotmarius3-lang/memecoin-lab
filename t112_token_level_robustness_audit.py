#!/usr/bin/env python3

import sqlite3
import os
import math
import statistics
from collections import defaultdict

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

SOURCE = "t110c_strict_stage_forward"

STAGES = [30, 60]

FEATURES = [
    "dump_level",

    "stage_return_from_trigger",

    "pre_stage_new_low",
    "pre_stage_reclaimed_trigger",

    "pre_stage_mfe",
    "pre_stage_mae",

    "pre60_swaps",
    "pre60_buys",
    "pre60_sells",

    "pre60_buy_sol",
    "pre60_sell_sol",
    "pre60_net_sol",
    "pre60_buy_share",

    "post_stage_swaps",
    "post_stage_buys",
    "post_stage_sells",

    "post_stage_buy_sol",
    "post_stage_sell_sol",
    "post_stage_net_sol",
    "post_stage_buy_share",

    "net_sol_rate_shift",
    "buy_share_shift",

    "liquidity_at_dump",
    "market_cap_at_dump",
    "volume_m5_at_dump",

    "liquidity_change_to_stage",
    "market_cap_change_to_stage",
    "volume_change_to_stage",
]

OUTCOMES = [
    "max_return_300",
    "min_return_300",
    "end_return_300",
]

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

        avg_rank = (
            i + 1 + j
        ) / 2.0

        for k in range(i, j):

            ranks[
                indexed[k][0]
            ] = avg_rank

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


def fmt(x, n=3):

    if x is None:
        return "NA"

    return f"{x:.{n}f}"


def sign(x):

    if x is None:
        return 0

    if x > 0:
        return 1

    if x < 0:
        return -1

    return 0


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
    "MEMECOIN LAB — T112 TOKEN-LEVEL / LEAVE-ONE-TOKEN-OUT ROBUSTNESS AUDIT"
)
print("=" * 190)

print(
    f"STRICT USABLE ROWS : {len(rows)}"
)

print(
    f"UNIQUE TOKENS      : "
    f"{len(set(r['token_mint'] for r in rows))}"
)

print()

print(
    "MODEL FITTING      : NONE"
)

print(
    "THRESHOLD SEARCH   : NONE"
)

print(
    "ROBUSTNESS UNIT    : TOKEN"
)

print(
    "TEST               : LEAVE-ONE-TOKEN-OUT"
)

print(
    "LEAKAGE POLICY     : ENTRY-TIME FEATURES ONLY"
)


# ============================================================
# STAGE AUDIT
# ============================================================

for stage in STAGES:

    stage_rows = [
        r
        for r in rows
        if r[
            "requested_stage_seconds"
        ] == stage
    ]


    tokens = sorted(
        {
            r["token_mint"]
            for r in stage_rows
        }
    )


    print()
    print("=" * 190)

    print(
        f"STAGE {stage}s "
        f"| ROWS={len(stage_rows)} "
        f"| TOKENS={len(tokens)}"
    )

    print("=" * 190)


    if len(tokens) < 4:

        print(
            "Insufficient tokens for meaningful LOTO."
        )

        continue


    feature_results = []


    for feature in FEATURES:

        full_x = [
            r[feature]
            for r in stage_rows
        ]

        full_outcomes = {
            o: [
                r[o]
                for r in stage_rows
            ]
            for o in OUTCOMES
        }


        full_rho = {
            o:
                spearman(
                    full_x,
                    full_outcomes[o]
                )

            for o in OUTCOMES
        }


        loto = {
            o: []
            for o in OUTCOMES
        }


        for removed_token in tokens:

            reduced = [
                r
                for r in stage_rows
                if r["token_mint"]
                != removed_token
            ]


            x = [
                r[feature]
                for r in reduced
            ]


            for outcome in OUTCOMES:

                y = [
                    r[outcome]
                    for r in reduced
                ]

                rho = spearman(
                    x,
                    y
                )

                loto[
                    outcome
                ].append(
                    (
                        removed_token,
                        rho
                    )
                )


        summary = {}


        for outcome in OUTCOMES:

            vals = [
                rho
                for _, rho
                in loto[outcome]
                if rho is not None
            ]


            full = full_rho[
                outcome
            ]


            same_sign = 0

            valid_count = 0


            if full is not None:

                for _, rho in loto[
                    outcome
                ]:

                    if rho is None:
                        continue

                    valid_count += 1

                    if sign(rho) == sign(full):
                        same_sign += 1


            min_rho = (
                min(vals)
                if vals
                else None
            )

            max_rho = (
                max(vals)
                if vals
                else None
            )

            med_rho = median(
                vals
            )

            mean_rho = mean(
                vals
            )


            worst_delta = None
            most_influential = None


            if (
                full is not None
                and vals
            ):

                influence = []

                for token, rho in loto[
                    outcome
                ]:

                    if rho is None:
                        continue

                    influence.append(
                        (
                            abs(
                                rho
                                - full
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


            summary[
                outcome
            ] = {
                "full":
                    full,

                "mean":
                    mean_rho,

                "median":
                    med_rho,

                "min":
                    min_rho,

                "max":
                    max_rho,

                "same_sign":
                    same_sign,

                "valid":
                    valid_count,

                "worst_delta":
                    worst_delta,

                "most_influential":
                    most_influential,
            }


        # ----------------------------------------------------
        # ROBUSTNESS SCORE
        # ----------------------------------------------------

        max_info = summary[
            "max_return_300"
        ]

        end_info = summary[
            "end_return_300"
        ]

        min_info = summary[
            "min_return_300"
        ]


        score_parts = []


        # upside stability
        if max_info["full"] is not None:

            sign_rate = (
                max_info["same_sign"]
                / max_info["valid"]

                if max_info["valid"]
                else 0
            )

            score_parts.append(
                sign_rate
            )

            score_parts.append(
                min(
                    abs(
                        max_info["median"]
                        or 0
                    ),
                    1.0
                )
            )


        # terminal return stability
        if end_info["full"] is not None:

            sign_rate = (
                end_info["same_sign"]
                / end_info["valid"]

                if end_info["valid"]
                else 0
            )

            score_parts.append(
                sign_rate
            )


        robustness_score = (
            mean(score_parts)
            if score_parts
            else None
        )


        feature_results.append({
            "feature":
                feature,

            "summary":
                summary,

            "score":
                robustness_score,
        })


    feature_results.sort(
        key=lambda x:
            x["score"]
            if x["score"] is not None
            else -1,

        reverse=True
    )


    print()
    print(
        "FEATURE ROBUSTNESS — LEAVE ONE TOKEN OUT"
    )

    print("-" * 190)

    print(
        f"{'FEATURE':<33}"
        f"{'FULL→MAX':>10}"
        f"{'MED LOTO':>10}"
        f"{'MIN':>9}"
        f"{'MAX':>9}"
        f"{'SIGN':>10}"
        f"{'ΔWORST':>10}"
        f"{'FULL→END':>11}"
        f"{'END SIGN':>10}"
        f"{'SCORE':>9}"
    )

    print("-" * 190)


    for item in feature_results:

        f = item[
            "feature"
        ]

        s = item[
            "summary"
        ]

        mx = s[
            "max_return_300"
        ]

        en = s[
            "end_return_300"
        ]


        sign_txt = (
            f"{mx['same_sign']}/"
            f"{mx['valid']}"
        )


        end_sign_txt = (
            f"{en['same_sign']}/"
            f"{en['valid']}"
        )


        print(
            f"{f:<33}"
            f"{fmt(mx['full']):>10}"
            f"{fmt(mx['median']):>10}"
            f"{fmt(mx['min']):>9}"
            f"{fmt(mx['max']):>9}"
            f"{sign_txt:>10}"
            f"{fmt(mx['worst_delta']):>10}"
            f"{fmt(en['full']):>11}"
            f"{end_sign_txt:>10}"
            f"{fmt(item['score']):>9}"
        )


    # ========================================================
    # MOST ROBUST CANDIDATES
    # ========================================================

    print()
    print("=" * 190)

    print(
        f"STAGE {stage}s — CANDIDATE FAMILIES"
    )

    print("=" * 190)


    for item in feature_results[:12]:

        s = item[
            "summary"
        ]

        mx = s[
            "max_return_300"
        ]

        mn = s[
            "min_return_300"
        ]

        en = s[
            "end_return_300"
        ]


        print()
        print(
            item["feature"]
        )

        print(
            f"  MAX : full={fmt(mx['full'])} "
            f"| loto median={fmt(mx['median'])} "
            f"| range=[{fmt(mx['min'])},{fmt(mx['max'])}] "
            f"| sign={mx['same_sign']}/{mx['valid']}"
        )

        print(
            f"  MIN : full={fmt(mn['full'])} "
            f"| loto median={fmt(mn['median'])} "
            f"| range=[{fmt(mn['min'])},{fmt(mn['max'])}] "
            f"| sign={mn['same_sign']}/{mn['valid']}"
        )

        print(
            f"  END : full={fmt(en['full'])} "
            f"| loto median={fmt(en['median'])} "
            f"| range=[{fmt(en['min'])},{fmt(en['max'])}] "
            f"| sign={en['same_sign']}/{en['valid']}"
        )

        if mx[
            "most_influential"
        ]:

            print(
                "  MOST INFLUENTIAL TOKEN : "
                f"{mx['most_influential'][:22]}"
            )


# ============================================================
# CROSS-STAGE STABILITY
# ============================================================

print()
print("=" * 190)
print(
    "CROSS-STAGE 30s ↔ 60s STABILITY"
)
print("=" * 190)


cross = {}


for stage in STAGES:

    stage_rows = [
        r
        for r in rows
        if r[
            "requested_stage_seconds"
        ] == stage
    ]


    for feature in FEATURES:

        x = [
            r[feature]
            for r in stage_rows
        ]

        y = [
            r["max_return_300"]
            for r in stage_rows
        ]

        cross.setdefault(
            feature,
            {}
        )

        cross[
            feature
        ][stage] = spearman(
            x,
            y
        )


stable = []


for feature, vals in cross.items():

    r30 = vals.get(30)
    r60 = vals.get(60)

    if (
        r30 is None
        or r60 is None
    ):
        continue


    same = (
        sign(r30)
        == sign(r60)
        and sign(r30) != 0
    )


    stable.append(
        (
            same,
            min(
                abs(r30),
                abs(r60)
            ),
            feature,
            r30,
            r60
        )
    )


stable.sort(
    key=lambda x:
        (
            x[0],
            x[1]
        ),
    reverse=True
)


print(
    f"{'FEATURE':<35}"
    f"{'RHO30→MAX':>14}"
    f"{'RHO60→MAX':>14}"
    f"{'SAME SIGN':>12}"
    f"{'MIN ABS':>10}"
)

print("-" * 100)


for (
    same,
    minabs,
    feature,
    r30,
    r60
) in stable:

    print(
        f"{feature:<35}"
        f"{fmt(r30):>14}"
        f"{fmt(r60):>14}"
        f"{str(same):>12}"
        f"{fmt(minabs):>10}"
    )


print()
print("=" * 190)
print(
    "DECISION SUPPORT"
)
print("=" * 190)

print("""
Interpretation rules:

🟢 Strong candidate:
- same correlation sign across nearly all leave-one-token-out runs
- median LOTO effect remains substantial
- no single token collapses the relationship
- preferably same direction at both 30s and 60s

🟡 Fragile candidate:
- strong full-sample effect
- but sign flips or collapses after removing one token

🔴 Reject:
- association mainly created by one token
- repeated sign instability
- inconsistent 30s vs 60s behavior

This audit still DOES NOT define an entry threshold.

If a small family survives T112:
next = T113 controlled multivariate / token-grouped audit,
not direct live trading.
""")

db.close()
