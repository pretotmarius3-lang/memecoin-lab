#!/usr/bin/env python3

import sqlite3
import os
import math
import statistics

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

SOURCE = "t110c_strict_stage_forward"

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# CONFIG
# ============================================================

STAGES = [30, 60]

FEATURES = [
    "dump_level",

    "stage_return_from_trigger",

    "pre_stage_new_low",
    "pre_stage_reclaimed_trigger",
    "pre_stage_reclaimed_old_peak",

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
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    return statistics.median(xs)


def rank(values):
    """
    Average-rank handling for ties.
    """

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
    token_mint,
    requested_stage_seconds,
    trigger_timestamp
""").fetchall()


print()
print("=" * 180)
print("MEMECOIN LAB — T111 STRICT CONFIRMATION INFORMATION AUDIT")
print("=" * 180)

print(
    f"STRICT USABLE ROWS : {len(rows)}"
)

print(
    f"UNIQUE TOKENS      : "
    f"{len(set(r['token_mint'] for r in rows))}"
)

print()

print(
    "MODE               : DESCRIPTIVE INFORMATION AUDIT"
)

print(
    "MODEL FITTING      : NONE"
)

print(
    "THRESHOLD SEARCH   : NONE"
)

print(
    "GROUP SPLIT        : NONE"
)

print(
    "LEAKAGE POLICY     : FEATURES KNOWN BY ACTUAL ENTRY ONLY"
)

print()


# ============================================================
# PER STAGE
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
    print("=" * 180)

    print(
        f"STAGE {stage}s "
        f"| ROWS={len(stage_rows)} "
        f"| TOKENS={len(tokens)}"
    )

    print("=" * 180)


    if len(stage_rows) < 3:

        print(
            "Insufficient rows."
        )

        continue


    # --------------------------------------------------------
    # OUTCOME DISTRIBUTION
    # --------------------------------------------------------

    print()
    print("OUTCOME DISTRIBUTION")
    print("-" * 180)


    for outcome in OUTCOMES:

        vals = [
            r[outcome]
            for r in stage_rows
            if valid(
                r[outcome]
            )
        ]

        print(
            f"{outcome:<24} "
            f"| N={len(vals):3d} "
            f"| MEAN={fmt(mean(vals),1):>8}% "
            f"| MEDIAN={fmt(median(vals),1):>8}% "
            f"| MIN={fmt(min(vals) if vals else None,1):>8}% "
            f"| MAX={fmt(max(vals) if vals else None,1):>8}%"
        )


    # --------------------------------------------------------
    # INFORMATION TABLE
    # --------------------------------------------------------

    print()
    print("FEATURE → STRICT FUTURE ASSOCIATION")
    print("-" * 180)

    print(
        f"{'FEATURE':<34}"
        f"{'N':>5}"
        f"{'RHO→MAX':>12}"
        f"{'RHO→MIN':>12}"
        f"{'RHO→END':>12}"
        f"{'P→MAX':>12}"
        f"{'P→MIN':>12}"
        f"{'P→END':>12}"
    )

    print("-" * 180)


    results = []


    for feature in FEATURES:

        feature_values = [
            r[feature]
            for r in stage_rows
        ]

        valid_n = sum(
            valid(x)
            for x in feature_values
        )


        rho_max = spearman(
            feature_values,
            [
                r["max_return_300"]
                for r in stage_rows
            ]
        )

        rho_min = spearman(
            feature_values,
            [
                r["min_return_300"]
                for r in stage_rows
            ]
        )

        rho_end = spearman(
            feature_values,
            [
                r["end_return_300"]
                for r in stage_rows
            ]
        )


        p_max = pearson(
            feature_values,
            [
                r["max_return_300"]
                for r in stage_rows
            ]
        )

        p_min = pearson(
            feature_values,
            [
                r["min_return_300"]
                for r in stage_rows
            ]
        )

        p_end = pearson(
            feature_values,
            [
                r["end_return_300"]
                for r in stage_rows
            ]
        )


        results.append({
            "feature":
                feature,

            "n":
                valid_n,

            "rho_max":
                rho_max,

            "rho_min":
                rho_min,

            "rho_end":
                rho_end,

            "p_max":
                p_max,

            "p_min":
                p_min,

            "p_end":
                p_end,
        })


    # Rank by strongest absolute Spearman relation
    results.sort(
        key=lambda r:
            max(
                abs(r["rho_max"] or 0),
                abs(r["rho_min"] or 0),
                abs(r["rho_end"] or 0),
            ),
        reverse=True
    )


    for r in results:

        print(
            f"{r['feature']:<34}"
            f"{r['n']:>5}"
            f"{fmt(r['rho_max']):>12}"
            f"{fmt(r['rho_min']):>12}"
            f"{fmt(r['rho_end']):>12}"
            f"{fmt(r['p_max']):>12}"
            f"{fmt(r['p_min']):>12}"
            f"{fmt(r['p_end']):>12}"
        )


    # --------------------------------------------------------
    # TOKEN DUPLICATION
    # --------------------------------------------------------

    print()
    print("TOKEN DEPENDENCE")
    print("-" * 180)


    by_token = {}


    for r in stage_rows:

        by_token.setdefault(
            r["token_mint"],
            0
        )

        by_token[
            r["token_mint"]
        ] += 1


    duplicates = sorted(
        by_token.items(),
        key=lambda x: x[1],
        reverse=True
    )


    for mint, n in duplicates[:15]:

        print(
            f"{mint[:22]:22} "
            f"| ROWS={n}"
        )


    # --------------------------------------------------------
    # QUICK ROBUSTNESS WARNING
    # --------------------------------------------------------

    max_rows_per_token = (
        max(
            by_token.values()
        )
        if by_token
        else 0
    )


    print()

    if len(tokens) < 20:

        print(
            "🔵 SAMPLE TOO SMALL FOR SIGNAL SELECTION."
        )

    if max_rows_per_token > 1:

        print(
            "🟡 ROWS ARE TOKEN-CLUSTERED. "
            "DO NOT INTERPRET ROW-LEVEL CORRELATIONS AS INDEPENDENT EVIDENCE."
        )

    print(
        "Next valid step = token-level robustness / leave-one-token-out,"
        " not threshold optimization."
    )


# ============================================================
# STAGE 30 vs 60 MATCHED TOKENS
# ============================================================

print()
print("=" * 180)
print("MATCHED STAGE30 vs STAGE60")
print("=" * 180)


by_key = {}

for r in rows:

    key = (
        r["t108_event_id"]
    )

    by_key.setdefault(
        key,
        {}
    )

    by_key[key][
        r["requested_stage_seconds"]
    ] = r


matched = [
    x
    for x in by_key.values()
    if (
        30 in x
        and 60 in x
    )
]


print(
    f"MATCHED DUMPS : {len(matched)}"
)


if matched:

    deltas = {
        "end":
            [],

        "max":
            [],

        "min":
            [],
    }


    for x in matched:

        r30 = x[30]
        r60 = x[60]

        if (
            valid(
                r30["end_return_300"]
            )
            and valid(
                r60["end_return_300"]
            )
        ):

            deltas["end"].append(
                r60["end_return_300"]
                - r30["end_return_300"]
            )


        if (
            valid(
                r30["max_return_300"]
            )
            and valid(
                r60["max_return_300"]
            )
        ):

            deltas["max"].append(
                r60["max_return_300"]
                - r30["max_return_300"]
            )


        if (
            valid(
                r30["min_return_300"]
            )
            and valid(
                r60["min_return_300"]
            )
        ):

            deltas["min"].append(
                r60["min_return_300"]
                - r30["min_return_300"]
            )


    print(
        "60s minus 30s remaining opportunity:"
    )

    print(
        f"END Δ   | "
        f"MEAN={fmt(mean(deltas['end']),1)}% "
        f"| MEDIAN={fmt(median(deltas['end']),1)}%"
    )

    print(
        f"MAX Δ   | "
        f"MEAN={fmt(mean(deltas['max']),1)}% "
        f"| MEDIAN={fmt(median(deltas['max']),1)}%"
    )

    print(
        f"MIN Δ   | "
        f"MEAN={fmt(mean(deltas['min']),1)}% "
        f"| MEDIAN={fmt(median(deltas['min']),1)}%"
    )


print()
print("=" * 180)
print("DECISION SUPPORT")
print("=" * 180)

print("""
T111 is descriptive only.

Do NOT turn the strongest correlation into an entry rule.

Current questions:

1. Which features repeatedly associate with upside across 30s and 60s?
2. Which features associate with reduced downside?
3. Does waiting from 30s to 60s materially reduce remaining upside?
4. Do apparent effects survive removing one token at a time?

Next step after enough tokens:
T112 = TOKEN-LEVEL / LEAVE-ONE-TOKEN-OUT ROBUSTNESS AUDIT.
""")

db.close()
