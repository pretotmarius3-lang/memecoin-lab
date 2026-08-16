import sqlite3
import math
import random
import statistics
from collections import defaultdict, Counter

from sklearn.metrics import roc_auc_score


# ============================================================
# CONFIG — FROZEN BEFORE T50
# ============================================================

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRIMARY = "recent_price_per_net_sol"

FEATURE_SIGNS = {
    "recent_price_per_net_sol": -1,   # LOWER => more RUN-like
    "recent_net_efficiency": +1,      # HIGHER => more RUN-like
    "early_flow_price_div": -1,       # LOWER => more RUN-like
}

BASE_EPS = 0.05

# Sensitivity audit only.
# These values do NOT select a new threshold.
EPS_GRID = [
    0.02,
    0.05,
    0.10,
    0.20,
]

BOOTSTRAPS = 5000
SEED = 50

PRE_EVENT_PROGRAM_WINDOW = 30.0


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [
        x for x in vals
        if valid(x)
    ]

    return (
        statistics.median(vals)
        if vals
        else None
    )


def avg(vals):
    vals = [
        x for x in vals
        if valid(x)
    ]

    return (
        statistics.mean(vals)
        if vals
        else None
    )


def percentile(vals, q):
    vals = sorted(
        x for x in vals
        if valid(x)
    )

    if not vals:
        return None

    idx = int(
        round(
            (q / 100.0)
            * (len(vals)-1)
        )
    )

    return vals[idx]


def sdiv(a, b, eps=BASE_EPS):

    if not valid(a) or not valid(b):
        return None

    if abs(b) < eps:
        return None

    return a / b


def label_r60(x):

    if not valid(x):
        return None

    if x >= RUNNER:
        return 1

    if x <= DUMP:
        return 0

    return None


def fmt(x, n=3):

    if x is None:
        return "NA"

    return f"{x:.{n}f}"


def auc_safe(y, score):

    pairs = [
        (yy, ss)
        for yy, ss
        in zip(y, score)
        if valid(ss)
    ]

    if len(pairs) < 4:
        return None

    yy = [
        p[0]
        for p in pairs
    ]

    ss = [
        p[1]
        for p in pairs
    ]

    if len(set(yy)) < 2:
        return None

    if len(set(ss)) < 2:
        return None

    try:
        return roc_auc_score(
            yy,
            ss
        )

    except Exception:
        return None


def directional_auc(rows, feature):

    sign = FEATURE_SIGNS[
        feature
    ]

    usable = [
        r for r in rows
        if valid(
            r[
                feature
            ]
        )
    ]

    y = [
        r[
            "label"
        ]
        for r in usable
    ]

    score = [
        sign
        * r[
            feature
        ]
        for r in usable
    ]

    return auc_safe(
        y,
        score
    )


def feature_diff(rows, feature):

    run = [
        r[
            feature
        ]
        for r in rows
        if (
            r[
                "label"
            ] == 1
            and valid(
                r[
                    feature
                ]
            )
        )
    ]

    dump = [
        r[
            feature
        ]
        for r in rows
        if (
            r[
                "label"
            ] == 0
            and valid(
                r[
                    feature
                ]
            )
        )
    ]

    if not run or not dump:
        return (
            None,
            len(run),
            len(dump)
        )

    return (
        med(run)
        - med(dump),
        len(run),
        len(dump)
    )


# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute(
    "PRAGMA busy_timeout=5000"
)


rows = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,
    e.dex_return_60s,

    s.recent_price_return,
    s.recent_net_sol,

    s.recent_buy_sol,
    s.recent_sell_sol,

    s.early_price_return,
    s.early_net_sol

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id = e.id

WHERE
    e.dex_return_60s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


# ============================================================
# DOMINANT PROGRAM CONTEXT
# ============================================================

def dominant_program(
    token,
    event_ts
):

    rr = db.execute("""
    SELECT
        program,
        COUNT(*) AS n
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND program IS NOT NULL
    GROUP BY program
    ORDER BY n DESC
    """, (
        token,
        event_ts
        - PRE_EVENT_PROGRAM_WINDOW,
        event_ts
    )).fetchall()

    if not rr:
        return "UNKNOWN"

    return rr[0][
        "program"
    ]


# ============================================================
# BUILD FROZEN FEATURES
# ============================================================

records = []


for r in rows:

    lab = label_r60(
        r[
            "dex_return_60s"
        ]
    )

    if lab is None:
        continue


    recent_price = r[
        "recent_price_return"
    ]

    recent_net = r[
        "recent_net_sol"
    ]


    recent_price_per_net_sol = sdiv(
        recent_price,
        recent_net,
        BASE_EPS
    )


    rb = r[
        "recent_buy_sol"
    ]

    rs = r[
        "recent_sell_sol"
    ]


    gross = (
        abs(rb)
        + abs(rs)
        if (
            valid(rb)
            and valid(rs)
        )
        else None
    )


    recent_net_efficiency = sdiv(
        recent_net,
        gross,
        BASE_EPS
    )


    early_flow_price_div = (
        r[
            "early_price_return"
        ]
        - r[
            "early_net_sol"
        ]
        if (
            valid(
                r[
                    "early_price_return"
                ]
            )
            and valid(
                r[
                    "early_net_sol"
                ]
            )
        )
        else None
    )


    records.append({
        "id":
            r[
                "id"
            ],

        "timestamp":
            r[
                "timestamp"
            ],

        "token_mint":
            r[
                "token_mint"
            ],

        "label":
            lab,

        "r60":
            r[
                "dex_return_60s"
            ],

        "recent_price_return":
            recent_price,

        "recent_net_sol":
            recent_net,

        "recent_buy_sol":
            rb,

        "recent_sell_sol":
            rs,

        "early_price_return":
            r[
                "early_price_return"
            ],

        "early_net_sol":
            r[
                "early_net_sol"
            ],

        "recent_price_per_net_sol":
            recent_price_per_net_sol,

        "recent_net_efficiency":
            recent_net_efficiency,

        "early_flow_price_div":
            early_flow_price_div,

        "program":
            dominant_program(
                r[
                    "token_mint"
                ],
                r[
                    "timestamp"
                ]
            ),
    })


records = sorted(
    records,
    key=lambda x:
        (
            x[
                "timestamp"
            ],
            x[
                "id"
            ]
        )
)


FEATURES = list(
    FEATURE_SIGNS.keys()
)


# ============================================================
# HEADER
# ============================================================

print(
    "=" * 185
)

print(
    "MEMECOIN LAB — T50 FROZEN CAPITAL-EFFICIENCY ROBUSTNESS AUDIT"
)

print(
    "=" * 185
)

print(
    f"LABELED EVENTS        : {len(records)}"
)

print(
    f"UNIQUE TOKENS         : "
    f"{len(set(r['token_mint'] for r in records))}"
)

print(
    f"PRIMARY               : {PRIMARY}"
)

print(
    "PRIMARY DIRECTION     : LOWER => RUN-like"
)

print(
    "BASE DENOMINATOR EPS  : 0.05 SOL"
)

print(
    "NO MODEL FITTING"
)

print(
    "NO SIGNAL THRESHOLD SEARCH"
)


# ============================================================
# A) GLOBAL FEATURE AUDIT
# ============================================================

print()
print(
    "=" * 185
)

print(
    "A) GLOBAL FROZEN FEATURES"
)

print(
    "=" * 185
)


for f in FEATURES:

    usable = [
        r for r in records
        if valid(
            r[
                f
            ]
        )
    ]


    run = [
        r[
            f
        ]
        for r in usable
        if r[
            "label"
        ] == 1
    ]


    dump = [
        r[
            f
        ]
        for r in usable
        if r[
            "label"
        ] == 0
    ]


    print(
        f"{f:32} "
        f"| N={len(usable):4d} "
        f"| TOK="
        f"{len(set(r['token_mint'] for r in usable)):3d} "
        f"| RUN_MED={fmt(med(run)):>8} "
        f"| DUMP_MED={fmt(med(dump)):>8} "
        f"| DIFF="
        f"{fmt((med(run)-med(dump)) if run and dump else None):>8} "
        f"| DIR_AUC="
        f"{fmt(directional_auc(usable,f)):>6}"
    )


# ============================================================
# B) CHRONOLOGICAL THIRDS / QUARTILES
# ============================================================

print()
print(
    "=" * 185
)

print(
    "B) CHRONOLOGICAL ROBUSTNESS"
)

print(
    "=" * 185
)


N = len(records)


thirds = [
    (
        "T1",
        records[
            :N//3
        ]
    ),
    (
        "T2",
        records[
            N//3:
            (2*N)//3
        ]
    ),
    (
        "T3",
        records[
            (2*N)//3:
        ]
    ),
]


qb = [
    0,
    N//4,
    N//2,
    (3*N)//4,
    N
]


quartiles = [
    (
        f"Q{i+1}",
        records[
            qb[i]:
            qb[i+1]
        ]
    )
    for i in range(4)
]


for f in FEATURES:

    print()
    print(
        f
    )

    print(
        "-" * 125
    )

    for name, rr in (
        thirds
        + quartiles
    ):

        diff, nr, nd = feature_diff(
            rr,
            f
        )

        auc = directional_auc(
            rr,
            f
        )

        print(
            f"{name:4} "
            f"| N={nr+nd:3d} "
            f"| RUN={nr:3d} "
            f"| DUMP={nd:3d} "
            f"| DIFF={fmt(diff):>9} "
            f"| DIR_AUC={fmt(auc):>6}"
        )


# ============================================================
# C) FIRST EVENT PER TOKEN
# ============================================================

print()
print(
    "=" * 185
)

print(
    "C) FIRST-EVENT/TOKEN"
)

print(
    "=" * 185
)


seen = set()
first = []


for r in records:

    tok = r[
        "token_mint"
    ]

    if tok in seen:
        continue

    seen.add(
        tok
    )

    first.append(
        r
    )


for f in FEATURES:

    diff, nr, nd = feature_diff(
        first,
        f
    )

    print(
        f"{f:32} "
        f"| N={nr+nd:3d} "
        f"| RUN={nr:3d} "
        f"| DUMP={nd:3d} "
        f"| DIFF={fmt(diff):>9} "
        f"| DIR_AUC="
        f"{fmt(directional_auc(first,f)):>6}"
    )


# ============================================================
# D) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS
# ============================================================

print()
print(
    "=" * 185
)

print(
    "D) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS"
)

print(
    "=" * 185
)


token_first_ts = {}


for r in records:

    token_first_ts.setdefault(
        r[
            "token_mint"
        ],
        r[
            "timestamp"
        ]
    )


ordered_tokens = sorted(
    token_first_ts,
    key=lambda t:
        token_first_ts[
            t
        ]
)


TB = [
    0,
    len(ordered_tokens)//4,
    len(ordered_tokens)//2,
    (3*len(ordered_tokens))//4,
    len(ordered_tokens)
]


token_blocks = []


for i in range(4):

    tokset = set(
        ordered_tokens[
            TB[i]:
            TB[i+1]
        ]
    )

    rr = [
        r for r in records
        if r[
            "token_mint"
        ] in tokset
    ]

    token_blocks.append(
        (
            f"TOK_Q{i+1}",
            rr
        )
    )


for f in FEATURES:

    print()
    print(
        f
    )

    print(
        "-" * 125
    )

    for name, rr in token_blocks:

        diff, nr, nd = feature_diff(
            rr,
            f
        )

        print(
            f"{name:8} "
            f"| N={nr+nd:3d} "
            f"| RUN={nr:3d} "
            f"| DUMP={nd:3d} "
            f"| DIFF={fmt(diff):>9} "
            f"| DIR_AUC="
            f"{fmt(directional_auc(rr,f)):>6}"
        )


# ============================================================
# E) TOKEN-LEVEL BOOTSTRAP — PRIMARY
# ============================================================

print()
print(
    "=" * 185
)

print(
    "E) TOKEN-LEVEL BOOTSTRAP — PRIMARY"
)

print(
    "=" * 185
)


primary_rows = [
    r for r in records
    if valid(
        r[
            PRIMARY
        ]
    )
]


by_token = defaultdict(
    list
)


for r in primary_rows:

    by_token[
        r[
            "token_mint"
        ]
    ].append(
        r
    )


tokens = list(
    by_token.keys()
)


rng = random.Random(
    SEED
)


boot = []


for _ in range(
    BOOTSTRAPS
):

    sampled = [
        rng.choice(
            tokens
        )
        for _ in range(
            len(tokens)
        )
    ]


    rr = []

    for tok in sampled:

        rr.extend(
            by_token[
                tok
            ]
        )


    auc = directional_auc(
        rr,
        PRIMARY
    )


    if auc is not None:

        boot.append(
            auc
        )


print(
    f"BOOT N={len(boot)}"
)


if boot:

    print(
        f"MED AUC       = "
        f"{med(boot):.3f}"
    )

    print(
        f"95% CI        = "
        f"[{percentile(boot,2.5):.3f}, "
        f"{percentile(boot,97.5):.3f}]"
    )

    print(
        f"P(AUC>0.50)   = "
        f"{100*sum(x>0.50 for x in boot)/len(boot):.1f}%"
    )

    print(
        f"P(AUC>0.55)   = "
        f"{100*sum(x>0.55 for x in boot)/len(boot):.1f}%"
    )

    print(
        f"P(AUC>0.60)   = "
        f"{100*sum(x>0.60 for x in boot)/len(boot):.1f}%"
    )


# ============================================================
# F) LEAVE-ONE-TOKEN-OUT — PRIMARY
# ============================================================

print()
print(
    "=" * 185
)

print(
    "F) LEAVE-ONE-TOKEN-OUT — PRIMARY"
)

print(
    "=" * 185
)


loo = []


for tok in tokens:

    rr = [
        r for r in primary_rows
        if r[
            "token_mint"
        ] != tok
    ]

    auc = directional_auc(
        rr,
        PRIMARY
    )

    if auc is not None:

        loo.append(
            (
                auc,
                tok,
                len(
                    by_token[
                        tok
                    ]
                )
            )
        )


loo.sort(
    key=lambda x:
        x[
            0
        ]
)


if loo:

    vals = [
        x[
            0
        ]
        for x in loo
    ]


    print(
        f"TOKENS={len(loo)}"
    )

    print(
        f"MED REMAINING AUC   = "
        f"{med(vals):.3f}"
    )

    print(
        f"WORST REMAINING AUC = "
        f"{min(vals):.3f}"
    )

    print(
        f"BEST REMAINING AUC  = "
        f"{max(vals):.3f}"
    )

    print()

    print(
        "5 REMOVALS THAT HURT MOST"
    )

    for auc, tok, nrows in loo[
        :5
    ]:

        print(
            f"{tok[:25]:25} "
            f"| N={nrows:2d} "
            f"| REMAINING AUC={auc:.3f}"
        )


    print()

    print(
        "5 REMOVALS THAT HELP MOST"
    )

    for auc, tok, nrows in loo[
        -5:
    ]:

        print(
            f"{tok[:25]:25} "
            f"| N={nrows:2d} "
            f"| REMAINING AUC={auc:.3f}"
        )


# ============================================================
# G) DENOMINATOR SENSITIVITY — PRIMARY
# ============================================================

print()
print(
    "=" * 185
)

print(
    "G) DENOMINATOR SENSITIVITY — PRIMARY"
)

print(
    "=" * 185
)

print(
    "This tests numerical robustness only."
)

print(
    "It does NOT choose a new denominator threshold."
)

print()


for eps in EPS_GRID:

    tmp = []

    for r in records:

        val = sdiv(
            r[
                "recent_price_return"
            ],
            r[
                "recent_net_sol"
            ],
            eps
        )

        if valid(
            val
        ):

            x = dict(
                r
            )

            x[
                "temp_primary"
            ] = val

            tmp.append(
                x
            )


    if not tmp:
        continue


    y = [
        r[
            "label"
        ]
        for r in tmp
    ]


    score = [
        -r[
            "temp_primary"
        ]
        for r in tmp
    ]


    auc = auc_safe(
        y,
        score
    )


    run = [
        r[
            "temp_primary"
        ]
        for r in tmp
        if r[
            "label"
        ] == 1
    ]


    dump = [
        r[
            "temp_primary"
        ]
        for r in tmp
        if r[
            "label"
        ] == 0
    ]


    print(
        f"EPS={eps:5.2f} "
        f"| N={len(tmp):3d} "
        f"| TOK="
        f"{len(set(r['token_mint'] for r in tmp)):3d} "
        f"| DIFF="
        f"{fmt(med(run)-med(dump) if run and dump else None):>9} "
        f"| DIR_AUC="
        f"{fmt(auc):>6}"
    )


# ============================================================
# H) WINSORIZATION / OUTLIER SENSITIVITY
# ============================================================

print()
print(
    "=" * 185
)

print(
    "H) OUTLIER SENSITIVITY — PRIMARY"
)

print(
    "=" * 185
)


vals = [
    r[
        PRIMARY
    ]
    for r in primary_rows
]


for tail in [
    0,
    1,
    2.5,
    5,
]:

    if tail == 0:

        rr = primary_rows

    else:

        lo = percentile(
            vals,
            tail
        )

        hi = percentile(
            vals,
            100-tail
        )


        rr = [
            r for r in primary_rows
            if (
                r[
                    PRIMARY
                ] >= lo
                and r[
                    PRIMARY
                ] <= hi
            )
        ]


    diff, nr, nd = feature_diff(
        rr,
        PRIMARY
    )


    print(
        f"TRIM EACH TAIL={tail:4.1f}% "
        f"| N={nr+nd:3d} "
        f"| DIFF={fmt(diff):>9} "
        f"| DIR_AUC="
        f"{fmt(directional_auc(rr,PRIMARY)):>6}"
    )


# ============================================================
# I) PROGRAM CONTEXT
# ============================================================

print()
print(
    "=" * 185
)

print(
    "I) DOMINANT PROGRAM CONTEXT"
)

print(
    "=" * 185
)


programs = sorted(
    set(
        r[
            "program"
        ]
        for r in records
    )
)


for f in FEATURES:

    print()
    print(
        f
    )

    print(
        "-" * 120
    )


    for program in programs:

        rr = [
            r for r in records
            if r[
                "program"
            ] == program
        ]


        diff, nr, nd = feature_diff(
            rr,
            f
        )


        if (
            nr + nd
        ) < 5:

            continue


        print(
            f"{program:12} "
            f"| N={nr+nd:3d} "
            f"| RUN={nr:3d} "
            f"| DUMP={nd:3d} "
            f"| DIFF={fmt(diff):>9} "
            f"| DIR_AUC="
            f"{fmt(directional_auc(rr,f)):>6}"
        )


# ============================================================
# J) PRIMARY ROBUSTNESS SCORECARD
# ============================================================

print()
print(
    "=" * 185
)

print(
    "J) PRIMARY ROBUSTNESS SCORECARD"
)

print(
    "=" * 185
)


audit_groups = (
    thirds
    + quartiles
    + [
        (
            "FIRST",
            first
        )
    ]
    + token_blocks
)


audits = []


for name, rr in audit_groups:

    diff, nr, nd = feature_diff(
        rr,
        PRIMARY
    )

    auc = directional_auc(
        rr,
        PRIMARY
    )


    if (
        nr + nd >= 8
        and nr >= 2
        and nd >= 2
        and auc is not None
    ):

        audits.append({
            "name":
                name,

            "diff":
                diff,

            "auc":
                auc,

            "n":
                nr+nd,
        })


correct_direction = [
    a for a in audits
    if (
        a[
            "diff"
        ] is not None
        and a[
            "diff"
        ] < 0
    )
]


auc55 = [
    a for a in audits
    if a[
        "auc"
    ] >= 0.55
]


auc60 = [
    a for a in audits
    if a[
        "auc"
    ] >= 0.60
]


print(
    f"USABLE AUDITS         = "
    f"{len(audits)}"
)

print(
    f"LOWER FOR RUN         = "
    f"{len(correct_direction)}/"
    f"{len(audits)}"
)

print(
    f"DIR-AUC >=0.55        = "
    f"{len(auc55)}/"
    f"{len(audits)}"
)

print(
    f"DIR-AUC >=0.60        = "
    f"{len(auc60)}/"
    f"{len(audits)}"
)


if audits:

    aa = [
        a[
            "auc"
        ]
        for a in audits
    ]

    print(
        f"MEDIAN AUDIT AUC      = "
        f"{med(aa):.3f}"
    )

    print(
        f"WORST AUDIT AUC       = "
        f"{min(aa):.3f}"
    )

    print(
        f"BEST AUDIT AUC        = "
        f"{max(aa):.3f}"
    )


# ============================================================
# K) SECONDARY SURVIVAL SUMMARY
# ============================================================

print()
print(
    "=" * 185
)

print(
    "K) SECONDARY FEATURE SURVIVAL"
)

print(
    "=" * 185
)


for f in [
    "recent_net_efficiency",
    "early_flow_price_div",
]:

    good_direction = 0
    usable_n = 0
    aucs = []


    expected = (
        1
        if FEATURE_SIGNS[
            f
        ] == 1
        else -1
    )


    for _, rr in audit_groups:

        diff, nr, nd = feature_diff(
            rr,
            f
        )

        auc = directional_auc(
            rr,
            f
        )


        if (
            nr + nd < 8
            or nr < 2
            or nd < 2
            or auc is None
        ):

            continue


        usable_n += 1

        if (
            diff is not None
            and (
                (
                    expected == 1
                    and diff > 0
                )
                or (
                    expected == -1
                    and diff < 0
                )
            )
        ):

            good_direction += 1


        aucs.append(
            auc
        )


    print(
        f"{f:30} "
        f"| DIR="
        f"{good_direction}/{usable_n} "
        f"| MED_AUC="
        f"{fmt(med(aucs)):>6} "
        f"| WORST="
        f"{fmt(min(aucs) if aucs else None):>6}"
    )


# ============================================================
# L) DECISION SUPPORT
# ============================================================

print()
print(
    "=" * 185
)

print(
    "L) DECISION SUPPORT"
)

print(
    "=" * 185
)


if audits:

    direction_rate = (
        len(
            correct_direction
        )
        / len(
            audits
        )
    )

    auc55_rate = (
        len(
            auc55
        )
        / len(
            audits
        )
    )

    audit_auc_med = med([
        a[
            "auc"
        ]
        for a in audits
    ])

else:

    direction_rate = 0
    auc55_rate = 0
    audit_auc_med = None


boot_gt50 = (
    sum(
        x > 0.50
        for x in boot
    )
    / len(
        boot
    )
    if boot
    else 0
)


if (
    audit_auc_med is not None
    and audit_auc_med >= 0.57
    and direction_rate >= 0.75
    and auc55_rate >= 0.60
    and boot_gt50 >= 0.80
):

    print(
        "🟢 RECENT PRICE / NET SOL SURVIVES ROBUSTNESS AUDIT."
    )

    print(
        "Candidate for prospective shadow validation."
    )

    print(
        "Do NOT integrate into execution yet."
    )


elif (
    audit_auc_med is not None
    and audit_auc_med >= 0.54
    and direction_rate >= 0.60
    and boot_gt50 >= 0.65
):

    print(
        "🟡 PRIMARY CAPITAL-EFFICIENCY METRIC SHOWS PARTIAL ROBUSTNESS."
    )

    print(
        "Keep collecting / audit before prospective promotion."
    )


else:

    print(
        "🔴 PRIMARY CAPITAL-EFFICIENCY METRIC FAILS ROBUSTNESS GATE."
    )

    print(
        "Do not integrate into the signal stack."
    )


print()
print("FROZEN FEATURES:")
print("• recent_price_per_net_sol : LOWER = RUN-like")
print("• recent_net_efficiency    : HIGHER = RUN-like")
print("• early_flow_price_div     : LOWER = RUN-like")

print()
print("IMPORTANT:")
print("• T50 fits no model.")
print("• No trading threshold optimization.")
print("• T49 feature definitions are frozen.")
print("• Denominator sensitivity is QA only.")
print("• Bootstrap resamples entire tokens.")
print("• Leave-one-token-out tests token dependence.")
print("• T23/T31/T32/T47 remain untouched.")
print("• T50 writes nothing to DB.")
print("• Research robustness audit only.")


db.close()
