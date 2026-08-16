import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0
FAST_MIN_PRIOR_TRADES = 1

CAP_EPS = 0.05

SEED = 52

MIN_TRAIN_N = 12
MIN_VALID_N = 5
MIN_TEST_N = 5

TOP_RULES = 20


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def pctile(xs, q):
    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    pos = (len(xs) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return (
        xs[lo] * (1-w)
        + xs[hi] * w
    )


def safe_div(a, b):
    if b is None or b == 0:
        return None
    return a / b


def cap_div(a, b):
    if not valid(a) or not valid(b):
        return None

    if abs(b) < CAP_EPS:
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


def fmt(x, n=2):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# LOAD EVENTS + FLOW FEATURES
# ============================================================

events = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,

    e.fa,
    e.new_wallets30,
    e.buyer_growth,
    e.wallet_growth,
    e.buy_concentration30,

    e.dex_return_60s,

    s.recent_price_return,
    s.recent_net_sol,
    s.recent_buy_sol,
    s.recent_sell_sol,

    s.early_price_return,
    s.early_net_sol,

    s.recent_buy_share,
    s.recent_net_share,

    s.buyer_diversity_trend,
    s.buy_concentration_trend,
    s.frequency_trend,

    s.late_chase_score,
    s.breadth_score

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id = e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL
    AND e.dex_return_60s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


events = [
    e
    for e in events
    if label_r60(
        e["dex_return_60s"]
    ) is not None
]


# ============================================================
# LOAD SWAPS FOR HISTORICAL FAST-FLIP
# ============================================================

swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    clean_price
FROM swaps
WHERE
    timestamp IS NOT NULL
    AND wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND side IN ('BUY','SELL')
    AND clean_price IS NOT NULL
    AND clean_price > 0
    AND (
        price_valid IS NULL
        OR price_valid=1
    )
ORDER BY timestamp
""").fetchall()


completed = defaultdict(int)
fast_flips = defaultdict(int)
open_pos = {}

swap_idx = 0


def process_swap(s):

    wallet = s["wallet"]
    token = s["token_mint"]
    side = s["side"]
    ts = s["timestamp"]

    key = (
        wallet,
        token
    )

    if side == "BUY":

        if key not in open_pos:

            open_pos[key] = ts

    elif side == "SELL":

        if key not in open_pos:
            return

        entry_ts = open_pos[key]

        hold = (
            ts
            - entry_ts
        )

        if hold >= 0:

            completed[
                wallet
            ] += 1

            if hold <= 60:

                fast_flips[
                    wallet
                ] += 1

        open_pos.pop(
            key,
            None
        )


# ============================================================
# REGIME LOOKUP
# ============================================================

regime_cols = [
    r["name"]
    for r in db.execute(
        "PRAGMA table_info(frozen_regime_v620)"
    ).fetchall()
]


regime_event_col = next(
    (
        c for c in [
            "event_id",
            "id"
        ]
        if c in regime_cols
    ),
    None
)


regime_col = next(
    (
        c for c in [
            "regime",
            "regime_id",
            "cluster",
            "assigned_regime"
        ]
        if c in regime_cols
    ),
    None
)


def get_regime(event_id):

    if not regime_event_col or not regime_col:
        return None

    try:

        r = db.execute(
            f"""
            SELECT {regime_col}
            FROM frozen_regime_v620
            WHERE {regime_event_col}=?
            LIMIT 1
            """,
            (
                event_id,
            )
        ).fetchone()

        return (
            r[0]
            if r
            else None
        )

    except Exception:
        return None


# ============================================================
# BUILD HISTORICAL EVENT FEATURES
# ============================================================

records = []


for e in events:

    event_ts = e["timestamp"]

    # Strictly historical wallet state
    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"]
        < event_ts
    ):

        process_swap(
            swaps[swap_idx]
        )

        swap_idx += 1


    # --------------------------------------------------------
    # BUYERS IN PRE-EVENT WINDOW
    # --------------------------------------------------------

    pre = db.execute("""
    SELECT DISTINCT wallet
    FROM swaps
    WHERE
        token_mint=?
        AND side='BUY'
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
    """, (
        e["token_mint"],
        event_ts - PRE_EVENT_SEC,
        event_ts
    )).fetchall()


    buyers = [
        r["wallet"]
        for r in pre
    ]


    exp_fast = []


    for w in buyers:

        n = completed[
            w
        ]

        if n < FAST_MIN_PRIOR_TRADES:
            continue

        ff = safe_div(
            fast_flips[
                w
            ],
            n
        )

        if valid(ff):
            exp_fast.append(
                ff
            )


    buyer_fast_mean = avg(
        exp_fast
    )


    exp_buyer_count = len(
        exp_fast
    )


    fast_coverage = safe_div(
        exp_buyer_count,
        len(
            buyers
        )
    )


    # --------------------------------------------------------
    # CAPITAL-EFFICIENCY FAMILY — FROZEN DEFINITIONS
    # --------------------------------------------------------

    recent_price = e[
        "recent_price_return"
    ]

    recent_net = e[
        "recent_net_sol"
    ]


    price_per_net = cap_div(
        recent_price,
        recent_net
    )


    rb = e[
        "recent_buy_sol"
    ]

    rs = e[
        "recent_sell_sol"
    ]


    gross = (
        abs(rb)
        + abs(rs)
        if valid(rb)
        and valid(rs)
        else None
    )


    net_eff = cap_div(
        recent_net,
        gross
    )


    early_div = (
        e[
            "early_price_return"
        ]
        - e[
            "early_net_sol"
        ]
        if (
            valid(
                e[
                    "early_price_return"
                ]
            )
            and valid(
                e[
                    "early_net_sol"
                ]
            )
        )
        else None
    )


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "r60":
            e["dex_return_60s"],

        "label":
            label_r60(
                e["dex_return_60s"]
            ),

        # T46
        "fast_flip":
            buyer_fast_mean,

        "exp_buyer_count":
            exp_buyer_count,

        "fast_coverage":
            fast_coverage,

        # T49/T50
        "price_per_net":
            price_per_net,

        "net_eff":
            net_eff,

        "early_div":
            early_div,

        # Flow / context
        "fa":
            e["fa"],

        "new_wallets30":
            e["new_wallets30"],

        "buyer_growth":
            e["buyer_growth"],

        "wallet_growth":
            e["wallet_growth"],

        "buy_concentration30":
            e["buy_concentration30"],

        "recent_buy_share":
            e["recent_buy_share"],

        "recent_net_share":
            e["recent_net_share"],

        "buyer_diversity_trend":
            e["buyer_diversity_trend"],

        "buy_concentration_trend":
            e["buy_concentration_trend"],

        "frequency_trend":
            e["frequency_trend"],

        "late_chase_score":
            e["late_chase_score"],

        "breadth_score":
            e["breadth_score"],

        "regime":
            get_regime(
                e["id"]
            ),
    })


# ============================================================
# TOKEN HOLDOUT SPLIT
# ============================================================

tokens = sorted(
    set(
        r[
            "token_mint"
        ]
        for r in records
    )
)


rng = random.Random(
    SEED
)

rng.shuffle(
    tokens
)


n = len(tokens)

n_train = int(
    0.60*n
)

n_valid = int(
    0.20*n
)


train_tokens = set(
    tokens[
        :n_train
    ]
)


valid_tokens = set(
    tokens[
        n_train:
        n_train+n_valid
    ]
)


test_tokens = set(
    tokens[
        n_train+n_valid:
    ]
)


def subset(tokset):

    return [
        r
        for r in records
        if r[
            "token_mint"
        ] in tokset
    ]


train = subset(
    train_tokens
)

valid_set = subset(
    valid_tokens
)

test = subset(
    test_tokens
)


# ============================================================
# BASELINE STATS
# ============================================================

def stats(rr):

    if not rr:

        return {
            "n": 0,
            "tokens": 0,
            "run_rate": None,
            "dump_rate": None,
            "median_r60": None,
            "avg_r60": None,
            "p25": None,
            "p75": None,
        }


    r60 = [
        r[
            "r60"
        ]
        for r in rr
        if valid(
            r[
                "r60"
            ]
        )
    ]


    runs = sum(
        r[
            "label"
        ] == 1
        for r in rr
    )


    dumps = sum(
        r[
            "label"
        ] == 0
        for r in rr
    )


    return {
        "n":
            len(
                rr
            ),

        "tokens":
            len(
                set(
                    r[
                        "token_mint"
                    ]
                    for r in rr
                )
            ),

        "run_rate":
            safe_div(
                runs,
                len(
                    rr
                )
            ),

        "dump_rate":
            safe_div(
                dumps,
                len(
                    rr
                )
            ),

        "median_r60":
            med(
                r60
            ),

        "avg_r60":
            avg(
                r60
            ),

        "p25":
            pctile(
                r60,
                0.25
            ),

        "p75":
            pctile(
                r60,
                0.75
            ),
    }


def print_stats(
    name,
    s,
    baseline=None
):

    uplift = None

    if (
        baseline
        and s[
            "run_rate"
        ] is not None
        and baseline[
            "run_rate"
        ] is not None
    ):

        uplift = (
            s[
                "run_rate"
            ]
            - baseline[
                "run_rate"
            ]
        )


    print(
        f"{name:46} "
        f"N={s['n']:3d} "
        f"TOK={s['tokens']:3d} "
        f"RUN={fmt(100*s['run_rate'] if s['run_rate'] is not None else None,1):>6}% "
        f"DUMP={fmt(100*s['dump_rate'] if s['dump_rate'] is not None else None,1):>6}% "
        f"MED={fmt(s['median_r60']):>8}% "
        f"AVG={fmt(s['avg_r60']):>8}% "
        f"UPLIFT={fmt(100*uplift if uplift is not None else None,1):>6}pp"
    )


# ============================================================
# TRAIN-ONLY THRESHOLDS
# ============================================================

CONTINUOUS = [
    "fast_flip",
    "price_per_net",
    "net_eff",
    "early_div",

    "fa",
    "new_wallets30",

    "buyer_growth",
    "wallet_growth",

    "buy_concentration30",
    "recent_buy_share",
    "recent_net_share",

    "buyer_diversity_trend",
    "buy_concentration_trend",
    "frequency_trend",

    "late_chase_score",
    "breadth_score",
]


thresholds = {}


for f in CONTINUOUS:

    vals = [
        r[
            f
        ]
        for r in train
        if valid(
            r[
                f
            ]
        )
    ]


    if len(vals) < 10:
        continue


    thresholds[
        f
    ] = {
        "q33":
            pctile(
                vals,
                0.33
            ),

        "q50":
            pctile(
                vals,
                0.50
            ),

        "q67":
            pctile(
                vals,
                0.67
            ),
    }


# ============================================================
# RULE GENERATION
#
# Only TRAIN thresholds.
# ============================================================

conditions = []


for f, th in thresholds.items():

    conditions.extend([
        {
            "name":
                f"{f} LOW33",

            "feature":
                f,

            "op":
                "<=",

            "value":
                th[
                    "q33"
                ]
        },

        {
            "name":
                f"{f} LOW50",

            "feature":
                f,

            "op":
                "<=",

            "value":
                th[
                    "q50"
                ]
        },

        {
            "name":
                f"{f} HIGH50",

            "feature":
                f,

            "op":
                ">=",

            "value":
                th[
                    "q50"
                ]
        },

        {
            "name":
                f"{f} HIGH67",

            "feature":
                f,

            "op":
                ">=",

            "value":
                th[
                    "q67"
                ]
        },
    ])


# Frozen conceptual directions get explicit conditions too.
# Threshold still TRAIN-only.

if "fast_flip" in thresholds:

    conditions.append({
        "name":
            "FAST-FLIP FAVORABLE",

        "feature":
            "fast_flip",

        "op":
            "<=",

        "value":
            thresholds[
                "fast_flip"
            ][
                "q50"
            ]
    })


if "price_per_net" in thresholds:

    conditions.append({
        "name":
            "CAP-EFF FAVORABLE",

        "feature":
            "price_per_net",

        "op":
            "<=",

        "value":
            thresholds[
                "price_per_net"
            ][
                "q50"
            ]
    })


# Regimes are categorical.
regimes_train = sorted(
    set(
        r[
            "regime"
        ]
        for r in train
        if r[
            "regime"
        ] is not None
    )
)


for regime in regimes_train:

    conditions.append({
        "name":
            f"REGIME R{regime}",

        "feature":
            "regime",

        "op":
            "==",

        "value":
            regime
    })


# ============================================================
# CONDITION EXECUTION
# ============================================================

def condition_pass(
    r,
    cond
):

    x = r.get(
        cond[
            "feature"
        ]
    )

    if x is None:
        return False


    op = cond[
        "op"
    ]

    v = cond[
        "value"
    ]


    if op == "<=":
        return valid(x) and x <= v

    if op == ">=":
        return valid(x) and x >= v

    if op == "==":
        return x == v

    return False


def filter_rule(
    rr,
    conds
):

    return [
        r
        for r in rr
        if all(
            condition_pass(
                r,
                c
            )
            for c in conds
        )
    ]


# ============================================================
# BASELINES
# ============================================================

base_train = stats(
    train
)

base_valid = stats(
    valid_set
)

base_test = stats(
    test
)


# ============================================================
# DISCOVERY — SINGLE + PAIR INTERACTIONS
# ============================================================

candidates = []


def candidate_score(s):

    if (
        s[
            "n"
        ] < MIN_TRAIN_N
        or s[
            "run_rate"
        ] is None
        or s[
            "dump_rate"
        ] is None
    ):

        return None


    run_uplift = (
        s[
            "run_rate"
        ]
        - base_train[
            "run_rate"
        ]
    )


    dump_improve = (
        base_train[
            "dump_rate"
        ]
        - s[
            "dump_rate"
        ]
    )


    # Reward practical improvement + sample size.
    return (
        1.5 * run_uplift
        + dump_improve
        + min(
            s[
                "n"
            ],
            40
        ) / 400.0
    )


# Single rules
for c in conditions:

    rr = filter_rule(
        train,
        [
            c
        ]
    )

    s = stats(
        rr
    )

    score = candidate_score(
        s
    )

    if score is not None:

        candidates.append({
            "conditions":
                [
                    c
                ],

            "name":
                c[
                    "name"
                ],

            "train_stats":
                s,

            "score":
                score,
        })


# Pair interactions
for i in range(
    len(
        conditions
    )
):

    for j in range(
        i+1,
        len(
            conditions
        )
    ):

        a = conditions[
            i
        ]

        b = conditions[
            j
        ]


        # Don't pair contradictory cuts
        # on the same feature.
        if (
            a[
                "feature"
            ]
            == b[
                "feature"
            ]
        ):
            continue


        rr = filter_rule(
            train,
            [
                a,
                b
            ]
        )


        s = stats(
            rr
        )

        score = candidate_score(
            s
        )


        if score is None:
            continue


        candidates.append({
            "conditions":
                [
                    a,
                    b
                ],

            "name":
                a[
                    "name"
                ]
                + " + "
                + b[
                    "name"
                ],

            "train_stats":
                s,

            "score":
                score,
        })


candidates.sort(
    key=lambda x:
        x[
            "score"
        ],
    reverse=True
)


selected = candidates[
    :TOP_RULES
]


# ============================================================
# OUTPUT
# ============================================================

print(
    "=" * 190
)

print(
    "MEMECOIN LAB — T52 EDGE INTERACTION DISCOVERY LAB"
)

print(
    "=" * 190
)


print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : {len(tokens)}"
)

print()

print(
    f"TRAIN : {len(train)} events | "
    f"{len(train_tokens)} tokens"
)

print(
    f"VALID : {len(valid_set)} events | "
    f"{len(valid_tokens)} tokens"
)

print(
    f"TEST  : {len(test)} events | "
    f"{len(test_tokens)} tokens"
)


# ============================================================
# A) BASELINES
# ============================================================

print()
print(
    "=" * 190
)

print(
    "A) BASELINE ECONOMIC OUTCOMES"
)

print(
    "=" * 190
)


print_stats(
    "TRAIN BASELINE",
    base_train
)

print_stats(
    "VALID BASELINE",
    base_valid
)

print_stats(
    "TEST BASELINE",
    base_test
)


# ============================================================
# B) TRAIN-ONLY THRESHOLDS
# ============================================================

print()
print(
    "=" * 190
)

print(
    "B) TRAIN-ONLY THRESHOLDS"
)

print(
    "=" * 190
)


for f, th in thresholds.items():

    print(
        f"{f:32} "
        f"Q33={fmt(th['q33']):>10} "
        f"MED={fmt(th['q50']):>10} "
        f"Q67={fmt(th['q67']):>10}"
    )


# ============================================================
# C) TOP TRAIN DISCOVERIES
# ============================================================

print()
print(
    "=" * 190
)

print(
    "C) TOP TRAIN INTERACTION DISCOVERIES"
)

print(
    "=" * 190
)


for i, c in enumerate(
    selected,
    start=1
):

    print_stats(
        f"{i:02d}. {c['name']}",
        c[
            "train_stats"
        ],
        base_train
    )


# ============================================================
# D) VALIDATION OF FROZEN TRAIN RULES
# ============================================================

print()
print(
    "=" * 190
)

print(
    "D) VALIDATION — SAME FROZEN RULES"
)

print(
    "=" * 190
)


validated = []


for i, c in enumerate(
    selected,
    start=1
):

    rr = filter_rule(
        valid_set,
        c[
            "conditions"
        ]
    )


    s = stats(
        rr
    )


    train_s = c[
        "train_stats"
    ]


    train_uplift = (
        train_s[
            "run_rate"
        ]
        - base_train[
            "run_rate"
        ]
    )


    valid_uplift = (
        s[
            "run_rate"
        ]
        - base_valid[
            "run_rate"
        ]
        if s[
            "run_rate"
        ] is not None
        else None
    )


    same_direction = (
        s[
            "n"
        ] >= MIN_VALID_N
        and valid_uplift is not None
        and train_uplift > 0
        and valid_uplift > 0
    )


    print_stats(
        f"{i:02d}. {c['name']}",
        s,
        base_valid
    )


    if same_direction:

        validated.append({
            **c,

            "valid_stats":
                s,

            "valid_uplift":
                valid_uplift,
        })


# ============================================================
# E) FINAL TEST AUDIT
#
# Only rules surviving VALID reach this section.
# ============================================================

print()
print(
    "=" * 190
)

print(
    "E) FINAL TEST — VALIDATION SURVIVORS ONLY"
)

print(
    "=" * 190
)


final_survivors = []


if not validated:

    print(
        "No TRAIN rule survived the VALID gate."
    )

else:

    for i, c in enumerate(
        validated,
        start=1
    ):

        rr = filter_rule(
            test,
            c[
                "conditions"
            ]
        )


        s = stats(
            rr
        )


        test_uplift = (
            s[
                "run_rate"
            ]
            - base_test[
                "run_rate"
            ]
            if s[
                "run_rate"
            ] is not None
            else None
        )


        print_stats(
            f"{i:02d}. {c['name']}",
            s,
            base_test
        )


        if (
            s[
                "n"
            ] >= MIN_TEST_N
            and test_uplift is not None
            and test_uplift > 0
        ):

            final_survivors.append({
                **c,

                "test_stats":
                    s,

                "test_uplift":
                    test_uplift,
            })


# ============================================================
# F) FIRST-EVENT/TOKEN AUDIT
# ============================================================

print()
print(
    "=" * 190
)

print(
    "F) FIRST-EVENT/TOKEN — FINAL SURVIVORS"
)

print(
    "=" * 190
)


def first_per_token(rr):

    seen = set()
    out = []


    for r in sorted(
        rr,
        key=lambda x:
            (
                x[
                    "timestamp"
                ],
                x[
                    "id"
                ]
            )
    ):

        tok = r[
            "token_mint"
        ]

        if tok in seen:
            continue

        seen.add(
            tok
        )

        out.append(
            r
        )


    return out


train_first = first_per_token(
    train
)

valid_first = first_per_token(
    valid_set
)

test_first = first_per_token(
    test
)


if not final_survivors:

    print(
        "No final survivor available."
    )

else:

    for i, c in enumerate(
        final_survivors,
        start=1
    ):

        print()
        print(
            f"{i:02d}. {c['name']}"
        )

        print(
            "-" * 160
        )


        for name, rr, baseline in [
            (
                "TRAIN FIRST",
                train_first,
                stats(
                    train_first
                )
            ),
            (
                "VALID FIRST",
                valid_first,
                stats(
                    valid_first
                )
            ),
            (
                "TEST FIRST",
                test_first,
                stats(
                    test_first
                )
            ),
        ]:

            filtered = filter_rule(
                rr,
                c[
                    "conditions"
                ]
            )


            print_stats(
                name,
                stats(
                    filtered
                ),
                baseline
            )


# ============================================================
# G) CHRONOLOGICAL AUDIT — FINAL SURVIVORS
# ============================================================

print()
print(
    "=" * 190
)

print(
    "G) CHRONOLOGICAL STABILITY — FINAL SURVIVORS"
)

print(
    "=" * 190
)


ordered = sorted(
    records,
    key=lambda r:
        (
            r[
                "timestamp"
            ],
            r[
                "id"
            ]
        )
)


N = len(
    ordered
)


blocks = [
    (
        "TIME Q1",
        ordered[
            :N//4
        ]
    ),
    (
        "TIME Q2",
        ordered[
            N//4:
            N//2
        ]
    ),
    (
        "TIME Q3",
        ordered[
            N//2:
            (3*N)//4
        ]
    ),
    (
        "TIME Q4",
        ordered[
            (3*N)//4:
        ]
    ),
]


if not final_survivors:

    print(
        "No final survivor available."
    )

else:

    for i, c in enumerate(
        final_survivors,
        start=1
    ):

        print()
        print(
            f"{i:02d}. {c['name']}"
        )

        print(
            "-" * 160
        )


        for name, rr in blocks:

            base = stats(
                rr
            )

            filtered = filter_rule(
                rr,
                c[
                    "conditions"
                ]
            )


            print_stats(
                name,
                stats(
                    filtered
                ),
                base
            )


# ============================================================
# H) CONCRETE EDGE SCORECARD
# ============================================================

print()
print(
    "=" * 190
)

print(
    "H) CONCRETE EDGE SCORECARD"
)

print(
    "=" * 190
)


if not final_survivors:

    print(
        "NO INTERACTION SURVIVED TRAIN -> VALID -> TEST."
    )

    print(
        "Do not force an interaction model."
    )

else:

    for i, c in enumerate(
        final_survivors,
        start=1
    ):

        ts = c[
            "train_stats"
        ]

        vs = c[
            "valid_stats"
        ]

        xs = c[
            "test_stats"
        ]


        print()
        print(
            f"{i:02d}. {c['name']}"
        )

        print(
            f"TRAIN | N={ts['n']} "
            f"| RUN={100*ts['run_rate']:.1f}% "
            f"| DUMP={100*ts['dump_rate']:.1f}% "
            f"| MED R60={fmt(ts['median_r60'])}%"
        )

        print(
            f"VALID | N={vs['n']} "
            f"| RUN={100*vs['run_rate']:.1f}% "
            f"| DUMP={100*vs['dump_rate']:.1f}% "
            f"| MED R60={fmt(vs['median_r60'])}%"
        )

        print(
            f"TEST  | N={xs['n']} "
            f"| RUN={100*xs['run_rate']:.1f}% "
            f"| DUMP={100*xs['dump_rate']:.1f}% "
            f"| MED R60={fmt(xs['median_r60'])}%"
        )


# ============================================================
# I) DECISION SUPPORT
# ============================================================

print()
print(
    "=" * 190
)

print(
    "I) DECISION SUPPORT"
)

print(
    "=" * 190
)


print(
    f"TRAIN CANDIDATES TESTED = "
    f"{len(candidates)}"
)

print(
    f"TOP TRAIN RULES AUDITED  = "
    f"{len(selected)}"
)

print(
    f"VALID SURVIVORS          = "
    f"{len(validated)}"
)

print(
    f"FINAL TEST SURVIVORS     = "
    f"{len(final_survivors)}"
)


if len(
    final_survivors
) >= 1:

    print()
    print(
        "🟢 AT LEAST ONE CONCRETE INTERACTION "
        "SURVIVED TRAIN -> VALID -> TEST."
    )

    print(
        "Do NOT trade it yet."
    )

    print(
        "Next step = dedicated frozen robustness audit "
        "of only these exact rules."
    )

else:

    print()
    print(
        "🔴 NO CONCRETE INTERACTION GENERALIZED "
        "ACROSS ALL THREE TOKEN HOLDOUTS."
    )

    print(
        "Do not force combinations from TRAIN."
    )


print()
print("IMPORTANT:")
print("• Thresholds are computed from TRAIN only.")
print("• VALID never changes thresholds.")
print("• TEST never changes thresholds or selected rules.")
print("• Token identities do not cross splits.")
print("• Minimum TRAIN sample is enforced.")
print("• Fast-flip history is strictly chronological.")
print("• Capital-efficiency definitions are frozen from T50.")
print("• Regime is context only.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T52 writes nothing to DB.")
print("• Discovery research only.")


db.close()
