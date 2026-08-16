import sqlite3
import math
import statistics
from collections import defaultdict

from sklearn.metrics import roc_auc_score


DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0
THRESHOLDS = [1, 2, 3, 5]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def safe_div(a, b):
    if b is None or b == 0:
        return None
    return a / b


def pct(vals, p):
    vals = sorted(x for x in vals if valid(x))
    if not vals:
        return None

    idx = int(round(
        (p / 100.0) * (len(vals) - 1)
    ))

    return vals[idx]


def label_r60(x):
    if not valid(x):
        return None

    if x >= RUNNER:
        return 1

    if x <= DUMP:
        return 0

    return None


def auc_safe(y, x):
    pairs = [
        (yy, xx)
        for yy, xx in zip(y, x)
        if valid(xx)
    ]

    if len(pairs) < 4:
        return None

    yy = [p[0] for p in pairs]
    xx = [p[1] for p in pairs]

    if len(set(yy)) < 2:
        return None

    if len(set(xx)) < 2:
        return None

    try:
        return roc_auc_score(yy, xx)
    except Exception:
        return None


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute(
    "PRAGMA busy_timeout=5000"
)


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s
FROM events
WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL
ORDER BY timestamp, id
""").fetchall()


events = [
    e for e in events
    if label_r60(
        e["dex_return_60s"]
    ) is not None
]


swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    clean_price,
    price_valid
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
        OR price_valid = 1
    )
ORDER BY timestamp
""").fetchall()


# ============================================================
# WALLET HISTORY
# ============================================================

completed = defaultdict(int)
wallet_tokens = defaultdict(set)

wallet_fast_flips = defaultdict(int)
wallet_mfe = defaultdict(list)

open_pos = {}

swap_idx = 0


def update_paths(token, price):

    if not valid(price) or price <= 0:
        return

    for key, st in list(
        open_pos.items()
    ):

        wallet, tok = key

        if tok != token:
            continue

        st["max_price"] = max(
            st["max_price"],
            price
        )

        st["min_price"] = min(
            st["min_price"],
            price
        )


def process_swap(s):

    wallet = s["wallet"]
    token = s["token_mint"]
    side = s["side"]
    price = s["clean_price"]
    ts = s["timestamp"]

    if (
        not valid(price)
        or price <= 0
    ):
        return

    wallet_tokens[
        wallet
    ].add(
        token
    )

    update_paths(
        token,
        price
    )

    key = (
        wallet,
        token
    )

    if side == "BUY":

        if key not in open_pos:

            open_pos[key] = {
                "entry_ts": ts,
                "entry_price": price,
                "max_price": price,
                "min_price": price,
            }

    elif side == "SELL":

        if key not in open_pos:
            return

        st = open_pos[
            key
        ]

        ep = st[
            "entry_price"
        ]

        if (
            not valid(ep)
            or ep <= 0
        ):
            open_pos.pop(
                key,
                None
            )
            return

        hold = (
            ts
            - st[
                "entry_ts"
            ]
        )

        if hold < 0:
            open_pos.pop(
                key,
                None
            )
            return

        mfe = (
            st["max_price"]
            / ep
            - 1.0
        ) * 100.0

        completed[
            wallet
        ] += 1

        wallet_mfe[
            wallet
        ].append(
            mfe
        )

        if hold <= 60:
            wallet_fast_flips[
                wallet
            ] += 1

        open_pos.pop(
            key,
            None
        )


def wallet_snapshot(wallet):

    n = completed[
        wallet
    ]

    return {
        "prior_trades":
            n,

        "prior_tokens":
            len(
                wallet_tokens[
                    wallet
                ]
            ),

        "median_mfe":
            med(
                wallet_mfe.get(
                    wallet,
                    []
                )
            ),

        "fast_flip_rate":
            safe_div(
                wallet_fast_flips[
                    wallet
                ],
                n
            ),
    }


# ============================================================
# CHRONOLOGICAL EVENT SNAPSHOTS
# ============================================================

records = []


for e in events:

    event_ts = e[
        "timestamp"
    ]

    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"]
        < event_ts
    ):

        process_swap(
            swaps[
                swap_idx
            ]
        )

        swap_idx += 1


    pre = db.execute("""
    SELECT
        wallet,
        side
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
    """, (
        e["token_mint"],
        event_ts - PRE_EVENT_SEC,
        event_ts
    )).fetchall()


    if not pre:
        continue


    buyers = sorted(
        set(
            x["wallet"]
            for x in pre
            if x["side"] == "BUY"
        )
    )


    wallets = sorted(
        set(
            x["wallet"]
            for x in pre
        )
    )


    buyer_states = {
        w: wallet_snapshot(w)
        for w in buyers
    }


    wallet_states = {
        w: wallet_snapshot(w)
        for w in wallets
    }


    records.append({
        "id":
            e["id"],

        "timestamp":
            event_ts,

        "token_mint":
            e["token_mint"],

        "label":
            label_r60(
                e["dex_return_60s"]
            ),

        "r60":
            e["dex_return_60s"],

        "buyers":
            buyer_states,

        "wallets":
            wallet_states,
    })


# ============================================================
# METRICS AT A GIVEN EXPERIENCE THRESHOLD
# ============================================================

def build_features(rec, threshold):

    exp_buyers = [
        st
        for st in rec[
            "buyers"
        ].values()
        if st[
            "prior_trades"
        ] >= threshold
    ]


    exp_wallets = [
        st
        for st in rec[
            "wallets"
        ].values()
        if st[
            "prior_trades"
        ] >= threshold
    ]


    fast = [
        x[
            "fast_flip_rate"
        ]
        for x in exp_buyers
        if valid(
            x[
                "fast_flip_rate"
            ]
        )
    ]


    mfe = [
        x[
            "median_mfe"
        ]
        for x in exp_buyers
        if valid(
            x[
                "median_mfe"
            ]
        )
    ]


    prior_trades = [
        x[
            "prior_trades"
        ]
        for x in exp_buyers
    ]


    token_counts = [
        x[
            "prior_tokens"
        ]
        for x in exp_wallets
    ]


    weighted_fast = None

    if fast and prior_trades:

        num = 0.0
        den = 0.0

        for st in exp_buyers:

            ff = st[
                "fast_flip_rate"
            ]

            n = st[
                "prior_trades"
            ]

            if (
                valid(ff)
                and n > 0
            ):
                num += (
                    ff * n
                )

                den += n

        if den > 0:
            weighted_fast = (
                num / den
            )


    low_fast_share = (
        safe_div(
            sum(
                x <= 0.25
                for x in fast
            ),
            len(fast)
        )
        if fast
        else None
    )


    high_fast_share = (
        safe_div(
            sum(
                x >= 0.50
                for x in fast
            ),
            len(fast)
        )
        if fast
        else None
    )


    return {
        "id":
            rec["id"],

        "timestamp":
            rec["timestamp"],

        "token_mint":
            rec["token_mint"],

        "label":
            rec["label"],

        "r60":
            rec["r60"],

        "exp_buyer_count":
            len(
                exp_buyers
            ),

        "exp_wallet_count":
            len(
                exp_wallets
            ),

        "buyer_fast_mean":
            avg(
                fast
            ),

        "buyer_fast_median":
            med(
                fast
            ),

        "buyer_fast_min":
            min(
                fast
            )
            if fast
            else None,

        "buyer_fast_max":
            max(
                fast
            )
            if fast
            else None,

        "buyer_fast_weighted":
            weighted_fast,

        "buyer_fast_low_share":
            low_fast_share,

        "buyer_fast_high_share":
            high_fast_share,

        "buyer_mfe_mean":
            avg(
                mfe
            ),

        "buyer_mfe_median":
            med(
                mfe
            ),

        "buyer_prior_trades_med":
            med(
                prior_trades
            ),

        "cohort_token_count":
            med(
                token_counts
            ),
    }


FEATURES = [
    "buyer_fast_mean",
    "buyer_fast_median",
    "buyer_fast_min",
    "buyer_fast_max",
    "buyer_fast_weighted",
    "buyer_fast_low_share",
    "buyer_fast_high_share",
    "buyer_mfe_mean",
    "buyer_mfe_median",
    "buyer_prior_trades_med",
    "cohort_token_count",
]


# ============================================================
# HEADER
# ============================================================

print(
    "=" * 175
)

print(
    "MEMECOIN LAB — T45 WALLET METRIC COVERAGE EXPANSION LAB"
)

print(
    "=" * 175
)

print(
    f"LABELED EVENT SNAPSHOTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS          : {len(set(r['token_mint'] for r in records))}"
)

print(
    "NO MODEL FITTING."
)

print(
    "NO THRESHOLD OPTIMIZATION."
)

print(
    "PURPOSE = COVERAGE / STABILITY / REPRESENTATION AUDIT."
)


# ============================================================
# A) COVERAGE BY EXPERIENCE THRESHOLD
# ============================================================

print()
print(
    "=" * 175
)

print(
    "A) COVERAGE BY EXPERIENCE THRESHOLD"
)

print(
    "=" * 175
)

print(
    f"{'THR':>4} "
    f"{'EVENTS':>8} "
    f"{'EXP-BUY':>8} "
    f"{'TOKENS':>8} "
    f"{'FAST N':>8} "
    f"{'FAST %':>8} "
    f"{'MFE N':>8} "
    f"{'MFE %':>8}"
)

print(
    "-" * 85
)


cache = {}


for threshold in THRESHOLDS:

    rr = [
        build_features(
            r,
            threshold
        )
        for r in records
    ]


    cache[
        threshold
    ] = rr


    exp = [
        r for r in rr
        if r[
            "exp_buyer_count"
        ] > 0
    ]


    fast = [
        r for r in rr
        if valid(
            r[
                "buyer_fast_mean"
            ]
        )
    ]


    mfe = [
        r for r in rr
        if valid(
            r[
                "buyer_mfe_mean"
            ]
        )
    ]


    print(
        f"{threshold:4d} "
        f"{len(rr):8d} "
        f"{len(exp):8d} "
        f"{len(set(r['token_mint'] for r in exp)):8d} "
        f"{len(fast):8d} "
        f"{100*len(fast)/len(rr):7.2f}% "
        f"{len(mfe):8d} "
        f"{100*len(mfe)/len(rr):7.2f}%"
    )


# ============================================================
# B) AGGREGATION REPRESENTATION AUDIT
# ============================================================

print()
print(
    "=" * 175
)

print(
    "B) FAST-FLIP AGGREGATION REPRESENTATION"
)

print(
    "=" * 175
)


fast_features = [
    "buyer_fast_mean",
    "buyer_fast_median",
    "buyer_fast_min",
    "buyer_fast_max",
    "buyer_fast_weighted",
    "buyer_fast_low_share",
    "buyer_fast_high_share",
]


for threshold in THRESHOLDS:

    print()
    print(
        f"THRESHOLD >= {threshold} PRIOR TRADES"
    )

    print(
        "-" * 120
    )


    rr = cache[
        threshold
    ]


    for feature in fast_features:

        usable = [
            r for r in rr
            if valid(
                r[
                    feature
                ]
            )
        ]


        run = [
            r[
                feature
            ]
            for r in usable
            if r[
                "label"
            ] == 1
        ]


        dump = [
            r[
                feature
            ]
            for r in usable
            if r[
                "label"
            ] == 0
        ]


        y = [
            r[
                "label"
            ]
            for r in usable
        ]


        x = [
            r[
                feature
            ]
            for r in usable
        ]


        auc = auc_safe(
            y,
            x
        )


        print(
            f"{feature:27} "
            f"N={len(usable):4d} "
            f"TOK={len(set(r['token_mint'] for r in usable)):3d} "
            f"RUN_MED={fmt(med(run)):>8} "
            f"DUMP_MED={fmt(med(dump)):>8} "
            f"DIFF={fmt((med(run)-med(dump)) if run and dump else None):>8} "
            f"AUC={fmt(auc):>6}"
        )


# ============================================================
# C) EXPERIENCED-BUYER COUNT DISTRIBUTION
# ============================================================

print()
print(
    "=" * 175
)

print(
    "C) EXPERIENCED BUYER COUNT DISTRIBUTION"
)

print(
    "=" * 175
)


for threshold in THRESHOLDS:

    rr = cache[
        threshold
    ]


    counts = [
        r[
            "exp_buyer_count"
        ]
        for r in rr
    ]


    print()
    print(
        f"THRESHOLD >= {threshold}"
    )


    for n in [
        0,
        1,
        2,
        3
    ]:

        if n < 3:

            c = sum(
                x == n
                for x in counts
            )

            label = f"={n}"

        else:

            c = sum(
                x >= 3
                for x in counts
            )

            label = ">=3"


        print(
            f"EXP BUYERS {label:>3} : "
            f"{c:4d} "
            f"({100*c/len(counts):5.1f}%)"
        )


# ============================================================
# D) TEMPORAL THIRDS
# ============================================================

print()
print(
    "=" * 175
)

print(
    "D) TEMPORAL THIRD STABILITY"
)

print(
    "=" * 175
)


for threshold in THRESHOLDS:

    rr = sorted(
        cache[
            threshold
        ],
        key=lambda r: (
            r[
                "timestamp"
            ],
            r[
                "id"
            ]
        )
    )


    n = len(rr)

    cuts = [
        0,
        n // 3,
        (2*n) // 3,
        n
    ]


    print()
    print(
        f"THRESHOLD >= {threshold}"
    )

    print(
        "-" * 115
    )


    for i in range(3):

        block = rr[
            cuts[i]:
            cuts[i+1]
        ]


        usable = [
            r for r in block
            if valid(
                r[
                    "buyer_fast_mean"
                ]
            )
        ]


        run = [
            r[
                "buyer_fast_mean"
            ]
            for r in usable
            if r[
                "label"
            ] == 1
        ]


        dump = [
            r[
                "buyer_fast_mean"
            ]
            for r in usable
            if r[
                "label"
            ] == 0
        ]


        y = [
            r[
                "label"
            ]
            for r in usable
        ]


        x = [
            r[
                "buyer_fast_mean"
            ]
            for r in usable
        ]


        print(
            f"T{i+1} "
            f"| N={len(usable):3d} "
            f"TOK={len(set(r['token_mint'] for r in usable)):3d} "
            f"| RUN_MED={fmt(med(run)):>7} "
            f"| DUMP_MED={fmt(med(dump)):>7} "
            f"| AUC={fmt(auc_safe(y,x)):>6}"
        )


# ============================================================
# E) FIRST EVENT / TOKEN COVERAGE
# ============================================================

print()
print(
    "=" * 175
)

print(
    "E) FIRST-EVENT/TOKEN COVERAGE"
)

print(
    "=" * 175
)


for threshold in THRESHOLDS:

    rr = sorted(
        cache[
            threshold
        ],
        key=lambda r:
            r[
                "id"
            ]
    )


    seen = set()
    first = []


    for r in rr:

        token = r[
            "token_mint"
        ]

        if token in seen:
            continue

        seen.add(
            token
        )

        first.append(
            r
        )


    usable = [
        r for r in first
        if valid(
            r[
                "buyer_fast_mean"
            ]
        )
    ]


    y = [
        r[
            "label"
        ]
        for r in usable
    ]


    x = [
        r[
            "buyer_fast_mean"
        ]
        for r in usable
    ]


    print(
        f"THR>={threshold:2d} "
        f"| FIRST TOKENS={len(first):3d} "
        f"| FAST COVER={len(usable):3d} "
        f"({100*len(usable)/max(1,len(first)):5.1f}%) "
        f"| AUC={fmt(auc_safe(y,x)):>6}"
    )


# ============================================================
# F) COHORT-TOKEN-COUNT CONTEXT
# ============================================================

print()
print(
    "=" * 175
)

print(
    "F) COHORT TOKEN COUNT CONTEXT"
)

print(
    "=" * 175
)


for threshold in THRESHOLDS:

    rr = cache[
        threshold
    ]


    usable = [
        r for r in rr
        if valid(
            r[
                "cohort_token_count"
            ]
        )
    ]


    run = [
        r[
            "cohort_token_count"
        ]
        for r in usable
        if r[
            "label"
        ] == 1
    ]


    dump = [
        r[
            "cohort_token_count"
        ]
        for r in usable
        if r[
            "label"
        ] == 0
    ]


    y = [
        r[
            "label"
        ]
        for r in usable
    ]


    x = [
        r[
            "cohort_token_count"
        ]
        for r in usable
    ]


    print(
        f"THR>={threshold:2d} "
        f"| N={len(usable):3d} "
        f"| TOK={len(set(r['token_mint'] for r in usable)):3d} "
        f"| RUN_MED={fmt(med(run)):>7} "
        f"| DUMP_MED={fmt(med(dump)):>7} "
        f"| AUC={fmt(auc_safe(y,x)):>6}"
    )


# ============================================================
# G) REPRESENTATION AGREEMENT
# ============================================================

print()
print(
    "=" * 175
)

print(
    "G) FAST-FLIP REPRESENTATION AGREEMENT"
)

print(
    "=" * 175
)


for threshold in THRESHOLDS:

    rr = cache[
        threshold
    ]


    print()
    print(
        f"THRESHOLD >= {threshold}"
    )


    pairs = [
        (
            "buyer_fast_mean",
            "buyer_fast_median"
        ),
        (
            "buyer_fast_mean",
            "buyer_fast_weighted"
        ),
        (
            "buyer_fast_mean",
            "buyer_fast_low_share"
        ),
        (
            "buyer_fast_mean",
            "buyer_fast_high_share"
        ),
    ]


    for a, b in pairs:

        vals = [
            (
                r[
                    a
                ],
                r[
                    b
                ]
            )
            for r in rr
            if (
                valid(
                    r[
                        a
                    ]
                )
                and valid(
                    r[
                        b
                    ]
                )
            )
        ]


        if len(vals) < 3:

            corr = None

        else:

            xa = [
                x[0]
                for x in vals
            ]

            xb = [
                x[1]
                for x in vals
            ]

            ma = avg(
                xa
            )

            mb = avg(
                xb
            )


            num = sum(
                (x-ma)
                * (y-mb)
                for x,y in vals
            )


            da = math.sqrt(
                sum(
                    (x-ma)**2
                    for x in xa
                )
            )


            dbb = math.sqrt(
                sum(
                    (y-mb)**2
                    for y in xb
                )
            )


            corr = (
                num / (
                    da * dbb
                )
                if (
                    da > 0
                    and dbb > 0
                )
                else None
            )


        print(
            f"{a:24} vs {b:24} "
            f"N={len(vals):3d} "
            f"CORR={fmt(corr):>6}"
        )


# ============================================================
# H) COVERAGE QUALITY SCORECARD
# ============================================================

print()
print(
    "=" * 175
)

print(
    "H) COVERAGE QUALITY SCORECARD"
)

print(
    "=" * 175
)

print(
    "This is NOT an edge score."
)

print(
    "It ranks information availability only."
)

print()


ranking = []


for threshold in THRESHOLDS:

    rr = cache[
        threshold
    ]


    fast = [
        r for r in rr
        if valid(
            r[
                "buyer_fast_mean"
            ]
        )
    ]


    tokens = len(
        set(
            r[
                "token_mint"
            ]
            for r in fast
        )
    )


    coverage = (
        len(fast)
        / len(rr)
    )


    multi_buyer = (
        sum(
            r[
                "exp_buyer_count"
            ] >= 2
            for r in rr
        )
        / len(rr)
    )


    score = (
        100 * coverage
        + 0.50 * tokens
        + 20 * multi_buyer
    )


    ranking.append(
        (
            score,
            threshold,
            coverage,
            tokens,
            multi_buyer
        )
    )


ranking.sort(
    reverse=True
)


for (
    score,
    threshold,
    coverage,
    tokens,
    multi_buyer
) in ranking:

    print(
        f"THR>={threshold:2d} "
        f"| QA SCORE={score:6.2f} "
        f"| COVER={100*coverage:5.1f}% "
        f"| TOK={tokens:3d} "
        f"| MULTI-BUYER={100*multi_buyer:5.1f}%"
    )


# ============================================================
# I) DECISION SUPPORT
# ============================================================

print()
print(
    "=" * 175
)

print(
    "I) DECISION SUPPORT"
)

print(
    "=" * 175
)


best = ranking[
    0
]


best_threshold = best[
    1
]

best_coverage = best[
    2
]

best_tokens = best[
    3
]


print(
    f"BEST COVERAGE CONFIGURATION = "
    f">= {best_threshold} PRIOR COMPLETED TRADES"
)

print(
    f"FAST-FLIP COVERAGE          = "
    f"{100*best_coverage:.2f}%"
)

print(
    f"UNIQUE TOKENS               = "
    f"{best_tokens}"
)

print()


if (
    best_coverage >= 0.50
    and best_tokens >= 30
):

    print(
        "🟢 WALLET FAST-FLIP COVERAGE IS NOW LARGE ENOUGH "
        "FOR A FRESH ROBUSTNESS AUDIT."
    )

    print(
        "Next step = T46 with frozen representation chosen "
        "from coverage/stability, not from best AUC."
    )

elif (
    best_coverage >= 0.35
    and best_tokens >= 20
):

    print(
        "🟡 WALLET METRIC COVERAGE IS IMPROVED BUT STILL LIMITED."
    )

    print(
        "T46 can be run cautiously, with explicit uncertainty."
    )

else:

    print(
        "🔴 WALLET METRIC REMAINS TOO SPARSE."
    )

    print(
        "Do not spend more iterations optimizing this metric yet."
    )

    print(
        "Collect more chronological wallet history."
    )


print()
print("IMPORTANT:")
print("• T45 fits no model.")
print("• T45 searches no trading threshold.")
print("• Experience levels are coverage regimes, not optimized signals.")
print("• Same-unit swaps.clean_price only.")
print("• Wallet history is chronological.")
print("• Current event outcome never enters prior wallet state.")
print("• T23/T31/T32 remain untouched.")
print("• T45 writes nothing to DB.")
print("• This is a data-quality / representation audit.")

db.close()
