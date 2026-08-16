import sqlite3
import math
import statistics
from collections import defaultdict, Counter

import numpy as np
from sklearn.metrics import roc_auc_score


# ============================================================
# CONFIG
# ============================================================

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0

# Frozen from T43 observation.
MIN_PRIOR_TRADES = 2

PRIMARY = "buyers_fast_flip_rate"

# Context / secondary metrics only.
SECONDARY = [
    "buyers_mfe",
    "cohort_token_count",
]

ALL_FEATURES = [
    PRIMARY,
    *SECONDARY,
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


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def safe_div(a, b):
    if b is None or b == 0:
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


def auc_raw(y, x):
    """
    Returns raw AUC.

    Important:
    AUC < 0.5 is NOT automatically bad here.
    Direction is evaluated separately.
    """

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


def directional_auc(y, x, expected_sign):
    """
    Converts the feature to the expected RUN direction.

    expected_sign:
      +1 => higher feature expected for RUN
      -1 => lower feature expected for RUN
    """

    if expected_sign not in (-1, 1):
        return None

    xx = [
        v * expected_sign
        if valid(v)
        else None
        for v in x
    ]

    return auc_raw(y, xx)


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def sign_of(x):
    if x is None:
        return 0

    if x > 0:
        return 1

    if x < 0:
        return -1

    return 0


# ============================================================
# LOAD DATABASE
# ============================================================

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row

db.execute("PRAGMA busy_timeout=5000")


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
    if label_r60(e["dex_return_60s"]) is not None
]


swaps = db.execute("""
SELECT
    timestamp,
    program,
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
# HISTORICAL WALLET STATE
# ============================================================

completed = defaultdict(int)
wallet_tokens = defaultdict(set)

wallet_mfe = defaultdict(list)
wallet_fast_flips = defaultdict(int)

open_pos = {}

swap_idx = 0


def update_paths(token, price):

    if not valid(price) or price <= 0:
        return

    for key, st in list(open_pos.items()):

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

    if not valid(price) or price <= 0:
        return

    wallet_tokens[wallet].add(token)

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

        st = open_pos[key]

        ep = st["entry_price"]

        if not valid(ep) or ep <= 0:
            open_pos.pop(
                key,
                None
            )
            return

        hold = (
            ts
            - st["entry_ts"]
        )

        if hold < 0:
            open_pos.pop(
                key,
                None
            )
            return

        mfe = (
            st["max_price"] / ep
            - 1.0
        ) * 100.0

        completed[wallet] += 1

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
# BUILD STRICTLY CHRONOLOGICAL EVENT FEATURES
# ============================================================

records = []


for e in events:

    event_ts = e[
        "timestamp"
    ]

    # Strictly historical wallet information only.
    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < event_ts
    ):

        process_swap(
            swaps[swap_idx]
        )

        swap_idx += 1


    pre = db.execute("""
    SELECT
        wallet,
        side,
        program
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


    all_wallets = sorted(
        set(
            x["wallet"]
            for x in pre
        )
    )


    experienced_buyers = []

    for w in buyers:

        st = wallet_snapshot(w)

        if (
            st["prior_trades"]
            >= MIN_PRIOR_TRADES
        ):
            experienced_buyers.append(
                st
            )


    experienced_wallets = []

    for w in all_wallets:

        st = wallet_snapshot(w)

        if (
            st["prior_trades"]
            >= MIN_PRIOR_TRADES
        ):
            experienced_wallets.append(
                st
            )


    buyers_fast_flip_rate = avg([
        x["fast_flip_rate"]
        for x in experienced_buyers
        if valid(
            x["fast_flip_rate"]
        )
    ])


    buyers_mfe = avg([
        x["median_mfe"]
        for x in experienced_buyers
        if valid(
            x["median_mfe"]
        )
    ])


    cohort_token_count = med([
        x["prior_tokens"]
        for x in experienced_wallets
    ])


    experienced_buyer_ratio = safe_div(
        len(
            experienced_buyers
        ),
        len(
            buyers
        )
    )


    experienced_wallet_ratio = safe_div(
        len(
            experienced_wallets
        ),
        len(
            all_wallets
        )
    )


    programs = Counter(
        x["program"]
        for x in pre
        if x["program"]
    )


    dominant_program = (
        programs.most_common(1)[0][0]
        if programs
        else "UNKNOWN"
    )


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

        "buyers_fast_flip_rate":
            buyers_fast_flip_rate,

        "buyers_mfe":
            buyers_mfe,

        "cohort_token_count":
            cohort_token_count,

        "experienced_buyer_ratio":
            experienced_buyer_ratio,

        "experienced_wallet_ratio":
            experienced_wallet_ratio,

        "experienced_buyer_count":
            len(
                experienced_buyers
            ),

        "experienced_wallet_count":
            len(
                experienced_wallets
            ),

        "buyer_count":
            len(
                buyers
            ),

        "wallet_count":
            len(
                all_wallets
            ),

        "program":
            dominant_program,
    })


# ============================================================
# GLOBAL EXPECTED FEATURE DIRECTION
#
# Direction is established on EARLY 50% ONLY.
# Late data cannot determine direction.
# ============================================================

records = sorted(
    records,
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)


cut = max(
    1,
    len(records) // 2
)

direction_reference = records[
    :cut
]


expected_direction = {}


for feature in ALL_FEATURES:

    run = [
        r[feature]
        for r in direction_reference
        if (
            r["label"] == 1
            and valid(
                r[feature]
            )
        )
    ]

    dump = [
        r[feature]
        for r in direction_reference
        if (
            r["label"] == 0
            and valid(
                r[feature]
            )
        )
    ]

    diff = (
        med(run) - med(dump)
        if run and dump
        else None
    )

    expected_direction[
        feature
    ] = sign_of(
        diff
    )


# ============================================================
# AUDIT FUNCTION
# ============================================================

def audit_group(name, rr, feature):

    usable = [
        r
        for r in rr
        if valid(
            r[feature]
        )
    ]


    run = [
        r[feature]
        for r in usable
        if r["label"] == 1
    ]

    dump = [
        r[feature]
        for r in usable
        if r["label"] == 0
    ]


    if run and dump:

        diff = (
            med(run)
            - med(dump)
        )

    else:
        diff = None


    y = [
        r["label"]
        for r in usable
    ]

    x = [
        r[feature]
        for r in usable
    ]


    raw_auc = auc_raw(
        y,
        x
    )


    direction = expected_direction[
        feature
    ]


    d_auc = (
        directional_auc(
            y,
            x,
            direction
        )
        if direction != 0
        else None
    )


    return {
        "name":
            name,

        "n":
            len(
                usable
            ),

        "tokens":
            len(
                set(
                    r["token_mint"]
                    for r in usable
                )
            ),

        "run":
            len(
                run
            ),

        "dump":
            len(
                dump
            ),

        "run_med":
            med(
                run
            ),

        "dump_med":
            med(
                dump
            ),

        "diff":
            diff,

        "raw_auc":
            raw_auc,

        "directional_auc":
            d_auc,

        "same_direction":
            (
                sign_of(
                    diff
                ) == direction
                and direction != 0
            ),
    }


def print_audit(a):

    print(
        f"{a['name']:<22} "
        f"N={a['n']:4d} "
        f"TOK={a['tokens']:3d} "
        f"RUN={a['run']:3d} "
        f"DUMP={a['dump']:3d} "
        f"RUN_MED={fmt(a['run_med']):>8} "
        f"DUMP_MED={fmt(a['dump_med']):>8} "
        f"DIFF={fmt(a['diff']):>8} "
        f"AUC={fmt(a['raw_auc']):>6} "
        f"DIR_AUC={fmt(a['directional_auc']):>6} "
        f"SAME={str(a['same_direction']):5}"
    )


# ============================================================
# HEADER
# ============================================================

print(
    "=" * 180
)

print(
    "MEMECOIN LAB — T44 WALLET METRIC TEMPORAL / REGIME ROBUSTNESS AUDIT"
)

print(
    "=" * 180
)

print(
    f"LABELED EVENTS              : {len(records)}"
)

print(
    f"UNIQUE TOKENS               : {len(set(r['token_mint'] for r in records))}"
)

print(
    f"FROZEN EXPERIENCE THRESHOLD : >= {MIN_PRIOR_TRADES} PRIOR COMPLETED TRADES"
)

print(
    f"PRIMARY FEATURE             : {PRIMARY}"
)

print(
    "NO MODEL FITTING."
)

print(
    "NO THRESHOLD OPTIMIZATION."
)


# ============================================================
# A) COVERAGE
# ============================================================

print()
print(
    "=" * 180
)

print(
    "A) FEATURE COVERAGE"
)

print(
    "=" * 180
)


for feature in ALL_FEATURES:

    usable = [
        r
        for r in records
        if valid(
            r[feature]
        )
    ]

    print(
        f"{feature:30} "
        f"N={len(usable):4d}/{len(records):4d} "
        f"COVERAGE="
        f"{100*len(usable)/max(1,len(records)):6.2f}% "
        f"TOKENS="
        f"{len(set(r['token_mint'] for r in usable)):3d}"
    )


# ============================================================
# B) DIRECTION REFERENCE
# ============================================================

print()
print(
    "=" * 180
)

print(
    "B) FROZEN EXPECTED DIRECTION — EARLY 50% ONLY"
)

print(
    "=" * 180
)


for feature in ALL_FEATURES:

    sign = expected_direction[
        feature
    ]

    text = (
        "HIGHER -> RUN"
        if sign == 1
        else
        "LOWER -> RUN"
        if sign == -1
        else
        "UNRESOLVED"
    )

    print(
        f"{feature:30} {text}"
    )


# ============================================================
# C) CHRONOLOGICAL QUARTILES
# ============================================================

print()
print(
    "=" * 180
)

print(
    "C) CHRONOLOGICAL QUARTILE STABILITY"
)

print(
    "=" * 180
)


n = len(records)

bounds = [
    0,
    n // 4,
    n // 2,
    (3 * n) // 4,
    n
]


for feature in ALL_FEATURES:

    print()
    print(
        feature
    )

    print(
        "-" * 170
    )

    audits = []

    for i in range(4):

        rr = records[
            bounds[i]:
            bounds[i+1]
        ]

        a = audit_group(
            f"Q{i+1}",
            rr,
            feature
        )

        audits.append(
            a
        )

        print_audit(
            a
        )


    valid_blocks = [
        a
        for a in audits
        if (
            a["n"] >= 8
            and a["run"] >= 2
            and a["dump"] >= 2
        )
    ]


    survived = sum(
        1
        for a in valid_blocks
        if a[
            "same_direction"
        ]
    )


    print(
        f"QUARTILE DIRECTION SURVIVAL: "
        f"{survived}/{len(valid_blocks)} usable blocks"
    )


# ============================================================
# D) EARLY / MIDDLE / LATE
# ============================================================

print()
print(
    "=" * 180
)

print(
    "D) EARLY / MIDDLE / LATE TEMPORAL AUDIT"
)

print(
    "=" * 180
)


n1 = len(records) // 3
n2 = (
    2 * len(records)
) // 3


temporal_groups = [
    (
        "EARLY",
        records[
            :n1
        ]
    ),
    (
        "MIDDLE",
        records[
            n1:n2
        ]
    ),
    (
        "LATE",
        records[
            n2:
        ]
    ),
]


for feature in ALL_FEATURES:

    print()
    print(
        feature
    )

    print(
        "-" * 170
    )

    for name, rr in temporal_groups:

        print_audit(
            audit_group(
                name,
                rr,
                feature
            )
        )


# ============================================================
# E) PROGRAM AUDIT
# ============================================================

print()
print(
    "=" * 180
)

print(
    "E) DOMINANT PROGRAM AUDIT"
)

print(
    "=" * 180
)


programs = sorted(
    set(
        r["program"]
        for r in records
    )
)


for feature in ALL_FEATURES:

    print()
    print(
        feature
    )

    print(
        "-" * 170
    )

    for program in programs:

        rr = [
            r
            for r in records
            if r["program"]
            == program
        ]

        if len(rr) < 5:
            continue

        print_audit(
            audit_group(
                program,
                rr,
                feature
            )
        )


# ============================================================
# F) EXPERIENCED BUYER COVERAGE REGIMES
# ============================================================

print()
print(
    "=" * 180
)

print(
    "F) EXPERIENCED BUYER RATIO REGIMES"
)

print(
    "=" * 180
)


ratio_groups = [
    (
        "RATIO <25%",
        lambda x:
            valid(x)
            and x < 0.25
    ),
    (
        "RATIO 25-50%",
        lambda x:
            valid(x)
            and 0.25 <= x < 0.50
    ),
    (
        "RATIO 50-75%",
        lambda x:
            valid(x)
            and 0.50 <= x < 0.75
    ),
    (
        "RATIO >=75%",
        lambda x:
            valid(x)
            and x >= 0.75
    ),
]


for feature in ALL_FEATURES:

    print()
    print(
        feature
    )

    print(
        "-" * 170
    )

    for name, fn in ratio_groups:

        rr = [
            r
            for r in records
            if fn(
                r[
                    "experienced_buyer_ratio"
                ]
            )
        ]

        print_audit(
            audit_group(
                name,
                rr,
                feature
            )
        )


# ============================================================
# G) EXPERIENCED BUYER COUNT
# ============================================================

print()
print(
    "=" * 180
)

print(
    "G) EXPERIENCED BUYER COUNT REGIMES"
)

print(
    "=" * 180
)


count_groups = [
    (
        "EXP BUYERS = 0",
        lambda x:
            x == 0
    ),
    (
        "EXP BUYERS = 1",
        lambda x:
            x == 1
    ),
    (
        "EXP BUYERS = 2",
        lambda x:
            x == 2
    ),
    (
        "EXP BUYERS >=3",
        lambda x:
            x >= 3
    ),
]


for feature in ALL_FEATURES:

    print()
    print(
        feature
    )

    print(
        "-" * 170
    )

    for name, fn in count_groups:

        rr = [
            r
            for r in records
            if fn(
                r[
                    "experienced_buyer_count"
                ]
            )
        ]

        print_audit(
            audit_group(
                name,
                rr,
                feature
            )
        )


# ============================================================
# H) FIRST EVENT PER TOKEN
# ============================================================

print()
print(
    "=" * 180
)

print(
    "H) FIRST-EVENT/PER-TOKEN AUDIT"
)

print(
    "=" * 180
)


seen = set()
first_records = []


for r in records:

    token = r[
        "token_mint"
    ]

    if token in seen:
        continue

    seen.add(
        token
    )

    first_records.append(
        r
    )


for feature in ALL_FEATURES:

    print_audit(
        audit_group(
            feature,
            first_records,
            feature
        )
    )


# ============================================================
# I) UNIQUE TOKEN BLOCKS
#
# Tokens assigned chronologically by first appearance.
# ============================================================

print()
print(
    "=" * 180
)

print(
    "I) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS"
)

print(
    "=" * 180
)


token_first_ts = {}


for r in records:

    token_first_ts.setdefault(
        r["token_mint"],
        r["timestamp"]
    )


ordered_tokens = sorted(
    token_first_ts,
    key=lambda t:
        token_first_ts[t]
)


tb = [
    0,
    len(ordered_tokens) // 4,
    len(ordered_tokens) // 2,
    (3 * len(ordered_tokens)) // 4,
    len(ordered_tokens)
]


for feature in ALL_FEATURES:

    print()
    print(
        feature
    )

    print(
        "-" * 170
    )

    for i in range(4):

        tokset = set(
            ordered_tokens[
                tb[i]:
                tb[i+1]
            ]
        )

        rr = [
            r
            for r in records
            if r[
                "token_mint"
            ] in tokset
        ]

        print_audit(
            audit_group(
                f"TOKEN BLOCK {i+1}",
                rr,
                feature
            )
        )


# ============================================================
# J) PRIMARY FEATURE SUMMARY
# ============================================================

print()
print(
    "=" * 180
)

print(
    "J) PRIMARY FEATURE ROBUSTNESS SCORECARD"
)

print(
    "=" * 180
)


primary_audits = []


# Quartiles
for i in range(4):

    primary_audits.append(
        audit_group(
            f"TIME_Q{i+1}",
            records[
                bounds[i]:
                bounds[i+1]
            ],
            PRIMARY
        )
    )


# Temporal thirds
for name, rr in temporal_groups:

    primary_audits.append(
        audit_group(
            f"TIME_{name}",
            rr,
            PRIMARY
        )
    )


# Programs
for program in programs:

    rr = [
        r
        for r in records
        if r[
            "program"
        ] == program
    ]

    if len(rr) >= 5:

        primary_audits.append(
            audit_group(
                f"PROGRAM_{program}",
                rr,
                PRIMARY
            )
        )


# Buyer ratio
for name, fn in ratio_groups:

    rr = [
        r
        for r in records
        if fn(
            r[
                "experienced_buyer_ratio"
            ]
        )
    ]

    primary_audits.append(
        audit_group(
            name,
            rr,
            PRIMARY
        )
    )


usable = [
    a
    for a in primary_audits
    if (
        a["n"] >= 8
        and a["run"] >= 2
        and a["dump"] >= 2
        and a[
            "directional_auc"
        ] is not None
    )
]


same = [
    a
    for a in usable
    if a[
        "same_direction"
    ]
]


auc55 = [
    a
    for a in usable
    if a[
        "directional_auc"
    ] >= 0.55
]


auc60 = [
    a
    for a in usable
    if a[
        "directional_auc"
    ] >= 0.60
]


print(
    f"USABLE SUBGROUPS             : {len(usable)}"
)

print(
    f"SAME EXPECTED DIRECTION      : {len(same)}/{len(usable)}"
)

print(
    f"DIRECTIONAL AUC >= 0.55      : {len(auc55)}/{len(usable)}"
)

print(
    f"DIRECTIONAL AUC >= 0.60      : {len(auc60)}/{len(usable)}"
)


if usable:

    vals = [
        a[
            "directional_auc"
        ]
        for a in usable
    ]

    print(
        f"MEDIAN SUBGROUP DIR-AUC      : {med(vals):.3f}"
    )

    print(
        f"WORST SUBGROUP DIR-AUC       : {min(vals):.3f}"
    )

    print(
        f"BEST SUBGROUP DIR-AUC        : {max(vals):.3f}"
    )


# ============================================================
# K) DECISION SUPPORT
# ============================================================

print()
print(
    "=" * 180
)

print(
    "K) DECISION SUPPORT"
)

print(
    "=" * 180
)


if not usable:

    print(
        "INSUFFICIENT COVERAGE TO JUDGE PRIMARY WALLET METRIC."
    )

else:

    direction_rate = (
        len(same)
        / len(usable)
    )

    auc55_rate = (
        len(auc55)
        / len(usable)
    )


    median_auc = med([
        a[
            "directional_auc"
        ]
        for a in usable
    ])


    if (
        direction_rate >= 0.75
        and auc55_rate >= 0.60
        and median_auc >= 0.57
    ):

        print(
            "🟢 PRIMARY WALLET METRIC SHOWS CROSS-SUBGROUP ROBUSTNESS."
        )

        print(
            "Candidate for a future prospective SHADOW recorder."
        )

        print(
            "Do NOT use as a live trading rule yet."
        )


    elif (
        direction_rate >= 0.60
        and median_auc >= 0.54
    ):

        print(
            "🟡 PRIMARY WALLET METRIC SHOWS PARTIAL ROBUSTNESS."
        )

        print(
            "Keep collecting data before prospective integration."
        )


    else:

        print(
            "🔴 PRIMARY WALLET METRIC DOES NOT SURVIVE ROBUSTNESS AUDIT."
        )

        print(
            "Do not integrate buyers_fast_flip_rate into the signal stack."
        )


print()
print(
    "PRIMARY FEATURE:"
)

print(
    f"• {PRIMARY}"
)

print(
    f"• experience threshold frozen at >= {MIN_PRIOR_TRADES} prior trades"
)


print()
print(
    "IMPORTANT:"
)

print(
    "• T44 performs no model fitting."
)

print(
    "• T44 performs no probability-threshold search."
)

print(
    "• Expected direction is frozen from the early 50% only."
)

print(
    "• Later periods cannot redefine feature direction."
)

print(
    "• Wallet history is strictly chronological."
)

print(
    "• Current event outcome never enters prior wallet state."
)

print(
    "• Same-unit swaps.clean_price only."
)

print(
    "• No dex_prices.price_usd."
)

print(
    "• No hardcoded SOL/USD conversion."
)

print(
    "• T23/T31/T32 remain untouched."
)

print(
    "• T44 writes nothing to the database."
)

print(
    "• This is a robustness audit, not a trading rule."
)


db.close()
