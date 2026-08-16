import sqlite3
import math
import statistics
import random
from collections import defaultdict

import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score


# ============================================================
# CONFIG
# ============================================================

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0

THRESHOLDS = [1, 2, 3, 5, 10]

RANDOM_SEED = 42


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


def auc_safe(y, p):
    if len(y) < 3:
        return None

    if len(set(y)) < 2:
        return None

    try:
        return roc_auc_score(y, p)
    except Exception:
        return None


# ============================================================
# DATABASE
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
wallet_exit_ret = defaultdict(list)
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

        if price > st["max_price"]:
            st["max_price"] = price

        if price < st["min_price"]:
            st["min_price"] = price


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
            open_pos.pop(key, None)
            return

        hold = (
            ts
            - st["entry_ts"]
        )

        if hold < 0:
            open_pos.pop(key, None)
            return

        exit_ret = (
            price / ep
            - 1.0
        ) * 100.0

        mfe = (
            st["max_price"] / ep
            - 1.0
        ) * 100.0

        completed[wallet] += 1

        wallet_exit_ret[
            wallet
        ].append(
            exit_ret
        )

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


# ============================================================
# WALLET SNAPSHOT
# ============================================================

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

        "median_exit_return":
            med(
                wallet_exit_ret.get(
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
# BUILD CHRONOLOGICAL EVENT SNAPSHOTS
# ============================================================

records = []


for e in events:

    event_ts = e[
        "timestamp"
    ]

    # Only historical swaps strictly before event
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


    wallets = sorted(
        set(
            x["wallet"]
            for x in pre
        )
    )


    buyers = sorted(
        set(
            x["wallet"]
            for x in pre
            if x["side"] == "BUY"
        )
    )


    wallet_info = {
        w:
        wallet_snapshot(w)
        for w in wallets
    }


    buyer_info = {
        w:
        wallet_snapshot(w)
        for w in buyers
    }


    rec = {
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

        "wallets":
            wallet_info,

        "buyers":
            buyer_info,
    }


    records.append(
        rec
    )


# ============================================================
# FIXED TOKEN SPLIT
#
# Same split for every threshold.
# Threshold is therefore the ONLY thing changing.
# ============================================================

tokens = sorted(
    set(
        r["token_mint"]
        for r in records
    )
)

rng = random.Random(
    RANDOM_SEED
)

rng.shuffle(
    tokens
)


n_tokens = len(tokens)

n_train = int(
    0.60 * n_tokens
)

n_valid = int(
    0.20 * n_tokens
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


def split_records(tokset):

    return [
        r
        for r in records
        if r["token_mint"]
        in tokset
    ]


base_train = split_records(
    train_tokens
)

base_valid = split_records(
    valid_tokens
)

base_test = split_records(
    test_tokens
)


# ============================================================
# THRESHOLD FEATURES
# ============================================================

def threshold_features(rec, threshold):

    experienced_wallets = [
        x
        for x in rec[
            "wallets"
        ].values()
        if x["prior_trades"] >= threshold
    ]


    experienced_buyers = [
        x
        for x in rec[
            "buyers"
        ].values()
        if x["prior_trades"] >= threshold
    ]


    all_wallets = list(
        rec[
            "wallets"
        ].values()
    )


    all_buyers = list(
        rec[
            "buyers"
        ].values()
    )


    exp_wallet_ratio = safe_div(
        len(
            experienced_wallets
        ),
        len(
            all_wallets
        )
    )


    exp_buyer_ratio = safe_div(
        len(
            experienced_buyers
        ),
        len(
            all_buyers
        )
    )


    # -----------------------------------------
    # T42 survivor #1
    # cohort_token_count
    # -----------------------------------------

    cohort_token_count = med([
        x["prior_tokens"]
        for x in experienced_wallets
    ])


    # -----------------------------------------
    # T42 survivor #2
    # buyers_mfe
    # -----------------------------------------

    buyers_mfe = avg([
        x["median_mfe"]
        for x in experienced_buyers
        if valid(
            x["median_mfe"]
        )
    ])


    # -----------------------------------------
    # T42 survivor #3
    # buyers_fast_flip_rate
    # -----------------------------------------

    buyers_fast_flip_rate = avg([
        x["fast_flip_rate"]
        for x in experienced_buyers
        if valid(
            x["fast_flip_rate"]
        )
    ])


    # Additional audit-only contextual metrics

    buyer_prior_trades = med([
        x["prior_trades"]
        for x in experienced_buyers
    ])


    wallet_prior_trades = med([
        x["prior_trades"]
        for x in experienced_wallets
    ])


    buyers_exit_return = avg([
        x["median_exit_return"]
        for x in experienced_buyers
        if valid(
            x["median_exit_return"]
        )
    ])


    return {
        "id":
            rec["id"],

        "token_mint":
            rec["token_mint"],

        "label":
            rec["label"],

        "experienced_wallet_count":
            len(
                experienced_wallets
            ),

        "experienced_buyer_count":
            len(
                experienced_buyers
            ),

        "experienced_wallet_ratio":
            exp_wallet_ratio,

        "experienced_buyer_ratio":
            exp_buyer_ratio,

        "cohort_token_count":
            cohort_token_count,

        "buyers_mfe":
            buyers_mfe,

        "buyers_fast_flip_rate":
            buyers_fast_flip_rate,

        "buyer_prior_trades":
            buyer_prior_trades,

        "wallet_prior_trades":
            wallet_prior_trades,

        "buyers_exit_return":
            buyers_exit_return,
    }


SURVIVORS = [
    "cohort_token_count",
    "buyers_mfe",
    "buyers_fast_flip_rate",
]


AUDIT_FEATURES = [
    "experienced_wallet_ratio",
    "experienced_buyer_ratio",
    "cohort_token_count",
    "buyers_mfe",
    "buyers_fast_flip_rate",
    "buyer_prior_trades",
    "wallet_prior_trades",
    "buyers_exit_return",
]


def transform(rr, threshold):

    return [
        threshold_features(
            r,
            threshold
        )
        for r in rr
    ]


# ============================================================
# MODEL
# ============================================================

def run_model(
    train,
    valid_set,
    test,
    features
):

    usable_train = [
        r for r in train
        if any(
            valid(
                r[f]
            )
            for f in features
        )
    ]

    usable_valid = [
        r for r in valid_set
        if any(
            valid(
                r[f]
            )
            for f in features
        )
    ]

    usable_test = [
        r for r in test
        if any(
            valid(
                r[f]
            )
            for f in features
        )
    ]


    if (
        len(usable_train) < 10
        or len(set(
            r["label"]
            for r in usable_train
        )) < 2
    ):

        return {
            "train_n":
                len(
                    usable_train
                ),

            "valid_n":
                len(
                    usable_valid
                ),

            "test_n":
                len(
                    usable_test
                ),

            "valid_auc":
                None,

            "test_auc":
                None,

            "model":
                None,
        }


    def make_x(rr):

        return np.asarray([
            [
                float(
                    r[f]
                )
                if valid(
                    r[f]
                )
                else np.nan

                for f in features
            ]

            for r in rr
        ], dtype=float)


    def make_y(rr):

        return np.asarray([
            r["label"]
            for r in rr
        ], dtype=int)


    pipe = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="median"
            )
        ),
        (
            "scale",
            StandardScaler()
        ),
        (
            "model",
            LogisticRegression(
                class_weight="balanced",
                max_iter=3000,
                random_state=42
            )
        )
    ])


    X_train = make_x(
        usable_train
    )

    y_train = make_y(
        usable_train
    )


    pipe.fit(
        X_train,
        y_train
    )


    def evaluate(rr):

        if len(rr) < 3:
            return None

        y = make_y(
            rr
        )

        if len(set(y)) < 2:
            return None

        p = pipe.predict_proba(
            make_x(rr)
        )[:, 1]

        return auc_safe(
            y,
            p
        )


    return {
        "train_n":
            len(
                usable_train
            ),

        "valid_n":
            len(
                usable_valid
            ),

        "test_n":
            len(
                usable_test
            ),

        "valid_auc":
            evaluate(
                usable_valid
            ),

        "test_auc":
            evaluate(
                usable_test
            ),

        "model":
            pipe,
    }


# ============================================================
# FIRST EVENT PER TOKEN
# ============================================================

def first_per_token(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x:
            x["id"]
    ):

        token = r[
            "token_mint"
        ]

        if token in seen:
            continue

        seen.add(
            token
        )

        out.append(
            r
        )

    return out


# ============================================================
# OUTPUT
# ============================================================

print(
    "=" * 170
)

print(
    "MEMECOIN LAB — T43 EXPERIENCED WALLET THRESHOLD / STABILITY AUDIT"
)

print(
    "=" * 170
)

print(
    f"LABELED EVENTS WITH WALLET COHORT : {len(records)}"
)

print(
    f"UNIQUE TOKENS                    : {len(tokens)}"
)

print(
    f"TRAIN TOKENS                     : {len(train_tokens)}"
)

print(
    f"VALID TOKENS                     : {len(valid_tokens)}"
)

print(
    f"TEST TOKENS                      : {len(test_tokens)}"
)


# ============================================================
# A) COVERAGE BY EXPERIENCE THRESHOLD
# ============================================================

print()
print(
    "=" * 170
)

print(
    "A) EXPERIENCE THRESHOLD COVERAGE"
)

print(
    "=" * 170
)

print(
    f"{'THR':>4} "
    f"{'EVENTS':>8} "
    f"{'EXP-WALLET':>12} "
    f"{'EXP-BUYER':>11} "
    f"{'MED W RATIO':>13} "
    f"{'MED B RATIO':>13}"
)

print(
    "-" * 75
)


threshold_cache = {}


for threshold in THRESHOLDS:

    all_t = transform(
        records,
        threshold
    )

    threshold_cache[
        threshold
    ] = all_t


    with_wallet = [
        r for r in all_t
        if r[
            "experienced_wallet_count"
        ] > 0
    ]


    with_buyer = [
        r for r in all_t
        if r[
            "experienced_buyer_count"
        ] > 0
    ]


    wr = [
        r[
            "experienced_wallet_ratio"
        ]
        for r in all_t
        if valid(
            r[
                "experienced_wallet_ratio"
            ]
        )
    ]


    br = [
        r[
            "experienced_buyer_ratio"
        ]
        for r in all_t
        if valid(
            r[
                "experienced_buyer_ratio"
            ]
        )
    ]


    print(
        f"{threshold:4d} "
        f"{len(all_t):8d} "
        f"{len(with_wallet):12d} "
        f"{len(with_buyer):11d} "
        f"{(med(wr) or 0):13.3f} "
        f"{(med(br) or 0):13.3f}"
    )


# ============================================================
# B) SURVIVOR DIRECTION BY THRESHOLD
# ============================================================

print()
print(
    "=" * 170
)

print(
    "B) T42 SURVIVOR DIRECTION BY EXPERIENCE THRESHOLD"
)

print(
    "=" * 170
)


for threshold in THRESHOLDS:

    print()
    print(
        f"THRESHOLD >= {threshold} PRIOR COMPLETED TRADES"
    )

    print(
        "-" * 110
    )


    tr = transform(
        base_train,
        threshold
    )

    va = transform(
        base_valid,
        threshold
    )

    te = transform(
        base_test,
        threshold
    )


    for feature in SURVIVORS:

        diffs = []

        counts = []

        for rr in [
            tr,
            va,
            te
        ]:

            run = [
                r[feature]
                for r in rr
                if (
                    r["label"] == 1
                    and valid(
                        r[feature]
                    )
                )
            ]

            dump = [
                r[feature]
                for r in rr
                if (
                    r["label"] == 0
                    and valid(
                        r[feature]
                    )
                )
            ]

            counts.append(
                (
                    len(run),
                    len(dump)
                )
            )

            if not run or not dump:
                diffs.append(
                    None
                )
            else:
                diffs.append(
                    med(run)
                    - med(dump)
                )


        same = False

        if all(
            valid(x)
            for x in diffs
        ):

            signs = [
                1 if x > 0
                else -1 if x < 0
                else 0
                for x in diffs
            ]

            same = (
                signs[0] != 0
                and signs[0]
                == signs[1]
                == signs[2]
            )


        print(
            f"{feature:30} "
            f"TRAIN={str(diffs[0]):>11} "
            f"VALID={str(diffs[1]):>11} "
            f"TEST={str(diffs[2]):>11} "
            f"SAME={str(same):5} "
            f"N={counts}"
        )


# ============================================================
# C) THREE-FEATURE MODEL BY THRESHOLD
# ============================================================

print()
print(
    "=" * 170
)

print(
    "C) FROZEN THREE-FEATURE MODEL BY EXPERIENCE THRESHOLD"
)

print(
    "=" * 170
)

print(
    "FEATURES = cohort_token_count + buyers_mfe + buyers_fast_flip_rate"
)

print()

print(
    f"{'THR':>4} "
    f"{'TRAIN N':>9} "
    f"{'VALID N':>9} "
    f"{'TEST N':>8} "
    f"{'VALID AUC':>11} "
    f"{'TEST AUC':>10}"
)

print(
    "-" * 65
)


results = {}


for threshold in THRESHOLDS:

    tr = transform(
        base_train,
        threshold
    )

    va = transform(
        base_valid,
        threshold
    )

    te = transform(
        base_test,
        threshold
    )


    res = run_model(
        tr,
        va,
        te,
        SURVIVORS
    )


    results[
        threshold
    ] = res


    va_auc = (
        f"{res['valid_auc']:.3f}"
        if res[
            "valid_auc"
        ] is not None
        else "NA"
    )


    te_auc = (
        f"{res['test_auc']:.3f}"
        if res[
            "test_auc"
        ] is not None
        else "NA"
    )


    print(
        f"{threshold:4d} "
        f"{res['train_n']:9d} "
        f"{res['valid_n']:9d} "
        f"{res['test_n']:8d} "
        f"{va_auc:>11} "
        f"{te_auc:>10}"
    )


# ============================================================
# D) EXPERIENCED COHORT ONLY
# ============================================================

print()
print(
    "=" * 170
)

print(
    "D) EXPERIENCED-WALLET COHORT ONLY"
)

print(
    "=" * 170
)

print(
    "An event remains only if >=25% of observed wallets satisfy the experience threshold."
)

print()

print(
    f"{'THR':>4} "
    f"{'TRAIN':>8} "
    f"{'VALID':>8} "
    f"{'TEST':>8} "
    f"{'VALID AUC':>11} "
    f"{'TEST AUC':>10}"
)

print(
    "-" * 60
)


cohort_results = {}


for threshold in THRESHOLDS:

    tr = [
        r
        for r in transform(
            base_train,
            threshold
        )
        if (
            valid(
                r[
                    "experienced_wallet_ratio"
                ]
            )
            and r[
                "experienced_wallet_ratio"
            ] >= 0.25
        )
    ]


    va = [
        r
        for r in transform(
            base_valid,
            threshold
        )
        if (
            valid(
                r[
                    "experienced_wallet_ratio"
                ]
            )
            and r[
                "experienced_wallet_ratio"
            ] >= 0.25
        )
    ]


    te = [
        r
        for r in transform(
            base_test,
            threshold
        )
        if (
            valid(
                r[
                    "experienced_wallet_ratio"
                ]
            )
            and r[
                "experienced_wallet_ratio"
            ] >= 0.25
        )
    ]


    res = run_model(
        tr,
        va,
        te,
        SURVIVORS
    )


    cohort_results[
        threshold
    ] = res


    va_auc = (
        f"{res['valid_auc']:.3f}"
        if res[
            "valid_auc"
        ] is not None
        else "NA"
    )


    te_auc = (
        f"{res['test_auc']:.3f}"
        if res[
            "test_auc"
        ] is not None
        else "NA"
    )


    print(
        f"{threshold:4d} "
        f"{len(tr):8d} "
        f"{len(va):8d} "
        f"{len(te):8d} "
        f"{va_auc:>11} "
        f"{te_auc:>10}"
    )


# ============================================================
# E) FIRST EVENT / TOKEN AUDIT
# ============================================================

print()
print(
    "=" * 170
)

print(
    "E) FIRST-EVENT/TOKEN AUDIT"
)

print(
    "=" * 170
)


for threshold in THRESHOLDS:

    tr = transform(
        base_train,
        threshold
    )

    va = first_per_token(
        transform(
            base_valid,
            threshold
        )
    )

    te = first_per_token(
        transform(
            base_test,
            threshold
        )
    )


    res = run_model(
        tr,
        va,
        te,
        SURVIVORS
    )


    va_auc = (
        f"{res['valid_auc']:.3f}"
        if res[
            "valid_auc"
        ] is not None
        else "NA"
    )


    te_auc = (
        f"{res['test_auc']:.3f}"
        if res[
            "test_auc"
        ] is not None
        else "NA"
    )


    print(
        f"THR>={threshold:2d} "
        f"| VALID FIRST N={len(va):3d} "
        f"AUC={va_auc} "
        f"| TEST FIRST N={len(te):3d} "
        f"AUC={te_auc}"
    )


# ============================================================
# F) SINGLE FEATURE AUDIT
# ============================================================

print()
print(
    "=" * 170
)

print(
    "F) SINGLE-FEATURE STABILITY"
)

print(
    "=" * 170
)


for feature in SURVIVORS:

    print()
    print(
        feature
    )

    print(
        "-" * 80
    )


    for threshold in THRESHOLDS:

        tr = transform(
            base_train,
            threshold
        )

        va = transform(
            base_valid,
            threshold
        )

        te = transform(
            base_test,
            threshold
        )


        res = run_model(
            tr,
            va,
            te,
            [feature]
        )


        va_auc = (
            f"{res['valid_auc']:.3f}"
            if res[
                "valid_auc"
            ] is not None
            else "NA"
        )


        te_auc = (
            f"{res['test_auc']:.3f}"
            if res[
                "test_auc"
            ] is not None
            else "NA"
        )


        print(
            f"THR>={threshold:2d} "
            f"| VALID={va_auc} "
            f"| TEST={te_auc} "
            f"| N="
            f"{res['valid_n']}/"
            f"{res['test_n']}"
        )


# ============================================================
# G) SAMPLE SIZE / RELIABILITY WARNING
# ============================================================

print()
print(
    "=" * 170
)

print(
    "G) SAMPLE-SIZE RELIABILITY"
)

print(
    "=" * 170
)


for threshold in THRESHOLDS:

    all_t = threshold_cache[
        threshold
    ]


    usable = [
        r
        for r in all_t
        if (
            r[
                "experienced_wallet_count"
            ] > 0
        )
    ]


    unique = len(
        set(
            r[
                "token_mint"
            ]
            for r in usable
        )
    )


    if unique >= 30:
        quality = "GOOD"

    elif unique >= 15:
        quality = "LIMITED"

    else:
        quality = "VERY THIN"


    print(
        f"THR>={threshold:2d} "
        f"| EVENTS={len(usable):4d} "
        f"| TOKENS={unique:3d} "
        f"| RELIABILITY={quality}"
    )


# ============================================================
# H) DECISION SUPPORT
# ============================================================

print()
print(
    "=" * 170
)

print(
    "H) DECISION SUPPORT"
)

print(
    "=" * 170
)


candidate_thresholds = []


for threshold in THRESHOLDS:

    r = results[
        threshold
    ]

    if (
        r["valid_auc"] is not None
        and r["valid_auc"] >= 0.60
        and r["valid_n"] >= 15
    ):

        candidate_thresholds.append(
            threshold
        )


if not candidate_thresholds:

    print(
        "NO EXPERIENCE THRESHOLD PASSES THE VALIDATION GATE."
    )

    print(
        "Do not build a prospective wallet score yet."
    )

else:

    best = max(
        candidate_thresholds,
        key=lambda t:
            results[t][
                "valid_auc"
            ]
    )


    print(
        f"VALIDATION CANDIDATE THRESHOLD = >= {best} PRIOR TRADES"
    )

    print(
        f"VALID AUC = "
        f"{results[best]['valid_auc']:.3f}"
    )


    if results[best][
        "test_auc"
    ] is not None:

        print(
            f"FINAL TEST AUDIT = "
            f"{results[best]['test_auc']:.3f}"
        )


    print()
    print(
        "IMPORTANT: threshold selection above uses VALIDATION only."
    )

    print(
        "TEST is displayed only as final audit and must not be used "
        "to choose another threshold."
    )


print()
print(
    "T42 FROZEN SURVIVORS:"
)

for f in SURVIVORS:
    print(
        "•",
        f
    )


print()
print("IMPORTANT:")
print("• Historical wallet state is strictly chronological.")
print("• Only swaps strictly before the current event enter wallet history.")
print("• Same-unit swaps.clean_price only.")
print("• No dex_prices.price_usd.")
print("• No hardcoded SOL/USD conversion.")
print("• Token identities do not cross TRAIN / VALID / TEST.")
print("• Threshold comparison uses identical token splits.")
print("• TEST must not be used to retune the threshold.")
print("• T23/T31/T32 remain untouched.")
print("• T43 writes nothing to the database.")
print("• T43 is a research audit, not a trading rule.")


db.close()
