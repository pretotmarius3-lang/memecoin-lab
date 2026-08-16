import sqlite3
import math
import random
import statistics
from collections import defaultdict, deque

import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

RANDOM_SEED = 42

# Event wallet observation window
PRE_EVENT_SEC = 30.0

# Historical wallet skill needs enough prior actions before
# being considered "experienced"
MIN_PRIOR_ACTIONS = 3


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
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def label_from_r60(x):
    if not valid(x):
        return None

    if x >= RUNNER:
        return 1

    if x <= DUMP:
        return 0

    return None


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# LOAD EVENTS
# ============================================================

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

labeled_events = [
    e for e in events
    if label_from_r60(e["dex_return_60s"]) is not None
]


# ============================================================
# LOAD SWAPS
# ============================================================

swaps = db.execute("""
SELECT
    signature,
    timestamp,
    wallet,
    side,
    token_mint,
    sol_delta,
    token_delta,
    clean_price,
    price_valid,
    program
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
# INDEX SWAPS BY TOKEN
# ============================================================

by_token = defaultdict(list)

for s in swaps:
    by_token[s["token_mint"]].append(s)


# ============================================================
# EVENT-DRIVEN ACTION METRICS
#
# For every historical BUY we derive metrics only when enough
# subsequent SAME-UNIT swaps already exist before the current
# event timestamp.
# ============================================================

# Per wallet history
wallet_holding_times = defaultdict(list)
wallet_mfe = defaultdict(list)
wallet_mae = defaultdict(list)
wallet_exit_returns = defaultdict(list)

wallet_good_entry = defaultdict(int)
wallet_bad_entry = defaultdict(int)

wallet_sell_before_drop = defaultdict(int)
wallet_sell_after_strength = defaultdict(int)

wallet_completed_trades = defaultdict(int)
wallet_fast_flips = defaultdict(int)

wallet_tokens = defaultdict(set)

# Open BUY queues by wallet-token
open_buys = defaultdict(deque)

# Last local token price
last_price_by_token = {}

# History of local token prices since last buy is handled
# through open position state.
open_state = {}

swap_idx = 0


def wallet_key(wallet, token):
    return (wallet, token)


def new_open_state(ts, price):
    return {
        "entry_ts": ts,
        "entry_price": price,
        "max_price": price,
        "min_price": price,
    }


def update_open_paths(token, price):
    """
    Update all currently open wallet positions for this token
    with latest observed same-unit price.
    """
    if not valid(price) or price <= 0:
        return

    for key, st in list(open_state.items()):
        w, tok = key

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


def close_position(wallet, token, ts, exit_price):
    key = wallet_key(wallet, token)

    st = open_state.get(key)

    if st is None:
        return

    entry_price = st["entry_price"]
    entry_ts = st["entry_ts"]

    if (
        not valid(entry_price)
        or entry_price <= 0
        or not valid(exit_price)
        or exit_price <= 0
    ):
        open_state.pop(key, None)
        return

    hold = ts - entry_ts

    if hold < 0:
        open_state.pop(key, None)
        return

    ret = (
        exit_price / entry_price
        - 1.0
    ) * 100.0

    mfe = (
        st["max_price"] / entry_price
        - 1.0
    ) * 100.0

    mae = (
        st["min_price"] / entry_price
        - 1.0
    ) * 100.0

    wallet_holding_times[wallet].append(
        hold
    )

    wallet_exit_returns[wallet].append(
        ret
    )

    wallet_mfe[wallet].append(
        mfe
    )

    wallet_mae[wallet].append(
        mae
    )

    wallet_completed_trades[wallet] += 1

    if hold <= 60:
        wallet_fast_flips[wallet] += 1

    # Entry quality
    if mfe >= 10:
        wallet_good_entry[wallet] += 1

    if mae <= -10:
        wallet_bad_entry[wallet] += 1

    # Sell timing proxies
    if ret > 0 and mfe > ret:
        wallet_sell_after_strength[wallet] += 1

    if mae <= -10 and ret > mae:
        wallet_sell_before_drop[wallet] += 1

    open_state.pop(
        key,
        None
    )


def process_swap(s):
    wallet = s["wallet"]
    token = s["token_mint"]
    side = s["side"]
    price = s["clean_price"]

    if not valid(price) or price <= 0:
        return

    wallet_tokens[wallet].add(
        token
    )

    # First update any open paths with this new market observation
    update_open_paths(
        token,
        price
    )

    key = wallet_key(
        wallet,
        token
    )

    if side == "BUY":

        # If wallet already has an open position proxy,
        # keep earliest buy as anchor.
        if key not in open_state:
            open_state[key] = new_open_state(
                s["timestamp"],
                price
            )

    elif side == "SELL":

        close_position(
            wallet,
            token,
            s["timestamp"],
            price
        )

    last_price_by_token[
        token
    ] = price


# ============================================================
# WALLET SKILL FEATURES
# ============================================================

def wallet_skill(wallet):

    completed = wallet_completed_trades[
        wallet
    ]

    exit_rets = wallet_exit_returns.get(
        wallet,
        []
    )

    mfes = wallet_mfe.get(
        wallet,
        []
    )

    maes = wallet_mae.get(
        wallet,
        []
    )

    holds = wallet_holding_times.get(
        wallet,
        []
    )

    skill_known = (
        completed >= MIN_PRIOR_ACTIONS
    )

    good_bad_total = (
        wallet_good_entry[wallet]
        + wallet_bad_entry[wallet]
    )

    return {
        "experienced":
            1.0 if skill_known else 0.0,

        "completed_trades":
            completed,

        "token_count":
            len(
                wallet_tokens[wallet]
            ),

        "median_holding_sec":
            med(holds),

        "median_exit_return":
            med(exit_rets),

        "avg_exit_return":
            avg(exit_rets),

        "positive_exit_rate":
            safe_div(
                sum(x > 0 for x in exit_rets),
                len(exit_rets)
            ),

        "median_mfe":
            med(mfes),

        "median_mae":
            med(maes),

        "entry_skill_rate":
            safe_div(
                wallet_good_entry[wallet],
                good_bad_total
            ),

        "bad_entry_rate":
            safe_div(
                wallet_bad_entry[wallet],
                good_bad_total
            ),

        "fast_flip_rate":
            safe_div(
                wallet_fast_flips[wallet],
                completed
            ),

        "sell_before_drop_rate":
            safe_div(
                wallet_sell_before_drop[wallet],
                completed
            ),

        "sell_after_strength_rate":
            safe_div(
                wallet_sell_after_strength[wallet],
                completed
            ),
    }


# ============================================================
# PRE-EVENT COHORT
# ============================================================

def pre_event_swaps(event):
    return db.execute("""
    SELECT
        timestamp,
        wallet,
        side,
        clean_price,
        sol_delta
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
        AND clean_price IS NOT NULL
        AND clean_price > 0
    ORDER BY timestamp
    """, (
        event["token_mint"],
        event["timestamp"] - PRE_EVENT_SEC,
        event["timestamp"],
    )).fetchall()


# ============================================================
# BUILD EVENT FEATURES CHRONOLOGICALLY
# ============================================================

records = []

for e in labeled_events:

    # Advance historical wallet state only with swaps
    # strictly prior to event.
    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"]
        < e["timestamp"]
    ):
        process_swap(
            swaps[swap_idx]
        )
        swap_idx += 1

    pre = pre_event_swaps(e)

    if not pre:
        continue

    wallets = sorted(
        set(
            r["wallet"]
            for r in pre
        )
    )

    buyers = sorted(
        set(
            r["wallet"]
            for r in pre
            if r["side"] == "BUY"
        )
    )

    sellers = sorted(
        set(
            r["wallet"]
            for r in pre
            if r["side"] == "SELL"
        )
    )

    skills = [
        wallet_skill(w)
        for w in wallets
    ]

    buyer_skills = [
        wallet_skill(w)
        for w in buyers
    ]

    seller_skills = [
        wallet_skill(w)
        for w in sellers
    ]

    def mean_feature(arr, name):
        vals = [
            x[name]
            for x in arr
            if valid(x[name])
        ]
        return avg(vals)

    def median_feature(arr, name):
        vals = [
            x[name]
            for x in arr
            if valid(x[name])
        ]
        return med(vals)

    experienced = sum(
        x["experienced"] > 0
        for x in skills
    )

    experienced_buyers = sum(
        x["experienced"] > 0
        for x in buyer_skills
    )

    experienced_sellers = sum(
        x["experienced"] > 0
        for x in seller_skills
    )

    rec = {
        "id":
            e["id"],

        "token_mint":
            e["token_mint"],

        "r60":
            e["dex_return_60s"],

        "label":
            label_from_r60(
                e["dex_return_60s"]
            ),

        "wallet_count":
            len(wallets),

        "buyer_count":
            len(buyers),

        "seller_count":
            len(sellers),

        "experienced_wallet_ratio":
            safe_div(
                experienced,
                len(wallets)
            ),

        "experienced_buyer_ratio":
            safe_div(
                experienced_buyers,
                len(buyers)
            ),

        "experienced_seller_ratio":
            safe_div(
                experienced_sellers,
                len(sellers)
            ),

        "cohort_completed_trades":
            median_feature(
                skills,
                "completed_trades"
            ),

        "cohort_token_count":
            median_feature(
                skills,
                "token_count"
            ),

        "cohort_holding_sec":
            median_feature(
                skills,
                "median_holding_sec"
            ),

        "cohort_exit_return":
            mean_feature(
                skills,
                "median_exit_return"
            ),

        "cohort_positive_exit_rate":
            mean_feature(
                skills,
                "positive_exit_rate"
            ),

        "cohort_mfe":
            mean_feature(
                skills,
                "median_mfe"
            ),

        "cohort_mae":
            mean_feature(
                skills,
                "median_mae"
            ),

        "cohort_entry_skill_rate":
            mean_feature(
                skills,
                "entry_skill_rate"
            ),

        "cohort_bad_entry_rate":
            mean_feature(
                skills,
                "bad_entry_rate"
            ),

        "cohort_fast_flip_rate":
            mean_feature(
                skills,
                "fast_flip_rate"
            ),

        "cohort_sell_before_drop_rate":
            mean_feature(
                skills,
                "sell_before_drop_rate"
            ),

        "cohort_sell_after_strength_rate":
            mean_feature(
                skills,
                "sell_after_strength_rate"
            ),

        # Buyer-only quality
        "buyers_exit_return":
            mean_feature(
                buyer_skills,
                "median_exit_return"
            ),

        "buyers_entry_skill_rate":
            mean_feature(
                buyer_skills,
                "entry_skill_rate"
            ),

        "buyers_bad_entry_rate":
            mean_feature(
                buyer_skills,
                "bad_entry_rate"
            ),

        "buyers_mfe":
            mean_feature(
                buyer_skills,
                "median_mfe"
            ),

        "buyers_mae":
            mean_feature(
                buyer_skills,
                "median_mae"
            ),

        "buyers_fast_flip_rate":
            mean_feature(
                buyer_skills,
                "fast_flip_rate"
            ),

        # Seller-only quality
        "sellers_exit_return":
            mean_feature(
                seller_skills,
                "median_exit_return"
            ),

        "sellers_sell_before_drop_rate":
            mean_feature(
                seller_skills,
                "sell_before_drop_rate"
            ),

        "sellers_sell_after_strength_rate":
            mean_feature(
                seller_skills,
                "sell_after_strength_rate"
            ),
    }

    records.append(rec)


FEATURES = [
    "wallet_count",
    "buyer_count",
    "seller_count",

    "experienced_wallet_ratio",
    "experienced_buyer_ratio",
    "experienced_seller_ratio",

    "cohort_completed_trades",
    "cohort_token_count",
    "cohort_holding_sec",

    "cohort_exit_return",
    "cohort_positive_exit_rate",

    "cohort_mfe",
    "cohort_mae",

    "cohort_entry_skill_rate",
    "cohort_bad_entry_rate",

    "cohort_fast_flip_rate",

    "cohort_sell_before_drop_rate",
    "cohort_sell_after_strength_rate",

    "buyers_exit_return",
    "buyers_entry_skill_rate",
    "buyers_bad_entry_rate",
    "buyers_mfe",
    "buyers_mae",
    "buyers_fast_flip_rate",

    "sellers_exit_return",
    "sellers_sell_before_drop_rate",
    "sellers_sell_after_strength_rate",
]


print("=" * 160)
print("MEMECOIN LAB — T42 EVENT-DRIVEN WALLET SKILL AUDIT")
print("=" * 160)

print(
    f"LABELED EVENTS            : "
    f"{len(labeled_events)}"
)

print(
    f"EVENTS WITH WALLET COHORT : "
    f"{len(records)}"
)

print(
    f"UNIQUE TOKENS             : "
    f"{len(set(r['token_mint'] for r in records))}"
)


# ============================================================
# A) COVERAGE
# ============================================================

print()
print("=" * 160)
print("A) EVENT-DRIVEN SKILL COVERAGE")
print("=" * 160)

for name in [
    "experienced_wallet_ratio",
    "experienced_buyer_ratio",
    "cohort_exit_return",
    "cohort_mfe",
    "cohort_mae",
    "buyers_entry_skill_rate",
    "sellers_sell_before_drop_rate",
]:

    vals = [
        r[name]
        for r in records
        if valid(r[name])
    ]

    print(
        f"{name:38} "
        f"| N={len(vals):4d}/{len(records)} "
        f"| COVERAGE="
        f"{100*len(vals)/len(records):6.1f}% "
        f"| MED="
        f"{med(vals) if vals else None}"
    )


# ============================================================
# B) GLOBAL SEPARATION
# ============================================================

print()
print("=" * 160)
print("B) RUNNER VS DUMP FEATURE SEPARATION")
print("=" * 160)

scores = []

for name in FEATURES:

    run = [
        r[name]
        for r in records
        if (
            r["label"] == 1
            and valid(r[name])
        )
    ]

    dump = [
        r[name]
        for r in records
        if (
            r["label"] == 0
            and valid(r[name])
        )
    ]

    if len(run) < 2 or len(dump) < 2:
        continue

    rm = med(run)
    dm = med(dump)

    pooled = statistics.pstdev(
        run + dump
    )

    sep = (
        abs(rm-dm) / pooled
        if pooled > 0
        else 0
    )

    scores.append({
        "name":
            name,

        "run_med":
            rm,

        "dump_med":
            dm,

        "diff":
            rm-dm,

        "sep":
            sep,

        "run_n":
            len(run),

        "dump_n":
            len(dump),
    })


scores.sort(
    key=lambda x:
        x["sep"],
    reverse=True
)


print(
    f"{'FEATURE':40} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8} "
    f"{'N':>9}"
)

print("-" * 110)

for x in scores[:25]:

    print(
        f"{x['name']:40} "
        f"{x['run_med']:+11.4f} "
        f"{x['dump_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f} "
        f"{x['run_n']:4d}/{x['dump_n']:4d}"
    )


# ============================================================
# TOKEN HOLDOUT SPLIT
# ============================================================

tokens = sorted(
    set(
        r["token_mint"]
        for r in records
    )
)

random.seed(
    RANDOM_SEED
)

random.shuffle(
    tokens
)

n = len(tokens)

n_train = int(
    n * 0.60
)

n_val = int(
    n * 0.20
)

train_tokens = set(
    tokens[:n_train]
)

val_tokens = set(
    tokens[
        n_train:
        n_train+n_val
    ]
)

test_tokens = set(
    tokens[
        n_train+n_val:
    ]
)


def subset(tokset):
    return [
        r for r in records
        if r["token_mint"] in tokset
    ]


train = subset(
    train_tokens
)

val = subset(
    val_tokens
)

test = subset(
    test_tokens
)


print()
print("=" * 160)
print("C) TOKEN HOLDOUT SPLITS")
print("=" * 160)

for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    print(
        f"{name:8} "
        f"| N={len(rr):4d} "
        f"| TOK="
        f"{len(set(r['token_mint'] for r in rr)):3d} "
        f"| RUN="
        f"{sum(r['label']==1 for r in rr):3d} "
        f"| DUMP="
        f"{sum(r['label']==0 for r in rr):3d}"
    )


# ============================================================
# D) DIRECTION SURVIVAL
# ============================================================

print()
print("=" * 160)
print("D) FEATURE DIRECTION SURVIVAL")
print("=" * 160)

survivors = []

for name in FEATURES:

    diffs = []

    for rr in [
        train,
        val,
        test
    ]:

        run = [
            r[name]
            for r in rr
            if (
                r["label"] == 1
                and valid(r[name])
            )
        ]

        dump = [
            r[name]
            for r in rr
            if (
                r["label"] == 0
                and valid(r[name])
            )
        ]

        if not run or not dump:
            diffs.append(None)
            continue

        diffs.append(
            med(run) - med(dump)
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
            and signs[0] == signs[1] == signs[2]
        )

    if same:
        survivors.append(
            name
        )

    print(
        f"{name:40} "
        f"TRAIN={str(diffs[0]):>12} "
        f"VALID={str(diffs[1]):>12} "
        f"TEST={str(diffs[2]):>12} "
        f"SAME={same}"
    )


# ============================================================
# E) MODEL
# ============================================================

selected = [
    x["name"]
    for x in scores[:12]
]


def X_for(rr):

    return np.asarray([
        [
            float(r[name])
            if valid(r[name])
            else np.nan
            for name in selected
        ]
        for r in rr
    ], dtype=float)


def y_for(rr):

    return np.asarray(
        [
            r["label"]
            for r in rr
        ],
        dtype=int
    )


X_train = X_for(
    train
)

X_val = X_for(
    val
)

X_test = X_for(
    test
)

y_train = y_for(
    train
)

y_val = y_for(
    val
)

y_test = y_for(
    test
)


pipe = Pipeline([
    (
        "imp",
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


pipe.fit(
    X_train,
    y_train
)


def get_auc(X, y):

    prob = pipe.predict_proba(
        X
    )[:, 1]

    if len(set(y)) < 2:
        return prob, None

    return (
        prob,
        roc_auc_score(
            y,
            prob
        )
    )


val_prob, val_auc = get_auc(
    X_val,
    y_val
)

test_prob, test_auc = get_auc(
    X_test,
    y_test
)


print()
print("=" * 160)
print("E) EVENT-DRIVEN WALLET MODEL — TOKEN HOLDOUT")
print("=" * 160)

print(
    f"VALID AUC = "
    f"{val_auc:.3f}"
    if val_auc is not None
    else "VALID AUC = NA"
)

print(
    f"TEST  AUC = "
    f"{test_auc:.3f}"
    if test_auc is not None
    else "TEST AUC = NA"
)


# ============================================================
# F) FIRST EVENT / TOKEN
# ============================================================

def first_per_token(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x:
            x["id"]
    ):

        tok = r[
            "token_mint"
        ]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


print()
print("=" * 160)
print("F) FIRST-EVENT/TOKEN")
print("=" * 160)

for title, rr in [
    (
        "VALID FIRST",
        first_per_token(val)
    ),
    (
        "TEST FIRST",
        first_per_token(test)
    ),
]:

    X = X_for(
        rr
    )

    y = y_for(
        rr
    )

    _, auc = get_auc(
        X,
        y
    )

    print(
        f"{title:12} "
        f"| N={len(rr):3d} "
        f"| AUC="
        f"{auc:.3f}"
        if auc is not None
        else "AUC=NA"
    )


# ============================================================
# G) EXPERIENCED-WALLET SUBSET
# ============================================================

print()
print("=" * 160)
print("G) EXPERIENCED-WALLET SUBSET")
print("=" * 160)

for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    exp = [
        r for r in rr
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

    if len(exp) < 3:

        print(
            f"{name:8} "
            f"| insufficient experienced-wallet sample"
        )

        continue

    X = X_for(
        exp
    )

    y = y_for(
        exp
    )

    _, auc = get_auc(
        X,
        y
    )

    print(
        f"{name:8} "
        f"| N={len(exp):3d} "
        f"| TOK="
        f"{len(set(r['token_mint'] for r in exp)):3d} "
        f"| AUC="
        f"{auc:.3f}"
        if auc is not None
        else "AUC=NA"
    )


# ============================================================
# H) FEATURE WEIGHTS
# ============================================================

weights = list(
    zip(
        selected,
        pipe.named_steps[
            "model"
        ].coef_[0]
    )
)

weights.sort(
    key=lambda x:
        abs(x[1]),
    reverse=True
)


print()
print("=" * 160)
print("H) STANDARDIZED FEATURE WEIGHTS")
print("=" * 160)

for name, weight in weights:

    print(
        f"{name:42} "
        f"{weight:+.4f}"
    )


# ============================================================
# I) DECISION SUPPORT
# ============================================================

print()
print("=" * 160)
print("I) DECISION SUPPORT")
print("=" * 160)

good = (
    val_auc is not None
    and test_auc is not None
    and val_auc >= 0.60
    and test_auc >= 0.60
    and len(survivors) >= 3
)


if good:

    print(
        "✅ EVENT-DRIVEN WALLET SKILL "
        "SHOWS TOKEN-HOLDOUT GENERALIZATION."
    )

    print(
        "Do NOT integrate into V2."
    )

    print(
        "Next step = isolate the strongest wallet skill "
        "components and freeze a prospective shadow score."
    )

else:

    print(
        "NO ROBUST EVENT-DRIVEN WALLET EDGE YET."
    )

    print(
        "Do not force wallet skill into V2."
    )

    print(
        "Use the coverage/direction results to decide "
        "whether richer wallet history is worth collecting."
    )


print()
print(
    f"SAME-DIRECTION FEATURES = "
    f"{len(survivors)}"
)

for name in survivors:
    print(
        "•",
        name
    )


print()
print("IMPORTANT:")
print("• No fixed-time post-buy return is used.")
print("• Same-unit swaps.clean_price only.")
print("• Wallet skill history is chronological.")
print("• Current/future event labels are never used in wallet features.")
print("• Token identities do not cross TRAIN/VALID/TEST.")
print("• TEST is final audit only.")
print("• T23/T31/T32 remain untouched.")
print("• T42 is research only, not a trading rule.")

db.close()
