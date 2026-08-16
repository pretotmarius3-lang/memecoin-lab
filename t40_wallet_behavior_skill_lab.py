import sqlite3
import math
import random
import statistics
from collections import defaultdict

import numpy as np

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

LOOKBACK_SEC = 30.0
POST_BUY_HORIZON = 60.0

RANDOM_SEED = 42


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
# EVENTS
# ============================================================

events = db.execute("""
SELECT
    id,
    token_mint,
    timestamp,
    dex_return_60s

FROM events

WHERE
    token_mint IS NOT NULL
    AND timestamp IS NOT NULL
    AND dex_return_60s IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


labeled_events = [
    e for e in events
    if label_from_r60(e["dex_return_60s"]) is not None
]


# ============================================================
# SWAPS
# ============================================================

swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    sol_delta,
    clean_price

FROM swaps

WHERE
    wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND timestamp IS NOT NULL
    AND side IN ('BUY','SELL')

ORDER BY timestamp
""").fetchall()


# ============================================================
# DEX PRICE CACHE
# ============================================================

dex_by_token = defaultdict(list)

for r in db.execute("""
SELECT
    token_mint,
    timestamp,
    price_usd

FROM dex_prices

WHERE
    token_mint IS NOT NULL
    AND timestamp IS NOT NULL
    AND price_usd IS NOT NULL
    AND price_usd > 0

ORDER BY token_mint, timestamp
""").fetchall():

    dex_by_token[r["token_mint"]].append(
        (
            r["timestamp"],
            r["price_usd"]
        )
    )


def nearest_price_after(token, ts, max_delay=POST_BUY_HORIZON):
    arr = dex_by_token.get(token, [])

    for t, p in arr:
        if t >= ts:
            if t - ts <= max_delay:
                return p
            return None

    return None


# ============================================================
# HISTORICAL WALLET BEHAVIOR STATE
# ============================================================

wallet_buy_count = defaultdict(int)
wallet_sell_count = defaultdict(int)

wallet_tokens = defaultdict(set)

wallet_fast_flip_count = defaultdict(int)
wallet_completed_roundtrips = defaultdict(int)

wallet_post_buy_returns = defaultdict(list)

wallet_buy_before_runner = defaultdict(int)
wallet_buy_before_dump = defaultdict(int)

wallet_sell_before_runner = defaultdict(int)
wallet_sell_before_dump = defaultdict(int)

wallet_event_count = defaultdict(int)

wallet_last_buy_ts_token = {}

# historical event labels assigned only AFTER event feature creation
token_event_history = defaultdict(list)

swap_idx = 0


def update_swap_history(s):
    w = s["wallet"]
    tok = s["token_mint"]
    side = s["side"]

    wallet_tokens[w].add(tok)

    if side == "BUY":
        wallet_buy_count[w] += 1
        wallet_last_buy_ts_token[(w, tok)] = s["timestamp"]

        p0 = s["clean_price"]
        p1 = nearest_price_after(
            tok,
            s["timestamp"] + POST_BUY_HORIZON,
            max_delay=15
        )

        if valid(p0) and p0 > 0 and valid(p1):
            r = (p1 / p0 - 1) * 100
            wallet_post_buy_returns[w].append(r)

    elif side == "SELL":
        wallet_sell_count[w] += 1

        key = (w, tok)

        if key in wallet_last_buy_ts_token:
            dt = (
                s["timestamp"]
                - wallet_last_buy_ts_token[key]
            )

            if dt >= 0:
                wallet_completed_roundtrips[w] += 1

                if dt <= 60:
                    wallet_fast_flip_count[w] += 1


def pre_signal_swaps(event):
    return db.execute("""
        SELECT
            timestamp,
            wallet,
            side,
            sol_delta

        FROM swaps

        WHERE
            token_mint=?
            AND timestamp >= ?
            AND timestamp < ?
            AND wallet IS NOT NULL

        ORDER BY timestamp
    """, (
        event["token_mint"],
        event["timestamp"] - LOOKBACK_SEC,
        event["timestamp"]
    )).fetchall()


def wallet_behavior_features(w):

    buys = wallet_buy_count[w]
    sells = wallet_sell_count[w]

    roundtrips = wallet_completed_roundtrips[w]

    post = wallet_post_buy_returns.get(w, [])

    prior_event_n = wallet_event_count[w]

    buy_outcomes = (
        wallet_buy_before_runner[w]
        + wallet_buy_before_dump[w]
    )

    sell_outcomes = (
        wallet_sell_before_runner[w]
        + wallet_sell_before_dump[w]
    )

    return {
        "prior_swap_count":
            buys + sells,

        "prior_token_count":
            len(wallet_tokens[w]),

        "buy_sell_ratio":
            safe_div(
                buys,
                buys + sells
            ),

        "fast_flip_ratio":
            safe_div(
                wallet_fast_flip_count[w],
                roundtrips
            ),

        "median_post_buy_return":
            med(post),

        "avg_post_buy_return":
            avg(post),

        "positive_post_buy_rate":
            (
                safe_div(
                    sum(x > 0 for x in post),
                    len(post)
                )
                if post
                else None
            ),

        "buy_before_runner_rate":
            safe_div(
                wallet_buy_before_runner[w],
                buy_outcomes
            ),

        "buy_before_dump_rate":
            safe_div(
                wallet_buy_before_dump[w],
                buy_outcomes
            ),

        "sell_before_runner_rate":
            safe_div(
                wallet_sell_before_runner[w],
                sell_outcomes
            ),

        "sell_before_dump_rate":
            safe_div(
                wallet_sell_before_dump[w],
                sell_outcomes
            ),

        "prior_labeled_events":
            prior_event_n,
    }


records = []


# ============================================================
# CHRONOLOGICAL FEATURE CREATION
# ============================================================

for e in labeled_events:

    # advance all historical swaps strictly before event
    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < e["timestamp"]
    ):
        update_swap_history(
            swaps[swap_idx]
        )
        swap_idx += 1

    pre = pre_signal_swaps(e)

    if not pre:
        continue

    wallets = sorted(
        set(r["wallet"] for r in pre)
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

    wallet_feats = [
        wallet_behavior_features(w)
        for w in wallets
    ]

    def cohort_mean(name):
        vals = [
            x[name]
            for x in wallet_feats
            if valid(x[name])
        ]
        return avg(vals)

    def cohort_median(name):
        vals = [
            x[name]
            for x in wallet_feats
            if valid(x[name])
        ]
        return med(vals)

    known_behavior_wallets = sum(
        x["prior_labeled_events"] > 0
        for x in wallet_feats
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

        "behavior_known_ratio":
            safe_div(
                known_behavior_wallets,
                len(wallets)
            ),

        "cohort_prior_swap_count":
            cohort_median(
                "prior_swap_count"
            ),

        "cohort_prior_token_count":
            cohort_median(
                "prior_token_count"
            ),

        "cohort_buy_sell_ratio":
            cohort_mean(
                "buy_sell_ratio"
            ),

        "cohort_fast_flip_ratio":
            cohort_mean(
                "fast_flip_ratio"
            ),

        "cohort_median_post_buy_return":
            cohort_mean(
                "median_post_buy_return"
            ),

        "cohort_avg_post_buy_return":
            cohort_mean(
                "avg_post_buy_return"
            ),

        "cohort_positive_post_buy_rate":
            cohort_mean(
                "positive_post_buy_rate"
            ),

        "cohort_buy_before_runner_rate":
            cohort_mean(
                "buy_before_runner_rate"
            ),

        "cohort_buy_before_dump_rate":
            cohort_mean(
                "buy_before_dump_rate"
            ),

        "cohort_sell_before_runner_rate":
            cohort_mean(
                "sell_before_runner_rate"
            ),

        "cohort_sell_before_dump_rate":
            cohort_mean(
                "sell_before_dump_rate"
            ),
    }

    records.append(rec)

    # ========================================================
    # ONLY NOW add the current event outcome to wallet histories
    # ========================================================

    for w in wallets:

        wallet_event_count[w] += 1

        if w in buyers:
            if rec["label"] == 1:
                wallet_buy_before_runner[w] += 1
            else:
                wallet_buy_before_dump[w] += 1

        if w in sellers:
            if rec["label"] == 1:
                wallet_sell_before_runner[w] += 1
            else:
                wallet_sell_before_dump[w] += 1


FEATURES = [
    "wallet_count",
    "buyer_count",
    "seller_count",
    "behavior_known_ratio",

    "cohort_prior_swap_count",
    "cohort_prior_token_count",

    "cohort_buy_sell_ratio",
    "cohort_fast_flip_ratio",

    "cohort_median_post_buy_return",
    "cohort_avg_post_buy_return",
    "cohort_positive_post_buy_rate",

    "cohort_buy_before_runner_rate",
    "cohort_buy_before_dump_rate",

    "cohort_sell_before_runner_rate",
    "cohort_sell_before_dump_rate",
]


print("="*150)
print("MEMECOIN LAB — T40 WALLET LEAD/LAG & BEHAVIORAL SKILL LAB")
print("="*150)

print(
    f"LABELED EVENTS             : {len(labeled_events)}"
)

print(
    f"EVENTS WITH WALLET FEATURES: {len(records)}"
)

print(
    f"UNIQUE TOKENS              : "
    f"{len(set(r['token_mint'] for r in records))}"
)


# ============================================================
# A) GLOBAL SEPARATION
# ============================================================

print()
print("="*150)
print("A) WALLET BEHAVIOR SEPARATION — RUNNER VS DUMP")
print("="*150)

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
        "name": name,
        "run_med": rm,
        "dump_med": dm,
        "diff": rm-dm,
        "sep": sep,
    })


scores.sort(
    key=lambda x:x["sep"],
    reverse=True
)

print(
    f"{'FEATURE':38} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-"*95)

for x in scores:
    print(
        f"{x['name']:38} "
        f"{x['run_med']:+11.4f} "
        f"{x['dump_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# TOKEN HOLDOUT
# ============================================================

tokens = sorted(
    set(r["token_mint"] for r in records)
)

random.seed(RANDOM_SEED)
random.shuffle(tokens)

n = len(tokens)

n_train = int(n*.60)
n_val = int(n*.20)

train_tokens = set(
    tokens[:n_train]
)

val_tokens = set(
    tokens[n_train:n_train+n_val]
)

test_tokens = set(
    tokens[n_train+n_val:]
)


def subset(tokset):
    return [
        r for r in records
        if r["token_mint"] in tokset
    ]


train = subset(train_tokens)
val = subset(val_tokens)
test = subset(test_tokens)


print()
print("="*150)
print("B) SPLITS")
print("="*150)

for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    print(
        f"{name:8} | "
        f"N={len(rr):3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| RUN={sum(r['label']==1 for r in rr):3d} "
        f"| DUMP={sum(r['label']==0 for r in rr):3d}"
    )


# ============================================================
# C) DIRECTION SURVIVAL
# ============================================================

print()
print("="*150)
print("C) FEATURE DIRECTION SURVIVAL")
print("="*150)

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
        survivors.append(name)

    print(
        f"{name:38} "
        f"TRAIN={str(diffs[0]):>12} "
        f"VALID={str(diffs[1]):>12} "
        f"TEST={str(diffs[2]):>12} "
        f"SAME={same}"
    )


# ============================================================
# D) LOGISTIC TOKEN HOLDOUT
# ============================================================

selected = [
    x["name"]
    for x in scores[:10]
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
        [r["label"] for r in rr],
        dtype=int
    )


X_train = X_for(train)
X_val = X_for(val)
X_test = X_for(test)

y_train = y_for(train)
y_val = y_for(val)
y_test = y_for(test)


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
            max_iter=2000,
            random_state=42
        )
    )
])

pipe.fit(
    X_train,
    y_train
)


def auc_for(X,y):
    p = pipe.predict_proba(X)[:,1]

    if len(set(y)) < 2:
        return p, None

    return p, roc_auc_score(y,p)


val_prob, val_auc = auc_for(
    X_val,
    y_val
)

test_prob, test_auc = auc_for(
    X_test,
    y_test
)


print()
print("="*150)
print("D) BEHAVIOR MODEL — TOKEN HOLDOUT")
print("="*150)

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
# E) FIRST EVENT PER TOKEN
# ============================================================

def first_per_token(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x:x["id"]
    ):

        if r["token_mint"] in seen:
            continue

        seen.add(
            r["token_mint"]
        )

        out.append(r)

    return out


print()
print("="*150)
print("E) FIRST-EVENT/TOKEN")
print("="*150)

for title, rr in [
    ("VALID FIRST", first_per_token(val)),
    ("TEST FIRST", first_per_token(test)),
]:

    X = X_for(rr)
    y = y_for(rr)

    _, auc = auc_for(
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
# F) HISTORY COVERAGE
# ============================================================

print()
print("="*150)
print("F) BEHAVIOR HISTORY COVERAGE")
print("="*150)

for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    vals = [
        r["behavior_known_ratio"]
        for r in rr
        if valid(
            r["behavior_known_ratio"]
        )
    ]

    post = [
        r["cohort_median_post_buy_return"]
        for r in rr
        if valid(
            r["cohort_median_post_buy_return"]
        )
    ]

    print(
        f"{name:8} | "
        f"KNOWN BEHAVIOR MED={med(vals):.3f} "
        f"| POST-BUY COVERAGE="
        f"{100*len(post)/len(rr):.1f}%"
    )


# ============================================================
# G) WEIGHTS
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
print("="*150)
print("G) STANDARDIZED BEHAVIOR FEATURE WEIGHTS")
print("="*150)

for name,w in weights:
    print(
        f"{name:40} {w:+.4f}"
    )


# ============================================================
# H) DECISION
# ============================================================

print()
print("="*150)
print("H) DECISION SUPPORT")
print("="*150)

good = (
    val_auc is not None
    and test_auc is not None
    and val_auc >= .60
    and test_auc >= .60
    and len(survivors) >= 3
)

if good:

    print(
        "WALLET BEHAVIOR EDGE SURVIVES TOKEN HOLDOUT."
    )

    print(
        "Do NOT integrate into V2."
    )

    print(
        "Next step = frozen prospective wallet-behavior score."
    )

else:

    print(
        "NO ROBUST WALLET-BEHAVIOR EDGE YET."
    )

    print(
        "Behavioral history is not stable enough "
        "for prospective use yet."
    )

print()
print(
    f"SAME-DIRECTION FEATURES: "
    f"{len(survivors)}"
)

for name in survivors:
    print("•", name)

print()
print("IMPORTANT:")
print("• All wallet history is chronological.")
print("• Current event outcome enters history only AFTER feature creation.")
print("• Token identities do not cross splits.")
print("• T23/T31/T32 remain untouched.")
print("• TEST is final audit only.")
print("• Do not retune T40 from TEST.")
print("• This is not a trading rule.")

db.close()
