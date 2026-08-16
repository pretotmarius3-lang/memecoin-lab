import sqlite3
import math
import statistics
import random
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
SEVERE = -20.0

LOOKBACK_SEC = 30.0
RANDOM_SEED = 42


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
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def label_from_return(r60):
    if not valid(r60):
        return None

    if r60 >= RUNNER:
        return "RUNNER"

    if r60 <= SEVERE:
        return "SEVERE"

    if r60 <= DUMP:
        return "DUMP"

    return "NEUTRAL"


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
    dex_return_60s IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


usable_events = [
    e for e in events
    if label_from_return(e["dex_return_60s"]) is not None
]


print("="*150)
print("MEMECOIN LAB — T39 WALLET IDENTITY / HISTORICAL REPUTATION LAB")
print("="*150)

print(
    f"EVENTS        : {len(usable_events)}"
)

print(
    f"UNIQUE TOKENS : "
    f"{len(set(e['token_mint'] for e in usable_events))}"
)


# ============================================================
# LOAD SWAPS ONCE
# ============================================================

swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    sol_delta

FROM swaps

WHERE
    wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND timestamp IS NOT NULL

ORDER BY timestamp
""").fetchall()


# ============================================================
# EVENT OUTCOME HISTORY
#
# wallet reputation can only use outcomes from EVENTS strictly
# before current event timestamp.
# ============================================================

events_by_token = defaultdict(list)

for e in usable_events:
    events_by_token[e["token_mint"]].append(e)


# ============================================================
# WALLET HISTORICAL STATE
# ============================================================

wallet_tokens = defaultdict(set)
wallet_event_labels = defaultdict(list)

wallet_first_seen = {}
wallet_last_seen = {}

# track wallet-token sides historically
wallet_token_sides = defaultdict(set)

# all event rows processed chronologically
processed_event_tokens = set()

# swap pointer
swap_idx = 0


def add_swap_to_history(s):
    w = s["wallet"]
    tok = s["token_mint"]
    side = s["side"]

    if w not in wallet_first_seen:
        wallet_first_seen[w] = s["timestamp"]

    wallet_last_seen[w] = s["timestamp"]

    wallet_tokens[w].add(tok)

    if side:
        wallet_token_sides[(w, tok)].add(side)


def event_wallets_before_signal(
    event_ts,
    token
):
    start = event_ts - LOOKBACK_SEC

    rows = db.execute("""
        SELECT
            wallet,
            side,
            sol_delta,
            timestamp

        FROM swaps

        WHERE
            token_mint=?
            AND timestamp >= ?
            AND timestamp < ?
            AND wallet IS NOT NULL

        ORDER BY timestamp
    """, (
        token,
        start,
        event_ts,
    )).fetchall()

    return rows


def wallet_prior_rates(wallet):
    labels = wallet_event_labels.get(
        wallet,
        []
    )

    if not labels:
        return {
            "events": 0,
            "runner_rate": None,
            "dump_rate": None,
            "severe_rate": None,
        }

    n = len(labels)

    return {
        "events":
            n,

        "runner_rate":
            sum(x == "RUNNER" for x in labels) / n,

        "dump_rate":
            sum(
                x in ["DUMP", "SEVERE"]
                for x in labels
            ) / n,

        "severe_rate":
            sum(x == "SEVERE" for x in labels) / n,
    }


def top_share(values):
    vals = [
        abs(x)
        for x in values
        if valid(x)
    ]

    if not vals:
        return None

    total = sum(vals)

    if total == 0:
        return None

    return max(vals) / total


records = []


# ============================================================
# BUILD FEATURES CHRONOLOGICALLY
# ============================================================

for i, e in enumerate(usable_events):

    event_ts = e["timestamp"]
    token = e["token_mint"]

    # --------------------------------------------------------
    # advance GLOBAL swap history only with swaps before event
    # --------------------------------------------------------

    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < event_ts
    ):
        add_swap_to_history(
            swaps[swap_idx]
        )
        swap_idx += 1

    pre = event_wallets_before_signal(
        event_ts,
        token
    )

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

    known = [
        w for w in wallets
        if len(wallet_tokens.get(w, set())) > 0
    ]

    multi_token = [
        w for w in wallets
        if len(wallet_tokens.get(w, set())) >= 2
    ]

    prior_event_wallets = [
        w for w in wallets
        if len(wallet_event_labels.get(w, [])) > 0
    ]

    runner_rates = []
    dump_rates = []
    severe_rates = []
    prior_counts = []

    for w in wallets:
        rr = wallet_prior_rates(w)

        if rr["events"] > 0:
            prior_counts.append(
                rr["events"]
            )

            runner_rates.append(
                rr["runner_rate"]
            )

            dump_rates.append(
                rr["dump_rate"]
            )

            severe_rates.append(
                rr["severe_rate"]
            )

    buyer_sol = defaultdict(float)
    seller_sol = defaultdict(float)

    for r in pre:
        w = r["wallet"]
        sol = r["sol_delta"]

        if not valid(sol):
            continue

        if r["side"] == "BUY":
            buyer_sol[w] += abs(sol)

        elif r["side"] == "SELL":
            seller_sol[w] += abs(sol)

    both_side = (
        set(buyers)
        & set(sellers)
    )

    # historical age in seconds
    ages = []

    for w in wallets:
        if w in wallet_first_seen:
            ages.append(
                event_ts - wallet_first_seen[w]
            )

    # cross-token familiarity
    token_counts = [
        len(wallet_tokens.get(w, set()))
        for w in wallets
    ]

    label = label_from_return(
        e["dex_return_60s"]
    )

    rec = {
        "id":
            e["id"],

        "token_mint":
            token,

        "timestamp":
            event_ts,

        "r60":
            e["dex_return_60s"],

        "label":
            label,

        "wallet_count":
            len(wallets),

        "buyer_count":
            len(buyers),

        "seller_count":
            len(sellers),

        "known_wallet_ratio":
            safe_div(
                len(known),
                len(wallets)
            ),

        "new_wallet_ratio":
            safe_div(
                len(wallets)-len(known),
                len(wallets)
            ),

        "multi_token_wallet_ratio":
            safe_div(
                len(multi_token),
                len(wallets)
            ),

        "prior_labeled_wallet_ratio":
            safe_div(
                len(prior_event_wallets),
                len(wallets)
            ),

        "avg_prior_event_count":
            avg(prior_counts),

        "median_wallet_age_sec":
            med(ages),

        "avg_prior_token_count":
            avg(token_counts),

        "cohort_prior_runner_rate":
            avg(runner_rates),

        "cohort_prior_dump_rate":
            avg(dump_rates),

        "cohort_prior_severe_rate":
            avg(severe_rates),

        "same_wallet_buy_sell_ratio":
            safe_div(
                len(both_side),
                len(wallets)
            ),

        "buyer_concentration_wallet":
            top_share(
                list(
                    buyer_sol.values()
                )
            ),

        "seller_concentration_wallet":
            top_share(
                list(
                    seller_sol.values()
                )
            ),
    }

    records.append(rec)

    # --------------------------------------------------------
    # AFTER calculating features, assign event outcome to
    # wallets that participated pre-signal.
    #
    # This preserves causality: current event outcome is not
    # available when building current event features.
    # --------------------------------------------------------

    for w in wallets:
        wallet_event_labels[w].append(
            label
        )


FEATURES = [
    "wallet_count",
    "buyer_count",
    "seller_count",

    "known_wallet_ratio",
    "new_wallet_ratio",
    "multi_token_wallet_ratio",
    "prior_labeled_wallet_ratio",

    "avg_prior_event_count",
    "median_wallet_age_sec",
    "avg_prior_token_count",

    "cohort_prior_runner_rate",
    "cohort_prior_dump_rate",
    "cohort_prior_severe_rate",

    "same_wallet_buy_sell_ratio",

    "buyer_concentration_wallet",
    "seller_concentration_wallet",
]


print(
    f"EVENTS WITH WALLET FEATURES : "
    f"{len(records)}"
)


# ============================================================
# A) RUNNER VS DUMP/SEVERE SEPARATION
# ============================================================

binary = [
    r for r in records
    if r["label"] in [
        "RUNNER",
        "DUMP",
        "SEVERE"
    ]
]


print()
print("="*150)
print("A) WALLET FEATURE SEPARATION — RUNNER VS DUMP")
print("="*150)

scores = []

for name in FEATURES:

    run = [
        r[name]
        for r in binary
        if (
            r["label"] == "RUNNER"
            and valid(r[name])
        )
    ]

    dump = [
        r[name]
        for r in binary
        if (
            r["label"] in ["DUMP", "SEVERE"]
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
        abs(rm-dm)/pooled
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
    f"{'FEATURE':32} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-"*85)

for x in scores:

    print(
        f"{x['name']:32} "
        f"{x['run_med']:+11.4f} "
        f"{x['dump_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# TOKEN HOLDOUT
# ============================================================

tokens = sorted(
    set(
        r["token_mint"]
        for r in binary
    )
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


def rows_for(tokset):
    return [
        r for r in binary
        if r["token_mint"] in tokset
    ]


train = rows_for(
    train_tokens
)

val = rows_for(
    val_tokens
)

test = rows_for(
    test_tokens
)


# ============================================================
# B) DIRECTION SURVIVAL
# ============================================================

print()
print("="*150)
print("B) FEATURE DIRECTION SURVIVAL")
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
                r["label"]=="RUNNER"
                and valid(r[name])
            )
        ]

        dump = [
            r[name]
            for r in rr
            if (
                r["label"] in [
                    "DUMP",
                    "SEVERE"
                ]
                and valid(r[name])
            )
        ]

        if not run or not dump:
            diffs.append(None)
            continue

        diffs.append(
            med(run)-med(dump)
        )

    same = False

    if all(
        valid(d)
        for d in diffs
    ):

        signs = [
            1 if d>0
            else -1 if d<0
            else 0
            for d in diffs
        ]

        same = (
            signs[0] != 0
            and signs[0] == signs[1] == signs[2]
        )

    if same:
        survivors.append(name)

    print(
        f"{name:32} "
        f"TRAIN={str(diffs[0]):>12} "
        f"VALID={str(diffs[1]):>12} "
        f"TEST={str(diffs[2]):>12} "
        f"SAME={same}"
    )


# ============================================================
# C) LOGISTIC TOKEN-HOLDOUT
# ============================================================

selected = [
    x["name"]
    for x in scores[:10]
]


def X_for(rr):
    X=[]

    for r in rr:
        X.append([
            float(r[name])
            if valid(r[name])
            else np.nan
            for name in selected
        ])

    return np.asarray(
        X,
        dtype=float
    )


def y_for(rr):
    return np.asarray([
        1
        if r["label"]=="RUNNER"
        else 0
        for r in rr
    ], dtype=int)


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


def get_auc(X,y):
    p = pipe.predict_proba(X)[:,1]

    if len(set(y)) < 2:
        return p, None

    return p, roc_auc_score(
        y,
        p
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
print("="*150)
print("C) WALLET IDENTITY MODEL — TOKEN HOLDOUT")
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
# D) FIRST EVENT PER TOKEN
# ============================================================

def first_per_token(rr):

    seen=set()
    out=[]

    for r in sorted(
        rr,
        key=lambda x:x["id"]
    ):
        tok=r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


print()
print("="*150)
print("D) FIRST-EVENT/TOKEN AUDIT")
print("="*150)

for title, rr in [
    (
        "VALID FIRST",
        first_per_token(val)
    ),
    (
        "TEST FIRST",
        first_per_token(test)
    )
]:

    X = X_for(rr)
    y = y_for(rr)

    p, auc = get_auc(
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
# E) WALLET REPUTATION COVERAGE
# ============================================================

print()
print("="*150)
print("E) WALLET HISTORY COVERAGE")
print("="*150)

for name, rr in [
    ("TRAIN",train),
    ("VALID",val),
    ("TEST",test),
]:

    known = [
        r["known_wallet_ratio"]
        for r in rr
        if valid(
            r["known_wallet_ratio"]
        )
    ]

    prior = [
        r["prior_labeled_wallet_ratio"]
        for r in rr
        if valid(
            r["prior_labeled_wallet_ratio"]
        )
    ]

    multi = [
        r["multi_token_wallet_ratio"]
        for r in rr
        if valid(
            r["multi_token_wallet_ratio"]
        )
    ]

    print(
        f"{name:8} | "
        f"KNOWN MED={med(known):.3f} "
        f"| PRIOR-LABEL MED={med(prior):.3f} "
        f"| MULTI-TOKEN MED={med(multi):.3f}"
    )


# ============================================================
# F) FEATURE WEIGHTS
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
print("F) STANDARDIZED WALLET FEATURE WEIGHTS")
print("="*150)

for name,w in weights:
    print(
        f"{name:34} "
        f"{w:+.4f}"
    )


# ============================================================
# G) DECISION
# ============================================================

print()
print("="*150)
print("G) DECISION SUPPORT")
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
        "WALLET-IDENTITY SIGNAL SHOWS OUT-OF-TOKEN GENERALIZATION."
    )

    print(
        "Do NOT integrate into V2."
    )

    print(
        "Next step = freeze wallet score prospectively."
    )

else:

    print(
        "NO ROBUST WALLET-IDENTITY EDGE YET."
    )

    print(
        "Wallet identity/reputation is not stable enough "
        "under current history depth."
    )

print()

print(
    f"SAME-DIRECTION FEATURES: "
    f"{len(survivors)}"
)

for name in survivors:
    print(
        "•",
        name
    )

print()
print("IMPORTANT:")
print("• Historical wallet state only uses prior information.")
print("• Current event outcome is added only AFTER feature creation.")
print("• Token identities do not cross splits.")
print("• T23/T31/T32 remain untouched.")
print("• TEST is final audit only.")
print("• Do not retune T39 from TEST.")
print("• This is not a trading rule.")

db.close()
