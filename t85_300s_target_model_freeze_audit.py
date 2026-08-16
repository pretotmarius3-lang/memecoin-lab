#!/usr/bin/env python3

import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

WINDOW = 30.0
SEED = 85
BOOT_N = 3000


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


def fmt(x, n=4):
    return "NA" if x is None else f"{x:.{n}f}"


def label_r300(x):
    if not valid(x):
        return None
    if x >= RUNNER:
        return 1
    if x <= DUMP:
        return 0
    return None


def sigmoid(z):
    z = max(min(z, 35.0), -35.0)
    return 1.0 / (1.0 + math.exp(-z))


def fit_logistic(X, y, l2=1.0, lr=0.05, epochs=2500):
    p = len(X[0])
    beta = [0.0] * (p + 1)
    n = len(X)

    for _ in range(epochs):
        grad = [0.0] * (p + 1)

        for xi, yi in zip(X, y):
            z = beta[0]

            for j in range(p):
                z += beta[j+1] * xi[j]

            pr = sigmoid(z)
            err = pr - yi

            grad[0] += err

            for j in range(p):
                grad[j+1] += err * xi[j]

        grad[0] /= n

        for j in range(p):
            grad[j+1] = (
                grad[j+1] / n
                + l2 * beta[j+1] / n
            )

        for j in range(p+1):
            beta[j] -= lr * grad[j]

    return beta


def predict(beta, X):
    out = []

    for xi in X:
        z = beta[0]

        for j, x in enumerate(xi):
            z += beta[j+1] * x

        out.append(sigmoid(z))

    return out


def auc(y, p):
    pos = [p[i] for i in range(len(y)) if y[i] == 1]
    neg = [p[i] for i in range(len(y)) if y[i] == 0]

    if not pos or not neg:
        return None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:
            total += 1
            if a > b:
                wins += 1
            elif a == b:
                wins += 0.5

    return wins / total


def logloss(y, p):
    if not y:
        return None

    eps = 1e-12

    return avg([
        -(
            yi * math.log(min(max(pi, eps), 1-eps))
            + (1-yi) * math.log(
                1-min(max(pi, eps), 1-eps)
            )
        )
        for yi, pi in zip(y, p)
    ])


def brier(y, p):
    if not y:
        return None

    return avg([
        (pi-yi)**2
        for yi, pi in zip(y, p)
    ])


def quantile(xs, q):
    xs = sorted(xs)

    if not xs:
        return None

    pos = (len(xs)-1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos-lo

    return xs[lo]*(1-w) + xs[hi]*w


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


rows = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,

    e.fa,
    e.new_wallets30,

    e.dex_return_300s,
    e.dex_done_300s,
    e.dex_delay_300s,

    s.recent_buy_share,
    s.recent_net_share,
    s.breadth_score,
    s.late_chase_score,

    s.early_price_return,
    s.early_net_sol

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL
    AND e.dex_return_300s IS NOT NULL
    AND e.dex_done_300s = 1

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


# ============================================================
# FEATURE BUILD
# ============================================================

records = []


for e in rows:

    y = label_r300(
        e["dex_return_300s"]
    )

    if y is None:
        continue


    early_div = None

    if (
        valid(e["early_price_return"])
        and valid(e["early_net_sol"])
    ):
        early_div = (
            e["early_price_return"]
            - e["early_net_sol"]
        )


    ts = e["timestamp"]


    buys = db.execute("""
    SELECT
        timestamp,
        wallet

    FROM swaps

    WHERE
        token_mint=?
        AND side='BUY'
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL

    ORDER BY timestamp
    """, (
        e["token_mint"],
        ts-WINDOW,
        ts
    )).fetchall()


    first_by_wallet = {}

    for r in buys:
        first_by_wallet.setdefault(
            r["wallet"],
            r["timestamp"]
        )


    arrivals = sorted(
        first_by_wallet.values()
    )


    if len(arrivals) >= 2:

        n10 = sum(
            t >= ts-10
            for t in arrivals
        )

        buyer_velocity_10 = (
            n10 / 10.0
        )

    else:

        buyer_velocity_10 = None


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "y":
            y,

        "r300":
            e["dex_return_300s"],

        "fa":
            e["fa"],

        "new_wallets30":
            e["new_wallets30"],

        "recent_buy_share":
            e["recent_buy_share"],

        "recent_net_share":
            e["recent_net_share"],

        "breadth_score":
            e["breadth_score"],

        "late_chase_score":
            e["late_chase_score"],

        "early_div":
            early_div,

        "buyer_velocity_10":
            buyer_velocity_10,
    })


# ============================================================
# TARGET DENSITY
# ============================================================

all300 = [
    r["r300"]
    for r in records
]

run_n = sum(x >= 10 for x in all300)
dump_n = sum(x <= -10 for x in all300)


# ============================================================
# MODEL FAMILY
# ============================================================

CONTEXT = [
    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
]


MODELS = {

    "M0_CONTEXT":
        CONTEXT,

    "M1_CAPV2":
        CONTEXT + [
            "early_div"
        ],

    "M2_BUYERVEL10":
        CONTEXT + [
            "buyer_velocity_10"
        ],

    "M3_CAPV2_BUYERVEL10":
        CONTEXT + [
            "early_div",
            "buyer_velocity_10"
        ],
}


all_features = sorted(
    set(
        f
        for fs in MODELS.values()
        for f in fs
    )
)


common = [
    r for r in records
    if all(
        valid(r.get(f))
        for f in all_features
    )
]


# ============================================================
# TOKEN SPLIT
# ============================================================

tokens = sorted(
    set(
        r["token_mint"]
        for r in common
    )
)

rng = random.Random(SEED)
rng.shuffle(tokens)

n = len(tokens)

n_train = int(0.60*n)
n_valid = int(0.20*n)

train_tokens = set(
    tokens[:n_train]
)

valid_tokens = set(
    tokens[n_train:n_train+n_valid]
)

test_tokens = set(
    tokens[n_train+n_valid:]
)


def subset(tokset):
    return [
        r for r in common
        if r["token_mint"] in tokset
    ]


train = subset(train_tokens)
valid_set = subset(valid_tokens)
test = subset(test_tokens)


# ============================================================
# STANDARDIZE TRAIN ONLY
# ============================================================

means = {}
stds = {}

for f in all_features:

    vals = [
        r[f]
        for r in train
    ]

    means[f] = avg(vals)

    sd = statistics.pstdev(vals)

    if sd <= 1e-12:
        sd = 1.0

    stds[f] = sd


def vectorize(rr, features):

    X = []
    y = []

    for r in rr:

        X.append([
            (
                r[f] - means[f]
            )
            / stds[f]

            for f in features
        ])

        y.append(r["y"])

    return X, y


# ============================================================
# FIT TRAIN ONLY
# ============================================================

fitted = {}

for name, features in MODELS.items():

    X, y = vectorize(
        train,
        features
    )

    fitted[name] = {
        "features":
            features,

        "beta":
            fit_logistic(
                X,
                y
            )
    }


def evaluate(name, rr):

    model = fitted[name]

    X, y = vectorize(
        rr,
        model["features"]
    )

    p = predict(
        model["beta"],
        X
    )

    return {
        "n":
            len(y),

        "auc":
            auc(y, p),

        "logloss":
            logloss(y, p),

        "brier":
            brier(y, p),
    }


results = defaultdict(dict)

for split_name, rr in [
    ("TRAIN", train),
    ("VALID", valid_set),
    ("TEST", test),
]:

    for name in MODELS:

        results[split_name][name] = evaluate(
            name,
            rr
        )


# ============================================================
# FIRST TEST TOKEN
# ============================================================

seen = set()
first_test = []

for r in sorted(
    test,
    key=lambda x: (
        x["timestamp"],
        x["id"]
    )
):

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first_test.append(r)


first_results = {
    name:
        evaluate(
            name,
            first_test
        )

    for name in MODELS
}


# ============================================================
# BOOTSTRAP M3 vs M1
# ============================================================

by_token = defaultdict(list)

for r in test:
    by_token[
        r["token_mint"]
    ].append(r)


test_tokens_list = list(
    by_token.keys()
)


rng = random.Random(
    SEED + 8500
)

boot_auc = []
boot_ll = []
boot_br = []


for _ in range(BOOT_N):

    sampled = [
        rng.choice(
            test_tokens_list
        )
        for _ in range(
            len(test_tokens_list)
        )
    ]

    rr = []

    for tok in sampled:
        rr.extend(
            by_token[tok]
        )

    base = evaluate(
        "M1_CAPV2",
        rr
    )

    combo = evaluate(
        "M3_CAPV2_BUYERVEL10",
        rr
    )

    if (
        base["auc"] is not None
        and combo["auc"] is not None
    ):
        boot_auc.append(
            combo["auc"]
            - base["auc"]
        )

    boot_ll.append(
        base["logloss"]
        - combo["logloss"]
    )

    boot_br.append(
        base["brier"]
        - combo["brier"]
    )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 175)
print(
    "MEMECOIN LAB — T85 300s TARGET / MODEL FREEZE AUDIT"
)
print("=" * 175)

print("MODE                : READ-ONLY")
print("MODEL REFIT LIVE    : NO")
print("THRESHOLD SEARCH    : NO")
print("DB WRITES           : NONE")
print("T59/T78/T82         : UNTOUCHED")

print()
print(
    f"300s BINARY EVENTS  : {len(records)}"
)

print(
    f"300s RUN            : {run_n}"
)

print(
    f"300s DUMP           : {dump_n}"
)

print(
    f"COMMON COMPLETE     : {len(common)}"
)

print(
    f"COMMON TOKENS       : {len(tokens)}"
)

print()

print(
    f"TRAIN | N={len(train):3d} "
    f"| TOK={len(train_tokens):2d}"
)

print(
    f"VALID | N={len(valid_set):3d} "
    f"| TOK={len(valid_tokens):2d}"
)

print(
    f"TEST  | N={len(test):3d} "
    f"| TOK={len(test_tokens):2d}"
)


# ============================================================
# A
# ============================================================

print()
print("=" * 175)
print("A) MODEL DEFINITIONS")
print("=" * 175)

for name, features in MODELS.items():
    print(
        f"{name:30} | "
        + ", ".join(features)
    )


# ============================================================
# B/C/D
# ============================================================

for split in [
    "TRAIN",
    "VALID",
    "TEST"
]:

    print()
    print("=" * 175)
    print(
        f"{split}) MODEL PERFORMANCE"
    )
    print("=" * 175)

    for name in MODELS:

        r = results[
            split
        ][
            name
        ]

        print(
            f"{name:30} "
            f"N={r['n']:3d} "
            f"AUC={fmt(r['auc'])} "
            f"LOGLOSS={fmt(r['logloss'])} "
            f"BRIER={fmt(r['brier'])}"
        )


# ============================================================
# E
# ============================================================

print()
print("=" * 175)
print(
    "E) BUYER VELOCITY INCREMENT AFTER CAP-v2 @300s"
)
print("=" * 175)


for split in [
    "VALID",
    "TEST"
]:

    base = results[
        split
    ][
        "M1_CAPV2"
    ]

    combo = results[
        split
    ][
        "M3_CAPV2_BUYERVEL10"
    ]

    print()
    print(split)
    print("-" * 90)

    print(
        f"ΔAUC     = "
        f"{combo['auc']-base['auc']:+.4f}"
    )

    print(
        f"ΔLOGLOSS = "
        f"{base['logloss']-combo['logloss']:+.4f}"
    )

    print(
        f"ΔBRIER   = "
        f"{base['brier']-combo['brier']:+.4f}"
    )


# ============================================================
# F
# ============================================================

print()
print("=" * 175)
print("F) FIRST-EVENT/TOKEN TEST")
print("=" * 175)

print(
    f"FIRST TEST TOKENS = {len(first_test)}"
)

for name in MODELS:

    r = first_results[name]

    print(
        f"{name:30} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# G
# ============================================================

print()
print("=" * 175)
print(
    "G) TOKEN-LEVEL BOOTSTRAP — M3 vs CAP-v2 @300s"
)
print("=" * 175)

print(
    f"BOOT N={len(boot_auc)}"
)

if boot_auc:

    print(
        f"ΔAUC MED = "
        f"{fmt(med(boot_auc))}"
    )

    print(
        f"ΔAUC 95% CI = ["
        f"{fmt(quantile(boot_auc,0.025))}, "
        f"{fmt(quantile(boot_auc,0.975))}]"
    )

    print(
        f"P(ΔAUC>0) = "
        f"{100*sum(x>0 for x in boot_auc)/len(boot_auc):.1f}%"
    )

print(
    f"ΔLOGLOSS MED = "
    f"{fmt(med(boot_ll))}"
)

print(
    f"P(ΔLOGLOSS>0) = "
    f"{100*sum(x>0 for x in boot_ll)/len(boot_ll):.1f}%"
)

print(
    f"ΔBRIER MED = "
    f"{fmt(med(boot_br))}"
)

print(
    f"P(ΔBRIER>0) = "
    f"{100*sum(x>0 for x in boot_br)/len(boot_br):.1f}%"
)


# ============================================================
# H
# ============================================================

print()
print("=" * 175)
print("H) EXACT FREEZE MATERIAL — M1 CAP-v2")
print("=" * 175)

m1 = fitted[
    "M1_CAPV2"
]

print(
    f"INTERCEPT = "
    f"{repr(m1['beta'][0])}"
)

for f, b in zip(
    m1["features"],
    m1["beta"][1:]
):

    print(
        f"{f:25} "
        f"MEAN={repr(means[f])} "
        f"STD={repr(stds[f])} "
        f"BETA={repr(b)}"
    )


# ============================================================
# I
# ============================================================

print()
print("=" * 175)
print(
    "I) EXACT FREEZE MATERIAL — M3 CAP-v2 + BuyerVel10"
)
print("=" * 175)

m3 = fitted[
    "M3_CAPV2_BUYERVEL10"
]

print(
    f"INTERCEPT = "
    f"{repr(m3['beta'][0])}"
)

for f, b in zip(
    m3["features"],
    m3["beta"][1:]
):

    print(
        f"{f:25} "
        f"MEAN={repr(means[f])} "
        f"STD={repr(stds[f])} "
        f"BETA={repr(b)}"
    )


# ============================================================
# J
# ============================================================

print()
print("=" * 175)
print("J) DECISION SUPPORT")
print("=" * 175)

vb = results[
    "VALID"
][
    "M1_CAPV2"
]

vc = results[
    "VALID"
][
    "M3_CAPV2_BUYERVEL10"
]

tb = results[
    "TEST"
][
    "M1_CAPV2"
]

tc = results[
    "TEST"
][
    "M3_CAPV2_BUYERVEL10"
]


valid_gain = (
    vc["auc"]
    - vb["auc"]
)

test_gain = (
    tc["auc"]
    - tb["auc"]
)

boot_prob = (
    sum(x > 0 for x in boot_auc)
    / len(boot_auc)
    if boot_auc
    else 0
)


print(
    f"VALID ΔAUC       = {valid_gain:+.4f}"
)

print(
    f"TEST ΔAUC        = {test_gain:+.4f}"
)

print(
    f"BOOT P(ΔAUC>0)   = {100*boot_prob:.1f}%"
)

print()


target_dense_enough = (
    run_n >= 10
    and dump_n >= 10
)


if not target_dense_enough:

    print(
        "🔴 300s ±10% TARGET STILL TOO SPARSE "
        "FOR A NEW FROZEN MODEL."
    )

elif (
    valid_gain > 0
    and test_gain > 0
    and boot_prob >= 0.80
):

    print(
        "🟢 300s TARGET IS USABLE AND BUYERVEL10 "
        "ADDS CONSISTENT VALUE."
    )

    print(
        "Candidate T86 = frozen M3 prospective 300s shadow."
    )

else:

    print(
        "🟡 300s TARGET IS USABLE, BUT BUYERVEL10 "
        "IS NOT YET A CLEAR INCREMENT."
    )

    print(
        "Candidate T86 should freeze CAP-v2 @300s first."
    )


print()
print("IMPORTANT:")
print("• 300s target fixed at ±10%.")
print("• No threshold search.")
print("• No future swaps used in BuyerVel10.")
print("• Splits are token-disjoint.")
print("• Standardization uses TRAIN only.")
print("• Models fit TRAIN only.")
print("• Bootstrap resamples TEST tokens.")
print("• T85 writes nothing.")
print("• Any T86 freeze must use a new prospective boundary.")

db.close()
