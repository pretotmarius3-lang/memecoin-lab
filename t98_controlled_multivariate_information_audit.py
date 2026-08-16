#!/usr/bin/env python3

import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

TARGET_THRESHOLD = 3.0
SEED = 98
BOOT_N = 5000

FEATURES = [
    "sellers60",
    "swaps60",
    "buyers60",
    "new_wallets30",
    "wallets30",
    "largest_buy30",
    "buyers30",
]

# Frozen candidate family chosen only from T97 first-token survivors.
MODELS = {
    "M0_SELLERS60":
        ["sellers60"],

    "M1_BUYERS60":
        ["buyers60"],

    "M2_NEW_WALLETS30":
        ["new_wallets30"],

    "M3_LARGEST_BUY30":
        ["largest_buy30"],

    # intentionally compact / low-dimensional
    "M4_BUYERS60_LARGESTBUY":
        ["buyers60", "largest_buy30"],

    "M5_SELLERS60_LARGESTBUY":
        ["sellers60", "largest_buy30"],

    "M6_NEWWALLETS30_LARGESTBUY":
        ["new_wallets30", "largest_buy30"],

    "M7_BUYERS60_NEWWALLETS30":
        ["buyers60", "new_wallets30"],

    "M8_BUYERS60_NEWWALLETS30_LARGESTBUY":
        ["buyers60", "new_wallets30", "largest_buy30"],
}


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
    return statistics.mean(xs) if xs else None


def med(xs):
    return statistics.median(xs) if xs else None


def quantile(xs, q):
    xs = sorted(xs)

    if not xs:
        return None

    pos = (len(xs)-1)*q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos-lo
    return xs[lo]*(1-w)+xs[hi]*w


def fmt(x, n=4):
    return "NA" if x is None else f"{x:.{n}f}"


def sigmoid(z):
    z = max(min(z, 35.0), -35.0)
    return 1.0 / (1.0 + math.exp(-z))


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
            yi*math.log(min(max(pi, eps), 1-eps))
            +
            (1-yi)*math.log(
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


def fit_logistic(X, y, l2=1.0, lr=0.05, epochs=2500):
    p = len(X[0])
    beta = [0.0]*(p+1)

    n = len(X)

    for _ in range(epochs):

        grad = [0.0]*(p+1)

        for xi, yi in zip(X, y):

            z = beta[0]

            for j in range(p):
                z += beta[j+1]*xi[j]

            pr = sigmoid(z)
            err = pr-yi

            grad[0] += err

            for j in range(p):
                grad[j+1] += err*xi[j]

        grad[0] /= n

        for j in range(p):

            grad[j+1] = (
                grad[j+1]/n
                + l2*beta[j+1]/n
            )

        for j in range(p+1):
            beta[j] -= lr*grad[j]

    return beta


def predict(beta, X):
    out = []

    for xi in X:
        z = beta[0]

        for j, x in enumerate(xi):
            z += beta[j+1]*x

        out.append(sigmoid(z))

    return out


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


boundary = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()[0]

boundary = int(boundary)


rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    sellers60,
    swaps60,
    buyers60,
    new_wallets30,
    wallets30,
    largest_buy30,
    buyers30,

    dex_return_30s,
    dex_done_30s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_done_30s=1
    AND dex_return_30s IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


records = []

for r in rows:

    y = int(
        abs(r["dex_return_30s"])
        >= TARGET_THRESHOLD
    )

    rec = {
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],
        "historical": r["id"] <= boundary,
        "y": y,
    }

    for f in FEATURES:
        rec[f] = r[f]

    records.append(rec)


# ============================================================
# COMMON COMPLETE CASE
# ============================================================

common = [
    r for r in records
    if all(
        valid(r[f])
        for f in FEATURES
    )
]


tokens = sorted(
    set(
        r["token_mint"]
        for r in common
    )
)


# ============================================================
# TOKEN-DISJOINT SPLIT
# 60/20/20 fixed seed
# ============================================================

rng = random.Random(SEED)

tokens_shuffled = list(tokens)
rng.shuffle(tokens_shuffled)

n = len(tokens_shuffled)

n_train = int(0.60*n)
n_valid = int(0.20*n)

train_tokens = set(
    tokens_shuffled[:n_train]
)

valid_tokens = set(
    tokens_shuffled[n_train:n_train+n_valid]
)

test_tokens = set(
    tokens_shuffled[n_train+n_valid:]
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
# STANDARDIZATION TRAIN ONLY
# ============================================================

means = {}
stds = {}

for f in FEATURES:

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
            ) / stds[f]

            for f in features
        ])

        y.append(
            r["y"]
        )

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
        "features": features,
        "beta": fit_logistic(
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
        "n": len(y),
        "auc": auc(y, p),
        "logloss": logloss(y, p),
        "brier": brier(y, p),
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
# BEST SINGLE SELECTED ON VALID ONLY
# ============================================================

single_names = [
    "M0_SELLERS60",
    "M1_BUYERS60",
    "M2_NEW_WALLETS30",
    "M3_LARGEST_BUY30",
]

valid_singles = [
    (
        results["VALID"][name]["auc"],
        name
    )
    for name in single_names
    if results["VALID"][name]["auc"] is not None
]

valid_singles.sort(
    reverse=True
)

BEST_SINGLE = valid_singles[0][1]


# ============================================================
# MULTIVARIATE CANDIDATES
# chosen by VALID only
# ============================================================

multi_names = [
    name
    for name in MODELS
    if name not in single_names
]

valid_multis = [
    (
        results["VALID"][name]["auc"],
        name
    )
    for name in multi_names
    if results["VALID"][name]["auc"] is not None
]

valid_multis.sort(
    reverse=True
)

BEST_MULTI = valid_multis[0][1]


# ============================================================
# FIRST EVENT / TOKEN TEST
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
    name: evaluate(
        name,
        first_test
    )
    for name in MODELS
}


# ============================================================
# HIST / PROS TEST DECOMPOSITION
# ============================================================

test_hist = [
    r for r in test
    if r["historical"]
]

test_pros = [
    r for r in test
    if not r["historical"]
]


# ============================================================
# TOKEN BOOTSTRAP BEST_MULTI vs BEST_SINGLE
# TEST ONLY
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
    SEED + 9800
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
        BEST_SINGLE,
        rr
    )

    multi = evaluate(
        BEST_MULTI,
        rr
    )

    if (
        base["auc"] is not None
        and multi["auc"] is not None
    ):
        boot_auc.append(
            multi["auc"]
            - base["auc"]
        )

    boot_ll.append(
        base["logloss"]
        - multi["logloss"]
    )

    boot_br.append(
        base["brier"]
        - multi["brier"]
    )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 190)
print(
    "MEMECOIN LAB — T98 CONTROLLED MULTIVARIATE INFORMATION AUDIT"
)
print("=" * 190)

print("MODE                : READ-ONLY")
print("TARGET              : |R30| >= 3%")
print("MODEL FAMILY        : MAX 3 FEATURES")
print("INTERACTIONS        : NONE")
print("THRESHOLD SEARCH    : NONE")
print("DB WRITES           : NONE")
print("T59/T78/T82/T86     : UNTOUCHED")
print()
print(f"COMMON EVENTS       : {len(common)}")
print(f"COMMON TOKENS       : {len(tokens)}")
print()
print(
    f"TRAIN | N={len(train):4d} "
    f"| TOK={len(train_tokens):3d}"
)
print(
    f"VALID | N={len(valid_set):4d} "
    f"| TOK={len(valid_tokens):3d}"
)
print(
    f"TEST  | N={len(test):4d} "
    f"| TOK={len(test_tokens):3d}"
)


# ============================================================
# A) MODEL FAMILY
# ============================================================

print()
print("=" * 190)
print("A) FROZEN MODEL FAMILY")
print("=" * 190)

for name, features in MODELS.items():

    print(
        f"{name:40} "
        f"| "
        + ", ".join(features)
    )


# ============================================================
# B-D) PERFORMANCE
# ============================================================

for split in [
    "TRAIN",
    "VALID",
    "TEST",
]:

    print()
    print("=" * 190)
    print(
        f"{split}) MODEL PERFORMANCE"
    )
    print("=" * 190)

    for name in MODELS:

        r = results[split][name]

        print(
            f"{name:40} "
            f"N={r['n']:4d} "
            f"AUC={fmt(r['auc'])} "
            f"LOGLOSS={fmt(r['logloss'])} "
            f"BRIER={fmt(r['brier'])}"
        )


# ============================================================
# E) VALID-ONLY SELECTION
# ============================================================

print()
print("=" * 190)
print("E) VALID-ONLY MODEL SELECTION")
print("=" * 190)

print(
    f"BEST SINGLE = {BEST_SINGLE}"
)

print(
    f"VALID AUC   = "
    f"{fmt(results['VALID'][BEST_SINGLE]['auc'])}"
)

print()

print(
    f"BEST MULTI  = {BEST_MULTI}"
)

print(
    f"VALID AUC   = "
    f"{fmt(results['VALID'][BEST_MULTI]['auc'])}"
)


# ============================================================
# F) TEST INCREMENT
# ============================================================

print()
print("=" * 190)
print("F) TEST INCREMENT — BEST MULTI vs BEST SINGLE")
print("=" * 190)

base = results[
    "TEST"
][
    BEST_SINGLE
]

multi = results[
    "TEST"
][
    BEST_MULTI
]

print(
    f"BEST SINGLE         : {BEST_SINGLE}"
)

print(
    f"BEST MULTI          : {BEST_MULTI}"
)

print()

print(
    f"ΔAUC                = "
    f"{multi['auc']-base['auc']:+.4f}"
)

print(
    f"ΔLOGLOSS            = "
    f"{base['logloss']-multi['logloss']:+.4f}"
)

print(
    f"ΔBRIER              = "
    f"{base['brier']-multi['brier']:+.4f}"
)


# ============================================================
# G) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 190)
print("G) FIRST-EVENT/TOKEN TEST")
print("=" * 190)

print(
    f"FIRST TEST TOKENS = {len(first_test)}"
)

for name in [
    BEST_SINGLE,
    BEST_MULTI,
]:

    r = first_results[name]

    print(
        f"{name:40} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# H) TEST HIST / PROS
# ============================================================

print()
print("=" * 190)
print("H) TEST REGIME DECOMPOSITION")
print("=" * 190)

for regime_name, rr in [
    ("HIST", test_hist),
    ("PROS", test_pros),
]:

    print()
    print(regime_name)

    for name in [
        BEST_SINGLE,
        BEST_MULTI,
    ]:

        r = evaluate(
            name,
            rr
        )

        print(
            f"{name:40} "
            f"N={r['n']:4d} "
            f"AUC={fmt(r['auc'])} "
            f"LOGLOSS={fmt(r['logloss'])} "
            f"BRIER={fmt(r['brier'])}"
        )


# ============================================================
# I) BOOTSTRAP
# ============================================================

print()
print("=" * 190)
print(
    "I) TOKEN-LEVEL TEST BOOTSTRAP — MULTI vs SINGLE"
)
print("=" * 190)

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
# J) COEFFICIENTS
# ============================================================

print()
print("=" * 190)
print("J) STANDARDIZED COEFFICIENTS")
print("=" * 190)

for name in [
    BEST_SINGLE,
    BEST_MULTI,
]:

    model = fitted[name]

    print()
    print(name)

    print(
        f"INTERCEPT = "
        f"{fmt(model['beta'][0])}"
    )

    for f, b in zip(
        model["features"],
        model["beta"][1:]
    ):

        print(
            f"{f:24} "
            f"BETA={fmt(b)}"
        )


# ============================================================
# K) DECISION
# ============================================================

print()
print("=" * 190)
print("K) DECISION SUPPORT")
print("=" * 190)

test_gain = (
    multi["auc"]
    - base["auc"]
)

first_base = first_results[
    BEST_SINGLE
]

first_multi = first_results[
    BEST_MULTI
]

first_gain = (
    first_multi["auc"]
    - first_base["auc"]
    if (
        first_multi["auc"] is not None
        and first_base["auc"] is not None
    )
    else None
)

boot_prob = (
    sum(x > 0 for x in boot_auc)
    / len(boot_auc)
    if boot_auc
    else 0
)

print(
    f"TEST ΔAUC        = {test_gain:+.4f}"
)

print(
    f"FIRST ΔAUC       = "
    f"{fmt(first_gain)}"
)

print(
    f"BOOT P(ΔAUC>0)   = "
    f"{100*boot_prob:.1f}%"
)

print()


if (
    test_gain > 0.03
    and first_gain is not None
    and first_gain > 0
    and boot_prob >= 0.80
):

    print(
        "🟢 EXISTING FEATURE SPACE CONTAINS ROBUST "
        "MULTIVARIATE INFORMATION."
    )

    print(
        "Single-feature gate failures were partly a representation problem."
    )

    print(
        "Next = dedicated robustness audit of the frozen compact subset."
    )

elif (
    test_gain > 0
    and boot_prob >= 0.60
):

    print(
        "🟡 PARTIAL MULTIVARIATE INFORMATION GAIN."
    )

    print(
        "Evidence is suggestive but insufficient for promotion."
    )

else:

    print(
        "🔴 COMBINING CURRENT FEATURES DOES NOT ADD "
        "ROBUST INFORMATION BEYOND THE BEST SINGLE FEATURE."
    )

    print(
        "Current observable feature space is likely near its useful limit."
    )

    print(
        "Next priority = repair/enrich DEX market-structure data."
    )


print()
print("IMPORTANT:")
print("• Candidate variables came from T97 only.")
print("• Maximum 3 features.")
print("• No interactions.")
print("• Token-disjoint split.")
print("• Standardization uses TRAIN only.")
print("• Coefficients fit TRAIN only.")
print("• VALID chooses best single and best multi.")
print("• TEST is untouched until final comparison.")
print("• Bootstrap resamples TEST tokens.")
print("• No operational threshold optimization.")
print("• T98 writes nothing to DB.")
print("• Frozen prospective branches remain untouched.")

db.close()
