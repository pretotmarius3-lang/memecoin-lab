import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

SEED = 53

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0
FAST_MIN_PRIOR_TRADES = 1
CAP_EPS = 0.05

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


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
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


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# SIMPLE LOGISTIC REGRESSION
#
# Implemented locally so T53 does not depend on sklearn.
# L2 regularization.
# ============================================================

def sigmoid(z):
    z = max(min(z, 35.0), -35.0)
    return 1.0 / (1.0 + math.exp(-z))


def fit_logistic(X, y, l2=1.0, lr=0.05, epochs=2500):

    if not X:
        return None

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

        for j in range(p + 1):
            beta[j] -= lr * grad[j]

    return beta


def predict_logistic(beta, X):

    out = []

    for xi in X:

        z = beta[0]

        for j, x in enumerate(xi):
            z += beta[j+1] * x

        out.append(sigmoid(z))

    return out


# ============================================================
# METRICS
# ============================================================

def auc(y, p):

    pos = [
        p[i]
        for i in range(len(y))
        if y[i] == 1
    ]

    neg = [
        p[i]
        for i in range(len(y))
        if y[i] == 0
    ]

    if not pos or not neg:
        return None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:

            total += 1

            if a > b:
                wins += 1.0
            elif a == b:
                wins += 0.5

    return wins / total


def logloss(y, p):

    if not y:
        return None

    eps = 1e-12

    vals = []

    for yi, pi in zip(y, p):

        pi = min(
            max(pi, eps),
            1.0 - eps
        )

        vals.append(
            -(
                yi * math.log(pi)
                + (1-yi) * math.log(1-pi)
            )
        )

    return avg(vals)


def brier(y, p):

    if not y:
        return None

    return avg([
        (pi - yi) ** 2
        for yi, pi in zip(y, p)
    ])


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# EVENTS
# ============================================================

events = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,
    e.fa,
    e.new_wallets30,
    e.dex_return_60s,

    s.recent_price_return,
    s.recent_net_sol,
    s.recent_buy_sol,
    s.recent_sell_sol,

    s.early_price_return,
    s.early_net_sol,

    s.recent_buy_share,
    s.recent_net_share,

    s.breadth_score,
    s.late_chase_score

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL
    AND e.dex_return_60s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


events = [
    e for e in events
    if label_r60(
        e["dex_return_60s"]
    ) is not None
]


# ============================================================
# HISTORICAL WALLET FAST-FLIP
# ============================================================

swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint
FROM swaps
WHERE
    timestamp IS NOT NULL
    AND wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND side IN ('BUY','SELL')
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

    key = (wallet, token)

    if side == "BUY":

        if key not in open_pos:
            open_pos[key] = ts

    elif side == "SELL":

        if key not in open_pos:
            return

        hold = ts - open_pos[key]

        if hold >= 0:

            completed[wallet] += 1

            if hold <= 60:
                fast_flips[wallet] += 1

        open_pos.pop(key, None)


# ============================================================
# BUILD RECORDS
# ============================================================

records = []


for e in events:

    event_ts = e["timestamp"]

    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < event_ts
    ):

        process_swap(swaps[swap_idx])
        swap_idx += 1


    buyers_rows = db.execute("""
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
        for r in buyers_rows
    ]


    ff = []

    for w in buyers:

        n = completed[w]

        if n < FAST_MIN_PRIOR_TRADES:
            continue

        v = safe_div(
            fast_flips[w],
            n
        )

        if valid(v):
            ff.append(v)


    fast_flip = avg(ff)

    experienced_buyers = len(ff)

    fast_coverage = safe_div(
        experienced_buyers,
        len(buyers)
    )


    recent_price = e["recent_price_return"]
    recent_net = e["recent_net_sol"]

    price_per_net = cap_div(
        recent_price,
        recent_net
    )


    rb = e["recent_buy_sol"]
    rs = e["recent_sell_sol"]

    gross = (
        abs(rb) + abs(rs)
        if valid(rb) and valid(rs)
        else None
    )

    net_eff = cap_div(
        recent_net,
        gross
    )


    early_div = (
        e["early_price_return"]
        - e["early_net_sol"]
        if (
            valid(e["early_price_return"])
            and valid(e["early_net_sol"])
        )
        else None
    )


    records.append({
        "id": e["id"],
        "timestamp": e["timestamp"],
        "token_mint": e["token_mint"],

        "y": label_r60(
            e["dex_return_60s"]
        ),

        "r60": e["dex_return_60s"],

        # baseline context
        "fa": e["fa"],
        "new_wallets30": e["new_wallets30"],
        "recent_buy_share": e["recent_buy_share"],
        "recent_net_share": e["recent_net_share"],
        "breadth_score": e["breadth_score"],
        "late_chase_score": e["late_chase_score"],

        # wallet
        "fast_flip": fast_flip,
        "fast_coverage": fast_coverage,
        "experienced_buyers": experienced_buyers,

        # capital efficiency
        "price_per_net": price_per_net,
        "net_eff": net_eff,
        "early_div": early_div,
    })


# ============================================================
# MODELS
#
# Nested structure:
#
# M0 = context only
# M1 = M0 + fast_flip
# M2 = M0 + capital efficiency
# M3 = M0 + fast_flip + capital efficiency
#
# This is the key T53 comparison.
# ============================================================

MODELS = {

    "M0_CONTEXT": [
        "fa",
        "new_wallets30",
        "recent_buy_share",
        "recent_net_share",
        "breadth_score",
        "late_chase_score",
    ],

    "M1_CONTEXT_FAST": [
        "fa",
        "new_wallets30",
        "recent_buy_share",
        "recent_net_share",
        "breadth_score",
        "late_chase_score",

        "fast_flip",
        "fast_coverage",
        "experienced_buyers",
    ],

    "M2_CONTEXT_CAP": [
        "fa",
        "new_wallets30",
        "recent_buy_share",
        "recent_net_share",
        "breadth_score",
        "late_chase_score",

        "price_per_net",
        "net_eff",
        "early_div",
    ],

    "M3_CONTEXT_FAST_CAP": [
        "fa",
        "new_wallets30",
        "recent_buy_share",
        "recent_net_share",
        "breadth_score",
        "late_chase_score",

        "fast_flip",
        "fast_coverage",
        "experienced_buyers",

        "price_per_net",
        "net_eff",
        "early_div",
    ],
}


# ============================================================
# COMMON COMPLETE-CASE COHORT
#
# Critical:
# all models evaluated on exact same events.
# ============================================================

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
# TOKEN HOLDOUT
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


def get_split(tokset):

    return [
        r for r in common
        if r["token_mint"] in tokset
    ]


train = get_split(train_tokens)
valid_set = get_split(valid_tokens)
test = get_split(test_tokens)


# ============================================================
# STANDARDIZATION
#
# TRAIN ONLY.
# ============================================================

means = {}
stds = {}


for f in all_features:

    vals = [
        r[f]
        for r in train
    ]

    mu = avg(vals)

    sd = statistics.pstdev(vals)

    if sd <= 1e-12:
        sd = 1.0

    means[f] = mu
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

        y.append(r["y"])

    return X, y


# ============================================================
# TRAIN ALL NESTED MODELS
# ============================================================

fitted = {}


for name, features in MODELS.items():

    X, y = vectorize(
        train,
        features
    )

    beta = fit_logistic(
        X,
        y
    )

    fitted[name] = {
        "features": features,
        "beta": beta
    }


# ============================================================
# EVALUATION
# ============================================================

def evaluate(model_name, rr):

    m = fitted[model_name]

    X, y = vectorize(
        rr,
        m["features"]
    )

    p = predict_logistic(
        m["beta"],
        X
    )

    return {
        "n": len(y),
        "auc": auc(y, p),
        "logloss": logloss(y, p),
        "brier": brier(y, p),
        "y": y,
        "p": p,
    }


results = defaultdict(dict)


for split_name, rr in [
    ("TRAIN", train),
    ("VALID", valid_set),
    ("TEST", test),
]:

    for model_name in MODELS:

        results[split_name][model_name] = evaluate(
            model_name,
            rr
        )


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

def first_per_token(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x: (
            x["timestamp"],
            x["id"]
        )
    ):

        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


test_first = first_per_token(test)


first_results = {
    name: evaluate(
        name,
        test_first
    )
    for name in MODELS
}


# ============================================================
# TOKEN BOOTSTRAP ON TEST
#
# Compare M3 vs M0 directly.
# Whole tokens are resampled.
# ============================================================

test_by_token = defaultdict(list)

for r in test:
    test_by_token[r["token_mint"]].append(r)


test_token_list = list(
    test_by_token.keys()
)


boot_delta_auc = []
boot_delta_ll = []
boot_delta_brier = []


rng = random.Random(
    SEED + 1000
)


for _ in range(BOOT_N):

    sampled_tokens = [
        rng.choice(test_token_list)
        for _ in range(len(test_token_list))
    ]

    sample = []

    for tok in sampled_tokens:
        sample.extend(
            test_by_token[tok]
        )

    r0 = evaluate(
        "M0_CONTEXT",
        sample
    )

    r3 = evaluate(
        "M3_CONTEXT_FAST_CAP",
        sample
    )

    if (
        r0["auc"] is not None
        and r3["auc"] is not None
    ):
        boot_delta_auc.append(
            r3["auc"] - r0["auc"]
        )

    if (
        r0["logloss"] is not None
        and r3["logloss"] is not None
    ):
        boot_delta_ll.append(
            r0["logloss"] - r3["logloss"]
        )

    if (
        r0["brier"] is not None
        and r3["brier"] is not None
    ):
        boot_delta_brier.append(
            r0["brier"] - r3["brier"]
        )


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

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


# ============================================================
# COEFFICIENTS
# ============================================================

def print_coefficients(name):

    m = fitted[name]

    print()
    print(name)
    print("-" * 100)

    pairs = list(zip(
        m["features"],
        m["beta"][1:]
    ))

    pairs.sort(
        key=lambda x: abs(x[1]),
        reverse=True
    )

    for f, b in pairs:
        print(
            f"{f:30} BETA={b:+.4f}"
        )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 180)
print("MEMECOIN LAB — T53 INCREMENTAL EDGE / NESTED MODEL AUDIT")
print("=" * 180)

print(
    f"ALL LABELED EVENTS       : {len(records)}"
)

print(
    f"COMMON COMPLETE EVENTS   : {len(common)}"
)

print(
    f"COMMON UNIQUE TOKENS     : {len(tokens)}"
)

print()

print(
    f"TRAIN : {len(train):3d} events | "
    f"{len(train_tokens):3d} tokens"
)

print(
    f"VALID : {len(valid_set):3d} events | "
    f"{len(valid_tokens):3d} tokens"
)

print(
    f"TEST  : {len(test):3d} events | "
    f"{len(test_tokens):3d} tokens"
)


# ============================================================
# A
# ============================================================

print()
print("=" * 180)
print("A) NESTED MODEL DEFINITIONS")
print("=" * 180)

for name, fs in MODELS.items():

    print(
        f"{name:24} | "
        + ", ".join(fs)
    )


# ============================================================
# B
# ============================================================

print()
print("=" * 180)
print("B) TRAIN PERFORMANCE")
print("=" * 180)

for name in MODELS:

    r = results["TRAIN"][name]

    print(
        f"{name:24} "
        f"N={r['n']:3d} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# C
# ============================================================

print()
print("=" * 180)
print("C) VALID PERFORMANCE — FROZEN TRAIN MODELS")
print("=" * 180)

for name in MODELS:

    r = results["VALID"][name]

    print(
        f"{name:24} "
        f"N={r['n']:3d} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# D
# ============================================================

print()
print("=" * 180)
print("D) TEST PERFORMANCE — FINAL HOLDOUT")
print("=" * 180)

for name in MODELS:

    r = results["TEST"][name]

    print(
        f"{name:24} "
        f"N={r['n']:3d} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# E — INCREMENTAL DELTAS
# ============================================================

print()
print("=" * 180)
print("E) INCREMENTAL OUT-OF-SAMPLE VALUE")
print("=" * 180)

comparisons = [
    (
        "FAST over CONTEXT",
        "M0_CONTEXT",
        "M1_CONTEXT_FAST"
    ),
    (
        "CAP over CONTEXT",
        "M0_CONTEXT",
        "M2_CONTEXT_CAP"
    ),
    (
        "FAST+CAP over CONTEXT",
        "M0_CONTEXT",
        "M3_CONTEXT_FAST_CAP"
    ),
    (
        "CAP added after FAST",
        "M1_CONTEXT_FAST",
        "M3_CONTEXT_FAST_CAP"
    ),
    (
        "FAST added after CAP",
        "M2_CONTEXT_CAP",
        "M3_CONTEXT_FAST_CAP"
    ),
]


for split_name in [
    "VALID",
    "TEST"
]:

    print()
    print(split_name)
    print("-" * 120)

    for label, base, new in comparisons:

        a = results[split_name][base]
        b = results[split_name][new]

        da = (
            b["auc"] - a["auc"]
            if (
                a["auc"] is not None
                and b["auc"] is not None
            )
            else None
        )

        dll = (
            a["logloss"] - b["logloss"]
            if (
                a["logloss"] is not None
                and b["logloss"] is not None
            )
            else None
        )

        dbrier = (
            a["brier"] - b["brier"]
            if (
                a["brier"] is not None
                and b["brier"] is not None
            )
            else None
        )

        print(
            f"{label:28} "
            f"ΔAUC={fmt(da):>8} "
            f"ΔLOGLOSS={fmt(dll):>8} "
            f"ΔBRIER={fmt(dbrier):>8}"
        )


# ============================================================
# F
# ============================================================

print()
print("=" * 180)
print("F) FIRST-EVENT/TOKEN TEST")
print("=" * 180)

print(
    f"FIRST TEST EVENTS = {len(test_first)}"
)

for name in MODELS:

    r = first_results[name]

    print(
        f"{name:24} "
        f"N={r['n']:3d} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# G
# ============================================================

print()
print("=" * 180)
print("G) TOKEN-LEVEL BOOTSTRAP — M3 VS M0 ON TEST")
print("=" * 180)

print(
    f"BOOT N = {BOOT_N}"
)

if boot_delta_auc:

    print(
        f"ΔAUC MED = {fmt(med(boot_delta_auc))}"
    )

    print(
        f"ΔAUC 95% CI = "
        f"[{fmt(quantile(boot_delta_auc,0.025))}, "
        f"{fmt(quantile(boot_delta_auc,0.975))}]"
    )

    print(
        f"P(ΔAUC>0) = "
        f"{100*sum(x>0 for x in boot_delta_auc)/len(boot_delta_auc):.1f}%"
    )

if boot_delta_ll:

    print(
        f"ΔLOGLOSS MED = {fmt(med(boot_delta_ll))}"
    )

    print(
        f"P(ΔLOGLOSS>0) = "
        f"{100*sum(x>0 for x in boot_delta_ll)/len(boot_delta_ll):.1f}%"
    )

if boot_delta_brier:

    print(
        f"ΔBRIER MED = {fmt(med(boot_delta_brier))}"
    )

    print(
        f"P(ΔBRIER>0) = "
        f"{100*sum(x>0 for x in boot_delta_brier)/len(boot_delta_brier):.1f}%"
    )


# ============================================================
# H
# ============================================================

print()
print("=" * 180)
print("H) STANDARDIZED COEFFICIENTS")
print("=" * 180)

print_coefficients(
    "M1_CONTEXT_FAST"
)

print_coefficients(
    "M2_CONTEXT_CAP"
)

print_coefficients(
    "M3_CONTEXT_FAST_CAP"
)


# ============================================================
# I — VERDICT
# ============================================================

print()
print("=" * 180)
print("I) DECISION SUPPORT")
print("=" * 180)


valid0 = results["VALID"]["M0_CONTEXT"]
valid3 = results["VALID"]["M3_CONTEXT_FAST_CAP"]

test0 = results["TEST"]["M0_CONTEXT"]
test3 = results["TEST"]["M3_CONTEXT_FAST_CAP"]


valid_auc_gain = (
    valid3["auc"] - valid0["auc"]
    if valid3["auc"] is not None
    and valid0["auc"] is not None
    else None
)

test_auc_gain = (
    test3["auc"] - test0["auc"]
    if test3["auc"] is not None
    and test0["auc"] is not None
    else None
)


valid_ll_gain = (
    valid0["logloss"] - valid3["logloss"]
)

test_ll_gain = (
    test0["logloss"] - test3["logloss"]
)


auc_prob = (
    sum(x > 0 for x in boot_delta_auc)
    / len(boot_delta_auc)
    if boot_delta_auc
    else 0
)


print(
    f"M3 VALID ΔAUC      = {fmt(valid_auc_gain)}"
)

print(
    f"M3 TEST ΔAUC       = {fmt(test_auc_gain)}"
)

print(
    f"M3 VALID ΔLOGLOSS  = {fmt(valid_ll_gain)}"
)

print(
    f"M3 TEST ΔLOGLOSS   = {fmt(test_ll_gain)}"
)

print(
    f"BOOT P(ΔAUC>0)     = {100*auc_prob:.1f}%"
)


if (
    valid_auc_gain is not None
    and test_auc_gain is not None
    and valid_auc_gain > 0
    and test_auc_gain > 0
    and valid_ll_gain > 0
    and test_ll_gain > 0
    and auc_prob >= 0.80
):

    print()
    print(
        "🟢 FAST-FLIP + CAPITAL-EFFICIENCY SHOW "
        "CONSISTENT INCREMENTAL OUT-OF-SAMPLE INFORMATION."
    )

    print(
        "Next step = T54 frozen incremental robustness audit."
    )

elif (
    valid_auc_gain is not None
    and test_auc_gain is not None
    and (
        valid_auc_gain > 0
        or test_auc_gain > 0
    )
):

    print()
    print(
        "🟡 INCREMENTAL SIGNAL EXISTS, "
        "BUT GENERALIZATION IS NOT YET STRONG ENOUGH."
    )

    print(
        "Keep collecting prospective data; "
        "do not optimize thresholds."
    )

else:

    print()
    print(
        "🔴 NO ROBUST INCREMENTAL VALUE FROM "
        "FAST-FLIP + CAPITAL-EFFICIENCY OVER CONTEXT."
    )

    print(
        "Do not force these features into execution."
    )


print()
print("IMPORTANT:")
print("• Same complete-case events for every model.")
print("• Token identities never cross TRAIN/VALID/TEST.")
print("• Standardization uses TRAIN only.")
print("• Model coefficients fit on TRAIN only.")
print("• VALID and TEST never refit the models.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• Fast-flip wallet history is chronological.")
print("• Bootstrap resamples whole TEST tokens.")
print("• First-event/token audit included.")
print("• T23/T31/T32/T47 remain untouched.")
print("• T53 writes nothing to DB.")
print("• Research audit only.")

db.close()
