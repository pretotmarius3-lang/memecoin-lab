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

SEED = 57
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


def label_r60(x):
    if not valid(x):
        return None
    if x >= RUNNER:
        return 1
    if x <= DUMP:
        return 0
    return None


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def cap_div(a, b, eps=CAP_EPS):
    if not valid(a) or not valid(b):
        return None
    if abs(b) < eps:
        return None
    return a / b


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def sigmoid(z):
    z = max(min(z, 35), -35)
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

        for j in range(p + 1):
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

        pi = min(max(pi, eps), 1.0-eps)

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
        (pi-yi)**2
        for yi, pi in zip(y,p)
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

    s.recent_buy_share,
    s.recent_net_share,
    s.breadth_score,
    s.late_chase_score,

    s.recent_price_return,
    s.recent_net_sol,
    s.recent_buy_sol,
    s.recent_sell_sol,

    s.early_price_return,
    s.early_net_sol

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
    e
    for e in events
    if label_r60(
        e["dex_return_60s"]
    ) is not None
]


# ============================================================
# WALLET FAST-FLIP HISTORY
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

    ts = e["timestamp"]

    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < ts
    ):

        process_swap(swaps[swap_idx])
        swap_idx += 1


    buyer_rows = db.execute("""
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
        ts-PRE_EVENT_SEC,
        ts
    )).fetchall()


    buyers = [
        r["wallet"]
        for r in buyer_rows
    ]


    ff = []

    for w in buyers:

        n = completed[w]

        if n < FAST_MIN_PRIOR_TRADES:
            continue

        x = safe_div(
            fast_flips[w],
            n
        )

        if valid(x):
            ff.append(x)


    fast_flip = avg(ff)

    experienced_buyers = len(ff)

    fast_coverage = safe_div(
        experienced_buyers,
        len(buyers)
    )


    price_per_net = cap_div(
        e["recent_price_return"],
        e["recent_net_sol"]
    )


    rb = e["recent_buy_sol"]
    rs = e["recent_sell_sol"]


    gross = (
        abs(rb) + abs(rs)
        if valid(rb) and valid(rs)
        else None
    )


    net_eff = cap_div(
        e["recent_net_sol"],
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


    sell_buy_ratio = cap_div(
        abs(rs) if valid(rs) else None,
        abs(rb) if valid(rb) else None
    )


    records.append({
        "id": e["id"],
        "timestamp": e["timestamp"],
        "token_mint": e["token_mint"],

        "y": label_r60(
            e["dex_return_60s"]
        ),

        # context
        "fa": e["fa"],
        "new_wallets30": e["new_wallets30"],
        "recent_buy_share": e["recent_buy_share"],
        "recent_net_share": e["recent_net_share"],
        "breadth_score": e["breadth_score"],
        "late_chase_score": e["late_chase_score"],

        # FAST
        "fast_flip": fast_flip,
        "fast_coverage": fast_coverage,
        "experienced_buyers": experienced_buyers,

        # CAP
        "price_per_net": price_per_net,
        "net_eff": net_eff,
        "early_div": early_div,

        # T56 secondary
        "sell_buy_ratio": sell_buy_ratio,
    })


# ============================================================
# NESTED MODEL DEFINITIONS
# ============================================================

CONTEXT = [
    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
]

FAST = [
    "fast_flip",
    "fast_coverage",
    "experienced_buyers",
]

CAP = [
    "price_per_net",
    "net_eff",
    "early_div",
]

SELL = [
    "sell_buy_ratio",
]


MODELS = {

    "M0_CONTEXT":
        CONTEXT,

    "M1_CONTEXT_FAST":
        CONTEXT + FAST,

    "M2_CONTEXT_CAP":
        CONTEXT + CAP,

    "M3_CONTEXT_FAST_CAP":
        CONTEXT + FAST + CAP,

    "M4_CONTEXT_CAP_SELL":
        CONTEXT + CAP + SELL,

    "M5_CONTEXT_FAST_CAP_SELL":
        CONTEXT + FAST + CAP + SELL,
}


# ============================================================
# COMMON COMPLETE CASES
# ============================================================

all_features = sorted(
    set(
        f
        for fs in MODELS.values()
        for f in fs
    )
)


common = [
    r
    for r in records
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


def subset(tokset):

    return [
        r
        for r in common
        if r["token_mint"] in tokset
    ]


train = subset(train_tokens)
valid_set = subset(valid_tokens)
test = subset(test_tokens)


# ============================================================
# STANDARDIZATION — TRAIN ONLY
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
            (r[f] - means[f]) / stds[f]
            for f in features
        ])

        y.append(r["y"])

    return X, y


# ============================================================
# FIT
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
        "beta": beta,
    }


# ============================================================
# EVAL
# ============================================================

def evaluate(name, rr):

    m = fitted[name]

    X, y = vectorize(
        rr,
        m["features"]
    )

    p = predict(
        m["beta"],
        X
    )

    return {
        "n": len(y),
        "auc": auc(y,p),
        "logloss": logloss(y,p),
        "brier": brier(y,p),
        "y": y,
        "p": p,
    }


results = defaultdict(dict)


for split_name, rr in [
    ("TRAIN", train),
    ("VALID", valid_set),
    ("TEST", test),
]:

    for model in MODELS:

        results[split_name][model] = evaluate(
            model,
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


first_test = first_per_token(
    test
)


first_results = {
    name: evaluate(
        name,
        first_test
    )
    for name in MODELS
}


# ============================================================
# BOOTSTRAP TEST TOKENS
#
# Critical comparisons:
# M4 vs M2  = SELL after CAP
# M5 vs M3  = SELL after FAST+CAP
# ============================================================

by_token = defaultdict(list)

for r in test:
    by_token[r["token_mint"]].append(r)


test_tokens_list = list(
    by_token.keys()
)


rng = random.Random(
    SEED + 1000
)


boot = {
    "M4_MINUS_M2_AUC": [],
    "M5_MINUS_M3_AUC": [],
    "M4_MINUS_M2_LL": [],
    "M5_MINUS_M3_LL": [],
}


for _ in range(BOOT_N):

    sampled = [
        rng.choice(test_tokens_list)
        for _ in range(len(test_tokens_list))
    ]

    rr = []

    for tok in sampled:
        rr.extend(
            by_token[tok]
        )


    r2 = evaluate(
        "M2_CONTEXT_CAP",
        rr
    )

    r3 = evaluate(
        "M3_CONTEXT_FAST_CAP",
        rr
    )

    r4 = evaluate(
        "M4_CONTEXT_CAP_SELL",
        rr
    )

    r5 = evaluate(
        "M5_CONTEXT_FAST_CAP_SELL",
        rr
    )


    if (
        r2["auc"] is not None
        and r4["auc"] is not None
    ):

        boot["M4_MINUS_M2_AUC"].append(
            r4["auc"] - r2["auc"]
        )


    if (
        r3["auc"] is not None
        and r5["auc"] is not None
    ):

        boot["M5_MINUS_M3_AUC"].append(
            r5["auc"] - r3["auc"]
        )


    if (
        r2["logloss"] is not None
        and r4["logloss"] is not None
    ):

        boot["M4_MINUS_M2_LL"].append(
            r2["logloss"] - r4["logloss"]
        )


    if (
        r3["logloss"] is not None
        and r5["logloss"] is not None
    ):

        boot["M5_MINUS_M3_LL"].append(
            r3["logloss"] - r5["logloss"]
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
# OUTPUT
# ============================================================

print("=" * 185)
print("MEMECOIN LAB — T57 INCREMENTAL FAMILY AUDIT")
print("=" * 185)

print(
    f"ALL LABELED EVENTS     : {len(records)}"
)

print(
    f"COMMON COMPLETE EVENTS : {len(common)}"
)

print(
    f"COMMON UNIQUE TOKENS   : {len(tokens)}"
)

print()

print(
    f"TRAIN | N={len(train):3d} | TOK={len(train_tokens):2d}"
)

print(
    f"VALID | N={len(valid_set):3d} | TOK={len(valid_tokens):2d}"
)

print(
    f"TEST  | N={len(test):3d} | TOK={len(test_tokens):2d}"
)


# ============================================================
# A) MODEL DEFINITIONS
# ============================================================

print()
print("=" * 185)
print("A) MODEL DEFINITIONS")
print("=" * 185)

for name, fs in MODELS.items():
    print(
        f"{name:30} | "
        + ", ".join(fs)
    )


# ============================================================
# B/C/D
# ============================================================

for split_name in [
    "TRAIN",
    "VALID",
    "TEST"
]:

    print()
    print("=" * 185)
    print(f"{split_name}) MODEL PERFORMANCE")
    print("=" * 185)

    for name in MODELS:

        r = results[split_name][name]

        print(
            f"{name:30} "
            f"N={r['n']:3d} "
            f"AUC={fmt(r['auc'])} "
            f"LOGLOSS={fmt(r['logloss'])} "
            f"BRIER={fmt(r['brier'])}"
        )


# ============================================================
# E) INCREMENTAL SELL-RATIO VALUE
# ============================================================

print()
print("=" * 185)
print("E) SELL-RATIO INCREMENTAL VALUE")
print("=" * 185)


comparisons = [
    (
        "SELL after CAP",
        "M2_CONTEXT_CAP",
        "M4_CONTEXT_CAP_SELL"
    ),
    (
        "SELL after FAST+CAP",
        "M3_CONTEXT_FAST_CAP",
        "M5_CONTEXT_FAST_CAP_SELL"
    ),
]


for split_name in [
    "VALID",
    "TEST"
]:

    print()
    print(split_name)
    print("-" * 125)

    for label, base, new in comparisons:

        a = results[split_name][base]
        b = results[split_name][new]

        da = (
            b["auc"] - a["auc"]
            if a["auc"] is not None
            and b["auc"] is not None
            else None
        )

        dll = (
            a["logloss"] - b["logloss"]
            if a["logloss"] is not None
            and b["logloss"] is not None
            else None
        )

        dbrier = (
            a["brier"] - b["brier"]
            if a["brier"] is not None
            and b["brier"] is not None
            else None
        )

        print(
            f"{label:24} "
            f"ΔAUC={fmt(da):>8} "
            f"ΔLOGLOSS={fmt(dll):>8} "
            f"ΔBRIER={fmt(dbrier):>8}"
        )


# ============================================================
# F) FIRST TOKEN
# ============================================================

print()
print("=" * 185)
print("F) FIRST-EVENT/TOKEN TEST")
print("=" * 185)

print(
    f"FIRST TEST TOKENS={len(first_test)}"
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
# G) BOOTSTRAP
# ============================================================

print()
print("=" * 185)
print("G) TOKEN-LEVEL BOOTSTRAP — SELL-RATIO INCREMENT")
print("=" * 185)


for key in [
    "M4_MINUS_M2_AUC",
    "M5_MINUS_M3_AUC",
]:

    xs = boot[key]

    print()
    print(key)

    if xs:

        print(
            f"MED={fmt(statistics.median(xs))}"
        )

        print(
            f"95% CI=["
            f"{fmt(quantile(xs,0.025))}, "
            f"{fmt(quantile(xs,0.975))}]"
        )

        print(
            f"P(ΔAUC>0)="
            f"{100*sum(x>0 for x in xs)/len(xs):.1f}%"
        )


for key in [
    "M4_MINUS_M2_LL",
    "M5_MINUS_M3_LL",
]:

    xs = boot[key]

    print()
    print(key)

    if xs:

        print(
            f"MED={fmt(statistics.median(xs))}"
        )

        print(
            f"P(ΔLOGLOSS>0)="
            f"{100*sum(x>0 for x in xs)/len(xs):.1f}%"
        )


# ============================================================
# H) COEFFICIENTS
# ============================================================

print()
print("=" * 185)
print("H) SELL-RATIO COEFFICIENTS")
print("=" * 185)


for name in [
    "M4_CONTEXT_CAP_SELL",
    "M5_CONTEXT_FAST_CAP_SELL"
]:

    model = fitted[name]

    pairs = list(zip(
        model["features"],
        model["beta"][1:]
    ))

    sell = [
        p for p in pairs
        if p[0] == "sell_buy_ratio"
    ]

    print(
        f"{name:30} "
        f"SELL_BETA="
        f"{fmt(sell[0][1] if sell else None)}"
    )


# ============================================================
# I) DECISION
# ============================================================

print()
print("=" * 185)
print("I) DECISION SUPPORT")
print("=" * 185)


v2 = results["VALID"]["M2_CONTEXT_CAP"]
v4 = results["VALID"]["M4_CONTEXT_CAP_SELL"]

t2 = results["TEST"]["M2_CONTEXT_CAP"]
t4 = results["TEST"]["M4_CONTEXT_CAP_SELL"]


valid_gain = (
    v4["auc"] - v2["auc"]
    if v4["auc"] is not None
    and v2["auc"] is not None
    else None
)

test_gain = (
    t4["auc"] - t2["auc"]
    if t4["auc"] is not None
    and t2["auc"] is not None
    else None
)


boot_prob = (
    sum(
        x > 0
        for x in boot["M4_MINUS_M2_AUC"]
    )
    / len(
        boot["M4_MINUS_M2_AUC"]
    )
    if boot["M4_MINUS_M2_AUC"]
    else 0
)


print(
    f"SELL after CAP VALID ΔAUC = {fmt(valid_gain)}"
)

print(
    f"SELL after CAP TEST  ΔAUC = {fmt(test_gain)}"
)

print(
    f"BOOT P(ΔAUC>0)            = {100*boot_prob:.1f}%"
)


if (
    valid_gain is not None
    and test_gain is not None
    and valid_gain > 0
    and test_gain > 0
    and boot_prob >= 0.80
):

    print()
    print(
        "🟢 SELL/BUY FLOW RATIO ADDS CONSISTENT "
        "INCREMENTAL INFORMATION AFTER CAP."
    )

    print(
        "Candidate for frozen prospective recording."
    )


elif (
    valid_gain is not None
    and test_gain is not None
    and (
        valid_gain > 0
        or test_gain > 0
    )
):

    print()
    print(
        "🟡 SELL/BUY RATIO SHOWS PARTIAL INCREMENTAL VALUE."
    )

    print(
        "Do not promote yet."
    )


else:

    print()
    print(
        "🔴 SELL/BUY RATIO DOES NOT ADD ROBUST "
        "INCREMENTAL INFORMATION AFTER CAP."
    )

    print(
        "Do not add it to the signal stack."
    )


print()
print("IMPORTANT:")
print("• Same complete-case cohort for every model.")
print("• Token identities do not cross splits.")
print("• Standardization uses TRAIN only.")
print("• Coefficients fit on TRAIN only.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• Fast-flip history is chronological.")
print("• CAP definitions frozen from T53/T56.")
print("• SELL ratio definition frozen from T56.")
print("• Bootstrap resamples whole TEST tokens.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T57 writes nothing to DB.")
print("• Research audit only.")

db.close()
