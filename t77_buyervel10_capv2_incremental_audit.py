import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

WINDOW = 30.0

SEED = 58
BOOT_N = 5000


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
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def label_r60(x):
    if not valid(x):
        return None

    if x >= RUNNER:
        return 1

    if x <= DUMP:
        return 0

    return None


def sigmoid(z):
    z = max(
        min(z, 35.0),
        -35.0
    )

    return (
        1.0
        / (
            1.0
            + math.exp(-z)
        )
    )


def fit_logistic(
    X,
    y,
    l2=1.0,
    lr=0.05,
    epochs=2500
):

    p = len(X[0])

    beta = [
        0.0
    ] * (p+1)

    n = len(X)

    for _ in range(epochs):

        grad = [
            0.0
        ] * (p+1)

        for xi, yi in zip(
            X,
            y
        ):

            z = beta[0]

            for j in range(p):
                z += (
                    beta[j+1]
                    * xi[j]
                )

            pr = sigmoid(z)

            err = (
                pr - yi
            )

            grad[0] += err

            for j in range(p):
                grad[j+1] += (
                    err * xi[j]
                )

        grad[0] /= n

        for j in range(p):

            grad[j+1] = (
                grad[j+1]/n
                + l2*beta[j+1]/n
            )

        for j in range(p+1):
            beta[j] -= (
                lr * grad[j]
            )

    return beta


def predict(
    beta,
    X
):

    out = []

    for xi in X:

        z = beta[0]

        for j,x in enumerate(
            xi
        ):
            z += (
                beta[j+1]
                * x
            )

        out.append(
            sigmoid(z)
        )

    return out


def auc(
    y,
    p
):

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

    return (
        wins / total
    )


def logloss(
    y,
    p
):

    if not y:
        return None

    eps = 1e-12

    vals = []

    for yi,pi in zip(
        y,
        p
    ):

        pi = min(
            max(pi, eps),
            1.0-eps
        )

        vals.append(
            -(
                yi*math.log(pi)
                + (1-yi)*math.log(1-pi)
            )
        )

    return avg(vals)


def brier(
    y,
    p
):

    if not y:
        return None

    return avg([
        (pi-yi)**2
        for yi,pi
        in zip(y,p)
    ])


def pearson(
    xs,
    ys
):

    pairs = [
        (x,y)
        for x,y in zip(xs,ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xx = [
        x for x,_ in pairs
    ]

    yy = [
        y for _,y in pairs
    ]

    mx = avg(xx)
    my = avg(yy)

    dx = math.sqrt(
        sum(
            (x-mx)**2
            for x in xx
        )
    )

    dy = math.sqrt(
        sum(
            (y-my)**2
            for y in yy
        )
    )

    if dx == 0 or dy == 0:
        return None

    return (
        sum(
            (x-mx)*(y-my)
            for x,y in pairs
        )
        / (dx*dy)
    )


def quantile(
    xs,
    q
):

    xs = sorted(xs)

    if not xs:
        return None

    pos = (
        (len(xs)-1)
        * q
    )

    lo = int(
        math.floor(pos)
    )

    hi = int(
        math.ceil(pos)
    )

    if lo == hi:
        return xs[lo]

    w = (
        pos - lo
    )

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


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


# ============================================================
# FEATURE BUILD
# ============================================================

records = []


for e in events:

    y = label_r60(
        e["dex_return_60s"]
    )

    if y is None:
        continue


    # --------------------------------------------------------
    # CAP-v2
    # --------------------------------------------------------

    early_div = None

    if (
        valid(
            e["early_price_return"]
        )
        and valid(
            e["early_net_sol"]
        )
    ):

        early_div = (
            e["early_price_return"]
            - e["early_net_sol"]
        )


    # --------------------------------------------------------
    # BUYER VELOCITY 10
    # Frozen T75/T76 definition
    # --------------------------------------------------------

    ts = e[
        "timestamp"
    ]


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

        buyer_unique_30 = (
            len(arrivals)
        )

    else:

        buyer_velocity_10 = None
        buyer_unique_30 = (
            len(arrivals)
        )


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "y":
            y,

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

        "buyer_unique_30":
            buyer_unique_30,
    })


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

    "M1_CAPV2_EARLYDIV":
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


# ============================================================
# COMPLETE-CASE COHORT
# ============================================================

all_features = sorted(
    set(
        f
        for features
        in MODELS.values()
        for f in features
    )
)


common = [
    r
    for r in records
    if all(
        valid(
            r.get(f)
        )
        for f in all_features
    )
]


tokens = sorted(
    set(
        r["token_mint"]
        for r in common
    )
)


# ============================================================
# TOKEN SPLIT
# ============================================================

rng = random.Random(
    SEED
)

rng.shuffle(
    tokens
)


n = len(tokens)

n_train = int(
    0.60*n
)

n_valid = int(
    0.20*n
)


train_tokens = set(
    tokens[:n_train]
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


def subset(
    tokset
):

    return [
        r for r in common
        if r["token_mint"]
        in tokset
    ]


train = subset(
    train_tokens
)

valid_set = subset(
    valid_tokens
)

test = subset(
    test_tokens
)


# ============================================================
# TRAIN-ONLY STANDARDIZATION
# ============================================================

means = {}
stds = {}


for f in all_features:

    vals = [
        r[f]
        for r in train
    ]

    mu = avg(
        vals
    )

    sd = statistics.pstdev(
        vals
    )

    if sd <= 1e-12:
        sd = 1.0

    means[f] = mu
    stds[f] = sd


def vectorize(
    rr,
    features
):

    X = []
    y = []

    for r in rr:

        X.append([
            (
                r[f]
                - means[f]
            )
            / stds[f]

            for f in features
        ])

        y.append(
            r["y"]
        )

    return X,y


# ============================================================
# FIT ON TRAIN ONLY
# ============================================================

fitted = {}


for name,features in MODELS.items():

    X,y = vectorize(
        train,
        features
    )

    fitted[
        name
    ] = {
        "features":
            features,

        "beta":
            fit_logistic(
                X,
                y
            )
    }


def evaluate(
    name,
    rr
):

    model = fitted[
        name
    ]

    X,y = vectorize(
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
            auc(
                y,
                p
            ),

        "logloss":
            logloss(
                y,
                p
            ),

        "brier":
            brier(
                y,
                p
            ),
    }


results = defaultdict(
    dict
)


for split_name,rr in [
    ("TRAIN",train),
    ("VALID",valid_set),
    ("TEST",test),
]:

    for name in MODELS:

        results[
            split_name
        ][
            name
        ] = evaluate(
            name,
            rr
        )


# ============================================================
# FIRST EVENT / TOKEN TEST
# ============================================================

def first_per_token(
    rr
):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x: (
            x["timestamp"],
            x["id"]
        )
    ):

        tok = r[
            "token_mint"
        ]

        if tok in seen:
            continue

        seen.add(
            tok
        )

        out.append(
            r
        )

    return out


first_test = first_per_token(
    test
)


first_results = {
    name:
        evaluate(
            name,
            first_test
        )

    for name in MODELS
}


# ============================================================
# TOKEN-LEVEL TEST BOOTSTRAP
#
# M3 - M1 = BUYER VELOCITY ADDED AFTER CAP-v2
# ============================================================

by_token = defaultdict(
    list
)

for r in test:

    by_token[
        r["token_mint"]
    ].append(r)


test_token_list = list(
    by_token.keys()
)


rng = random.Random(
    SEED + 7700
)


boot_auc = []
boot_ll = []
boot_brier = []


for _ in range(
    BOOT_N
):

    sampled = [
        rng.choice(
            test_token_list
        )

        for _ in range(
            len(
                test_token_list
            )
        )
    ]


    rr = []

    for tok in sampled:

        rr.extend(
            by_token[
                tok
            ]
        )


    base = evaluate(
        "M1_CAPV2_EARLYDIV",
        rr
    )

    challenger = evaluate(
        "M3_CAPV2_BUYERVEL10",
        rr
    )


    if (
        base["auc"]
        is not None

        and challenger[
            "auc"
        ] is not None
    ):

        boot_auc.append(
            challenger["auc"]
            - base["auc"]
        )


    boot_ll.append(
        base["logloss"]
        - challenger["logloss"]
    )


    boot_brier.append(
        base["brier"]
        - challenger["brier"]
    )


# ============================================================
# TEST LEAVE-ONE-TOKEN-OUT
# ============================================================

loo = []


for tok in test_token_list:

    rr = [
        r for r in test
        if r["token_mint"]
        != tok
    ]


    base = evaluate(
        "M1_CAPV2_EARLYDIV",
        rr
    )

    challenger = evaluate(
        "M3_CAPV2_BUYERVEL10",
        rr
    )


    if (
        base["auc"]
        is not None

        and challenger[
            "auc"
        ] is not None
    ):

        loo.append(
            (
                challenger["auc"]
                - base["auc"],
                tok
            )
        )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 185)
print(
    "MEMECOIN LAB — T77 BUYER VELOCITY 10 + CAP-V2 INCREMENTAL AUDIT"
)
print("=" * 185)

print(
    f"ALL LABELED EVENTS      : {len(records)}"
)

print(
    f"COMMON COMPLETE EVENTS  : {len(common)}"
)

print(
    f"COMMON UNIQUE TOKENS    : {len(tokens)}"
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
print("=" * 185)
print("A) FROZEN MODEL DEFINITIONS")
print("=" * 185)


for name,features in MODELS.items():

    print(
        f"{name:30} | "
        + ", ".join(
            features
        )
    )


print()

print(
    "CAP-v2         = early_price_return - early_net_sol"
)

print(
    "BUYER_VEL_10   = unique buyer arrivals in final 10s / 10"
)

print(
    "FROZEN DIR     = LOWER => RUN-like (T75/T76)"
)


# ============================================================
# B/C/D PERFORMANCE
# ============================================================

for split in [
    "TRAIN",
    "VALID",
    "TEST"
]:

    print()
    print("=" * 185)
    print(
        f"{split}) MODEL PERFORMANCE"
    )
    print("=" * 185)

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
# E) INCREMENT
# ============================================================

print()
print("=" * 185)
print(
    "E) BUYER-VELOCITY INCREMENT AFTER CAP-V2"
)
print("=" * 185)


for split in [
    "VALID",
    "TEST"
]:

    base = results[
        split
    ][
        "M1_CAPV2_EARLYDIV"
    ]

    challenger = results[
        split
    ][
        "M3_CAPV2_BUYERVEL10"
    ]


    da = (
        challenger["auc"]
        - base["auc"]
    )

    dll = (
        base["logloss"]
        - challenger["logloss"]
    )

    dbrier = (
        base["brier"]
        - challenger["brier"]
    )


    print()
    print(split)
    print("-" * 100)

    print(
        f"ΔAUC     = {da:+.4f}"
    )

    print(
        f"ΔLOGLOSS = {dll:+.4f}"
    )

    print(
        f"ΔBRIER   = {dbrier:+.4f}"
    )


# ============================================================
# F) FIRST TOKEN
# ============================================================

print()
print("=" * 185)
print(
    "F) FIRST-EVENT/TOKEN TEST"
)
print("=" * 185)

print(
    f"FIRST TEST TOKENS = {len(first_test)}"
)


for name in MODELS:

    r = first_results[
        name
    ]

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
print(
    "G) TOKEN-LEVEL TEST BOOTSTRAP — M3 vs CAP-v2"
)
print("=" * 185)

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
    f"{fmt(med(boot_brier))}"
)

print(
    f"P(ΔBRIER>0) = "
    f"{100*sum(x>0 for x in boot_brier)/len(boot_brier):.1f}%"
)


# ============================================================
# H) REDUNDANCY
# ============================================================

print()
print("=" * 185)
print(
    "H) CAP-v2 / BUYER VELOCITY REDUNDANCY"
)
print("=" * 185)


feature_corr = pearson(
    [
        r["early_div"]
        for r in common
    ],
    [
        r["buyer_velocity_10"]
        for r in common
    ]
)


wallet_corr = pearson(
    [
        r["buyer_velocity_10"]
        for r in common
    ],
    [
        r["new_wallets30"]
        for r in common
    ]
)


unique_corr = pearson(
    [
        r["buyer_velocity_10"]
        for r in common
    ],
    [
        r["buyer_unique_30"]
        for r in common
    ]
)


print(
    f"CORR(early_div, buyer_velocity_10) = "
    f"{fmt(feature_corr)}"
)

print(
    f"CORR(buyer_velocity_10, new_wallets30) = "
    f"{fmt(wallet_corr)}"
)

print(
    f"CORR(buyer_velocity_10, buyer_unique_30) = "
    f"{fmt(unique_corr)}"
)


# ============================================================
# I) COEFFICIENTS
# ============================================================

print()
print("=" * 185)
print(
    "I) STANDARDIZED COEFFICIENTS — COMBINED MODEL"
)
print("=" * 185)


model = fitted[
    "M3_CAPV2_BUYERVEL10"
]


for feature,beta in zip(
    model["features"],
    model["beta"][1:]
):

    print(
        f"{feature:28} "
        f"BETA={beta:+.4f}"
    )


# ============================================================
# J) LOO
# ============================================================

print()
print("=" * 185)
print(
    "J) TEST LEAVE-ONE-TOKEN-OUT — ΔAUC M3 vs CAP-v2"
)
print("=" * 185)


if loo:

    vals = [
        x[0]
        for x in loo
    ]

    print(
        f"TOKENS={len(vals)} "
        f"| MED ΔAUC={fmt(med(vals))} "
        f"| WORST={fmt(min(vals))} "
        f"| BEST={fmt(max(vals))}"
    )


    loo.sort()


    print()
    print(
        "WORST 5"
    )

    for delta,tok in loo[:5]:

        print(
            f"{tok[:28]:28} "
            f"| ΔAUC={delta:+.4f}"
        )


    print()
    print(
        "BEST 5"
    )

    for delta,tok in loo[-5:]:

        print(
            f"{tok[:28]:28} "
            f"| ΔAUC={delta:+.4f}"
        )


# ============================================================
# K) SAMPLE CONTEXT
# ============================================================

print()
print("=" * 185)
print(
    "K) BUYER-VELOCITY COHORT CONTEXT"
)
print("=" * 185)


print(
    f"COMMON N = {len(common)}"
)

print(
    f"MED BUYER VELOCITY 10 = "
    f"{fmt(med([
        r['buyer_velocity_10']
        for r in common
    ]))}"
)

print(
    f"MED UNIQUE BUYERS 30  = "
    f"{fmt(med([
        r['buyer_unique_30']
        for r in common
    ]))}"
)


# ============================================================
# L) DECISION
# ============================================================

print()
print("=" * 185)
print(
    "L) DECISION SUPPORT"
)
print("=" * 185)


vb = results[
    "VALID"
][
    "M1_CAPV2_EARLYDIV"
]

vn = results[
    "VALID"
][
    "M3_CAPV2_BUYERVEL10"
]

tb = results[
    "TEST"
][
    "M1_CAPV2_EARLYDIV"
]

tn = results[
    "TEST"
][
    "M3_CAPV2_BUYERVEL10"
]


valid_auc_gain = (
    vn["auc"]
    - vb["auc"]
)

test_auc_gain = (
    tn["auc"]
    - tb["auc"]
)

valid_ll_gain = (
    vb["logloss"]
    - vn["logloss"]
)

test_ll_gain = (
    tb["logloss"]
    - tn["logloss"]
)

valid_brier_gain = (
    vb["brier"]
    - vn["brier"]
)

test_brier_gain = (
    tb["brier"]
    - tn["brier"]
)


boot_prob = (
    sum(
        x > 0
        for x in boot_auc
    )
    / len(boot_auc)

    if boot_auc
    else 0
)


print(
    f"VALID ΔAUC       = "
    f"{valid_auc_gain:+.4f}"
)

print(
    f"TEST ΔAUC        = "
    f"{test_auc_gain:+.4f}"
)

print(
    f"VALID ΔLOGLOSS   = "
    f"{valid_ll_gain:+.4f}"
)

print(
    f"TEST ΔLOGLOSS    = "
    f"{test_ll_gain:+.4f}"
)

print(
    f"VALID ΔBRIER     = "
    f"{valid_brier_gain:+.4f}"
)

print(
    f"TEST ΔBRIER      = "
    f"{test_brier_gain:+.4f}"
)

print(
    f"BOOT P(ΔAUC>0)   = "
    f"{100*boot_prob:.1f}%"
)

print(
    f"FEATURE CORR     = "
    f"{fmt(feature_corr)}"
)

print()


if (
    valid_auc_gain > 0
    and test_auc_gain > 0

    and valid_ll_gain >= 0
    and test_ll_gain >= 0

    and boot_prob >= 0.80
):

    print(
        "🟢 BUYER VELOCITY 10 ADDS CONSISTENT "
        "INCREMENTAL INFORMATION AFTER CAP-v2."
    )

    print(
        "Candidate for a NEW frozen prospective shadow."
    )

    print(
        "Do NOT modify T59."
    )


elif (
    valid_auc_gain > 0
    or test_auc_gain > 0
):

    print(
        "🟡 BUYER VELOCITY 10 SHOWS "
        "PARTIAL INCREMENTAL VALUE."
    )

    print(
        "Historical evidence insufficient for promotion."
    )


else:

    print(
        "🔴 BUYER VELOCITY 10 DOES NOT ADD "
        "ROBUST INCREMENTAL VALUE AFTER CAP-v2."
    )

    print(
        "Keep T59 CAP-v2 simple."
    )


print()
print("IMPORTANT:")
print("• Same complete-case cohort for every model.")
print("• buyer_velocity_10 definition frozen from T75/T76.")
print("• Buyer window is strict pre-event 30s.")
print("• buyer_velocity_10 = unique buyer arrivals final 10s / 10.")
print("• No future swaps.")
print("• Token identities do not cross splits.")
print("• Standardization uses TRAIN only.")
print("• Coefficients fit on TRAIN only.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• Bootstrap resamples whole TEST tokens.")
print("• T59 remains frozen and untouched.")
print("• T77 writes nothing to DB.")
print("• Historical incremental diagnostic only.")
print("• Final confirmation must be prospective.")

db.close()
