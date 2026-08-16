import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
CAP_EPS = 0.05

SEED = 58
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


def fmt(x, n=4):
    return "NA" if x is None else f"{x:.{n}f}"


def label_r60(x):
    if not valid(x):
        return None
    if x >= RUNNER:
        return 1
    if x <= DUMP:
        return 0
    return None


def sdiv(a, b, eps=CAP_EPS):
    if not valid(a) or not valid(b):
        return None
    if abs(b) < eps:
        return None
    return a / b


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
        p[i] for i in range(len(y))
        if y[i] == 1
    ]

    neg = [
        p[i] for i in range(len(y))
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


# ============================================================
# FEATURE BUILD
# ============================================================

records = []


for r in rows:

    y = label_r60(
        r["dex_return_60s"]
    )

    if y is None:
        continue


    price_per_net = sdiv(
        r["recent_price_return"],
        r["recent_net_sol"]
    )


    rb = r["recent_buy_sol"]
    rs = r["recent_sell_sol"]


    gross = (
        abs(rb) + abs(rs)
        if valid(rb) and valid(rs)
        else None
    )


    net_eff = sdiv(
        r["recent_net_sol"],
        gross
    )


    early_div = (
        r["early_price_return"]
        - r["early_net_sol"]
        if (
            valid(r["early_price_return"])
            and valid(r["early_net_sol"])
        )
        else None
    )


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],
        "y": y,

        "fa": r["fa"],
        "new_wallets30": r["new_wallets30"],
        "recent_buy_share": r["recent_buy_share"],
        "recent_net_share": r["recent_net_share"],
        "breadth_score": r["breadth_score"],
        "late_chase_score": r["late_chase_score"],

        "price_per_net": price_per_net,
        "net_eff": net_eff,
        "early_div": early_div,
    })


CONTEXT = [
    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
]


# ============================================================
# ABLATION FAMILY
# ============================================================

MODELS = {

    "M0_CONTEXT":
        CONTEXT,

    "M1_PRICE_ONLY":
        CONTEXT + [
            "price_per_net"
        ],

    "M2_NETEFF_ONLY":
        CONTEXT + [
            "net_eff"
        ],

    "M3_EARLYDIV_ONLY":
        CONTEXT + [
            "early_div"
        ],

    "M4_PRICE_NETEFF":
        CONTEXT + [
            "price_per_net",
            "net_eff",
        ],

    "M5_PRICE_EARLYDIV":
        CONTEXT + [
            "price_per_net",
            "early_div",
        ],

    "M6_NETEFF_EARLYDIV":
        CONTEXT + [
            "net_eff",
            "early_div",
        ],

    "M7_FULL_CAP":
        CONTEXT + [
            "price_per_net",
            "net_eff",
            "early_div",
        ],
}


# ============================================================
# COMMON COMPLETE-CASE COHORT
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


rng = random.Random(SEED)
rng.shuffle(tokens)

n = len(tokens)

n_train = int(0.60*n)
n_valid = int(0.20*n)

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


def subset(tokset):

    return [
        r for r in common
        if r["token_mint"] in tokset
    ]


train = subset(train_tokens)
valid_set = subset(valid_tokens)
test = subset(test_tokens)


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
                r[f]
                - means[f]
            ) / stds[f]
            for f in features
        ])

        y.append(
            r["y"]
        )

    return X, y


# ============================================================
# FIT ALL MODELS
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
        ),
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
        "auc": auc(y,p),
        "logloss": logloss(y,p),
        "brier": brier(y,p),
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
# TEST TOKEN BOOTSTRAP
#
# Every CAP model vs context
# ============================================================

by_token = defaultdict(list)

for r in test:
    by_token[
        r["token_mint"]
    ].append(r)


test_token_list = list(
    by_token.keys()
)


boot = {
    name: {
        "auc": [],
        "ll": [],
        "brier": [],
    }
    for name in MODELS
    if name != "M0_CONTEXT"
}


rng = random.Random(
    SEED + 1000
)


for _ in range(BOOT_N):

    sampled_tokens = [
        rng.choice(
            test_token_list
        )
        for _ in range(
            len(test_token_list)
        )
    ]

    rr = []

    for tok in sampled_tokens:
        rr.extend(
            by_token[tok]
        )


    base = evaluate(
        "M0_CONTEXT",
        rr
    )


    for name in boot:

        candidate = evaluate(
            name,
            rr
        )


        if (
            base["auc"] is not None
            and candidate["auc"] is not None
        ):
            boot[name]["auc"].append(
                candidate["auc"]
                - base["auc"]
            )


        boot[name]["ll"].append(
            base["logloss"]
            - candidate["logloss"]
        )


        boot[name]["brier"].append(
            base["brier"]
            - candidate["brier"]
        )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 185)
print("MEMECOIN LAB — T58 CAP ABLATION / SIMPLIFICATION AUDIT")
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
# A
# ============================================================

print()
print("=" * 185)
print("A) MODEL DEFINITIONS")
print("=" * 185)

for name, features in MODELS.items():

    print(
        f"{name:24} | "
        + ", ".join(features)
    )


# ============================================================
# B/C/D
# ============================================================

for split in [
    "TRAIN",
    "VALID",
    "TEST",
]:

    print()
    print("=" * 185)
    print(f"{split}) PERFORMANCE")
    print("=" * 185)

    for name in MODELS:

        r = results[split][name]

        print(
            f"{name:24} "
            f"N={r['n']:3d} "
            f"AUC={fmt(r['auc'])} "
            f"LOGLOSS={fmt(r['logloss'])} "
            f"BRIER={fmt(r['brier'])}"
        )


# ============================================================
# E) INCREMENT OVER CONTEXT
# ============================================================

print()
print("=" * 185)
print("E) INCREMENTAL VALUE OVER CONTEXT")
print("=" * 185)


for split in [
    "VALID",
    "TEST",
]:

    print()
    print(split)
    print("-" * 125)


    base = results[
        split
    ][
        "M0_CONTEXT"
    ]


    for name in MODELS:

        if name == "M0_CONTEXT":
            continue


        r = results[
            split
        ][
            name
        ]


        da = (
            r["auc"]
            - base["auc"]
            if (
                r["auc"] is not None
                and base["auc"] is not None
            )
            else None
        )


        dll = (
            base["logloss"]
            - r["logloss"]
        )


        dbrier = (
            base["brier"]
            - r["brier"]
        )


        print(
            f"{name:24} "
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
    f"FIRST TEST TOKENS = {len(first_test)}"
)


for name in MODELS:

    r = first_results[
        name
    ]

    print(
        f"{name:24} "
        f"AUC={fmt(r['auc'])} "
        f"LOGLOSS={fmt(r['logloss'])} "
        f"BRIER={fmt(r['brier'])}"
    )


# ============================================================
# G) BOOTSTRAP
# ============================================================

print()
print("=" * 185)
print("G) TOKEN-LEVEL TEST BOOTSTRAP — EACH MODEL VS CONTEXT")
print("=" * 185)


for name in boot:

    a = boot[name]["auc"]
    ll = boot[name]["ll"]
    br = boot[name]["brier"]


    print()
    print(name)
    print("-" * 100)


    if a:

        print(
            f"ΔAUC MED={fmt(statistics.median(a))}"
        )

        print(
            f"ΔAUC 95% CI=["
            f"{fmt(quantile(a,0.025))}, "
            f"{fmt(quantile(a,0.975))}]"
        )

        print(
            f"P(ΔAUC>0)="
            f"{100*sum(x>0 for x in a)/len(a):.1f}%"
        )


    print(
        f"ΔLOGLOSS MED="
        f"{fmt(statistics.median(ll))}"
    )

    print(
        f"P(ΔLOGLOSS>0)="
        f"{100*sum(x>0 for x in ll)/len(ll):.1f}%"
    )


    print(
        f"ΔBRIER MED="
        f"{fmt(statistics.median(br))}"
    )

    print(
        f"P(ΔBRIER>0)="
        f"{100*sum(x>0 for x in br)/len(br):.1f}%"
    )


# ============================================================
# H) COEFFICIENTS
# ============================================================

print()
print("=" * 185)
print("H) FULL CAP STANDARDIZED COEFFICIENTS")
print("=" * 185)


full = fitted[
    "M7_FULL_CAP"
]


for f, b in zip(
    full["features"],
    full["beta"][1:]
):

    print(
        f"{f:28} "
        f"BETA={b:+.4f}"
    )


# ============================================================
# I) PRACTICAL SCORECARD
# ============================================================

print()
print("=" * 185)
print("I) PRACTICAL SIMPLIFICATION SCORECARD")
print("=" * 185)


base_v = results[
    "VALID"
][
    "M0_CONTEXT"
]

base_t = results[
    "TEST"
][
    "M0_CONTEXT"
]


scorecard = []


for name in MODELS:

    if name == "M0_CONTEXT":
        continue


    rv = results[
        "VALID"
    ][
        name
    ]


    rt = results[
        "TEST"
    ][
        name
    ]


    v_gain = (
        rv["auc"]
        - base_v["auc"]
    )


    t_gain = (
        rt["auc"]
        - base_t["auc"]
    )


    boot_prob = (
        sum(
            x > 0
            for x in boot[name]["auc"]
        )
        / len(
            boot[name]["auc"]
        )
        if boot[name]["auc"]
        else 0
    )


    both_auc = (
        v_gain > 0
        and t_gain > 0
    )


    both_cal = (
        base_v["logloss"]
        > rv["logloss"]
        and base_t["logloss"]
        > rt["logloss"]
    )


    scorecard.append(
        (
            name,
            v_gain,
            t_gain,
            boot_prob,
            both_auc,
            both_cal,
            len(
                MODELS[name]
            )
            - len(
                CONTEXT
            )
        )
    )


for (
    name,
    vg,
    tg,
    bp,
    both_auc,
    both_cal,
    cap_count
) in scorecard:

    print(
        f"{name:24} "
        f"| CAP_FEATURES={cap_count} "
        f"| VALID ΔAUC={vg:+.4f} "
        f"| TEST ΔAUC={tg:+.4f} "
        f"| BOOT+={100*bp:5.1f}% "
        f"| BOTH_AUC={both_auc} "
        f"| BOTH_CAL={both_cal}"
    )


# ============================================================
# J) DECISION
# ============================================================

print()
print("=" * 185)
print("J) DECISION SUPPORT")
print("=" * 185)


eligible = [
    x
    for x in scorecard
    if (
        x[4]       # both AUC positive
        and x[3] >= 0.80
    )
]


if eligible:

    # Prefer fewer CAP features,
    # then higher minimum holdout gain.
    eligible.sort(
        key=lambda x: (
            x[6],
            -min(
                x[1],
                x[2]
            )
        )
    )


    best = eligible[0]


    print(
        "🟢 AT LEAST ONE SIMPLIFIED CAP SPECIFICATION "
        "SURVIVES THE INCREMENTAL GATE."
    )

    print(
        f"PREFERRED MINIMAL CANDIDATE = {best[0]}"
    )

    print(
        "Do not alter it from TEST."
    )

    print(
        "Next = compare this exact frozen specification "
        "prospectively against T54/full CAP."
    )


else:

    print(
        "🟡/🔴 NO SIMPLIFIED CAP SPECIFICATION "
        "GENERALIZES STRONGLY ENOUGH YET."
    )

    print(
        "Keep T51/T54 collecting. "
        "Do not select a component from TEST."
    )


print()
print("IMPORTANT:")
print("• Same exact complete-case cohort for all models.")
print("• Token identities do not cross splits.")
print("• Standardization uses TRAIN only.")
print("• Coefficients fit on TRAIN only.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• CAP EPS remains frozen at 0.05.")
print("• Bootstrap resamples whole TEST tokens.")
print("• Simpler models are compared to the same CONTEXT baseline.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T58 writes nothing to DB.")
print("• Research ablation audit only.")

db.close()

# ============================================================
# T59 — PERMANENT CAP-V2 FREEZE EXPORT
# ============================================================

import json
import time
import hashlib
from pathlib import Path

FREEZE_FILE = Path("t59_capv2_frozen.json")

if FREEZE_FILE.exists():
    raise RuntimeError(
        "t59_capv2_frozen.json already exists. "
        "T59 freeze is immutable — do not overwrite it."
    )

TARGET_MODELS = [
    "M0_CONTEXT",
    "M3_EARLYDIV_ONLY",
]

for model_name in TARGET_MODELS:
    if model_name not in fitted:
        raise RuntimeError(
            f"Missing fitted model: {model_name}"
        )

freeze_db = sqlite3.connect(DB)

boundary_id = int(
    freeze_db.execute(
        "SELECT COALESCE(MAX(id),0) FROM events"
    ).fetchone()[0]
)

freeze_db.close()

payload = {
    "experiment": "T59_CAPV2_PROSPECTIVE",
    "frozen": True,
    "created_at_unix": time.time(),

    "source_lab": "T58",
    "seed": SEED,
    "cap_eps": CAP_EPS,

    "boundary_id": boundary_id,

    "outcome": {
        "runner_threshold": RUNNER,
        "dump_threshold": DUMP,
        "target": "dex_return_60s",
    },

    "control_model": "M0_CONTEXT",
    "primary_model": "M3_EARLYDIV_ONLY",

    "models": {},

    "standardization": {
        "means": {
            k: float(v)
            for k, v in means.items()
        },
        "stds": {
            k: float(v)
            for k, v in stds.items()
        },
    },

    "methodology": {
        "standardization": "TRAIN_ONLY",
        "coefficient_fit": "TRAIN_ONLY",
        "prospective_refit": False,
        "threshold_optimization": False,
        "primary_feature": "early_div",
        "primary_definition": (
            "early_price_return - early_net_sol"
        ),
        "net_eff": "DIAGNOSTIC_ONLY",
    },
}

for model_name in TARGET_MODELS:

    model = fitted[model_name]

    payload["models"][model_name] = {
        "features": list(
            model["features"]
        ),

        "intercept": float(
            model["beta"][0]
        ),

        "coefficients": {
            feature: float(beta)
            for feature, beta in zip(
                model["features"],
                model["beta"][1:]
            )
        },
    }

canonical = json.dumps(
    payload,
    sort_keys=True,
    separators=(",", ":"),
).encode()

payload["freeze_sha256"] = hashlib.sha256(
    canonical
).hexdigest()

FREEZE_FILE.write_text(
    json.dumps(
        payload,
        indent=2,
        sort_keys=True,
    )
)

print()
print("=" * 110)
print("T59 CAP-V2 FREEZE CREATED")
print("=" * 110)

print(f"BOUNDARY ID  : {boundary_id}")
print(f"CONTROL      : {payload['control_model']}")
print(f"PRIMARY      : {payload['primary_model']}")
print("EARLY_DIV    : early_price_return - early_net_sol")
print("NET_EFF      : diagnostic only")
print(f"SHA256       : {payload['freeze_sha256']}")
print(f"FILE         : {FREEZE_FILE}")

print()
print(
    "⚠️ DO NOT regenerate this freeze after prospective collection begins."
)
