import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

CAP_EPS = 0.05


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
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


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

    vals = []

    for yi, pi in zip(y, p):

        pi = min(max(pi, eps), 1-eps)

        vals.append(
            -(
                yi * math.log(pi)
                + (1-yi) * math.log(1-pi)
            )
        )

    return avg(vals)


def brier(y, p):
    return avg([
        (pi - yi)**2
        for yi, pi in zip(y, p)
    ])


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# HISTORICAL TRAIN SET
#
# Uses same features as T53 context + CAP branch.
# ============================================================

hist = db.execute("""
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
    e.dex_return_60s IS NOT NULL

ORDER BY e.timestamp, e.id
""").fetchall()


records = []


for r in hist:

    y = label_r60(
        r["dex_return_60s"]
    )

    if y is None:
        continue


    price_per_net = cap_div(
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


    net_eff = cap_div(
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


    rec = {
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
    }


    if all(
        valid(rec[k])
        for k in [
            "fa",
            "new_wallets30",
            "recent_buy_share",
            "recent_net_share",
            "breadth_score",
            "late_chase_score",
            "price_per_net",
            "net_eff",
            "early_div",
        ]
    ):
        records.append(rec)


# ============================================================
# MODELS
# ============================================================

M0 = [
    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
]

M2 = M0 + [
    "price_per_net",
    "net_eff",
    "early_div",
]


# ============================================================
# FREEZE TRAIN ON HISTORICAL PRE-T51 BOUNDARY
#
# Critical:
# use only events before T51 prospective boundary.
# ============================================================

boundary_row = db.execute("""
SELECT value
FROM t51_meta
WHERE key='T51_BOUNDARY_ID'
""").fetchone()


if boundary_row is None:
    raise RuntimeError(
        "T51 boundary not found. Start T51 first."
    )


boundary_id = int(
    boundary_row["value"]
)


train = [
    r for r in records
    if r["id"] <= boundary_id
]


# ============================================================
# STANDARDIZATION — HISTORICAL TRAIN ONLY
# ============================================================

all_features = sorted(
    set(M0 + M2)
)

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

        if not all(
            valid(r.get(f))
            for f in features
        ):
            continue

        X.append([
            (r[f] - means[f]) / stds[f]
            for f in features
        ])

        y.append(r["y"])

    return X, y


X0, y0 = vectorize(
    train,
    M0
)

X2, y2 = vectorize(
    train,
    M2
)


beta0 = fit_logistic(
    X0,
    y0
)

beta2 = fit_logistic(
    X2,
    y2
)


# ============================================================
# T51 FORWARD DATA
# ============================================================

forward_rows = db.execute("""
SELECT
    f.event_id,
    f.token_mint,
    f.event_timestamp,
    f.recent_price_per_net_sol,
    f.recent_net_efficiency,
    f.early_flow_price_div,
    f.dex_return_60s,
    f.binary_label,
    f.labeled_60,

    e.fa,
    e.new_wallets30,

    s.recent_buy_share,
    s.recent_net_share,
    s.breadth_score,
    s.late_chase_score

FROM t51_capital_efficiency_forward f

JOIN events e
    ON e.id=f.event_id

JOIN event_sequence_features_v340 s
    ON s.event_id=f.event_id

WHERE
    f.labeled_60=1
    AND f.binary_label IS NOT NULL

ORDER BY
    f.event_id
""").fetchall()


forward = []


for r in forward_rows:

    rec = {
        "id": r["event_id"],
        "token_mint": r["token_mint"],
        "timestamp": r["event_timestamp"],

        "y": r["binary_label"],

        "fa": r["fa"],
        "new_wallets30": r["new_wallets30"],
        "recent_buy_share": r["recent_buy_share"],
        "recent_net_share": r["recent_net_share"],
        "breadth_score": r["breadth_score"],
        "late_chase_score": r["late_chase_score"],

        "price_per_net": r["recent_price_per_net_sol"],
        "net_eff": r["recent_net_efficiency"],
        "early_div": r["early_flow_price_div"],
    }


    if all(
        valid(rec.get(f))
        for f in M2
    ):
        forward.append(rec)


# ============================================================
# EVALUATE
# ============================================================

def eval_model(beta, features, rr):

    X, y = vectorize(
        rr,
        features
    )

    if not y:
        return None

    p = predict(
        beta,
        X
    )

    return {
        "n": len(y),
        "y": y,
        "p": p,
        "auc": auc(y,p),
        "logloss": logloss(y,p),
        "brier": brier(y,p),
    }


r0 = eval_model(
    beta0,
    M0,
    forward
)

r2 = eval_model(
    beta2,
    M2,
    forward
)


# ============================================================
# FIRST EVENT/TOKEN
# ============================================================

seen = set()
first = []

for r in forward:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


f0 = eval_model(
    beta0,
    M0,
    first
)

f2 = eval_model(
    beta2,
    M2,
    first
)


# ============================================================
# OUTPUT
# ============================================================

print("=" * 170)
print("MEMECOIN LAB — T54 CAP-ONLY INCREMENTAL FORWARD AUDIT")
print("=" * 170)

print(
    f"T51 BOUNDARY ID        : {boundary_id}"
)

print(
    f"HISTORICAL TRAIN EVENTS: {len(train)}"
)

print(
    f"FORWARD BINARY EVENTS  : {len(forward)}"
)

print(
    f"FORWARD UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in forward))}"
)


print()
print("=" * 170)
print("A) FROZEN MODEL DEFINITIONS")
print("=" * 170)

print(
    "M0_CONTEXT = "
    + ", ".join(M0)
)

print(
    "M2_CONTEXT_CAP = "
    + ", ".join(M2)
)


print()
print("=" * 170)
print("B) FORWARD PERFORMANCE")
print("=" * 170)


if r0 is None or r2 is None:

    print(
        "Not enough forward binary events yet."
    )

else:

    print(
        f"M0_CONTEXT     "
        f"N={r0['n']:3d} "
        f"AUC={fmt(r0['auc'])} "
        f"LOGLOSS={fmt(r0['logloss'])} "
        f"BRIER={fmt(r0['brier'])}"
    )

    print(
        f"M2_CONTEXT_CAP "
        f"N={r2['n']:3d} "
        f"AUC={fmt(r2['auc'])} "
        f"LOGLOSS={fmt(r2['logloss'])} "
        f"BRIER={fmt(r2['brier'])}"
    )


    da = (
        r2["auc"] - r0["auc"]
        if (
            r2["auc"] is not None
            and r0["auc"] is not None
        )
        else None
    )

    dll = (
        r0["logloss"]
        - r2["logloss"]
    )

    dbrier = (
        r0["brier"]
        - r2["brier"]
    )


    print()

    print(
        f"ΔAUC      = {fmt(da)}"
    )

    print(
        f"ΔLOGLOSS  = {fmt(dll)}"
    )

    print(
        f"ΔBRIER    = {fmt(dbrier)}"
    )


print()
print("=" * 170)
print("C) FIRST-EVENT/TOKEN FORWARD")
print("=" * 170)


if f0 is None or f2 is None:

    print(
        "Not enough first-token binary observations yet."
    )

else:

    print(
        f"FIRST TOKENS = {len(first)}"
    )

    print(
        f"M0_CONTEXT     "
        f"AUC={fmt(f0['auc'])} "
        f"LOGLOSS={fmt(f0['logloss'])} "
        f"BRIER={fmt(f0['brier'])}"
    )

    print(
        f"M2_CONTEXT_CAP "
        f"AUC={fmt(f2['auc'])} "
        f"LOGLOSS={fmt(f2['logloss'])} "
        f"BRIER={fmt(f2['brier'])}"
    )


print()
print("=" * 170)
print("D) CHECKPOINT")
print("=" * 170)


n = len(forward)


if n < 15:

    print(
        f"⏳ {n}/15 binary forward events"
    )

elif n < 30:

    print(
        f"🟡 {n}/30 — early forward comparison only"
    )

elif n < 50:

    print(
        f"🟢 {n}/50 — first serious CAP incremental audit"
    )

else:

    print(
        f"🔵 N={n} — stronger forward evidence available"
    )


print()
print("=" * 170)
print("E) DECISION SUPPORT")
print("=" * 170)


if (
    r0 is not None
    and r2 is not None
    and r0["auc"] is not None
    and r2["auc"] is not None
):

    da = (
        r2["auc"]
        - r0["auc"]
    )

    dll = (
        r0["logloss"]
        - r2["logloss"]
    )

    dbrier = (
        r0["brier"]
        - r2["brier"]
    )


    if (
        n >= 30
        and da > 0
        and dll > 0
        and dbrier > 0
    ):

        print(
            "🟢 CAP adds consistent forward value over context."
        )

        print(
            "Eligible for deeper frozen robustness audit."
        )

    elif n >= 15:

        print(
            "🟡 Forward evidence still preliminary."
        )

        print(
            "Do not retune CAP features."
        )

    else:

        print(
            "⏳ Too early for a verdict."
        )

else:

    print(
        "⏳ Waiting for enough RUN/DUMP forward cases."
    )


print()
print("IMPORTANT:")
print("• Historical training stops at the T51 frozen boundary.")
print("• Forward events are never used to refit coefficients.")
print("• M0 and M2 are evaluated on identical forward events.")
print("• CAP definitions are frozen from T53.")
print("• No threshold optimization.")
print("• No fast-flip in T54.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T54 writes nothing to DB.")
print("• Research forward audit only.")

db.close()
