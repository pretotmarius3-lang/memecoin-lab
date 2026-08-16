import sqlite3
import statistics
import math
import random
from collections import defaultdict

import numpy as np

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score,
    balanced_accuracy_score,
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
)

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

RANDOM_SEED = 42

# Important:
# we do NOT use T23 prospective-only rows to tune V2.
# This is an independent discovery track over the broader dataset.

FEATURES = [
    "fa",
    "nf30",
    "new_wallets10",
    "new_wallets30",

    "volume_m5",
    "liquidity_usd",
    "market_cap",
    "vol_liq",

    "mid_buy_count",
    "mid_sell_count",
    "mid_flow_balance",

    "recent_unique_buyers",

    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",
    "swap_velocity_mean",

    "buy_concentration_trend",

    "recent_price_return",
    "mid_price_return",

    "recent_sell_sol",
    "recent_net_sol",
    "recent_buy_share",

    "late_chase_score",
    "breadth_score",
]


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


def separation(a, b):
    a = [x for x in a if valid(x)]
    b = [x for x in b if valid(x)]

    if len(a) < 2 or len(b) < 2:
        return None

    ma = med(a)
    mb = med(b)

    pooled = (
        statistics.pstdev(a + b)
        if len(a + b) > 1
        else 0
    )

    if pooled == 0:
        return 0.0

    return abs(ma - mb) / pooled


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


def table_exists(db, name):
    return db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
    """, (name,)).fetchone() is not None


def load_rows(db):

    if not table_exists(
        db,
        "event_sequence_features_v340"
    ):
        raise RuntimeError(
            "Missing event_sequence_features_v340"
        )

    return db.execute("""
        WITH first_dex AS (
            SELECT d.*
            FROM dex_prices d
            JOIN (
                SELECT event_id, MIN(timestamp) AS first_time
                FROM dex_prices
                GROUP BY event_id
            ) x
              ON d.event_id=x.event_id
             AND d.timestamp=x.first_time
        )

        SELECT
            e.id,
            e.token_mint,
            e.dex_return_60s,

            e.fa,
            e.nf30,
            e.new_wallets10,
            e.new_wallets30,

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,

            s.mid_buy_count,
            s.mid_sell_count,
            s.recent_unique_buyers,

            s.early_swaps_per_sec,
            s.mid_swaps_per_sec,
            s.recent_swaps_per_sec,

            s.buy_concentration_trend,

            s.recent_price_return,
            s.mid_price_return,

            s.recent_sell_sol,
            s.recent_net_sol,
            s.recent_buy_share,

            s.late_chase_score,
            s.breadth_score

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        LEFT JOIN first_dex d
        ON d.event_id=e.id

        WHERE
            e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()


def feat(r, name):

    if name in r.keys():
        return r[name]

    if name == "vol_liq":
        return safe_div(
            r["volume_m5"],
            r["liquidity_usd"]
        )

    if name == "mid_flow_balance":
        b = r["mid_buy_count"]
        s = r["mid_sell_count"]

        if not valid(b) or not valid(s):
            return None

        return b - s

    if name == "swap_velocity_mean":
        vals = [
            r["early_swaps_per_sec"],
            r["mid_swaps_per_sec"],
            r["recent_swaps_per_sec"],
        ]

        vals = [x for x in vals if valid(x)]

        return avg(vals) if vals else None

    return None


def build_matrix(rows, selected_features):
    X = []

    for r in rows:
        vals = []

        for name in selected_features:
            v = feat(r, name)

            vals.append(
                float(v)
                if valid(v)
                else np.nan
            )

        X.append(vals)

    return np.asarray(X, dtype=float)


def binary_label(r):
    x = r["dex_return_60s"]

    if not valid(x):
        return None

    if x >= RUNNER:
        return 1

    if x <= DUMP:
        return 0

    return None


db = connect()
rows = load_rows(db)

labeled = [
    r for r in rows
    if binary_label(r) is not None
]

print("=" * 140)
print("MEMECOIN LAB — T33 INDEPENDENT DISCOVERY LAB")
print("=" * 140)

print(
    f"LABELED EVENTS : {len(labeled)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in labeled))}"
)

# ============================================================
# TOKEN HOLDOUT SPLIT
# ============================================================

tokens = sorted(
    set(r["token_mint"] for r in labeled)
)

random.seed(RANDOM_SEED)
random.shuffle(tokens)

n = len(tokens)

n_train = max(
    1,
    int(n * 0.60)
)

n_val = max(
    1,
    int(n * 0.20)
)

train_tokens = set(
    tokens[:n_train]
)

val_tokens = set(
    tokens[n_train:n_train+n_val]
)

test_tokens = set(
    tokens[n_train+n_val:]
)

train = [
    r for r in labeled
    if r["token_mint"] in train_tokens
]

val = [
    r for r in labeled
    if r["token_mint"] in val_tokens
]

test = [
    r for r in labeled
    if r["token_mint"] in test_tokens
]

print()
print(
    f"TRAIN      : {len(train)} events | "
    f"{len(train_tokens)} tokens"
)

print(
    f"VALIDATION : {len(val)} events | "
    f"{len(val_tokens)} tokens"
)

print(
    f"TEST       : {len(test)} events | "
    f"{len(test_tokens)} tokens"
)

# ============================================================
# FEATURE SEPARATION — TRAIN ONLY
# ============================================================

print()
print("=" * 140)
print("A) TRAIN FEATURE SEPARATION")
print("=" * 140)

feature_scores = []

for name in FEATURES:

    run_vals = [
        feat(r, name)
        for r in train
        if binary_label(r) == 1
    ]

    dump_vals = [
        feat(r, name)
        for r in train
        if binary_label(r) == 0
    ]

    run_vals = [x for x in run_vals if valid(x)]
    dump_vals = [x for x in dump_vals if valid(x)]

    if not run_vals or not dump_vals:
        continue

    s = separation(
        run_vals,
        dump_vals
    )

    diff = (
        med(run_vals)
        - med(dump_vals)
    )

    feature_scores.append({
        "name": name,
        "sep": s if s is not None else 0,
        "run_med": med(run_vals),
        "dump_med": med(dump_vals),
        "diff": diff,
        "run_n": len(run_vals),
        "dump_n": len(dump_vals),
    })

feature_scores.sort(
    key=lambda x: x["sep"],
    reverse=True
)

print(
    f"{'FEATURE':28} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-" * 80)

for x in feature_scores[:20]:

    print(
        f"{x['name']:28} "
        f"{x['run_med']:+11.4f} "
        f"{x['dump_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# TOP FEATURES ONLY
# ============================================================

TOP_N = min(
    10,
    len(feature_scores)
)

selected = [
    x["name"]
    for x in feature_scores[:TOP_N]
]

print()
print("=" * 140)
print("B) SELECTED FEATURES — TRAIN ONLY")
print("=" * 140)

for name in selected:
    print(f"• {name}")


# ============================================================
# LOGISTIC MODEL
# ============================================================

def y(rows):
    return np.asarray(
        [
            binary_label(r)
            for r in rows
        ],
        dtype=int
    )


X_train = build_matrix(
    train,
    selected
)

X_val = build_matrix(
    val,
    selected
)

X_test = build_matrix(
    test,
    selected
)

y_train = y(train)
y_val = y(val)
y_test = y(test)

pipe = Pipeline([
    (
        "imputer",
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
            max_iter=2000,
            random_state=42
        )
    ),
])

pipe.fit(
    X_train,
    y_train
)


def evaluate(name, X, yy):

    if len(yy) == 0:
        return None

    prob = pipe.predict_proba(X)[:, 1]
    pred = (
        prob >= 0.50
    ).astype(int)

    auc = (
        roc_auc_score(
            yy,
            prob
        )
        if len(set(yy)) > 1
        else None
    )

    return {
        "name": name,
        "n": len(yy),

        "acc":
            accuracy_score(
                yy,
                pred
            ),

        "bal":
            balanced_accuracy_score(
                yy,
                pred
            ),

        "prec":
            precision_score(
                yy,
                pred,
                zero_division=0
            ),

        "rec":
            recall_score(
                yy,
                pred,
                zero_division=0
            ),

        "f1":
            f1_score(
                yy,
                pred,
                zero_division=0
            ),

        "auc":
            auc,

        "prob":
            prob,
    }


val_result = evaluate(
    "VALIDATION",
    X_val,
    y_val
)

test_result = evaluate(
    "TEST",
    X_test,
    y_test
)

print()
print("=" * 140)
print("C) TOKEN-HOLDOUT MODEL PERFORMANCE")
print("=" * 140)

for res in [
    val_result,
    test_result
]:

    if not res:
        continue

    print(
        f"{res['name']:10} | "
        f"N={res['n']:3d} | "
        f"ACC={res['acc']:.3f} | "
        f"BAL={res['bal']:.3f} | "
        f"PREC={res['prec']:.3f} | "
        f"REC={res['rec']:.3f} | "
        f"F1={res['f1']:.3f} | "
        f"AUC="
        f"{res['auc']:.3f}"
        if res["auc"] is not None
        else "AUC=NA"
    )


# ============================================================
# THRESHOLD TABLE
# ============================================================

def threshold_table(name, rows, yy, probs):

    print()
    print(name)
    print("-" * 90)

    print(
        f"{'THRESH':>8} "
        f"{'SIGNALS':>8} "
        f"{'RUN':>6} "
        f"{'DUMP':>6} "
        f"{'PREC':>8} "
        f"{'TOKENS':>8}"
    )

    for t in [
        0.50,
        0.55,
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
    ]:

        idx = [
            i
            for i,p in enumerate(probs)
            if p >= t
        ]

        if not idx:
            continue

        run = sum(
            yy[i] == 1
            for i in idx
        )

        dump = sum(
            yy[i] == 0
            for i in idx
        )

        tok = len(
            set(
                rows[i]["token_mint"]
                for i in idx
            )
        )

        print(
            f"{t:8.2f} "
            f"{len(idx):8d} "
            f"{run:6d} "
            f"{dump:6d} "
            f"{100*run/len(idx):7.1f}% "
            f"{tok:8d}"
        )


print()
print("=" * 140)
print("D) PROBABILITY THRESHOLDS")
print("=" * 140)

if val_result:
    threshold_table(
        "VALIDATION",
        val,
        y_val,
        val_result["prob"]
    )

if test_result:
    threshold_table(
        "FINAL TEST",
        test,
        y_test,
        test_result["prob"]
    )


# ============================================================
# FIRST EVENT PER TOKEN TEST
# ============================================================

def first_per_token(rows):

    seen = set()
    out = []

    for r in sorted(
        rows,
        key=lambda x: x["id"]
    ):

        token = r["token_mint"]

        if token in seen:
            continue

        seen.add(token)
        out.append(r)

    return out


first_test = first_per_token(
    test
)

if first_test:

    X_first = build_matrix(
        first_test,
        selected
    )

    y_first = y(
        first_test
    )

    first_result = evaluate(
        "FIRST/TOKEN",
        X_first,
        y_first
    )

    print()
    print("=" * 140)
    print("E) FINAL TEST — FIRST EVENT PER TOKEN")
    print("=" * 140)

    if first_result:

        print(
            f"N={first_result['n']} | "
            f"ACC={first_result['acc']:.3f} | "
            f"BAL={first_result['bal']:.3f} | "
            f"PREC={first_result['prec']:.3f} | "
            f"REC={first_result['rec']:.3f} | "
            f"F1={first_result['f1']:.3f} | "
            f"AUC="
            f"{first_result['auc']:.3f}"
            if first_result["auc"] is not None
            else "AUC=NA"
        )


# ============================================================
# FEATURE WEIGHTS
# ============================================================

model = pipe.named_steps[
    "model"
]

weights = list(
    zip(
        selected,
        model.coef_[0]
    )
)

weights.sort(
    key=lambda x:
        abs(x[1]),
    reverse=True
)

print()
print("=" * 140)
print("F) STANDARDIZED FEATURE WEIGHTS")
print("=" * 140)

for name, w in weights:
    print(
        f"{name:30} {w:+.4f}"
    )


# ============================================================
# DECISION SUPPORT
# ============================================================

print()
print("=" * 140)
print("G) DECISION SUPPORT")
print("=" * 140)

if (
    val_result
    and test_result
    and val_result["auc"] is not None
    and test_result["auc"] is not None
    and val_result["auc"] >= 0.60
    and test_result["auc"] >= 0.60
):

    print(
        "SECOND-SIGNAL CANDIDATE SURVIVES TOKEN-HOLDOUT."
    )

    print(
        "Do NOT deploy it."
    )

    print(
        "Next step would be a separately frozen prospective validator."
    )

else:

    print(
        "NO ROBUST SECOND SIGNAL YET."
    )

    print(
        "Do not force a model from this dataset."
    )

print()
print("IMPORTANT:")
print("• T23/T31/T32 remain untouched.")
print("• This script is independent discovery only.")
print("• Feature selection used TRAIN only.")
print("• Validation/Test tokens are held out from training.")
print("• Final test must not be used to retune this exact model.")

db.close()
