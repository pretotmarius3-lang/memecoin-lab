import sqlite3
import math
from collections import Counter, defaultdict

import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, balanced_accuracy_score

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

TRAIN_FRAC = 0.60
VAL_FRAC = 0.20

FEATURES = [
    "mid_buy_count",
    "mid_sell_count",
    "recent_unique_buyers",
    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",
    "buy_concentration_trend",
    "recent_price_return",
    "mid_price_return",
    "recent_sell_sol",
    "recent_net_sol",
    "recent_buy_share",
]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def short(token):
    if token is None:
        return "NONE"
    return token[:18]


def load_rows():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row

    rows = db.execute("""
        SELECT
            e.id,
            e.token_mint,
            e.timestamp,
            e.dex_return_60s,
            s.*
        FROM events e
        JOIN event_sequence_features_v340 s
          ON s.event_id = e.id
        WHERE e.dex_return_60s IS NOT NULL
          AND (
              e.dex_return_60s >= ?
              OR e.dex_return_60s <= ?
          )
        ORDER BY e.id ASC
    """, (RUNNER, DUMP)).fetchall()

    db.close()
    return rows


def build_xy(rows):
    X = []
    y = []

    for r in rows:
        vals = []

        for f in FEATURES:
            v = r[f]
            vals.append(float(v) if valid(v) else np.nan)

        X.append(vals)
        y.append(1 if r["dex_return_60s"] >= RUNNER else 0)

    return np.asarray(X, dtype=float), np.asarray(y, dtype=int)


def new_model():
    return Pipeline([
        (
            "imputer",
            SimpleImputer(strategy="median")
        ),
        (
            "scale",
            StandardScaler()
        ),
        (
            "clf",
            LogisticRegression(
                C=0.5,
                max_iter=500,
                class_weight="balanced",
                random_state=42,
            )
        ),
    ])


def safe_auc(y, p):
    if len(y) < 2 or len(set(y.tolist())) < 2:
        return None
    return roc_auc_score(y, p)


def report_eval(name, model, rows):
    print()
    print(name)
    print("-" * 110)

    if not rows:
        print("NO EVENTS")
        return

    X, y = build_xy(rows)

    p = model.predict_proba(X)[:, 1]
    pred = (p >= 0.50).astype(int)

    auc = safe_auc(y, p)

    print(
        f"EVENTS={len(rows)} | "
        f"TOKENS={len(set(r['token_mint'] for r in rows))} | "
        f"RUNNERS={int(y.sum())} | "
        f"DUMPS={int(len(y)-y.sum())}"
    )

    print(
        "AUC="
        + (f"{auc:.3f}" if auc is not None else "NA")
        + f" | BAL_ACC={balanced_accuracy_score(y, pred):.3f}"
    )

    print()
    print(
        f"{'THRESH':>7} "
        f"{'SIGNALS':>8} "
        f"{'RUN':>6} "
        f"{'DUMP':>6} "
        f"{'PREC':>8} "
        f"{'UNIQUE TOK':>11}"
    )

    for t in [.50, .60, .65, .70, .75]:
        mask = p >= t
        n = int(mask.sum())

        if n == 0:
            continue

        yy = y[mask]
        selected = [
            rows[i]
            for i, flag in enumerate(mask)
            if flag
        ]

        nr = int(yy.sum())
        nd = n - nr
        precision = nr / n
        ntok = len(set(r["token_mint"] for r in selected))

        print(
            f"{t:7.2f} "
            f"{n:8d} "
            f"{nr:6d} "
            f"{nd:6d} "
            f"{precision:8.1%} "
            f"{ntok:11d}"
        )


rows = load_rows()

n = len(rows)
train_end = int(n * TRAIN_FRAC)
val_end = int(n * (TRAIN_FRAC + VAL_FRAC))

train = rows[:train_end]
val = rows[train_end:val_end]
test = rows[val_end:]

train_tokens = set(r["token_mint"] for r in train)
val_tokens = set(r["token_mint"] for r in val)
test_tokens = set(r["token_mint"] for r in test)

print("=" * 110)
print("MEMECOIN LAB — V4 TOKEN / LEAKAGE AUDIT")
print("=" * 110)

print()
print(f"TOTAL LABELED : {n}")
print(
    f"TRAIN         : {len(train)} events | "
    f"{len(train_tokens)} unique tokens | "
    f"IDs {train[0]['id']}→{train[-1]['id']}"
)
print(
    f"VALIDATION    : {len(val)} events | "
    f"{len(val_tokens)} unique tokens | "
    f"IDs {val[0]['id']}→{val[-1]['id']}"
)
print(
    f"TEST          : {len(test)} events | "
    f"{len(test_tokens)} unique tokens | "
    f"IDs {test[0]['id']}→{test[-1]['id']}"
)

print()
print("=" * 110)
print("TOKEN OVERLAP")
print("=" * 110)

tv = train_tokens & val_tokens
tt = train_tokens & test_tokens
vt = val_tokens & test_tokens

print(f"TRAIN ∩ VALIDATION : {len(tv)} tokens")
print(f"TRAIN ∩ TEST       : {len(tt)} tokens")
print(f"VALIDATION ∩ TEST  : {len(vt)} tokens")

test_seen_train = [
    r for r in test
    if r["token_mint"] in train_tokens
]

test_unseen_train = [
    r for r in test
    if r["token_mint"] not in train_tokens
]

test_seen_any = [
    r for r in test
    if (
        r["token_mint"] in train_tokens
        or r["token_mint"] in val_tokens
    )
]

test_fully_new = [
    r for r in test
    if (
        r["token_mint"] not in train_tokens
        and r["token_mint"] not in val_tokens
    )
]

print()
print(
    f"TEST events whose token appeared in TRAIN : "
    f"{len(test_seen_train)}/{len(test)} "
    f"({len(test_seen_train)/len(test):.1%})"
)

print(
    f"TEST events with token NEW vs TRAIN       : "
    f"{len(test_unseen_train)}/{len(test)} "
    f"({len(test_unseen_train)/len(test):.1%})"
)

print(
    f"TEST events with token NEW vs TRAIN+VAL   : "
    f"{len(test_fully_new)}/{len(test)} "
    f"({len(test_fully_new)/len(test):.1%})"
)

print()
print("=" * 110)
print("MOST REPEATED TOKENS — FULL LABELED SET")
print("=" * 110)

counts = Counter(r["token_mint"] for r in rows)

for token, count in counts.most_common(15):
    token_rows = [r for r in rows if r["token_mint"] == token]

    nr = sum(
        1 for r in token_rows
        if r["dex_return_60s"] >= RUNNER
    )

    nd = sum(
        1 for r in token_rows
        if r["dex_return_60s"] <= DUMP
    )

    print(
        f"{short(token):20} "
        f"EVENTS={count:3d} | "
        f"RUN={nr:3d} | "
        f"DUMP={nd:3d}"
    )

# ============================================================
# ORIGINAL V4 MODEL — SAME TRAINING SET
# ============================================================

X_train, y_train = build_xy(train)

model = new_model()
model.fit(X_train, y_train)

print()
print("=" * 110)
print("ORIGINAL V4 MODEL — TOKEN-SLICED TEST")
print("=" * 110)

report_eval(
    "A) ALL ORIGINAL TEST EVENTS",
    model,
    test
)

report_eval(
    "B) TEST — TOKEN ALREADY SEEN IN TRAIN",
    model,
    test_seen_train
)

report_eval(
    "C) TEST — TOKEN UNSEEN IN TRAIN",
    model,
    test_unseen_train
)

report_eval(
    "D) TEST — COMPLETELY NEW TOKEN VS TRAIN+VALIDATION",
    model,
    test_fully_new
)

# ============================================================
# ONE EVENT PER TOKEN
#
# Prevent one token with many events from dominating metrics.
# Use FIRST chronologically occurring event for each token.
# ============================================================

def first_per_token(data):
    seen = set()
    out = []

    for r in data:
        token = r["token_mint"]

        if token in seen:
            continue

        seen.add(token)
        out.append(r)

    return out


test_one_per_token = first_per_token(test)

report_eval(
    "E) ORIGINAL TEST — FIRST EVENT PER TOKEN ONLY",
    model,
    test_one_per_token
)

# ============================================================
# STRICT TOKEN-HOLDOUT EXPERIMENT
#
# Tokens are assigned according to FIRST appearance.
# A token belongs to exactly one split.
# This is an audit, NOT a replacement final test.
# ============================================================

print()
print("=" * 110)
print("STRICT TOKEN-HOLDOUT AUDIT")
print("=" * 110)

token_first_id = {}

for r in rows:
    token = r["token_mint"]

    if token not in token_first_id:
        token_first_id[token] = r["id"]

ordered_tokens = sorted(
    token_first_id,
    key=lambda t: token_first_id[t]
)

nt = len(ordered_tokens)

token_train_end = int(nt * TRAIN_FRAC)
token_val_end = int(nt * (TRAIN_FRAC + VAL_FRAC))

strict_train_tokens = set(
    ordered_tokens[:token_train_end]
)

strict_val_tokens = set(
    ordered_tokens[token_train_end:token_val_end]
)

strict_test_tokens = set(
    ordered_tokens[token_val_end:]
)

strict_train = [
    r for r in rows
    if r["token_mint"] in strict_train_tokens
]

strict_val = [
    r for r in rows
    if r["token_mint"] in strict_val_tokens
]

strict_test = [
    r for r in rows
    if r["token_mint"] in strict_test_tokens
]

print(
    f"TOKEN TRAIN : {len(strict_train_tokens)} tokens "
    f"| {len(strict_train)} events"
)

print(
    f"TOKEN VAL   : {len(strict_val_tokens)} tokens "
    f"| {len(strict_val)} events"
)

print(
    f"TOKEN TEST  : {len(strict_test_tokens)} tokens "
    f"| {len(strict_test)} events"
)

if (
    strict_train
    and strict_test
):

    Xs, ys = build_xy(strict_train)

    if len(set(ys.tolist())) >= 2:
        strict_model = new_model()
        strict_model.fit(Xs, ys)

        report_eval(
            "F) STRICT TOKEN-HOLDOUT TEST — ALL EVENTS",
            strict_model,
            strict_test
        )

        report_eval(
            "G) STRICT TOKEN-HOLDOUT TEST — FIRST EVENT PER TOKEN",
            strict_model,
            first_per_token(strict_test)
        )

# ============================================================
# TEST TOKEN DETAIL
# ============================================================

print()
print("=" * 110)
print("ORIGINAL TEST — TOKEN DETAIL")
print("=" * 110)

groups = defaultdict(list)

for r in test:
    groups[r["token_mint"]].append(r)

for token, rr in sorted(
    groups.items(),
    key=lambda x: min(r["id"] for r in x[1])
):
    status = (
        "SEEN_TRAIN"
        if token in train_tokens
        else "NEW"
    )

    print()
    print(
        f"{short(token)} | "
        f"{status} | "
        f"{len(rr)} events"
    )

    for r in rr:
        label = (
            "RUN"
            if r["dex_return_60s"] >= RUNNER
            else "DUMP"
        )

        print(
            f"   ID={r['id']:4d} | "
            f"{label:4} | "
            f"R60={r['dex_return_60s']:+8.2f}%"
        )

print()
print("=" * 110)
print("INTERPRETATION RULES")
print("=" * 110)

print("""
1. If ALL TEST AUC is high but UNSEEN-TOKEN AUC collapses:
   token repetition is inflating V4.

2. If UNSEEN-TOKEN and STRICT TOKEN-HOLDOUT remain strong:
   evidence for generalizable flow structure becomes much stronger.

3. FIRST EVENT PER TOKEN is important:
   one active token must not dominate the score.

4. Do NOT optimize V4 from this audit.

5. ID > 417 remains the prospective future OOS boundary.
""")
