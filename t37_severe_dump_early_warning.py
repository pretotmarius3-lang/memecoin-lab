import sqlite3
import statistics
import math
import random
import itertools
import numpy as np

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    f1_score,
)

DB = "validation_v090.db"

SEVERE = -20.0
RANDOM_SEED = 42

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


def feat(r, name):

    if name in r.keys():
        return r[name]

    if name == "vol_liq":
        return safe_div(
            r["volume_m5"],
            r["liquidity_usd"]
        )

    if name == "mid_flow_balance":
        if (
            valid(r["mid_buy_count"])
            and valid(r["mid_sell_count"])
        ):
            return (
                r["mid_buy_count"]
                - r["mid_sell_count"]
            )

    if name == "swap_velocity_mean":
        vals = [
            r["early_swaps_per_sec"],
            r["mid_swaps_per_sec"],
            r["recent_swaps_per_sec"],
        ]

        vals = [x for x in vals if valid(x)]

        return avg(vals) if vals else None

    return None


def label(r):
    x = r["dex_return_60s"]

    if not valid(x):
        return None

    return 1 if x <= SEVERE else 0


def build_matrix(rows, selected):
    out = []

    for r in rows:
        row = []

        for name in selected:
            x = feat(r, name)

            row.append(
                float(x)
                if valid(x)
                else np.nan
            )

        out.append(row)

    return np.asarray(out, dtype=float)


def first_per_token(rows):
    seen = set()
    out = []

    for r in sorted(rows, key=lambda x: x["id"]):
        token = r["token_mint"]

        if token in seen:
            continue

        seen.add(token)
        out.append(r)

    return out


db = connect()

if not table_exists(
    db,
    "event_sequence_features_v340"
):
    raise RuntimeError(
        "Missing event_sequence_features_v340"
    )


rows = db.execute("""
WITH first_dex AS (
    SELECT d.*
    FROM dex_prices d
    JOIN (
        SELECT
            event_id,
            MIN(timestamp) AS first_time
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


usable = [
    r for r in rows
    if label(r) is not None
]

print("=" * 150)
print("MEMECOIN LAB — T37 SEVERE DUMP EARLY WARNING")
print("=" * 150)

print(
    f"EVENTS        : {len(usable)}"
)

print(
    f"UNIQUE TOKENS : "
    f"{len(set(r['token_mint'] for r in usable))}"
)

print(
    f"SEVERE <=-20 : "
    f"{sum(label(r)==1 for r in usable)}"
)


# ============================================================
# TOKEN HOLDOUT
# ============================================================

tokens = sorted(
    set(r["token_mint"] for r in usable)
)

random.seed(RANDOM_SEED)
random.shuffle(tokens)

n = len(tokens)

n_train = int(n * 0.60)
n_val = int(n * 0.20)

train_tokens = set(tokens[:n_train])
val_tokens = set(tokens[n_train:n_train+n_val])
test_tokens = set(tokens[n_train+n_val:])

train = [
    r for r in usable
    if r["token_mint"] in train_tokens
]

val = [
    r for r in usable
    if r["token_mint"] in val_tokens
]

test = [
    r for r in usable
    if r["token_mint"] in test_tokens
]


for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    severe = sum(
        label(r) == 1
        for r in rr
    )

    print(
        f"{name:8} | "
        f"N={len(rr):3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| SEVERE={severe:3d} "
        f"| NON={len(rr)-severe:3d}"
    )


# ============================================================
# A) FEATURE SEPARATION — TRAIN ONLY
# ============================================================

scores = []

for name in FEATURES:

    sev = [
        feat(r, name)
        for r in train
        if (
            label(r) == 1
            and valid(feat(r, name))
        )
    ]

    non = [
        feat(r, name)
        for r in train
        if (
            label(r) == 0
            and valid(feat(r, name))
        )
    ]

    if len(sev) < 2 or len(non) < 2:
        continue

    ms = med(sev)
    mn = med(non)

    pooled = (
        statistics.pstdev(
            sev + non
        )
        if len(sev + non) > 1
        else 0
    )

    sep = (
        abs(ms-mn) / pooled
        if pooled > 0
        else 0
    )

    scores.append({
        "name": name,
        "sev_med": ms,
        "non_med": mn,
        "diff": ms-mn,
        "sep": sep,
    })


scores.sort(
    key=lambda x: x["sep"],
    reverse=True
)

print()
print("=" * 150)
print("A) TRAIN FEATURE SEPARATION — SEVERE VS NON")
print("=" * 150)

print(
    f"{'FEATURE':28} "
    f"{'SEV MED':>12} "
    f"{'NON MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-" * 80)

for x in scores[:20]:
    print(
        f"{x['name']:28} "
        f"{x['sev_med']:+11.4f} "
        f"{x['non_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# SELECT TRAIN TOP FEATURES
# ============================================================

TOP_N = min(
    10,
    len(scores)
)

selected = [
    x["name"]
    for x in scores[:TOP_N]
]

print()
print("=" * 150)
print("B) SELECTED TRAIN FEATURES")
print("=" * 150)

for name in selected:
    print("•", name)


# ============================================================
# LOGISTIC MODEL
# ============================================================

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

y_train = np.asarray(
    [label(r) for r in train],
    dtype=int
)

y_val = np.asarray(
    [label(r) for r in val],
    dtype=int
)

y_test = np.asarray(
    [label(r) for r in test],
    dtype=int
)


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
            class_weight="balanced",
            random_state=42
        )
    )
])

pipe.fit(
    X_train,
    y_train
)


def evaluate(name, X, y):

    prob = pipe.predict_proba(X)[:,1]

    auc = (
        roc_auc_score(
            y,
            prob
        )
        if len(set(y)) > 1
        else None
    )

    return {
        "name": name,
        "prob": prob,
        "auc": auc,
    }


val_result = evaluate(
    "VALID",
    X_val,
    y_val
)

test_result = evaluate(
    "TEST",
    X_test,
    y_test
)


print()
print("=" * 150)
print("C) TOKEN-HOLDOUT AUC")
print("=" * 150)

for r in [
    val_result,
    test_result
]:

    print(
        f"{r['name']:8} | "
        f"AUC="
        f"{r['auc']:.3f}"
        if r["auc"] is not None
        else "AUC=NA"
    )


# ============================================================
# D) ALERT THRESHOLDS
#
# severe warning => probability >= threshold
# ============================================================

def threshold_stats(rows, y, prob, threshold):

    idx = [
        i for i,p in enumerate(prob)
        if p >= threshold
    ]

    severe_total = sum(y == 1)
    non_total = sum(y == 0)

    severe_hit = sum(
        y[i] == 1
        for i in idx
    )

    false_hit = sum(
        y[i] == 0
        for i in idx
    )

    return {
        "alerts":
            len(idx),

        "severe_recall":
            (
                100*severe_hit/severe_total
                if severe_total
                else 0
            ),

        "non_flag":
            (
                100*false_hit/non_total
                if non_total
                else 0
            ),

        "precision":
            (
                100*severe_hit/len(idx)
                if idx
                else 0
            ),

        "tokens":
            len(set(
                rows[i]["token_mint"]
                for i in idx
            )),
    }


print()
print("=" * 150)
print("D) SEVERE WARNING THRESHOLDS")
print("=" * 150)

print(
    f"{'SPLIT':8} "
    f"{'THRESH':>7} "
    f"{'ALERT':>7} "
    f"{'SEV REC':>9} "
    f"{'NON FLAG':>9} "
    f"{'PREC':>8} "
    f"{'TOK':>5}"
)

print("-" * 70)

for split_name, rows_, y_, result in [
    ("VALID", val, y_val, val_result),
    ("TEST", test, y_test, test_result),
]:

    for threshold in [
        .40,
        .50,
        .60,
        .70,
        .80,
        .90
    ]:

        m = threshold_stats(
            rows_,
            y_,
            result["prob"],
            threshold
        )

        print(
            f"{split_name:8} "
            f"{threshold:7.2f} "
            f"{m['alerts']:7d} "
            f"{m['severe_recall']:8.1f}% "
            f"{m['non_flag']:8.1f}% "
            f"{m['precision']:7.1f}% "
            f"{m['tokens']:5d}"
        )


# ============================================================
# E) FIRST EVENT / TOKEN
# ============================================================

first_val = first_per_token(
    val
)

first_test = first_per_token(
    test
)

for title, rows_ in [
    ("VALID FIRST", first_val),
    ("TEST FIRST", first_test),
]:

    X = build_matrix(
        rows_,
        selected
    )

    yy = np.asarray(
        [label(r) for r in rows_],
        dtype=int
    )

    prob = pipe.predict_proba(X)[:,1]

    auc = (
        roc_auc_score(
            yy,
            prob
        )
        if len(set(yy)) > 1
        else None
    )

    print()
    print("=" * 150)
    print(
        f"E) {title}"
    )
    print("=" * 150)

    print(
        f"N={len(rows_)} "
        f"| SEVERE={sum(yy==1)} "
        f"| AUC="
        f"{auc:.3f}"
        if auc is not None
        else "AUC=NA"
    )

    for threshold in [
        .50,
        .60,
        .70,
        .80
    ]:

        m = threshold_stats(
            rows_,
            yy,
            prob,
            threshold
        )

        print(
            f"P>={threshold:.2f} "
            f"| ALERT={m['alerts']:2d} "
            f"| SEV REC={m['severe_recall']:5.1f}% "
            f"| NON FLAG={m['non_flag']:5.1f}% "
            f"| PREC={m['precision']:5.1f}%"
        )


# ============================================================
# F) FEATURE WEIGHTS
# ============================================================

weights = list(
    zip(
        selected,
        pipe.named_steps["model"].coef_[0]
    )
)

weights.sort(
    key=lambda x:
        abs(x[1]),
    reverse=True
)

print()
print("=" * 150)
print("F) STANDARDIZED FEATURE WEIGHTS")
print("=" * 150)

for name, w in weights:
    print(
        f"{name:30} {w:+.4f}"
    )


# ============================================================
# G) SIMPLE TRAIN-ONLY SEVERE RULES
# ============================================================

atoms = []

for x in scores[:12]:

    name = x["name"]

    vals = [
        feat(r,name)
        for r in train
        if valid(feat(r,name))
    ]

    if len(vals) < 10:
        continue

    for q in [
        10,20,25,33,50,67,75,80,90
    ]:

        cut = float(
            np.percentile(vals,q)
        )

        atoms.append(
            (name,"LOW",cut)
        )

        atoms.append(
            (name,"HIGH",cut)
        )


def atom_flag(r, atom):

    name, direction, cut = atom

    x = feat(r,name)

    if not valid(x):
        return False

    if direction == "LOW":
        return x <= cut

    return x >= cut


def rule_flag(r, rule):
    return all(
        atom_flag(r,a)
        for a in rule
    )


def rule_metrics(rows_, rule):

    severe = [
        r for r in rows_
        if label(r) == 1
    ]

    non = [
        r for r in rows_
        if label(r) == 0
    ]

    sev_hit = sum(
        rule_flag(r,rule)
        for r in severe
    )

    non_hit = sum(
        rule_flag(r,rule)
        for r in non
    )

    return {
        "sev_recall":
            100*sev_hit/len(severe)
            if severe else 0,

        "non_flag":
            100*non_hit/len(non)
            if non else 0,
    }


atom_scores = []

for atom in atoms:

    m = rule_metrics(
        train,
        (atom,)
    )

    score = (
        2*m["sev_recall"]
        - 3*m["non_flag"]
    )

    atom_scores.append(
        (
            score,
            atom
        )
    )


atom_scores.sort(
    reverse=True
)

top_atoms = [
    a for _,a
    in atom_scores[:40]
]

rules = []

for k in [2,3]:

    for combo in itertools.combinations(
        top_atoms,
        k
    ):

        names = [
            x[0]
            for x in combo
        ]

        if len(set(names)) != len(names):
            continue

        tr = rule_metrics(
            train,
            combo
        )

        if tr["non_flag"] > 15:
            continue

        if tr["sev_recall"] < 20:
            continue

        va = rule_metrics(
            val,
            combo
        )

        te = rule_metrics(
            test,
            combo
        )

        score = (
            2*tr["sev_recall"]
            - 4*tr["non_flag"]
        )

        rules.append(
            (
                score,
                combo,
                tr,
                va,
                te
            )
        )


rules.sort(
    reverse=True,
    key=lambda x:x[0]
)


print()
print("=" * 150)
print("G) TRAIN-SELECTED SEVERE RULES")
print("=" * 150)


def rule_text(rule):

    parts = []

    for name,direction,cut in rule:

        op = "<=" if direction == "LOW" else ">="

        parts.append(
            f"{name}{op}{cut:.3g}"
        )

    return " & ".join(parts)


for _,rule,tr,va,te in rules[:20]:

    print(
        f"{rule_text(rule)[:72]:72} "
        f"| TR SEV={tr['sev_recall']:5.1f}% "
        f"NON={tr['non_flag']:5.1f}% "
        f"| VA SEV={va['sev_recall']:5.1f}% "
        f"NON={va['non_flag']:5.1f}% "
        f"| TE SEV={te['sev_recall']:5.1f}% "
        f"NON={te['non_flag']:5.1f}%"
    )


# ============================================================
# H) DECISION
# ============================================================

print()
print("=" * 150)
print("H) DECISION SUPPORT")
print("=" * 150)

candidate = False

if (
    val_result["auc"] is not None
    and test_result["auc"] is not None
    and val_result["auc"] >= .65
    and test_result["auc"] >= .65
):
    candidate = True


rule_candidate = False

for _,rule,tr,va,te in rules:

    if (
        tr["sev_recall"] >= 25
        and va["sev_recall"] >= 25
        and te["sev_recall"] >= 25
        and tr["non_flag"] <= 15
        and va["non_flag"] <= 15
        and te["non_flag"] <= 15
    ):
        rule_candidate = True
        break


if candidate or rule_candidate:

    print(
        "SEVERE-DUMP WARNING CANDIDATE FOUND."
    )

    print(
        "Do NOT integrate into V2."
    )

    print(
        "Next step = separate frozen prospective severe-risk monitor."
    )

else:

    print(
        "NO ROBUST SEVERE-DUMP WARNING YET."
    )

    print(
        "Do not force a crash filter from this historical sample."
    )


print()
print("IMPORTANT:")
print("• T23/T31/T32 remain untouched.")
print("• Token identities do not cross splits.")
print("• Feature selection uses TRAIN only.")
print("• TEST is final audit only.")
print("• Severe warning is NOT a trading rule.")
print("• Do not retune T37 using TEST.")

db.close()
