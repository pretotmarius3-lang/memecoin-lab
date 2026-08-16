import sqlite3
import statistics
import math
import random
import numpy as np

from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

DB = "validation_v090.db"

SEVERE = -20.0
RANDOM_SEED = 42

# We focus on transitions between early / mid / recent windows.
TRANSITION_FEATURES = [
    "delta_buy_count_mid_to_recent",
    "delta_sell_count_mid_to_recent",
    "delta_unique_buyers_mid_to_recent",
    "delta_unique_sellers_mid_to_recent",

    "delta_buy_sol_mid_to_recent",
    "delta_sell_sol_mid_to_recent",
    "delta_net_sol_mid_to_recent",

    "delta_buy_share_mid_to_recent",

    "delta_swaps_per_sec_early_to_mid",
    "delta_swaps_per_sec_mid_to_recent",

    "delta_price_early_to_mid",
    "delta_price_mid_to_recent",

    "delta_buy_concentration",
    "delta_breadth_proxy",
]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


def label(r):
    x = r["dex_return_60s"]

    if not valid(x):
        return None

    return 1 if x <= SEVERE else 0


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


rows = db.execute("""
SELECT
    e.id,
    e.token_mint,
    e.dex_return_60s,

    s.early_buy_count,
    s.mid_buy_count,
    s.recent_buy_count,

    s.early_sell_count,
    s.mid_sell_count,
    s.recent_sell_count,

    s.early_unique_buyers,
    s.mid_unique_buyers,
    s.recent_unique_buyers,

    s.early_unique_sellers,
    s.mid_unique_sellers,
    s.recent_unique_sellers,

    s.early_buy_sol,
    s.mid_buy_sol,
    s.recent_buy_sol,

    s.early_sell_sol,
    s.mid_sell_sol,
    s.recent_sell_sol,

    s.early_net_sol,
    s.mid_net_sol,
    s.recent_net_sol,

    s.early_swaps_per_sec,
    s.mid_swaps_per_sec,
    s.recent_swaps_per_sec,

    s.early_price_return,
    s.mid_price_return,
    s.recent_price_return,

    s.early_buy_concentration,
    s.mid_buy_concentration,
    s.recent_buy_concentration,

    s.breadth_score

FROM events e

JOIN event_sequence_features_v340 s
ON s.event_id=e.id

WHERE
    e.dex_return_60s IS NOT NULL

ORDER BY e.id
""").fetchall()


def feature_value(r, name):

    if name == "delta_buy_count_mid_to_recent":
        return (
            r["recent_buy_count"]
            - r["mid_buy_count"]
            if valid(r["recent_buy_count"])
            and valid(r["mid_buy_count"])
            else None
        )

    if name == "delta_sell_count_mid_to_recent":
        return (
            r["recent_sell_count"]
            - r["mid_sell_count"]
            if valid(r["recent_sell_count"])
            and valid(r["mid_sell_count"])
            else None
        )

    if name == "delta_unique_buyers_mid_to_recent":
        return (
            r["recent_unique_buyers"]
            - r["mid_unique_buyers"]
            if valid(r["recent_unique_buyers"])
            and valid(r["mid_unique_buyers"])
            else None
        )

    if name == "delta_unique_sellers_mid_to_recent":
        return (
            r["recent_unique_sellers"]
            - r["mid_unique_sellers"]
            if valid(r["recent_unique_sellers"])
            and valid(r["mid_unique_sellers"])
            else None
        )

    if name == "delta_buy_sol_mid_to_recent":
        return (
            r["recent_buy_sol"]
            - r["mid_buy_sol"]
            if valid(r["recent_buy_sol"])
            and valid(r["mid_buy_sol"])
            else None
        )

    if name == "delta_sell_sol_mid_to_recent":
        return (
            r["recent_sell_sol"]
            - r["mid_sell_sol"]
            if valid(r["recent_sell_sol"])
            and valid(r["mid_sell_sol"])
            else None
        )

    if name == "delta_net_sol_mid_to_recent":
        return (
            r["recent_net_sol"]
            - r["mid_net_sol"]
            if valid(r["recent_net_sol"])
            and valid(r["mid_net_sol"])
            else None
        )

    if name == "delta_buy_share_mid_to_recent":

        mid_total = (
            r["mid_buy_count"]
            + r["mid_sell_count"]
            if valid(r["mid_buy_count"])
            and valid(r["mid_sell_count"])
            else None
        )

        recent_total = (
            r["recent_buy_count"]
            + r["recent_sell_count"]
            if valid(r["recent_buy_count"])
            and valid(r["recent_sell_count"])
            else None
        )

        mid_share = safe_div(
            r["mid_buy_count"],
            mid_total
        )

        recent_share = safe_div(
            r["recent_buy_count"],
            recent_total
        )

        if valid(mid_share) and valid(recent_share):
            return recent_share - mid_share

        return None

    if name == "delta_swaps_per_sec_early_to_mid":
        return (
            r["mid_swaps_per_sec"]
            - r["early_swaps_per_sec"]
            if valid(r["mid_swaps_per_sec"])
            and valid(r["early_swaps_per_sec"])
            else None
        )

    if name == "delta_swaps_per_sec_mid_to_recent":
        return (
            r["recent_swaps_per_sec"]
            - r["mid_swaps_per_sec"]
            if valid(r["recent_swaps_per_sec"])
            and valid(r["mid_swaps_per_sec"])
            else None
        )

    if name == "delta_price_early_to_mid":
        return (
            r["mid_price_return"]
            - r["early_price_return"]
            if valid(r["mid_price_return"])
            and valid(r["early_price_return"])
            else None
        )

    if name == "delta_price_mid_to_recent":
        return (
            r["recent_price_return"]
            - r["mid_price_return"]
            if valid(r["recent_price_return"])
            and valid(r["mid_price_return"])
            else None
        )

    if name == "delta_buy_concentration":
        return (
            r["recent_buy_concentration"]
            - r["mid_buy_concentration"]
            if valid(r["recent_buy_concentration"])
            and valid(r["mid_buy_concentration"])
            else None
        )

    if name == "delta_breadth_proxy":
        early = safe_div(
            r["early_unique_buyers"],
            (
                r["early_unique_buyers"]
                + r["early_unique_sellers"]
            )
            if valid(r["early_unique_buyers"])
            and valid(r["early_unique_sellers"])
            else None
        )

        recent = safe_div(
            r["recent_unique_buyers"],
            (
                r["recent_unique_buyers"]
                + r["recent_unique_sellers"]
            )
            if valid(r["recent_unique_buyers"])
            and valid(r["recent_unique_sellers"])
            else None
        )

        if valid(early) and valid(recent):
            return recent - early

        return None

    return None


usable = [
    r for r in rows
    if label(r) is not None
]

print("=" * 155)
print("MEMECOIN LAB — T38 SEVERE DUMP TRANSITION LAB")
print("=" * 155)

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
# TOKEN HOLDOUT SPLIT
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
        label(r)==1
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
# A) TRANSITION FEATURE SEPARATION
# ============================================================

scores = []

for name in TRANSITION_FEATURES:

    sev = [
        feature_value(r,name)
        for r in train
        if (
            label(r)==1
            and valid(feature_value(r,name))
        )
    ]

    non = [
        feature_value(r,name)
        for r in train
        if (
            label(r)==0
            and valid(feature_value(r,name))
        )
    ]

    if len(sev)<2 or len(non)<2:
        continue

    ms = med(sev)
    mn = med(non)

    pooled = statistics.pstdev(
        sev+non
    )

    sep = (
        abs(ms-mn)/pooled
        if pooled>0
        else 0
    )

    scores.append({
        "name":name,
        "sev_med":ms,
        "non_med":mn,
        "diff":ms-mn,
        "sep":sep,
        "sev_n":len(sev),
        "non_n":len(non),
    })


scores.sort(
    key=lambda x:x["sep"],
    reverse=True
)

print()
print("=" * 155)
print("A) TRAIN TRANSITION FEATURE SEPARATION")
print("=" * 155)

print(
    f"{'FEATURE':36} "
    f"{'SEV MED':>12} "
    f"{'NON MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-"*90)

for x in scores:

    print(
        f"{x['name']:36} "
        f"{x['sev_med']:+11.4f} "
        f"{x['non_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# B) DIRECTION SURVIVAL
# ============================================================

print()
print("=" * 155)
print("B) DIRECTION SURVIVAL — TRAIN / VALID / TEST")
print("=" * 155)

print(
    f"{'FEATURE':36} "
    f"{'TRAIN Δ':>10} "
    f"{'VALID Δ':>10} "
    f"{'TEST Δ':>10} "
    f"{'SAME DIR':>10}"
)

print("-"*85)

survivors = []

for x in scores:

    name = x["name"]

    diffs = []

    for rr in [
        train,
        val,
        test
    ]:

        sev = [
            feature_value(r,name)
            for r in rr
            if (
                label(r)==1
                and valid(feature_value(r,name))
            )
        ]

        non = [
            feature_value(r,name)
            for r in rr
            if (
                label(r)==0
                and valid(feature_value(r,name))
            )
        ]

        if not sev or not non:
            diffs.append(None)
            continue

        diffs.append(
            med(sev)-med(non)
        )

    same_dir = False

    if all(
        valid(d)
        for d in diffs
    ):

        signs = [
            1 if d>0
            else -1 if d<0
            else 0
            for d in diffs
        ]

        same_dir = (
            signs[0] != 0
            and signs[0] == signs[1] == signs[2]
        )

    if same_dir:
        survivors.append(name)

    print(
        f"{name:36} "
        f"{(diffs[0] if valid(diffs[0]) else 0):+9.3f} "
        f"{(diffs[1] if valid(diffs[1]) else 0):+9.3f} "
        f"{(diffs[2] if valid(diffs[2]) else 0):+9.3f} "
        f"{str(same_dir):>10}"
    )


# ============================================================
# C) MODEL ON TRANSITIONS ONLY
# ============================================================

selected = [
    x["name"]
    for x in scores[:10]
]


def build_matrix(rr):

    X=[]

    for r in rr:

        row=[]

        for name in selected:

            v=feature_value(r,name)

            row.append(
                float(v)
                if valid(v)
                else np.nan
            )

        X.append(row)

    return np.asarray(
        X,
        dtype=float
    )


X_train = build_matrix(train)
X_val = build_matrix(val)
X_test = build_matrix(test)

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
        "imp",
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
            class_weight="balanced",
            max_iter=2000,
            random_state=42
        )
    )
])

pipe.fit(
    X_train,
    y_train
)


def auc_for(X,y):

    prob = pipe.predict_proba(
        X
    )[:,1]

    auc = (
        roc_auc_score(
            y,
            prob
        )
        if len(set(y))>1
        else None
    )

    return prob, auc


val_prob, val_auc = auc_for(
    X_val,
    y_val
)

test_prob, test_auc = auc_for(
    X_test,
    y_test
)

print()
print("="*155)
print("C) TRANSITION-ONLY MODEL")
print("="*155)

print(
    f"VALID AUC = "
    f"{val_auc:.3f}"
    if val_auc is not None
    else "VALID AUC = NA"
)

print(
    f"TEST  AUC = "
    f"{test_auc:.3f}"
    if test_auc is not None
    else "TEST AUC = NA"
)


# ============================================================
# D) ALERT THRESHOLDS
# ============================================================

def alert_stats(
    rr,
    y,
    prob,
    threshold
):

    idx = [
        i
        for i,p in enumerate(prob)
        if p >= threshold
    ]

    severe_total = sum(
        y==1
    )

    non_total = sum(
        y==0
    )

    severe_hit = sum(
        y[i]==1
        for i in idx
    )

    false_hit = sum(
        y[i]==0
        for i in idx
    )

    return {
        "alerts":len(idx),

        "sev_recall":
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
                rr[i]["token_mint"]
                for i in idx
            )),
    }


print()
print("="*155)
print("D) TRANSITION WARNING THRESHOLDS")
print("="*155)

print(
    f"{'SPLIT':8} "
    f"{'P':>6} "
    f"{'ALERT':>7} "
    f"{'SEV REC':>9} "
    f"{'NON FLAG':>9} "
    f"{'PREC':>8}"
)

print("-"*65)

for split_name,rr,y,prob in [
    (
        "VALID",
        val,
        y_val,
        val_prob
    ),
    (
        "TEST",
        test,
        y_test,
        test_prob
    )
]:

    for threshold in [
        .40,
        .50,
        .60,
        .70,
        .80
    ]:

        m=alert_stats(
            rr,
            y,
            prob,
            threshold
        )

        print(
            f"{split_name:8} "
            f"{threshold:6.2f} "
            f"{m['alerts']:7d} "
            f"{m['sev_recall']:8.1f}% "
            f"{m['non_flag']:8.1f}% "
            f"{m['precision']:7.1f}%"
        )


# ============================================================
# E) FIRST EVENT PER TOKEN
# ============================================================

print()
print("="*155)
print("E) FIRST-EVENT/TOKEN AUDIT")
print("="*155)

for title,rr in [
    (
        "VALID FIRST",
        first_per_token(val)
    ),
    (
        "TEST FIRST",
        first_per_token(test)
    )
]:

    X=build_matrix(rr)

    y=np.asarray(
        [label(r) for r in rr],
        dtype=int
    )

    prob=pipe.predict_proba(
        X
    )[:,1]

    auc=(
        roc_auc_score(
            y,
            prob
        )
        if len(set(y))>1
        else None
    )

    print()
    print(
        f"{title} "
        f"| N={len(rr)} "
        f"| SEV={sum(y==1)} "
        f"| AUC="
        f"{auc:.3f}"
        if auc is not None
        else "AUC=NA"
    )

    for threshold in [
        .50,
        .60,
        .70
    ]:

        m=alert_stats(
            rr,
            y,
            prob,
            threshold
        )

        print(
            f"P>={threshold:.2f} "
            f"| ALERT={m['alerts']:2d} "
            f"| SEV REC={m['sev_recall']:5.1f}% "
            f"| NON FLAG={m['non_flag']:5.1f}% "
            f"| PREC={m['precision']:5.1f}%"
        )


# ============================================================
# F) FEATURE WEIGHTS
# ============================================================

weights=list(
    zip(
        selected,
        pipe.named_steps[
            "model"
        ].coef_[0]
    )
)

weights.sort(
    key=lambda x:
        abs(x[1]),
    reverse=True
)

print()
print("="*155)
print("F) TRANSITION FEATURE WEIGHTS")
print("="*155)

for name,w in weights:

    print(
        f"{name:38} "
        f"{w:+.4f}"
    )


# ============================================================
# G) DECISION
# ============================================================

print()
print("="*155)
print("G) DECISION SUPPORT")
print("="*155)

good_auc = (
    val_auc is not None
    and test_auc is not None
    and val_auc >= .65
    and test_auc >= .65
)

enough_direction = (
    len(survivors) >= 3
)

if good_auc and enough_direction:

    print(
        "DYNAMIC SEVERE-DUMP TRANSITION SIGNAL FOUND."
    )

    print(
        "Do NOT integrate into V2."
    )

    print(
        "Next step = freeze transition score "
        "for prospective shadow validation."
    )

else:

    print(
        "NO ROBUST DYNAMIC CRASH TRANSITION YET."
    )

    print(
        "Transition features do not generalize "
        "cleanly enough across token holdouts."
    )


print()
print(
    f"FEATURES WITH SAME DIRECTION "
    f"TRAIN/VALID/TEST: "
    f"{len(survivors)}"
)

for name in survivors:
    print(
        "•",
        name
    )


print()
print("IMPORTANT:")
print("• T23/T31/T32 remain untouched.")
print("• Transition features only.")
print("• Token identities do not cross splits.")
print("• TEST is final audit only.")
print("• Do not retune T38 from TEST.")
print("• This is not a trading rule.")

db.close()
