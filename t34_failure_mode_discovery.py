import sqlite3
import statistics
import math
import random
from collections import defaultdict

import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score

DB = "validation_v090.db"

DUMP = -10.0
SEVERE_DUMP = -20.0
RUNNER = 10.0

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

    return None


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


# ============================================================
# LABELS
# ============================================================

def cls(r):

    x = r["dex_return_60s"]

    if not valid(x):
        return None

    if x <= SEVERE_DUMP:
        return "SEVERE_DUMP"

    if x <= DUMP:
        return "DUMP"

    if x >= RUNNER:
        return "RUNNER"

    return "NEUTRAL"


usable = [
    r for r in rows
    if cls(r) is not None
]

dumps = [
    r for r in usable
    if cls(r) in ["DUMP", "SEVERE_DUMP"]
]

severe = [
    r for r in usable
    if cls(r) == "SEVERE_DUMP"
]

runners = [
    r for r in usable
    if cls(r) == "RUNNER"
]

print("=" * 150)
print("MEMECOIN LAB — T34 FAILURE MODE DISCOVERY")
print("=" * 150)

print(
    f"TOTAL EVENTS : {len(usable)}"
)

print(
    f"RUNNERS      : {len(runners)}"
)

print(
    f"DUMPS <=-10  : {len(dumps)}"
)

print(
    f"SEVERE <=-20 : {len(severe)}"
)


# ============================================================
# A) BASIC FAILURE SEPARATION
# ============================================================

print()
print("=" * 150)
print("A) FAILURE SIGNATURES — DUMP VS RUNNER")
print("=" * 150)

scores = []

for name in FEATURES:

    dv = [
        feat(r, name)
        for r in dumps
        if valid(feat(r, name))
    ]

    rv = [
        feat(r, name)
        for r in runners
        if valid(feat(r, name))
    ]

    if not dv or not rv:
        continue

    diff = (
        med(dv)
        - med(rv)
    )

    pooled = (
        statistics.pstdev(
            dv + rv
        )
        if len(dv + rv) > 1
        else 0
    )

    sep = (
        abs(diff) / pooled
        if pooled > 0
        else 0
    )

    scores.append({
        "name": name,
        "dump_med": med(dv),
        "run_med": med(rv),
        "diff": diff,
        "sep": sep,
    })

scores.sort(
    key=lambda x: x["sep"],
    reverse=True
)

print(
    f"{'FEATURE':28} "
    f"{'DUMP MED':>12} "
    f"{'RUN MED':>12} "
    f"{'DIFF D-R':>12} "
    f"{'SEP':>8}"
)

print("-" * 80)

for x in scores[:20]:

    print(
        f"{x['name']:28} "
        f"{x['dump_med']:+11.4f} "
        f"{x['run_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# B) CLUSTER FAILURE MODES ONLY
# ============================================================

cluster_features = [
    x["name"]
    for x in scores[:12]
]

X = []

for r in dumps:

    row = []

    for name in cluster_features:
        v = feat(r, name)

        row.append(
            float(v)
            if valid(v)
            else np.nan
        )

    X.append(row)

X = np.asarray(
    X,
    dtype=float
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
    )
])

XZ = pipe.fit_transform(X)

results = []

for k in [2,3,4,5]:

    if len(dumps) <= k:
        continue

    km = KMeans(
        n_clusters=k,
        random_state=RANDOM_SEED,
        n_init=20
    )

    lab = km.fit_predict(XZ)

    sil = silhouette_score(
        XZ,
        lab
    )

    results.append(
        (k, sil, lab)
    )

results.sort(
    key=lambda x: x[1],
    reverse=True
)

print()
print("=" * 150)
print("B) FAILURE CLUSTER COUNT")
print("=" * 150)

for k, sil, _ in results:
    print(
        f"K={k} | SILHOUETTE={sil:.4f}"
    )

best_k, best_sil, labels = results[0]

print()
print(
    f"BEST K={best_k} "
    f"| SILHOUETTE={best_sil:.4f}"
)


# ============================================================
# C) FAILURE MODE PROFILES
# ============================================================

print()
print("=" * 150)
print("C) FAILURE MODE PROFILES")
print("=" * 150)

for cluster in range(best_k):

    idx = [
        i
        for i,lab in enumerate(labels)
        if lab == cluster
    ]

    subset = [
        dumps[i]
        for i in idx
    ]

    r60 = [
        r["dex_return_60s"]
        for r in subset
        if valid(r["dex_return_60s"])
    ]

    tokens = len(
        set(
            r["token_mint"]
            for r in subset
        )
    )

    severe_pct = (
        100
        * sum(
            cls(r) == "SEVERE_DUMP"
            for r in subset
        )
        / len(subset)
    )

    print()
    print(
        f"FAILURE MODE F{cluster} "
        f"| N={len(subset)} "
        f"| TOKENS={tokens} "
        f"| MED60={med(r60):+.2f}% "
        f"| SEVERE={severe_pct:.1f}%"
    )

    print("-" * 100)

    feature_profile = []

    for name in cluster_features:

        vals = [
            feat(r, name)
            for r in subset
            if valid(feat(r, name))
        ]

        all_vals = [
            feat(r, name)
            for r in dumps
            if valid(feat(r, name))
        ]

        if not vals or not all_vals:
            continue

        cm = med(vals)
        gm = med(all_vals)

        scale = (
            statistics.pstdev(all_vals)
            if len(all_vals) > 1
            else 0
        )

        rel = (
            (cm-gm)/scale
            if scale > 0
            else 0
        )

        feature_profile.append(
            (
                abs(rel),
                name,
                cm,
                gm,
                rel
            )
        )

    feature_profile.sort(
        reverse=True
    )

    for _, name, cm, gm, rel in feature_profile[:10]:

        print(
            f"{name:28} "
            f"MODE={cm:+.4f} "
            f"| DUMP GLOBAL={gm:+.4f} "
            f"| REL={rel:+.2f}"
        )


# ============================================================
# D) TOKEN-BALANCED FAILURE MODES
# ============================================================

print()
print("=" * 150)
print("D) TOKEN CONCENTRATION BY FAILURE MODE")
print("=" * 150)

for cluster in range(best_k):

    idx = [
        i
        for i,lab in enumerate(labels)
        if lab == cluster
    ]

    subset = [
        dumps[i]
        for i in idx
    ]

    counts = defaultdict(int)

    for r in subset:
        counts[
            r["token_mint"]
        ] += 1

    sorted_counts = sorted(
        counts.items(),
        key=lambda x: x[1],
        reverse=True
    )

    print()
    print(
        f"F{cluster} "
        f"| EVENTS={len(subset)} "
        f"| TOKENS={len(counts)}"
    )

    for token, n in sorted_counts[:8]:

        print(
            f"{token[:20]:20} "
            f"N={n}"
        )


# ============================================================
# E) SIMPLE VETO CANDIDATES
#
# discovery only / descriptive
# ============================================================

print()
print("=" * 150)
print("E) SIMPLE VETO CANDIDATES")
print("=" * 150)

print(
    "Goal = flag a meaningful share of dumps "
    "while rejecting as few runners as possible."
)

candidates = []

for x in scores[:12]:

    name = x["name"]

    vals = [
        feat(r, name)
        for r in usable
        if valid(feat(r, name))
    ]

    if len(vals) < 20:
        continue

    q25 = np.percentile(vals,25)
    q50 = np.percentile(vals,50)
    q75 = np.percentile(vals,75)

    for direction, cut in [
        ("LOW", q25),
        ("LOW", q50),
        ("HIGH", q50),
        ("HIGH", q75),
    ]:

        def flagged(r):

            v = feat(r, name)

            if not valid(v):
                return False

            if direction == "LOW":
                return v <= cut

            return v >= cut

        dump_hit = sum(
            flagged(r)
            for r in dumps
        )

        runner_hit = sum(
            flagged(r)
            for r in runners
        )

        dump_rate = (
            100*dump_hit/len(dumps)
            if dumps
            else 0
        )

        runner_rate = (
            100*runner_hit/len(runners)
            if runners
            else 0
        )

        score = (
            dump_rate
            - 1.5*runner_rate
        )

        candidates.append({
            "feature": name,
            "dir": direction,
            "cut": cut,
            "dump_rate": dump_rate,
            "runner_rate": runner_rate,
            "score": score,
        })


candidates.sort(
    key=lambda x: x["score"],
    reverse=True
)

print(
    f"{'FEATURE':28} "
    f"{'DIR':>5} "
    f"{'CUT':>12} "
    f"{'DUMP FLAG':>11} "
    f"{'RUN FLAG':>10} "
    f"{'SCORE':>9}"
)

print("-" * 90)

for x in candidates[:20]:

    print(
        f"{x['feature']:28} "
        f"{x['dir']:>5} "
        f"{x['cut']:+11.4f} "
        f"{x['dump_rate']:10.1f}% "
        f"{x['runner_rate']:9.1f}% "
        f"{x['score']:+8.1f}"
    )


# ============================================================
# F) FIRST-EVENT/TOKEN FAILURE AUDIT
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


first_dump = first_per_token(
    dumps
)

first_run = first_per_token(
    runners
)

print()
print("=" * 150)
print("F) FIRST EVENT / TOKEN — FAILURE SIGNATURES")
print("=" * 150)

print(
    f"FIRST DUMPS={len(first_dump)} "
    f"| FIRST RUNNERS={len(first_run)}"
)

for x in scores[:10]:

    name = x["name"]

    dv = [
        feat(r,name)
        for r in first_dump
        if valid(feat(r,name))
    ]

    rv = [
        feat(r,name)
        for r in first_run
        if valid(feat(r,name))
    ]

    if not dv or not rv:
        continue

    print(
        f"{name:28} "
        f"DUMP={med(dv):+.4f} "
        f"| RUN={med(rv):+.4f} "
        f"| DIFF={med(dv)-med(rv):+.4f}"
    )


# ============================================================
# G) DECISION
# ============================================================

print()
print("=" * 150)
print("G) DECISION SUPPORT")
print("=" * 150)

good_vetoes = [
    x for x in candidates
    if (
        x["dump_rate"] >= 35
        and x["runner_rate"] <= 15
    )
]

if good_vetoes:

    print(
        "POTENTIAL FAILURE VETO FOUND."
    )

    print(
        "Do NOT add it to V2 yet."
    )

    print(
        "Next step = token-holdout validation "
        "of the veto only."
    )

else:

    print(
        "NO CLEAN FAILURE VETO YET."
    )

    print(
        "Failure modes may be heterogeneous "
        "or too overlapping with runners."
    )

print()
print("IMPORTANT:")
print("• T23/T31/T32 remain untouched.")
print("• This is historical failure discovery only.")
print("• No V2 parameter is modified.")
print("• No prospective data is used for tuning.")
print("• Do not deploy any veto from T34 directly.")

db.close()
