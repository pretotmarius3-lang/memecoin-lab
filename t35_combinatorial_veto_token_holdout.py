import sqlite3
import statistics
import math
import random
import itertools
from collections import defaultdict

import numpy as np

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
SEVERE_DUMP = -20.0

RANDOM_SEED = 42

# Max complexity of a veto rule
MAX_CONDITIONS = 3

FEATURES = [
    "mid_buy_count",
    "mid_sell_count",
    "mid_flow_balance",
    "new_wallets10",
    "new_wallets30",
    "recent_swaps_per_sec",
    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_buy_share",
    "breadth_score",
    "volume_m5",
    "vol_liq",
    "recent_net_sol",
    "mid_price_return",
    "recent_price_return",
    "buy_concentration_trend",
    "fa",
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


def label(r):
    x = r["dex_return_60s"]

    if not valid(x):
        return None

    if x >= RUNNER:
        return "RUNNER"

    if x <= SEVERE_DUMP:
        return "SEVERE_DUMP"

    if x <= DUMP:
        return "DUMP"

    return "NEUTRAL"


db = connect()

if not table_exists(db, "event_sequence_features_v340"):
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
    e.new_wallets10,
    e.new_wallets30,

    d.volume_m5,
    d.liquidity_usd,

    s.mid_buy_count,
    s.mid_sell_count,
    s.recent_unique_buyers,

    s.early_swaps_per_sec,
    s.mid_swaps_per_sec,
    s.recent_swaps_per_sec,

    s.buy_concentration_trend,

    s.recent_price_return,
    s.mid_price_return,

    s.recent_net_sol,
    s.recent_buy_share,

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
    if label(r) in [
        "RUNNER",
        "DUMP",
        "SEVERE_DUMP"
    ]
]

print("=" * 155)
print("MEMECOIN LAB — T35 COMBINATORIAL VETO / TOKEN HOLDOUT")
print("=" * 155)

print(
    f"LABELED EVENTS : {len(usable)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in usable))}"
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

n_train = max(1, int(n * 0.60))
n_val = max(1, int(n * 0.20))

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


def counts(rows):
    return {
        "runner": sum(
            label(r) == "RUNNER"
            for r in rows
        ),

        "dump": sum(
            label(r) in [
                "DUMP",
                "SEVERE_DUMP"
            ]
            for r in rows
        ),

        "severe": sum(
            label(r) == "SEVERE_DUMP"
            for r in rows
        ),

        "tokens":
            len(set(
                r["token_mint"]
                for r in rows
            ))
    }


print()

for name, subset in [
    ("TRAIN", train),
    ("VALIDATION", val),
    ("TEST", test),
]:

    c = counts(subset)

    print(
        f"{name:10} | "
        f"EVENTS={len(subset):3d} | "
        f"TOK={c['tokens']:3d} | "
        f"RUN={c['runner']:3d} | "
        f"DUMP={c['dump']:3d} | "
        f"SEVERE={c['severe']:3d}"
    )


# ============================================================
# FIRST EVENT PER TOKEN
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


first_train = first_per_token(train)
first_val = first_per_token(val)
first_test = first_per_token(test)


# ============================================================
# GENERATE TRAIN-ONLY CUTS
# ============================================================

cuts = {}

for name in FEATURES:

    vals = [
        feat(r, name)
        for r in train
        if valid(feat(r, name))
    ]

    if len(vals) < 10:
        continue

    unique = sorted(set(vals))

    q = [
        np.percentile(vals, 10),
        np.percentile(vals, 20),
        np.percentile(vals, 25),
        np.percentile(vals, 33),
        np.percentile(vals, 50),
        np.percentile(vals, 67),
        np.percentile(vals, 75),
        np.percentile(vals, 80),
        np.percentile(vals, 90),
    ]

    # avoid tons of duplicate thresholds
    q = sorted(
        set(
            round(float(x), 8)
            for x in q
        )
    )

    cuts[name] = q


# ============================================================
# CONDITION
# ============================================================

def condition_pass(r, cond):

    name, direction, cut = cond

    v = feat(r, name)

    if not valid(v):
        return False

    if direction == "LOW":
        return v <= cut

    return v >= cut


def rule_flagged(r, rule):

    return all(
        condition_pass(r, c)
        for c in rule
    )


# ============================================================
# RULE METRICS
# ============================================================

def metrics(rows, rule):

    runners = [
        r for r in rows
        if label(r) == "RUNNER"
    ]

    dumps = [
        r for r in rows
        if label(r) in [
            "DUMP",
            "SEVERE_DUMP"
        ]
    ]

    severe = [
        r for r in rows
        if label(r) == "SEVERE_DUMP"
    ]

    run_flag = sum(
        rule_flagged(r, rule)
        for r in runners
    )

    dump_flag = sum(
        rule_flagged(r, rule)
        for r in dumps
    )

    severe_flag = sum(
        rule_flagged(r, rule)
        for r in severe
    )

    flagged_all = [
        r for r in rows
        if rule_flagged(r, rule)
    ]

    return {
        "runner_n":
            len(runners),

        "dump_n":
            len(dumps),

        "severe_n":
            len(severe),

        "runner_sacrifice":
            (
                100 * run_flag / len(runners)
                if runners
                else 0
            ),

        "dump_capture":
            (
                100 * dump_flag / len(dumps)
                if dumps
                else 0
            ),

        "severe_capture":
            (
                100 * severe_flag / len(severe)
                if severe
                else 0
            ),

        "flagged":
            len(flagged_all),

        "flagged_tokens":
            len(set(
                r["token_mint"]
                for r in flagged_all
            )),
    }


# ============================================================
# BUILD ATOMIC CONDITIONS
# ============================================================

atomic = []

for name, feature_cuts in cuts.items():

    for cut in feature_cuts:

        atomic.append(
            (
                name,
                "LOW",
                cut
            )
        )

        atomic.append(
            (
                name,
                "HIGH",
                cut
            )
        )


# ============================================================
# PRE-FILTER SINGLE CONDITIONS
#
# Keep conditions that at least have some dump information.
# ============================================================

atom_scored = []

for cond in atomic:

    m = metrics(
        train,
        (cond,)
    )

    # loose filter, only to reduce combinatorial explosion
    if (
        m["dump_capture"] >= 10
        and m["flagged"] >= 3
    ):
        score = (
            m["dump_capture"]
            + 0.50*m["severe_capture"]
            - 3.0*m["runner_sacrifice"]
        )

        atom_scored.append(
            (
                score,
                cond,
                m
            )
        )


atom_scored.sort(
    key=lambda x:
        x[0],
    reverse=True
)

# limit search space
TOP_ATOMS = [
    x[1]
    for x in atom_scored[:60]
]


print()
print("=" * 155)
print("A) TOP TRAIN ATOMIC CONDITIONS")
print("=" * 155)

print(
    f"{'FEATURE':25} "
    f"{'DIR':>5} "
    f"{'CUT':>12} "
    f"{'DUMP':>9} "
    f"{'SEVERE':>9} "
    f"{'RUN LOSS':>9}"
)

print("-" * 80)

for score, cond, m in atom_scored[:20]:

    name, direction, cut = cond

    print(
        f"{name:25} "
        f"{direction:>5} "
        f"{cut:+11.4f} "
        f"{m['dump_capture']:8.1f}% "
        f"{m['severe_capture']:8.1f}% "
        f"{m['runner_sacrifice']:8.1f}%"
    )


# ============================================================
# SEARCH RULES
# ============================================================

rules = []

for k in range(
    2,
    MAX_CONDITIONS + 1
):

    for combo in itertools.combinations(
        TOP_ATOMS,
        k
    ):

        # don't allow two conditions on same feature
        names = [
            c[0]
            for c in combo
        ]

        if len(set(names)) != len(names):
            continue

        m = metrics(
            train,
            combo
        )

        # hard preference for runner preservation
        if m["runner_sacrifice"] > 15:
            continue

        if m["dump_capture"] < 10:
            continue

        score = (
            2.0*m["dump_capture"]
            + 1.0*m["severe_capture"]
            - 6.0*m["runner_sacrifice"]
        )

        rules.append({
            "rule":
                combo,

            "train":
                m,

            "score":
                score,
        })


rules.sort(
    key=lambda x:
        x["score"],
    reverse=True
)


# ============================================================
# VALIDATION OF TRAIN-SELECTED RULES
# ============================================================

for r in rules[:100]:

    r["val"] = metrics(
        val,
        r["rule"]
    )

    r["test"] = metrics(
        test,
        r["rule"]
    )

    r["first_val"] = metrics(
        first_val,
        r["rule"]
    )

    r["first_test"] = metrics(
        first_test,
        r["rule"]
    )


# ============================================================
# OUTPUT TOP RULES
# ============================================================

print()
print("=" * 155)
print("B) TRAIN-SELECTED COMBINATORIAL VETOES")
print("=" * 155)

print(
    f"{'RULE':72} | "
    f"{'TR DUMP':>8} "
    f"{'TR RUN':>7} | "
    f"{'VA DUMP':>8} "
    f"{'VA RUN':>7} | "
    f"{'TE DUMP':>8} "
    f"{'TE RUN':>7}"
)

print("-" * 135)

def rule_text(rule):

    parts = []

    for name, direction, cut in rule:

        op = "<=" if direction == "LOW" else ">="

        parts.append(
            f"{name}{op}{cut:.3g}"
        )

    return " & ".join(parts)


for r in rules[:30]:

    if "val" not in r:
        continue

    print(
        f"{rule_text(r['rule'])[:72]:72} | "
        f"{r['train']['dump_capture']:7.1f}% "
        f"{r['train']['runner_sacrifice']:6.1f}% | "
        f"{r['val']['dump_capture']:7.1f}% "
        f"{r['val']['runner_sacrifice']:6.1f}% | "
        f"{r['test']['dump_capture']:7.1f}% "
        f"{r['test']['runner_sacrifice']:6.1f}%"
    )


# ============================================================
# C) STRICT SURVIVORS
# ============================================================

strict = []

for r in rules[:100]:

    if "val" not in r:
        continue

    tr = r["train"]
    va = r["val"]
    te = r["test"]

    if (
        tr["runner_sacrifice"] <= 10
        and va["runner_sacrifice"] <= 10
        and te["runner_sacrifice"] <= 10
        and tr["dump_capture"] >= 20
        and va["dump_capture"] >= 20
        and te["dump_capture"] >= 20
    ):
        strict.append(r)


print()
print("=" * 155)
print("C) STRICT SURVIVORS")
print("=" * 155)

if not strict:

    print(
        "NO RULE satisfies:"
    )

    print(
        "runner sacrifice <=10% in TRAIN/VAL/TEST"
    )

    print(
        "and dump capture >=20% in TRAIN/VAL/TEST"
    )

else:

    for r in strict[:20]:

        print()
        print(
            rule_text(
                r["rule"]
            )
        )

        for name, m in [
            ("TRAIN", r["train"]),
            ("VALID", r["val"]),
            ("TEST", r["test"]),
        ]:

            print(
                f"{name:6} | "
                f"DUMP={m['dump_capture']:5.1f}% | "
                f"SEVERE={m['severe_capture']:5.1f}% | "
                f"RUN LOSS={m['runner_sacrifice']:5.1f}% | "
                f"FLAG TOK={m['flagged_tokens']}"
            )


# ============================================================
# D) FIRST EVENT PER TOKEN
# ============================================================

print()
print("=" * 155)
print("D) FIRST-EVENT/TOKEN AUDIT")
print("=" * 155)

audit_pool = (
    strict[:10]
    if strict
    else rules[:10]
)

for r in audit_pool:

    if "first_val" not in r:
        continue

    print()
    print(
        rule_text(
            r["rule"]
        )
    )

    for name, m in [
        ("VAL FIRST", r["first_val"]),
        ("TEST FIRST", r["first_test"]),
    ]:

        print(
            f"{name:10} | "
            f"DUMP={m['dump_capture']:5.1f}% | "
            f"SEVERE={m['severe_capture']:5.1f}% | "
            f"RUN LOSS={m['runner_sacrifice']:5.1f}% | "
            f"FLAG TOK={m['flagged_tokens']}"
        )


# ============================================================
# E) ASYMMETRIC FRONTIER
# ============================================================

print()
print("=" * 155)
print("E) ASYMMETRIC FRONTIER — TEST")
print("=" * 155)

frontier = []

for r in rules[:100]:

    if "test" not in r:
        continue

    te = r["test"]

    # prioritize runner preservation
    if te["runner_sacrifice"] <= 10:

        frontier.append(r)


frontier.sort(
    key=lambda r:
        (
            r["test"]["runner_sacrifice"],
            -r["test"]["severe_capture"],
            -r["test"]["dump_capture"],
        )
)


for r in frontier[:20]:

    te = r["test"]
    va = r["val"]

    print(
        f"{rule_text(r['rule'])[:75]:75} "
        f"| TEST RUN LOSS={te['runner_sacrifice']:5.1f}% "
        f"| TEST DUMP={te['dump_capture']:5.1f}% "
        f"| TEST SEVERE={te['severe_capture']:5.1f}% "
        f"| VAL DUMP={va['dump_capture']:5.1f}%"
    )


# ============================================================
# F) DECISION
# ============================================================

print()
print("=" * 155)
print("F) DECISION SUPPORT")
print("=" * 155)

if strict:

    best = strict[0]

    print(
        "COMBINATORIAL VETO SURVIVES TOKEN-HOLDOUT."
    )

    print(
        "BEST CANDIDATE:"
    )

    print(
        rule_text(
            best["rule"]
        )
    )

    print()

    print(
        "Do NOT add it to V2."
    )

    print(
        "Next step = freeze this veto separately "
        "and test prospectively."
    )

else:

    print(
        "NO ROBUST COMBINATORIAL VETO."
    )

    print(
        "Do not force a veto from historical data."
    )


print()
print("IMPORTANT:")
print("• T23/T31/T32 remain untouched.")
print("• Train thresholds are generated from TRAIN only.")
print("• Token identities do not cross splits.")
print("• TEST is final audit only.")
print("• Do not retune this exact T35 using TEST.")
print("• V2 Frozen remains unchanged.")

db.close()
