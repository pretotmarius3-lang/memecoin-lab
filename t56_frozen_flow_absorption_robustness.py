import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

BASE_EPS = 0.05
BOOT_N = 5000
SEED = 56


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def avg(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def sdiv(a, b, eps=BASE_EPS):
    if not valid(a) or not valid(b):
        return None
    if abs(b) < eps:
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


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def auc(y, score):

    pos = [score[i] for i in range(len(y)) if y[i] == 1]
    neg = [score[i] for i in range(len(y)) if y[i] == 0]

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


def directional_auc(rows, feature):

    rr = [
        r for r in rows
        if valid(r[feature])
    ]

    if not rr:
        return None

    y = [r["label"] for r in rr]

    # both frozen directions are LOWER = RUN-like
    score = [-r[feature] for r in rr]

    return auc(y, score)


def feature_diff(rows, feature):

    run = [
        r[feature]
        for r in rows
        if r["label"] == 1 and valid(r[feature])
    ]

    dump = [
        r[feature]
        for r in rows
        if r["label"] == 0 and valid(r[feature])
    ]

    if not run or not dump:
        return None, len(run), len(dump)

    return med(run) - med(dump), len(run), len(dump)


def percentile(xs, q):

    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    pos = (len(xs)-1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return xs[lo]*(1-w) + xs[hi]*w


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


rows = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,
    e.dex_return_60s,

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


for r in rows:

    lab = label_r60(r["dex_return_60s"])

    if lab is None:
        continue


    recent_marginal_response = sdiv(
        r["recent_price_return"],
        r["recent_net_sol"],
        BASE_EPS
    )


    recent_sell_buy_sol_ratio = sdiv(
        abs(r["recent_sell_sol"])
        if valid(r["recent_sell_sol"])
        else None,
        abs(r["recent_buy_sol"])
        if valid(r["recent_buy_sol"])
        else None,
        BASE_EPS
    )


    # CAP family context from T49/T50/T53
    price_per_net = recent_marginal_response

    gross = (
        abs(r["recent_buy_sol"])
        + abs(r["recent_sell_sol"])
        if (
            valid(r["recent_buy_sol"])
            and valid(r["recent_sell_sol"])
        )
        else None
    )

    net_eff = sdiv(
        r["recent_net_sol"],
        gross,
        BASE_EPS
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
        "label": lab,

        "recent_marginal_response":
            recent_marginal_response,

        "recent_sell_buy_sol_ratio":
            recent_sell_buy_sol_ratio,

        "price_per_net":
            price_per_net,

        "net_eff":
            net_eff,

        "early_div":
            early_div,
    })


PRIMARY = "recent_marginal_response"
SECONDARY = "recent_sell_buy_sol_ratio"


print("=" * 180)
print("MEMECOIN LAB — T56 FROZEN FLOW-ABSORPTION ROBUSTNESS + INCREMENTAL AUDIT")
print("=" * 180)

print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : {len(set(r['token_mint'] for r in records))}"
)

print(
    f"PRIMARY        : {PRIMARY} | LOWER = RUN-like"
)

print(
    f"SECONDARY      : {SECONDARY} | LOWER = RUN-like"
)


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 180)
print("A) GLOBAL FROZEN FEATURE AUDIT")
print("=" * 180)

for f in [PRIMARY, SECONDARY]:

    diff, nr, nd = feature_diff(records, f)

    print(
        f"{f:34} "
        f"N={nr+nd:3d} "
        f"RUN={nr:3d} "
        f"DUMP={nd:3d} "
        f"DIFF={fmt(diff):>9} "
        f"DIR_AUC={fmt(directional_auc(records,f)):>6}"
    )


# ============================================================
# B) CHRONOLOGICAL
# ============================================================

print()
print("=" * 180)
print("B) CHRONOLOGICAL ROBUSTNESS")
print("=" * 180)

ordered = sorted(
    records,
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)

N = len(ordered)

blocks = [
    ("T1", ordered[:N//3]),
    ("T2", ordered[N//3:(2*N)//3]),
    ("T3", ordered[(2*N)//3:]),

    ("Q1", ordered[:N//4]),
    ("Q2", ordered[N//4:N//2]),
    ("Q3", ordered[N//2:(3*N)//4]),
    ("Q4", ordered[(3*N)//4:]),
]


for f in [PRIMARY, SECONDARY]:

    print()
    print(f)
    print("-" * 120)

    for name, rr in blocks:

        diff, nr, nd = feature_diff(rr, f)

        print(
            f"{name:4} "
            f"| N={nr+nd:3d} "
            f"| RUN={nr:3d} "
            f"| DUMP={nd:3d} "
            f"| DIFF={fmt(diff):>9} "
            f"| AUC={fmt(directional_auc(rr,f)):>6}"
        )


# ============================================================
# C) FIRST EVENT TOKEN
# ============================================================

print()
print("=" * 180)
print("C) FIRST-EVENT/TOKEN")
print("=" * 180)

seen = set()
first = []

for r in ordered:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


for f in [PRIMARY, SECONDARY]:

    diff, nr, nd = feature_diff(first, f)

    print(
        f"{f:34} "
        f"N={nr+nd:3d} "
        f"RUN={nr:3d} "
        f"DUMP={nd:3d} "
        f"DIFF={fmt(diff):>9} "
        f"AUC={fmt(directional_auc(first,f)):>6}"
    )


# ============================================================
# D) TOKEN BOOTSTRAP
# ============================================================

print()
print("=" * 180)
print("D) TOKEN-LEVEL BOOTSTRAP")
print("=" * 180)

by_token = defaultdict(list)

for r in records:
    by_token[r["token_mint"]].append(r)

tokens = list(by_token.keys())

rng = random.Random(SEED)


for f in [PRIMARY, SECONDARY]:

    boots = []

    for _ in range(BOOT_N):

        sample_tokens = [
            rng.choice(tokens)
            for _ in range(len(tokens))
        ]

        rr = []

        for tok in sample_tokens:
            rr.extend(by_token[tok])

        a = directional_auc(rr, f)

        if a is not None:
            boots.append(a)


    print()
    print(f)

    print(
        f"BOOT N={len(boots)}"
    )

    if boots:

        print(
            f"MED AUC={fmt(med(boots))}"
        )

        print(
            f"95% CI=["
            f"{fmt(percentile(boots,0.025))}, "
            f"{fmt(percentile(boots,0.975))}]"
        )

        print(
            f"P(AUC>0.50)="
            f"{100*sum(x>0.5 for x in boots)/len(boots):.1f}%"
        )

        print(
            f"P(AUC>0.55)="
            f"{100*sum(x>0.55 for x in boots)/len(boots):.1f}%"
        )


# ============================================================
# E) LEAVE ONE TOKEN OUT
# ============================================================

print()
print("=" * 180)
print("E) LEAVE-ONE-TOKEN-OUT")
print("=" * 180)

for f in [PRIMARY, SECONDARY]:

    vals = []

    for tok in tokens:

        rr = [
            r for r in records
            if r["token_mint"] != tok
        ]

        a = directional_auc(rr, f)

        if a is not None:
            vals.append(a)

    print()
    print(f)

    print(
        f"N={len(vals)} "
        f"| MED={fmt(med(vals))} "
        f"| WORST={fmt(min(vals) if vals else None)} "
        f"| BEST={fmt(max(vals) if vals else None)}"
    )


# ============================================================
# F) DENOMINATOR SENSITIVITY
# ============================================================

print()
print("=" * 180)
print("F) DENOMINATOR SENSITIVITY")
print("=" * 180)

for eps in [
    0.02,
    0.05,
    0.10,
    0.20
]:

    temp_primary = []
    temp_secondary = []

    for r in rows:

        lab = label_r60(r["dex_return_60s"])

        if lab is None:
            continue

        p = sdiv(
            r["recent_price_return"],
            r["recent_net_sol"],
            eps
        )

        s = sdiv(
            abs(r["recent_sell_sol"])
            if valid(r["recent_sell_sol"])
            else None,
            abs(r["recent_buy_sol"])
            if valid(r["recent_buy_sol"])
            else None,
            eps
        )

        if valid(p):
            temp_primary.append({
                "label": lab,
                "x": p
            })

        if valid(s):
            temp_secondary.append({
                "label": lab,
                "x": s
            })


    def temp_auc(rr):
        y = [r["label"] for r in rr]
        score = [-r["x"] for r in rr]
        return auc(y, score)


    print(
        f"EPS={eps:.2f} "
        f"| PRIMARY N={len(temp_primary):3d} "
        f"AUC={fmt(temp_auc(temp_primary))} "
        f"| SECONDARY N={len(temp_secondary):3d} "
        f"AUC={fmt(temp_auc(temp_secondary))}"
    )


# ============================================================
# G) REDUNDANCY WITH CAP
# ============================================================

print()
print("=" * 180)
print("G) REDUNDANCY WITH CAP FAMILY")
print("=" * 180)


def corr(a, b):

    pairs = [
        (x,y)
        for x,y in zip(a,b)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xs = [x for x,_ in pairs]
    ys = [y for _,y in pairs]

    mx = avg(xs)
    my = avg(ys)

    num = sum(
        (x-mx)*(y-my)
        for x,y in pairs
    )

    dx = math.sqrt(
        sum((x-mx)**2 for x in xs)
    )

    dy = math.sqrt(
        sum((y-my)**2 for y in ys)
    )

    if dx == 0 or dy == 0:
        return None

    return num/(dx*dy)


pairs = [
    (
        SECONDARY,
        "price_per_net"
    ),
    (
        SECONDARY,
        "net_eff"
    ),
    (
        SECONDARY,
        "early_div"
    ),
]


for a,b in pairs:

    c = corr(
        [r[a] for r in records],
        [r[b] for r in records]
    )

    print(
        f"{a:34} vs {b:20} "
        f"CORR={fmt(c)}"
    )


# ============================================================
# H) SIMPLE INCREMENTAL RANK AUDIT
#
# Is SECONDARY useful after PRIMARY/CAP direction?
# ============================================================

print()
print("=" * 180)
print("H) INCREMENTAL RANK AUDIT — SECONDARY AFTER PRIMARY")
print("=" * 180)

usable = [
    r for r in records
    if (
        valid(r[PRIMARY])
        and valid(r[SECONDARY])
    )
]


primary_auc = directional_auc(
    usable,
    PRIMARY
)


# Equal-weight rank-style score.
# Not optimized.
combo_rows = []

for r in usable:

    combo_rows.append({
        "label": r["label"],
        "combo": (
            -r[PRIMARY]
            -r[SECONDARY]
        )
    })


combo_auc = auc(
    [r["label"] for r in combo_rows],
    [r["combo"] for r in combo_rows]
)


print(
    f"PRIMARY AUC = {fmt(primary_auc)}"
)

print(
    f"PRIMARY + SECONDARY EQUAL-WEIGHT AUC = {fmt(combo_auc)}"
)

if (
    primary_auc is not None
    and combo_auc is not None
):

    print(
        f"ΔAUC = {fmt(combo_auc-primary_auc)}"
    )


# ============================================================
# I) SCORECARD
# ============================================================

print()
print("=" * 180)
print("I) DECISION SUPPORT")
print("=" * 180)

print(
    "Frozen primary: recent_marginal_response"
)

print(
    "Frozen secondary: recent_sell_buy_sol_ratio"
)

print()


primary_global = directional_auc(
    records,
    PRIMARY
)

secondary_global = directional_auc(
    records,
    SECONDARY
)


if (
    primary_global is not None
    and primary_global >= 0.56
):

    print(
        "🟡/🟢 PRIMARY retains non-trivial directional information."
    )

else:

    print(
        "🔴 PRIMARY does not retain enough robust information."
    )


if (
    combo_auc is not None
    and primary_auc is not None
    and combo_auc > primary_auc
):

    print(
        "🟢 SECONDARY adds incremental rank information over PRIMARY."
    )

else:

    print(
        "🟡 SECONDARY does not clearly add incremental rank information."
    )


print()
print("IMPORTANT:")
print("• No model fitting.")
print("• No threshold search.")
print("• Feature definitions frozen from T55.")
print("• LOWER direction frozen for both features.")
print("• Bootstrap resamples whole tokens.")
print("• Leave-one-token-out included.")
print("• Denominator sensitivity is QA only.")
print("• Equal-weight combo is diagnostic only.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T56 writes nothing to DB.")
print("• Research robustness audit only.")

db.close()
