#!/usr/bin/env python3

import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

PRIMARY = "new_wallets10"
FROZEN_DIRECTION = "HIGHER"
ACTIVATION_THRESHOLD = 3.0

BOOT_N = 5000
SEED = 91


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


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def percentile(xs, q):
    xs = sorted(x for x in xs if valid(x))

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


def pearson(xs, ys):

    pairs = [
        (x, y)
        for x, y in zip(xs, ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xx = [x for x,_ in pairs]
    yy = [y for _,y in pairs]

    mx = avg(xx)
    my = avg(yy)

    dx = math.sqrt(
        sum((x-mx)**2 for x in xx)
    )

    dy = math.sqrt(
        sum((y-my)**2 for y in yy)
    )

    if dx == 0 or dy == 0:
        return None

    return sum(
        (x-mx)*(y-my)
        for x,y in pairs
    ) / (dx*dy)


def auc(rows):

    pos = [
        r[PRIMARY]
        for r in rows
        if r["activation"] == 1
        and valid(r[PRIMARY])
    ]

    neg = [
        r[PRIMARY]
        for r in rows
        if r["activation"] == 0
        and valid(r[PRIMARY])
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


def diff(rows):

    pos = [
        r[PRIMARY]
        for r in rows
        if r["activation"] == 1
        and valid(r[PRIMARY])
    ]

    neg = [
        r[PRIMARY]
        for r in rows
        if r["activation"] == 0
        and valid(r[PRIMARY])
    ]

    if not pos or not neg:
        return None, len(pos), len(neg)

    return (
        med(pos)-med(neg),
        len(pos),
        len(neg)
    )


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


boundary = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()[0]

boundary = int(boundary)


rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    new_wallets10,
    new_wallets30,

    buyers10,
    buyers30,
    buyers60,

    wallets30,
    wallets60,

    swaps10,
    swaps30,
    swaps60,

    dex_return_30s,
    dex_done_30s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_30s IS NOT NULL
    AND dex_done_30s = 1

ORDER BY
    timestamp,
    id
""").fetchall()


records = []


for r in rows:

    activation = (
        1
        if abs(r["dex_return_30s"]) >= ACTIVATION_THRESHOLD
        else 0
    )

    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],

        "historical":
            r["id"] <= boundary,

        "activation":
            activation,

        "new_wallets10":
            r["new_wallets10"],

        "new_wallets30":
            r["new_wallets30"],

        "buyers10":
            r["buyers10"],

        "buyers30":
            r["buyers30"],

        "buyers60":
            r["buyers60"],

        "wallets30":
            r["wallets30"],

        "wallets60":
            r["wallets60"],

        "swaps10":
            r["swaps10"],

        "swaps30":
            r["swaps30"],

        "swaps60":
            r["swaps60"],
    })


usable = [
    r
    for r in records
    if valid(r[PRIMARY])
]


hist = [
    r for r in usable
    if r["historical"]
]

pros = [
    r for r in usable
    if not r["historical"]
]


# ============================================================
# HEADER
# ============================================================

print("=" * 180)
print(
    "MEMECOIN LAB — T91 FROZEN NEW_WALLETS10 ACTIVATION ROBUSTNESS AUDIT"
)
print("=" * 180)

print(f"PRIMARY FEATURE       : {PRIMARY}")
print(f"FROZEN DIRECTION      : {FROZEN_DIRECTION} => MORE ACTIVATION")
print(f"ACTIVATION TARGET     : |R30| >= {ACTIVATION_THRESHOLD:.1f}%")
print()
print(f"ALL USABLE EVENTS     : {len(usable)}")
print(
    f"UNIQUE TOKENS         : "
    f"{len(set(r['token_mint'] for r in usable))}"
)
print()
print("MODEL FITTING         : NONE")
print("THRESHOLD SEARCH      : NONE")
print("DB WRITES             : NONE")
print("T59/T78/T82/T86       : UNTOUCHED")


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 180)
print("A) GLOBAL ROBUSTNESS")
print("=" * 180)

d, pos, neg = diff(usable)

print(
    f"N={pos+neg} "
    f"| ACTIVATED={pos} "
    f"| NON-ACTIVATED={neg}"
)

print(
    f"ACT MED={fmt(med([
        r[PRIMARY]
        for r in usable
        if r['activation']==1
    ]))}"
)

print(
    f"NON MED={fmt(med([
        r[PRIMARY]
        for r in usable
        if r['activation']==0
    ]))}"
)

print(
    f"DIFF={fmt(d)}"
)

print(
    f"AUC={fmt(auc(usable))}"
)


# ============================================================
# B) HISTORICAL / PROSPECTIVE
# ============================================================

print()
print("=" * 180)
print("B) REGIME ROBUSTNESS")
print("=" * 180)

for name, rr in [
    ("HISTORICAL", hist),
    ("PROSPECTIVE", pros),
]:

    d, pos, neg = diff(rr)

    print(
        f"{name:12} "
        f"| N={pos+neg:4d} "
        f"| YES={pos:4d} "
        f"| NO={neg:4d} "
        f"| DIFF={fmt(d):>7} "
        f"| AUC={fmt(auc(rr))}"
    )


# ============================================================
# C) CHRONO BLOCKS
# ============================================================

print()
print("=" * 180)
print("C) CHRONOLOGICAL BLOCK ROBUSTNESS")
print("=" * 180)

N = len(usable)

blocks = [
    ("T1", usable[:N//3]),
    ("T2", usable[N//3:(2*N)//3]),
    ("T3", usable[(2*N)//3:]),

    ("Q1", usable[:N//4]),
    ("Q2", usable[N//4:N//2]),
    ("Q3", usable[N//2:(3*N)//4]),
    ("Q4", usable[(3*N)//4:]),
]


for name, rr in blocks:

    d, pos, neg = diff(rr)

    print(
        f"{name:4} "
        f"| N={pos+neg:4d} "
        f"| YES={pos:3d} "
        f"| NO={neg:3d} "
        f"| DIFF={fmt(d):>7} "
        f"| AUC={fmt(auc(rr))}"
    )


# ============================================================
# D) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 180)
print("D) FIRST-EVENT/TOKEN")
print("=" * 180)

seen = set()
first = []

for r in usable:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


d, pos, neg = diff(first)

print(
    f"N={len(first)} "
    f"| YES={pos} "
    f"| NO={neg} "
    f"| DIFF={fmt(d)} "
    f"| AUC={fmt(auc(first))}"
)


# ============================================================
# E) FIRST TOKEN BY REGIME
# ============================================================

print()
print("=" * 180)
print("E) FIRST-EVENT/TOKEN BY REGIME")
print("=" * 180)


for name, base in [
    ("HIST", hist),
    ("PROS", pros),
]:

    seen = set()
    rr = []

    for r in base:

        if r["token_mint"] in seen:
            continue

        seen.add(r["token_mint"])
        rr.append(r)

    d, pos, neg = diff(rr)

    print(
        f"{name:5} "
        f"| TOK={len(rr):3d} "
        f"| YES={pos:3d} "
        f"| NO={neg:3d} "
        f"| DIFF={fmt(d):>7} "
        f"| AUC={fmt(auc(rr))}"
    )


# ============================================================
# F) TOKEN-LEVEL BOOTSTRAP
# ============================================================

print()
print("=" * 180)
print("F) TOKEN-LEVEL BOOTSTRAP")
print("=" * 180)

by_token = defaultdict(list)

for r in usable:
    by_token[
        r["token_mint"]
    ].append(r)


tokens = list(
    by_token.keys()
)

rng = random.Random(SEED)

boots = []


for _ in range(BOOT_N):

    sampled = [
        rng.choice(tokens)
        for _ in range(len(tokens))
    ]

    rr = []

    for tok in sampled:
        rr.extend(
            by_token[tok]
        )

    a = auc(rr)

    if a is not None:
        boots.append(a)


print(
    f"BOOT N={len(boots)}"
)

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
    f"{100*sum(x>0.50 for x in boots)/len(boots):.1f}%"
)

print(
    f"P(AUC>0.55)="
    f"{100*sum(x>0.55 for x in boots)/len(boots):.1f}%"
)

print(
    f"P(AUC>0.60)="
    f"{100*sum(x>0.60 for x in boots)/len(boots):.1f}%"
)


# ============================================================
# G) LEAVE-ONE-TOKEN-OUT
# ============================================================

print()
print("=" * 180)
print("G) LEAVE-ONE-TOKEN-OUT")
print("=" * 180)

loo = []


for tok in tokens:

    rr = [
        r
        for r in usable
        if r["token_mint"] != tok
    ]

    a = auc(rr)

    if a is not None:

        loo.append(
            (
                a,
                tok,
                len(by_token[tok])
            )
        )


loo.sort()

vals = [
    x[0]
    for x in loo
]


print(
    f"TOKENS={len(vals)} "
    f"| MED={fmt(med(vals))} "
    f"| WORST={fmt(min(vals))} "
    f"| BEST={fmt(max(vals))}"
)


print()
print("WORST 5")

for a,tok,n in loo[:5]:

    print(
        f"{tok[:30]:30} "
        f"| N={n:3d} "
        f"| REMAINING AUC={a:.3f}"
    )


print()
print("BEST 5")

for a,tok,n in loo[-5:]:

    print(
        f"{tok[:30]:30} "
        f"| N={n:3d} "
        f"| REMAINING AUC={a:.3f}"
    )


# ============================================================
# H) REDUNDANCY
# ============================================================

print()
print("=" * 180)
print("H) REDUNDANCY")
print("=" * 180)


for other in [
    "new_wallets30",
    "buyers10",
    "buyers30",
    "buyers60",
    "wallets30",
    "wallets60",
    "swaps10",
    "swaps30",
    "swaps60",
]:

    c = pearson(
        [
            r[PRIMARY]
            for r in usable
        ],
        [
            r[other]
            for r in usable
        ]
    )

    print(
        f"CORR({PRIMARY}, {other}) "
        f"= {fmt(c)}"
    )


# ============================================================
# I) VALUE DISTRIBUTION
# ============================================================

print()
print("=" * 180)
print("I) DISCRETE VALUE DISTRIBUTION")
print("=" * 180)

values = sorted(
    set(
        r[PRIMARY]
        for r in usable
        if valid(r[PRIMARY])
    )
)


for x in values:

    rr = [
        r
        for r in usable
        if r[PRIMARY] == x
    ]

    yes = sum(
        r["activation"] == 1
        for r in rr
    )

    no = sum(
        r["activation"] == 0
        for r in rr
    )

    print(
        f"{x:6.1f} "
        f"| YES={yes:4d} "
        f"| NO={no:4d} "
        f"| N={len(rr):4d} "
        f"| ACT_RATE="
        f"{100*yes/len(rr):5.1f}%"
    )


# ============================================================
# J) COUNT REGIME SENSITIVITY
# ============================================================

print()
print("=" * 180)
print("J) NEW_WALLETS10 COUNT REGIME SENSITIVITY")
print("=" * 180)

regimes = [
    ("NW10=0", lambda r: r[PRIMARY] == 0),
    ("NW10=1", lambda r: r[PRIMARY] == 1),
    ("NW10=2", lambda r: r[PRIMARY] == 2),
    ("NW10>=3", lambda r: r[PRIMARY] >= 3),
]


for name, fn in regimes:

    rr = [
        r for r in usable
        if fn(r)
    ]

    yes = sum(
        r["activation"] == 1
        for r in rr
    )

    no = sum(
        r["activation"] == 0
        for r in rr
    )

    print(
        f"{name:8} "
        f"| N={len(rr):4d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| YES={yes:3d} "
        f"| NO={no:3d} "
        f"| ACT_RATE="
        f"{100*yes/len(rr):5.1f}%"
        if rr
        else
        f"{name:8} | N=0"
    )


# ============================================================
# K) ROBUSTNESS SCORECARD
# ============================================================

print()
print("=" * 180)
print("K) ROBUSTNESS SCORECARD")
print("=" * 180)

audits = []


for name, rr in blocks:

    d, pos, neg = diff(rr)
    a = auc(rr)

    if (
        pos >= 3
        and neg >= 3
        and a is not None
    ):
        audits.append(
            (
                name,
                d,
                a
            )
        )


for name, rr in [
    ("HIST", hist),
    ("PROS", pros),
    ("FIRST", first),
]:

    d, pos, neg = diff(rr)
    a = auc(rr)

    if (
        pos >= 3
        and neg >= 3
        and a is not None
    ):
        audits.append(
            (
                name,
                d,
                a
            )
        )


higher = sum(
    d is not None
    and d >= 0
    for _,d,_ in audits
)

auc55 = sum(
    a >= 0.55
    for _,_,a in audits
)

auc60 = sum(
    a >= 0.60
    for _,_,a in audits
)


print(
    f"USABLE AUDITS      = {len(audits)}"
)

print(
    f"HIGHER/EQUAL YES   = "
    f"{higher}/{len(audits)}"
)

print(
    f"AUC >=0.55         = "
    f"{auc55}/{len(audits)}"
)

print(
    f"AUC >=0.60         = "
    f"{auc60}/{len(audits)}"
)

print(
    f"MED AUDIT AUC      = "
    f"{fmt(med([a for _,_,a in audits]))}"
)

print(
    f"WORST AUDIT AUC    = "
    f"{fmt(min(a for _,_,a in audits))}"
)

print(
    f"BEST AUDIT AUC     = "
    f"{fmt(max(a for _,_,a in audits))}"
)


# ============================================================
# L) DECISION
# ============================================================

print()
print("=" * 180)
print("L) DECISION SUPPORT")
print("=" * 180)


global_auc = auc(
    usable
)

hist_auc = auc(
    hist
)

pros_auc = auc(
    pros
)

boot_prob = (
    sum(
        x > 0.50
        for x in boots
    )
    / len(boots)
)


print(
    f"GLOBAL AUC          = {fmt(global_auc)}"
)

print(
    f"HIST AUC            = {fmt(hist_auc)}"
)

print(
    f"PROS AUC            = {fmt(pros_auc)}"
)

print(
    f"BOOT P(AUC>0.50)    = "
    f"{100*boot_prob:.1f}%"
)

print()


robust = (
    global_auc is not None
    and global_auc >= 0.58

    and hist_auc is not None
    and hist_auc >= 0.58

    and pros_auc is not None
    and pros_auc >= 0.58

    and len(audits) >= 7

    and higher / len(audits) >= 0.75

    and auc55 / len(audits) >= 0.70

    and boot_prob >= 0.95
)


if robust:

    print(
        "🟢 FROZEN NEW_WALLETS10 SURVIVES "
        "ACTIVATION ROBUSTNESS."
    )

    print(
        "Candidate next step = staged activation-gate audit."
    )

    print(
        "Do NOT choose an operational threshold yet."
    )

else:

    print(
        "🔴 NEW_WALLETS10 DOES NOT SURVIVE "
        "THE ACTIVATION ROBUSTNESS GATE."
    )

    print(
        "Do not build an activation gate from it."
    )


print()
print("IMPORTANT:")
print("• Feature selected in T90.")
print("• Direction frozen HIGHER before T91.")
print("• Activation target frozen at |R30| >= 3%.")
print("• No threshold optimization.")
print("• No model fitting.")
print("• Bootstrap resamples whole tokens.")
print("• Historical and prospective regimes are audited separately.")
print("• T91 writes nothing to DB.")
print("• All frozen prospective experiments remain untouched.")

db.close()
