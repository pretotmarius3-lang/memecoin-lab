#!/usr/bin/env python3

import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

WINDOW = 30.0
PRIMARY = "buyer_velocity_10"
DIRECTION = "LOWER"

BOOT_N = 5000
SEED = 76


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


def label_r60(x):
    if not valid(x):
        return None
    if x >= RUNNER:
        return 1
    if x <= DUMP:
        return 0
    return None


def auc(y, score):

    pos = [
        score[i] for i in range(len(y))
        if y[i] == 1
    ]

    neg = [
        score[i] for i in range(len(y))
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


def directional_auc(rows):

    rr = [
        r for r in rows
        if valid(r[PRIMARY])
    ]

    if not rr:
        return None

    # LOWER feature => more RUN-like
    return auc(
        [r["label"] for r in rr],
        [-r[PRIMARY] for r in rr]
    )


def diff(rows):

    run = [
        r[PRIMARY]
        for r in rows
        if r["label"] == 1
        and valid(r[PRIMARY])
    ]

    dump = [
        r[PRIMARY]
        for r in rows
        if r["label"] == 0
        and valid(r[PRIMARY])
    ]

    if not run or not dump:
        return None, len(run), len(dump)

    return (
        med(run) - med(dump),
        len(run),
        len(dump)
    )


def percentile(xs, q):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    p = (len(xs)-1)*q
    lo = int(math.floor(p))
    hi = int(math.ceil(p))

    if lo == hi:
        return xs[lo]

    w = p-lo

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


def pearson(xs, ys):

    pairs = [
        (x,y)
        for x,y in zip(xs,ys)
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


# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s,
    new_wallets30

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


# ============================================================
# FEATURE RECONSTRUCTION — EXACT T75 DEFINITION
# ============================================================

records = []


for e in events:

    y = label_r60(
        e["dex_return_60s"]
    )

    if y is None:
        continue


    ts = e["timestamp"]


    buys = db.execute("""
    SELECT
        timestamp,
        wallet

    FROM swaps

    WHERE
        token_mint=?
        AND side='BUY'
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL

    ORDER BY timestamp
    """, (
        e["token_mint"],
        ts-WINDOW,
        ts
    )).fetchall()


    first_by_wallet = {}

    for r in buys:

        first_by_wallet.setdefault(
            r["wallet"],
            r["timestamp"]
        )


    arrivals = sorted(
        first_by_wallet.values()
    )


    if len(arrivals) < 2:
        continue


    n10 = sum(
        t >= ts-10
        for t in arrivals
    )

    n30 = len(arrivals)


    buyer_velocity_10 = (
        n10 / 10.0
    )


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "label":
            y,

        "buyer_velocity_10":
            buyer_velocity_10,

        "buyer_unique_30":
            n30,

        "new_wallets30":
            e["new_wallets30"],
    })


usable = sorted(
    [
        r for r in records
        if valid(r[PRIMARY])
    ],
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)


# ============================================================
# HEADER
# ============================================================

print("=" * 185)
print(
    "MEMECOIN LAB — T76 FROZEN BUYER VELOCITY 10 ROBUSTNESS AUDIT"
)
print("=" * 185)

print(
    f"LABELED EVENTS : {len(usable)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in usable))}"
)

print(
    "PRIMARY        : buyer_velocity_10"
)

print(
    "DEFINITION     : unique buyer arrivals in final 10s / 10"
)

print(
    "FROZEN DIR     : LOWER => RUN-like"
)

print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 185)
print("A) GLOBAL")
print("=" * 185)

d, nr, nd = diff(
    usable
)

print(
    f"N={nr+nd} "
    f"| RUN={nr} "
    f"| DUMP={nd}"
)

print(
    f"RUN MED={fmt(med([
        r[PRIMARY]
        for r in usable
        if r['label']==1
    ]))}"
)

print(
    f"DUMP MED={fmt(med([
        r[PRIMARY]
        for r in usable
        if r['label']==0
    ]))}"
)

print(
    f"DIFF={fmt(d)}"
)

print(
    f"DIR_AUC={fmt(directional_auc(usable))}"
)


# ============================================================
# B) CHRONOLOGICAL BLOCKS
# ============================================================

print()
print("=" * 185)
print("B) CHRONOLOGICAL ROBUSTNESS")
print("=" * 185)

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

    d, nr, nd = diff(rr)

    print(
        f"{name:4} "
        f"| N={nr+nd:3d} "
        f"| RUN={nr:3d} "
        f"| DUMP={nd:3d} "
        f"| DIFF={fmt(d):>8} "
        f"| AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# C) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 185)
print("C) FIRST-EVENT/TOKEN")
print("=" * 185)

seen = set()
first = []

for r in usable:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


d, nr, nd = diff(
    first
)

print(
    f"N={nr+nd} "
    f"| TOK={len(first)} "
    f"| RUN={nr} "
    f"| DUMP={nd} "
    f"| DIFF={fmt(d)} "
    f"| AUC={fmt(directional_auc(first))}"
)


# ============================================================
# D) UNIQUE-TOKEN QUARTILES
# ============================================================

print()
print("=" * 185)
print("D) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS")
print("=" * 185)


first_ts = {}

for r in usable:

    first_ts.setdefault(
        r["token_mint"],
        r["timestamp"]
    )


token_order = sorted(
    first_ts,
    key=lambda t: first_ts[t]
)

tn = len(token_order)

cuts = [
    0,
    tn//4,
    tn//2,
    (3*tn)//4,
    tn
]


for i in range(4):

    toks = set(
        token_order[
            cuts[i]:
            cuts[i+1]
        ]
    )

    rr = [
        r for r in usable
        if r["token_mint"] in toks
    ]

    d, nr, nd = diff(rr)

    print(
        f"TOK_Q{i+1} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(toks):3d} "
        f"| DIFF={fmt(d):>8} "
        f"| AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# E) TOKEN BOOTSTRAP
# ============================================================

print()
print("=" * 185)
print("E) TOKEN-LEVEL BOOTSTRAP")
print("=" * 185)


by_token = defaultdict(list)

for r in usable:

    by_token[
        r["token_mint"]
    ].append(r)


tokens = list(
    by_token.keys()
)


rng = random.Random(
    SEED
)

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

    a = directional_auc(
        rr
    )

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
# F) LEAVE ONE TOKEN OUT
# ============================================================

print()
print("=" * 185)
print("F) LEAVE-ONE-TOKEN-OUT")
print("=" * 185)


loo = []


for tok in tokens:

    rr = [
        r for r in usable
        if r["token_mint"] != tok
    ]

    a = directional_auc(
        rr
    )

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


# ============================================================
# G) BUYER COUNT REGIMES
# ============================================================

print()
print("=" * 185)
print("G) UNIQUE-BUYER COUNT REGIMES")
print("=" * 185)


regimes = [
    (
        "BUYERS=2",
        lambda r:
            r["buyer_unique_30"] == 2
    ),

    (
        "BUYERS=3",
        lambda r:
            r["buyer_unique_30"] == 3
    ),

    (
        "BUYERS>=4",
        lambda r:
            r["buyer_unique_30"] >= 4
    ),
]


for name, fn in regimes:

    rr = [
        r for r in usable
        if fn(r)
    ]

    d, nr, nd = diff(
        rr
    )

    print(
        f"{name:10} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| DIFF={fmt(d):>8} "
        f"| AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# H) REDUNDANCY
# ============================================================

print()
print("=" * 185)
print("H) REDUNDANCY")
print("=" * 185)

print(
    f"CORR(primary, buyer_unique_30) = "
    f"{fmt(pearson(
        [r[PRIMARY] for r in usable],
        [r['buyer_unique_30'] for r in usable]
    ))}"
)

print(
    f"CORR(primary, new_wallets30)   = "
    f"{fmt(pearson(
        [r[PRIMARY] for r in usable],
        [r['new_wallets30'] for r in usable]
    ))}"
)


# ============================================================
# I) DISCRETENESS
# ============================================================

print()
print("=" * 185)
print("I) VALUE DISTRIBUTION / DISCRETENESS")
print("=" * 185)


dist = defaultdict(
    lambda: [0,0]
)


for r in usable:

    x = r[PRIMARY]

    if r["label"] == 1:
        dist[x][0] += 1
    else:
        dist[x][1] += 1


for x in sorted(dist):

    run_n, dump_n = dist[x]

    print(
        f"{x:8.3f} "
        f"| RUN={run_n:3d} "
        f"| DUMP={dump_n:3d} "
        f"| N={run_n+dump_n:3d}"
    )


# ============================================================
# J) SCORECARD
# ============================================================

print()
print("=" * 185)
print("J) ROBUSTNESS SCORECARD")
print("=" * 185)


audits = []


for name, rr in blocks:

    d, nr, nd = diff(rr)
    a = directional_auc(rr)

    if (
        nr >= 3
        and nd >= 3
        and a is not None
    ):
        audits.append(
            (
                name,
                d,
                a
            )
        )


fd, fr, fdu = diff(
    first
)

fa = directional_auc(
    first
)


if (
    fr >= 3
    and fdu >= 3
    and fa is not None
):

    audits.append(
        (
            "FIRST",
            fd,
            fa
        )
    )


lower_for_run = sum(
    d is not None
    and d <= 0
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
    f"USABLE AUDITS    = {len(audits)}"
)

print(
    f"LOWER/EQUAL RUN  = "
    f"{lower_for_run}/{len(audits)}"
)

print(
    f"AUC >=0.55       = "
    f"{auc55}/{len(audits)}"
)

print(
    f"AUC >=0.60       = "
    f"{auc60}/{len(audits)}"
)

print(
    f"MED AUDIT AUC    = "
    f"{fmt(med([a for _,_,a in audits]))}"
)

print(
    f"WORST AUDIT AUC  = "
    f"{fmt(min(a for _,_,a in audits))}"
)

print(
    f"BEST AUDIT AUC   = "
    f"{fmt(max(a for _,_,a in audits))}"
)


# ============================================================
# K) DECISION
# ============================================================

print()
print("=" * 185)
print("K) DECISION SUPPORT")
print("=" * 185)


global_auc = directional_auc(
    usable
)

boot_prob = (
    sum(
        x > 0.50
        for x in boots
    )
    / len(boots)
)


robust = (
    global_auc is not None
    and global_auc >= 0.56

    and len(audits) >= 6

    and (
        lower_for_run
        / len(audits)
    ) >= 0.75

    and (
        auc55
        / len(audits)
    ) >= 0.60

    and boot_prob >= 0.90
)


print(
    f"GLOBAL DIR-AUC   = {fmt(global_auc)}"
)

print(
    f"BOOT P(AUC>0.50) = "
    f"{100*boot_prob:.1f}%"
)

print()


if robust:

    print(
        "🟢 FROZEN BUYER VELOCITY 10 "
        "SURVIVES ROBUSTNESS."
    )

    print(
        "Next = T77 incremental audit against CAP-v2."
    )

    print(
        "Do NOT optimize thresholds."
    )

else:

    print(
        "🔴 BUYER VELOCITY 10 DOES NOT "
        "SURVIVE THE ROBUSTNESS GATE."
    )

    print(
        "Do not add it to CAP-v2."
    )


print()
print("IMPORTANT:")
print("• Definition frozen from T75.")
print("• LOWER = RUN-like frozen before T76.")
print("• Final 10-second window is fixed.")
print("• Unique buyers are deduplicated by first appearance.")
print("• No future swaps.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Bootstrap resamples whole tokens.")
print("• T59 remains frozen and untouched.")
print("• T76 writes nothing to DB.")

db.close()
