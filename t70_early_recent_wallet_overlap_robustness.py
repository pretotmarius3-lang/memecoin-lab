import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

N_SWAPS = 12
PRIMARY = "early_recent_wallet_overlap"
DIRECTION = "LOWER"

BOOT_N = 5000
SEED = 70


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
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def label_r60(x):
    if not valid(x):
        return None
    if x >= RUNNER:
        return 1
    if x <= DUMP:
        return 0
    return None


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


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
                wins += 1.0
            elif a == b:
                wins += 0.5

    return wins / total


def directional_auc(rows):
    rr = [r for r in rows if valid(r[PRIMARY])]
    if not rr:
        return None

    return auc(
        [r["label"] for r in rr],
        [-r[PRIMARY] for r in rr]  # LOWER => RUN
    )


def diff(rows):
    run = [
        r[PRIMARY]
        for r in rows
        if r["label"] == 1 and valid(r[PRIMARY])
    ]

    dump = [
        r[PRIMARY]
        for r in rows
        if r["label"] == 0 and valid(r[PRIMARY])
    ]

    if not run or not dump:
        return None, len(run), len(dump)

    return med(run) - med(dump), len(run), len(dump)


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
    return xs[lo]*(1-w) + xs[hi]*w


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

    dx = math.sqrt(sum((x-mx)**2 for x in xx))
    dy = math.sqrt(sum((y-my)**2 for y in yy))

    if dx == 0 or dy == 0:
        return None

    return sum(
        (x-mx)*(y-my)
        for x,y in pairs
    ) / (dx*dy)


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s
FROM events
WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL
ORDER BY timestamp, id
""").fetchall()


records = []


for e in events:

    y = label_r60(e["dex_return_60s"])

    if y is None:
        continue

    rows = db.execute("""
    SELECT
        timestamp,
        wallet,
        side
    FROM swaps
    WHERE
        token_mint=?
        AND price_valid=1
        AND timestamp < ?
        AND wallet IS NOT NULL
        AND side IN ('BUY','SELL')
    ORDER BY timestamp DESC
    LIMIT ?
    """, (
        e["token_mint"],
        e["timestamp"],
        N_SWAPS
    )).fetchall()[::-1]

    if len(rows) < N_SWAPS:
        continue

    early = rows[0:4]
    mid = rows[4:8]
    recent = rows[8:12]

    def wallets(block):
        return set(
            r["wallet"]
            for r in block
            if r["wallet"]
        )

    ea = wallets(early)
    ma = wallets(mid)
    ra = wallets(recent)

    overlap = safe_div(
        len(ea & ra),
        len(ea | ra)
    )

    mid_recent_overlap = safe_div(
        len(ma & ra),
        len(ma | ra)
    )

    early_count = len(ea)
    mid_count = len(ma)
    recent_count = len(ra)

    union_count = len(
        ea | ma | ra
    )

    records.append({
        "id": e["id"],
        "timestamp": e["timestamp"],
        "token_mint": e["token_mint"],
        "label": y,

        PRIMARY:
            overlap,

        "mid_recent_wallet_overlap":
            mid_recent_overlap,

        "early_wallet_count":
            early_count,

        "mid_wallet_count":
            mid_count,

        "recent_wallet_count":
            recent_count,

        "union_wallet_count":
            union_count,
    })


usable = [
    r for r in records
    if valid(r[PRIMARY])
]


ordered = sorted(
    usable,
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)


print("=" * 185)
print("MEMECOIN LAB — T70 FROZEN EARLY/RECENT WALLET OVERLAP ROBUSTNESS AUDIT")
print("=" * 185)

print(f"LABELED EVENTS  : {len(records)}")
print(f"PRIMARY USABLE  : {len(usable)}")
print(
    f"UNIQUE TOKENS   : "
    f"{len(set(r['token_mint'] for r in usable))}"
)
print(
    "PRIMARY         : early_recent_wallet_overlap"
)
print(
    "DEFINITION      : |EARLY wallets ∩ RECENT wallets| / |EARLY wallets ∪ RECENT wallets|"
)
print(
    "FROZEN DIR      : LOWER => RUN-like"
)
print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


print()
print("=" * 185)
print("A) GLOBAL")
print("=" * 185)

dd, nr, nd = diff(usable)

print(
    f"N={nr+nd} | RUN={nr} | DUMP={nd}"
)

print(
    f"RUN MED  = "
    f"{fmt(med([r[PRIMARY] for r in usable if r['label']==1]))}"
)

print(
    f"DUMP MED = "
    f"{fmt(med([r[PRIMARY] for r in usable if r['label']==0]))}"
)

print(
    f"DIFF RUN-DUMP = {fmt(dd)}"
)

print(
    f"DIR_AUC = {fmt(directional_auc(usable))}"
)


print()
print("=" * 185)
print("B) CHRONOLOGICAL BLOCKS")
print("=" * 185)

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

for name, rr in blocks:

    d, nrun, ndump = diff(rr)

    print(
        f"{name:4} "
        f"| N={nrun+ndump:3d} "
        f"| RUN={nrun:3d} "
        f"| DUMP={ndump:3d} "
        f"| DIFF={fmt(d):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


print()
print("=" * 185)
print("C) FIRST-EVENT/TOKEN")
print("=" * 185)

seen = set()
first = []

for r in ordered:
    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)

d, nr, nd = diff(first)

print(
    f"N={nr+nd} "
    f"| TOK={len(first)} "
    f"| RUN={nr} "
    f"| DUMP={nd} "
    f"| DIFF={fmt(d)} "
    f"| DIR_AUC={fmt(directional_auc(first))}"
)


print()
print("=" * 185)
print("D) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS")
print("=" * 185)

first_ts = {}

for r in ordered:
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
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


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

    a = directional_auc(rr)

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

    a = directional_auc(rr)

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
print("=" * 185)
print("G) WALLET-COUNT REGIMES")
print("=" * 185)

regimes = [
    (
        "EARLY<=2",
        lambda r:
            r["early_wallet_count"] <= 2
    ),

    (
        "EARLY=3",
        lambda r:
            r["early_wallet_count"] == 3
    ),

    (
        "EARLY>=4",
        lambda r:
            r["early_wallet_count"] >= 4
    ),
]

for name, fn in regimes:

    rr = [
        r for r in usable
        if fn(r)
    ]

    d, nr, nd = diff(rr)

    print(
        f"{name:9} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| DIFF={fmt(d):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


print()
print("=" * 185)
print("H) REDUNDANCY")
print("=" * 185)

print(
    f"CORR(primary, mid_recent_overlap) = "
    f"{fmt(pearson(
        [r[PRIMARY] for r in usable],
        [r['mid_recent_wallet_overlap'] for r in usable]
    ))}"
)

print(
    f"CORR(primary, early_wallet_count) = "
    f"{fmt(pearson(
        [r[PRIMARY] for r in usable],
        [r['early_wallet_count'] for r in usable]
    ))}"
)

print(
    f"CORR(primary, recent_wallet_count) = "
    f"{fmt(pearson(
        [r[PRIMARY] for r in usable],
        [r['recent_wallet_count'] for r in usable]
    ))}"
)

print(
    f"CORR(primary, union_wallet_count) = "
    f"{fmt(pearson(
        [r[PRIMARY] for r in usable],
        [r['union_wallet_count'] for r in usable]
    ))}"
)


print()
print("=" * 185)
print("I) OUTLIER / DISCRETENESS SENSITIVITY")
print("=" * 185)

vals_primary = [
    r[PRIMARY]
    for r in usable
]

for tail in [
    0,
    0.01,
    0.025,
    0.05
]:

    if tail == 0:
        rr = usable

    else:
        lo = percentile(
            vals_primary,
            tail
        )

        hi = percentile(
            vals_primary,
            1-tail
        )

        rr = [
            r for r in usable
            if (
                r[PRIMARY] >= lo
                and r[PRIMARY] <= hi
            )
        ]

    d, nr, nd = diff(rr)

    print(
        f"TRIM={100*tail:4.1f}% "
        f"| N={nr+nd:3d} "
        f"| DIFF={fmt(d):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


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

fd, fr, fdu = diff(first)
fa = directional_auc(first)

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
    d is not None and d < 0
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
    f"LOWER FOR RUN    = "
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


print()
print("=" * 185)
print("K) DECISION SUPPORT")
print("=" * 185)

global_auc = directional_auc(usable)

boot_prob = (
    sum(x>0.50 for x in boots)
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
    f"BOOT P(AUC>0.50) = {100*boot_prob:.1f}%"
)

print()

if robust:

    print(
        "🟢 FROZEN EARLY/RECENT WALLET OVERLAP "
        "SURVIVES ROBUSTNESS."
    )

    print(
        "Next = T71 incremental audit against CAP-v2."
    )

else:

    print(
        "🔴 EARLY/RECENT WALLET OVERLAP "
        "DOES NOT SURVIVE ROBUSTNESS."
    )

    print(
        "Do not add it to CAP-v2."
    )


print()
print("IMPORTANT:")
print("• Definition frozen from T69.")
print("• LOWER = RUN-like frozen before T70.")
print("• Uses exactly last 12 valid pre-event swaps.")
print("• EARLY=0:4, MID=4:8, RECENT=8:12.")
print("• No future swaps.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Bootstrap resamples whole tokens.")
print("• Leave-one-token-out included.")
print("• T59 remains frozen and untouched.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T70 writes nothing to DB.")

db.close()
