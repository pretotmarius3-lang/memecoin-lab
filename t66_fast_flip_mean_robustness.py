import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0
FAST_FLIP_SEC = 60.0

PRIMARY = "fast_flip_mean"
DIRECTION = "LOWER"

BOOT_N = 5000
SEED = 66


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


def auc(y, score):

    pos = [
        score[i]
        for i in range(len(y))
        if y[i] == 1
    ]

    neg = [
        score[i]
        for i in range(len(y))
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

    y = [
        r["label"]
        for r in rr
    ]

    # LOWER fast flip => higher RUN score
    score = [
        -r[PRIMARY]
        for r in rr
    ]

    return auc(y, score)


def diff(rows):

    run = [
        r[PRIMARY]
        for r in rows
        if (
            r["label"] == 1
            and valid(r[PRIMARY])
        )
    ]

    dump = [
        r[PRIMARY]
        for r in rows
        if (
            r["label"] == 0
            and valid(r[PRIMARY])
        )
    ]

    if not run or not dump:
        return None, len(run), len(dump)

    return (
        med(run)-med(dump),
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
# DB
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
    dex_return_60s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint

FROM swaps

WHERE
    timestamp IS NOT NULL
    AND wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND side IN ('BUY','SELL')

ORDER BY
    timestamp
""").fetchall()


# ============================================================
# CHRONOLOGICAL WALLET HISTORY
# ============================================================

completed = defaultdict(int)
fast_flips = defaultdict(int)

open_pos = {}

swap_idx = 0


def process_swap(s):

    wallet = s["wallet"]
    token = s["token_mint"]
    side = s["side"]
    ts = s["timestamp"]

    key = (
        wallet,
        token
    )

    if side == "BUY":

        if key not in open_pos:
            open_pos[key] = ts

    elif side == "SELL":

        if key not in open_pos:
            return

        hold = (
            ts
            - open_pos[key]
        )

        if hold >= 0:

            completed[wallet] += 1

            if hold <= FAST_FLIP_SEC:
                fast_flips[wallet] += 1

        open_pos.pop(
            key,
            None
        )


# ============================================================
# BUILD RECORDS
# ============================================================

records = []


for e in events:

    y = label_r60(
        e["dex_return_60s"]
    )

    if y is None:
        continue


    ts = e["timestamp"]


    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < ts
    ):

        process_swap(
            swaps[swap_idx]
        )

        swap_idx += 1


    buyer_rows = db.execute("""
    SELECT DISTINCT wallet

    FROM swaps

    WHERE
        token_mint=?
        AND side='BUY'
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
    """, (
        e["token_mint"],
        ts-PRE_EVENT_SEC,
        ts
    )).fetchall()


    buyers = [
        r["wallet"]
        for r in buyer_rows
    ]


    if not buyers:
        continue


    experienced = []

    for w in buyers:

        n = completed[w]

        if n < 1:
            continue

        rate = (
            fast_flips[w]
            / n
        )

        experienced.append(
            rate
        )


    fast_flip_mean = (
        avg(experienced)
        if experienced
        else None
    )


    coverage = (
        len(experienced)
        / len(buyers)
        if buyers
        else None
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

        "fast_flip_mean":
            fast_flip_mean,

        "experienced_buyers":
            len(experienced),

        "buyer_count":
            len(buyers),

        "fast_flip_coverage":
            coverage,
    })


usable = [
    r for r in records
    if valid(
        r[PRIMARY]
    )
]


ordered = sorted(
    usable,
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)


# ============================================================
# HEADER
# ============================================================

print("=" * 185)
print("MEMECOIN LAB — T66 FROZEN FAST-FLIP MEAN ROBUSTNESS AUDIT")
print("=" * 185)

print(
    f"ALL LABELED EVENTS : {len(records)}"
)

print(
    f"PRIMARY USABLE      : {len(usable)}"
)

print(
    f"UNIQUE TOKENS       : "
    f"{len(set(r['token_mint'] for r in usable))}"
)

print(
    "PRIMARY FEATURE     : fast_flip_mean"
)

print(
    "FROZEN DIRECTION    : LOWER FAST-FLIP => MORE RUN-LIKE"
)

print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 185)
print("A) GLOBAL PRIMARY FEATURE")
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
    f"RUN MED  = "
    f"{fmt(med([r[PRIMARY] for r in usable if r['label']==1]))}"
)

print(
    f"DUMP MED = "
    f"{fmt(med([r[PRIMARY] for r in usable if r['label']==0]))}"
)

print(
    f"DIFF RUN-DUMP = "
    f"{fmt(d)}"
)

print(
    f"DIRECTIONAL AUC = "
    f"{fmt(directional_auc(usable))}"
)


# ============================================================
# B) CHRONOLOGICAL BLOCKS
# ============================================================

print()
print("=" * 185)
print("B) CHRONOLOGICAL BLOCK ROBUSTNESS")
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

    dd, nrun, ndump = diff(
        rr
    )

    print(
        f"{name:4} "
        f"| N={nrun+ndump:3d} "
        f"| RUN={nrun:3d} "
        f"| DUMP={ndump:3d} "
        f"| DIFF={fmt(dd):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
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


for r in ordered:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


dd, nr, nd = diff(
    first
)

print(
    f"FIRST "
    f"N={nr+nd:3d} "
    f"TOK={len(first):3d} "
    f"RUN={nr:3d} "
    f"DUMP={nd:3d} "
    f"DIFF={fmt(dd):>8} "
    f"DIR_AUC={fmt(directional_auc(first)):>6}"
)


# ============================================================
# D) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS
# ============================================================

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
    key=lambda t:
        first_ts[t]
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

    dd, nr, nd = diff(
        rr
    )

    print(
        f"TOK_Q{i+1} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(toks):3d} "
        f"| RUN={nr:3d} "
        f"| DUMP={nd:3d} "
        f"| DIFF={fmt(dd):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# E) TOKEN-LEVEL BOOTSTRAP
# ============================================================

print()
print("=" * 185)
print("E) TOKEN-LEVEL BOOTSTRAP DIRECTIONAL AUC")
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
        for _ in range(
            len(tokens)
        )
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
    f"MED AUC = {fmt(med(boots))}"
)

print(
    f"95% CI  = ["
    f"{fmt(percentile(boots,0.025))}, "
    f"{fmt(percentile(boots,0.975))}]"
)

print(
    f"P(AUC>0.50) = "
    f"{100*sum(x>0.50 for x in boots)/len(boots):.1f}%"
)

print(
    f"P(AUC>0.55) = "
    f"{100*sum(x>0.55 for x in boots)/len(boots):.1f}%"
)

print(
    f"P(AUC>0.60) = "
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
                len(
                    by_token[tok]
                )
            )
        )


loo.sort()

vals = [
    x[0]
    for x in loo
]


print(
    f"N TOKENS = {len(vals)}"
)

print(
    f"MED REMAINING AUC = "
    f"{fmt(med(vals))}"
)

print(
    f"WORST REMAINING AUC = "
    f"{fmt(min(vals))}"
)

print(
    f"BEST REMAINING AUC = "
    f"{fmt(max(vals))}"
)


# ============================================================
# G) EXPERIENCED BUYER COUNT REGIMES
# ============================================================

print()
print("=" * 185)
print("G) EXPERIENCED BUYER COUNT REGIMES")
print("=" * 185)


regimes = [
    (
        "EXP=1",
        lambda r:
            r["experienced_buyers"] == 1
    ),

    (
        "EXP=2",
        lambda r:
            r["experienced_buyers"] == 2
    ),

    (
        "EXP>=3",
        lambda r:
            r["experienced_buyers"] >= 3
    ),
]


for name, fn in regimes:

    rr = [
        r for r in usable
        if fn(r)
    ]

    dd, nr, nd = diff(
        rr
    )

    print(
        f"{name:8} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| RUN={nr:3d} "
        f"| DUMP={nd:3d} "
        f"| DIFF={fmt(dd):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# H) COVERAGE REDUNDANCY
# ============================================================

print()
print("=" * 185)
print("H) COVERAGE / EXPERIENCE REDUNDANCY")
print("=" * 185)


print(
    f"CORR(fast_flip_mean, fast_flip_coverage) = "
    f"{fmt(pearson(
        [r['fast_flip_mean'] for r in usable],
        [r['fast_flip_coverage'] for r in usable]
    ))}"
)

print(
    f"CORR(fast_flip_mean, experienced_buyers) = "
    f"{fmt(pearson(
        [r['fast_flip_mean'] for r in usable],
        [r['experienced_buyers'] for r in usable]
    ))}"
)

print(
    f"CORR(fast_flip_coverage, experienced_buyers) = "
    f"{fmt(pearson(
        [r['fast_flip_coverage'] for r in usable],
        [r['experienced_buyers'] for r in usable]
    ))}"
)


# ============================================================
# I) MINIMUM EXPERIENCE FILTER SENSITIVITY
#
# QA ONLY — no threshold optimization.
# ============================================================

print()
print("=" * 185)
print("I) MIN EXPERIENCED-BUYER SENSITIVITY")
print("=" * 185)


for nmin in [
    1,
    2,
    3
]:

    rr = [
        r for r in usable
        if r["experienced_buyers"] >= nmin
    ]

    dd, nr, nd = diff(
        rr
    )

    print(
        f"MIN_EXP={nmin} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| DIFF={fmt(dd):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# J) OUTLIER SENSITIVITY
# ============================================================

print()
print("=" * 185)
print("J) FAST-FLIP OUTLIER SENSITIVITY")
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


    dd, nr, nd = diff(
        rr
    )

    print(
        f"TRIM={100*tail:4.1f}% "
        f"| N={nr+nd:3d} "
        f"| DIFF={fmt(dd):>8} "
        f"| DIR_AUC={fmt(directional_auc(rr)):>6}"
    )


# ============================================================
# K) SCORECARD
# ============================================================

print()
print("=" * 185)
print("K) ROBUSTNESS SCORECARD")
print("=" * 185)


audits = []


for name, rr in blocks:

    dd, nr, nd = diff(
        rr
    )

    aa = directional_auc(
        rr
    )

    if (
        nr >= 3
        and nd >= 3
        and aa is not None
    ):
        audits.append(
            (
                name,
                dd,
                aa
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
    dd is not None
    and dd < 0
    for _,dd,_ in audits
)


auc55 = sum(
    aa >= 0.55
    for _,_,aa in audits
)


auc60 = sum(
    aa >= 0.60
    for _,_,aa in audits
)


print(
    f"USABLE AUDITS         = {len(audits)}"
)

print(
    f"LOWER FOR RUN         = "
    f"{lower_for_run}/{len(audits)}"
)

print(
    f"DIR-AUC >=0.55        = "
    f"{auc55}/{len(audits)}"
)

print(
    f"DIR-AUC >=0.60        = "
    f"{auc60}/{len(audits)}"
)

print(
    f"MEDIAN AUDIT AUC      = "
    f"{fmt(med([a for _,_,a in audits]))}"
)

print(
    f"WORST AUDIT AUC       = "
    f"{fmt(min(a for _,_,a in audits))}"
)

print(
    f"BEST AUDIT AUC        = "
    f"{fmt(max(a for _,_,a in audits))}"
)


# ============================================================
# L) DECISION
# ============================================================

print()
print("=" * 185)
print("L) DECISION SUPPORT")
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
    f"GLOBAL DIR-AUC     = {fmt(global_auc)}"
)

print(
    f"BOOT P(AUC>0.50)   = {100*boot_prob:.1f}%"
)

print()


if robust:

    print(
        "🟢 FROZEN FAST-FLIP MEAN SURVIVES ROBUSTNESS."
    )

    print(
        "Next = T67 incremental audit against frozen CAP-v2."
    )

    print(
        "Do NOT optimize thresholds."
    )

else:

    print(
        "🔴 FAST-FLIP MEAN DOES NOT SURVIVE "
        "THE FULL ROBUSTNESS GATE."
    )

    print(
        "Do not add it to CAP-v2."
    )


print()
print("IMPORTANT:")
print("• Experience threshold >=1 is frozen.")
print("• Buyer cohort is the 30s pre-event window.")
print("• Fast flip means completed prior trade held <=60s.")
print("• Wallet history is strictly chronological.")
print("• Direction LOWER = RUN-like frozen before T66.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Bootstrap resamples entire tokens.")
print("• Leave-one-token-out included.")
print("• Minimum-experience sensitivity is QA only.")
print("• T59 remains frozen and untouched.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T66 writes nothing to DB.")
print("• Research robustness audit only.")

db.close()
