import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRIMARY = "price_recent_minus_early"
FROZEN_DIRECTION = "HIGHER"

BOOT_N = 5000
SEED = 61


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

    if len(pairs) < 4:
        return None

    xv = [x for x,_ in pairs]
    yv = [y for _,y in pairs]

    mx = avg(xv)
    my = avg(yv)

    num = sum(
        (x-mx)*(y-my)
        for x,y in pairs
    )

    dx = math.sqrt(
        sum((x-mx)**2 for x in xv)
    )

    dy = math.sqrt(
        sum((y-my)**2 for y in yv)
    )

    if dx == 0 or dy == 0:
        return None

    return num/(dx*dy)


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

    return wins/total


def directional_auc(rows):

    usable = [
        r for r in rows
        if valid(r[PRIMARY])
    ]

    return auc(
        [r["label"] for r in usable],
        [r[PRIMARY] for r in usable]
    )


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

    return med(run)-med(dump), len(run), len(dump)


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


rows = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,

    e.dex_return_5s,
    e.dex_return_10s,
    e.dex_return_20s,
    e.dex_return_30s,
    e.dex_return_60s,

    s.event_timestamp AS seq_timestamp,

    s.early_price_return,
    s.mid_price_return,
    s.recent_price_return

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.dex_return_60s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


records = []


for r in rows:

    lab = label_r60(
        r["dex_return_60s"]
    )

    if lab is None:
        continue

    primary = None

    if (
        valid(r["recent_price_return"])
        and valid(r["early_price_return"])
    ):
        primary = (
            r["recent_price_return"]
            - r["early_price_return"]
        )


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "seq_timestamp": r["seq_timestamp"],
        "token_mint": r["token_mint"],
        "label": lab,

        PRIMARY: primary,

        "early_price_return":
            r["early_price_return"],

        "mid_price_return":
            r["mid_price_return"],

        "recent_price_return":
            r["recent_price_return"],

        "r5":
            r["dex_return_5s"],

        "r10":
            r["dex_return_10s"],

        "r20":
            r["dex_return_20s"],

        "r30":
            r["dex_return_30s"],

        "r60":
            r["dex_return_60s"],
    })


ordered = sorted(
    records,
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)


# ============================================================
# HEADER
# ============================================================

print("=" * 185)
print("MEMECOIN LAB — T61 FROZEN PRICE-PERSISTENCE ROBUSTNESS + TEMPORAL LEAKAGE AUDIT")
print("=" * 185)

print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in records))}"
)

print(
    f"PRIMARY        : {PRIMARY}"
)

print(
    "DEFINITION     : recent_price_return - early_price_return"
)

print(
    "FROZEN DIR     : HIGHER => RUN-like"
)

print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 185)
print("A) GLOBAL FROZEN AUDIT")
print("=" * 185)

d, nr, nd = diff(records)
a = directional_auc(records)

print(
    f"N={nr+nd} | RUN={nr} | DUMP={nd}"
)

print(
    f"RUN-DUMP MEDIAN DIFF = {fmt(d)}"
)

print(
    f"DIRECTIONAL AUC       = {fmt(a)}"
)


# ============================================================
# B) CHRONOLOGICAL THIRDS / QUARTILES
# ============================================================

print()
print("=" * 185)
print("B) CHRONOLOGICAL ROBUSTNESS")
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

    dd, nrun, ndump = diff(rr)

    print(
        f"{name:4} "
        f"| N={nrun+ndump:3d} "
        f"| RUN={nrun:3d} "
        f"| DUMP={ndump:3d} "
        f"| DIFF={fmt(dd):>9} "
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

for r in ordered:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


dd, nr, nd = diff(first)

print(
    f"N={nr+nd} | RUN={nr} | DUMP={nd} "
    f"| DIFF={fmt(dd)} "
    f"| AUC={fmt(directional_auc(first))}"
)


# ============================================================
# D) UNIQUE-TOKEN CHRONOLOGICAL BLOCKS
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
        r for r in records
        if r["token_mint"] in toks
    ]

    dd, nr, nd = diff(rr)

    print(
        f"TOK_Q{i+1} "
        f"| N={nr+nd:3d} "
        f"| TOK={len(toks):2d} "
        f"| DIFF={fmt(dd):>9} "
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

for r in records:
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

    aa = directional_auc(rr)

    if aa is not None:
        boots.append(aa)


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
        r for r in records
        if r["token_mint"] != tok
    ]

    aa = directional_auc(rr)

    if aa is not None:
        loo.append(
            (
                aa,
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
print("5 REMOVALS THAT HURT MOST")

for aa, tok, nrows in loo[:5]:

    print(
        f"{tok[:25]:25} "
        f"| N={nrows:2d} "
        f"| REMAINING AUC={aa:.3f}"
    )

print()
print("5 REMOVALS THAT HELP MOST")

for aa, tok, nrows in loo[-5:]:

    print(
        f"{tok[:25]:25} "
        f"| N={nrows:2d} "
        f"| REMAINING AUC={aa:.3f}"
    )


# ============================================================
# G) OUTLIER / WINSORIZATION SENSITIVITY
# ============================================================

print()
print("=" * 185)
print("G) OUTLIER SENSITIVITY")
print("=" * 185)

pv = [
    r[PRIMARY]
    for r in records
    if valid(r[PRIMARY])
]


for tail in [
    0,
    0.01,
    0.025,
    0.05
]:

    if tail == 0:
        rr = [
            r for r in records
            if valid(r[PRIMARY])
        ]

    else:

        lo = percentile(
            pv,
            tail
        )

        hi = percentile(
            pv,
            1-tail
        )

        rr = [
            r for r in records
            if (
                valid(r[PRIMARY])
                and r[PRIMARY] >= lo
                and r[PRIMARY] <= hi
            )
        ]

    dd, nr, nd = diff(rr)

    print(
        f"TRIM={100*tail:4.1f}% EACH TAIL "
        f"| N={nr+nd:3d} "
        f"| DIFF={fmt(dd):>9} "
        f"| AUC={fmt(directional_auc(rr))}"
    )


# ============================================================
# H) EVENT / SEQUENCE TIMESTAMP CONSISTENCY
# ============================================================

print()
print("=" * 185)
print("H) TEMPORAL AUDIT — EVENT vs SEQUENCE TIMESTAMP")
print("=" * 185)

deltas = [
    r["seq_timestamp"] - r["timestamp"]
    for r in records
    if (
        valid(r["seq_timestamp"])
        and valid(r["timestamp"])
    )
]


if deltas:

    print(
        f"N={len(deltas)}"
    )

    print(
        f"MED Δ={med(deltas):+.6f}s"
    )

    print(
        f"MIN Δ={min(deltas):+.6f}s"
    )

    print(
        f"MAX Δ={max(deltas):+.6f}s"
    )

    print(
        f"SEQ TIMESTAMP AFTER EVENT = "
        f"{sum(x>1e-6 for x in deltas)}/{len(deltas)}"
    )

    print(
        f"EXACT/NEAR MATCH ±1ms = "
        f"{sum(abs(x)<=0.001 for x in deltas)}/{len(deltas)}"
    )

else:

    print(
        "No comparable timestamps."
    )


# ============================================================
# I) FUTURE-RETURN CORRELATION PROFILE
#
# Diagnostic only:
# correlation should not be interpreted as proof of leakage.
# We inspect whether the feature suspiciously behaves like an
# already-realized future return.
# ============================================================

print()
print("=" * 185)
print("I) TEMPORAL AUDIT — FEATURE vs FUTURE RETURN HORIZONS")
print("=" * 185)

for key, label in [
    ("r5",  "DEX +5s"),
    ("r10", "DEX +10s"),
    ("r20", "DEX +20s"),
    ("r30", "DEX +30s"),
    ("r60", "DEX +60s"),
]:

    c = pearson(
        [
            r[PRIMARY]
            for r in records
        ],
        [
            r[key]
            for r in records
        ]
    )

    print(
        f"{label:10} CORR={fmt(c)}"
    )


# ============================================================
# J) NEAR-IDENTITY / POSSIBLE FUTURE CONTAMINATION
#
# If recent_price_return were literally copied from one of
# the future-return fields, values could match almost exactly.
# ============================================================

print()
print("=" * 185)
print("J) TEMPORAL AUDIT — NEAR-IDENTITY CHECK")
print("=" * 185)


for source in [
    "recent_price_return",
    PRIMARY,
]:

    print()
    print(source)
    print("-" * 100)

    for key, label in [
        ("r5",  "DEX +5s"),
        ("r10", "DEX +10s"),
        ("r20", "DEX +20s"),
        ("r30", "DEX +30s"),
        ("r60", "DEX +60s"),
    ]:

        pairs = [
            (
                r[source],
                r[key]
            )
            for r in records
            if (
                valid(r[source])
                and valid(r[key])
            )
        ]

        if not pairs:
            continue

        exact = sum(
            abs(a-b) <= 1e-9
            for a,b in pairs
        )

        near001 = sum(
            abs(a-b) <= 0.01
            for a,b in pairs
        )

        near01 = sum(
            abs(a-b) <= 0.10
            for a,b in pairs
        )

        print(
            f"{label:10} "
            f"| N={len(pairs):3d} "
            f"| EXACT={100*exact/len(pairs):5.1f}% "
            f"| ±0.01={100*near001/len(pairs):5.1f}% "
            f"| ±0.10={100*near01/len(pairs):5.1f}%"
        )


# ============================================================
# K) STRICT PRE-EVENT SWAP PROXY
#
# Reconstructs simple returns using ONLY swaps with
# timestamp < event timestamp.
#
# This does not need to exactly equal V340 segmentation.
# It checks whether the price-persistence concept exists
# using indisputably pre-event information.
# ============================================================

print()
print("=" * 185)
print("K) STRICT PRE-EVENT SWAP PRICE PROXY")
print("=" * 185)


def pre_price(token, ts, seconds_back):

    r = db.execute("""
    SELECT clean_price
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp <= ?
        AND timestamp >= ?
        AND clean_price IS NOT NULL
        AND clean_price > 0
        AND (
            price_valid IS NULL
            OR price_valid=1
        )
    ORDER BY ABS(timestamp-?)
    LIMIT 1
    """, (
        token,
        ts,
        ts-seconds_back,
        ts-seconds_back
    )).fetchone()

    return (
        r["clean_price"]
        if r
        else None
    )


proxy_rows = []


for r in records:

    token = r["token_mint"]
    ts = r["timestamp"]

    p_now = pre_price(
        token,
        ts,
        2
    )

    p_early = pre_price(
        token,
        ts,
        30
    )

    p_recent = pre_price(
        token,
        ts,
        10
    )

    proxy = None

    if (
        valid(p_now)
        and valid(p_early)
        and valid(p_recent)
        and p_early > 0
        and p_recent > 0
    ):

        early_ret = (
            p_recent/p_early - 1
        ) * 100

        recent_ret = (
            p_now/p_recent - 1
        ) * 100

        proxy = (
            recent_ret
            - early_ret
        )


    if valid(proxy):

        proxy_rows.append({
            "label":
                r["label"],

            "primary":
                r[PRIMARY],

            "proxy":
                proxy,
        })


if proxy_rows:

    proxy_auc = auc(
        [
            r["label"]
            for r in proxy_rows
        ],
        [
            r["proxy"]
            for r in proxy_rows
        ]
    )

    proxy_corr = pearson(
        [
            r["primary"]
            for r in proxy_rows
        ],
        [
            r["proxy"]
            for r in proxy_rows
        ]
    )

    print(
        f"N={len(proxy_rows)}"
    )

    print(
        f"STRICT PRE-EVENT PROXY AUC = "
        f"{fmt(proxy_auc)}"
    )

    print(
        f"CORR(V340 PRIMARY, STRICT PROXY) = "
        f"{fmt(proxy_corr)}"
    )

else:

    print(
        "Insufficient pre-event swap coverage."
    )


# ============================================================
# L) FINAL SCORECARD
# ============================================================

print()
print("=" * 185)
print("L) ROBUSTNESS SCORECARD")
print("=" * 185)


audits = []


for name, rr in blocks:

    dd, nr, nd = diff(rr)
    aa = directional_auc(rr)

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


good_direction = sum(
    dd is not None and dd > 0
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
    f"USABLE AUDITS      = {len(audits)}"
)

print(
    f"HIGHER FOR RUN     = "
    f"{good_direction}/{len(audits)}"
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
    f"BOOT P(AUC>0.50)   = "
    f"{100*sum(x>0.50 for x in boots)/len(boots):.1f}%"
)


# ============================================================
# M) DECISION
# ============================================================

print()
print("=" * 185)
print("M) DECISION SUPPORT")
print("=" * 185)


timestamp_suspicious = (
    bool(deltas)
    and sum(x > 1e-6 for x in deltas) > 0
)


identity_suspicious = False

for r in records:

    if not valid(
        r["recent_price_return"]
    ):
        continue

    for key in [
        "r5",
        "r10",
        "r20",
        "r30",
        "r60"
    ]:

        if (
            valid(r[key])
            and abs(
                r["recent_price_return"]
                - r[key]
            ) <= 1e-9
        ):

            identity_suspicious = True
            break


robust_enough = (
    a is not None
    and a >= 0.57
    and len(audits) >= 6
    and good_direction/len(audits) >= 0.75
    and auc55/len(audits) >= 0.60
    and (
        sum(x>0.50 for x in boots)
        / len(boots)
    ) >= 0.90
)


if (
    robust_enough
    and not timestamp_suspicious
    and not identity_suspicious
):

    print(
        "🟢 PRICE-PERSISTENCE SURVIVES THE ROBUSTNESS GATE."
    )

    print(
        "No obvious temporal leakage signature detected "
        "by the database-level audit."
    )

    print(
        "Next = T62 incremental audit against frozen CAP-v2."
    )

elif robust_enough:

    print(
        "🟠 STATISTICAL EDGE SURVIVES, "
        "BUT TEMPORAL AUDIT RAISED A WARNING."
    )

    print(
        "Do NOT promote until sequence-window construction "
        "is inspected directly."
    )

else:

    print(
        "🔴 PRICE-PERSISTENCE DOES NOT SURVIVE "
        "THE FULL ROBUSTNESS GATE."
    )

    print(
        "Do not integrate it with CAP-v2."
    )


print()
print("IMPORTANT:")
print("• Primary definition frozen from T60.")
print("• Direction HIGHER = RUN-like frozen before T61.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Bootstrap resamples whole tokens.")
print("• Leave-one-token-out included.")
print("• Strict pre-event proxy only uses swaps timestamp <= event timestamp.")
print("• Future-return correlations are diagnostics, not proof of leakage.")
print("• Near-identity checks look for obvious future-field contamination.")
print("• T59 remains untouched.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T61 writes nothing to DB.")
print("• Research robustness/leakage audit only.")

db.close()
