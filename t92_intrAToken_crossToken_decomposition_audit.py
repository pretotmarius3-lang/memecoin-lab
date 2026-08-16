#!/usr/bin/env python3

import sqlite3
import math
import statistics
from collections import defaultdict, Counter

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

TARGET_THRESHOLD = 3.0
PRIMARY = "new_wallets10"

# Also inspect a few related count/activity variables so that
# new_wallets10 is not interpreted in isolation.
FEATURES = [
    "new_wallets10",
    "new_wallets30",
    "buyers10",
    "buyers30",
    "swaps10",
    "swaps30",
]

MIN_PRIOR_EVENTS = 1


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def pct(n, d):
    return "NA" if not d else f"{100*n/d:.1f}%"


def auc_from_pairs(pairs):
    """
    pairs = [(target, feature), ...]
    Returns raw AUC where HIGHER feature predicts target=1.
    """

    pairs = [
        (y, x)
        for y, x in pairs
        if y in (0, 1) and valid(x)
    ]

    pos = [x for y, x in pairs if y == 1]
    neg = [x for y, x in pairs if y == 0]

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


def directional_auc(pairs):
    raw = auc_from_pairs(pairs)

    if raw is None:
        return None, None

    if raw >= 0.5:
        return raw, "HIGHER"

    return 1.0 - raw, "LOWER"


def activation_rate(rr):
    if not rr:
        return None

    return sum(r["activation"] for r in rr) / len(rr)


def corr(xs, ys):
    pairs = [
        (x, y)
        for x, y in zip(xs, ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xx = [x for x, _ in pairs]
    yy = [y for _, y in pairs]

    mx = mean(xx)
    my = mean(yy)

    sx = math.sqrt(sum((x-mx)**2 for x in xx))
    sy = math.sqrt(sum((y-my)**2 for y in yy))

    if sx == 0 or sy == 0:
        return None

    return (
        sum((x-mx)*(y-my) for x, y in pairs)
        / (sx*sy)
    )


# ============================================================
# DATABASE
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

if boundary is None:
    raise RuntimeError("Could not determine T59 boundary.")

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

    swaps10,
    swaps30,

    dex_return_30s,
    dex_done_30s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_done_30s = 1
    AND dex_return_30s IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


records = []

for r in rows:

    activation = int(
        abs(r["dex_return_30s"]) >= TARGET_THRESHOLD
    )

    rec = {
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],
        "historical": r["id"] <= boundary,
        "activation": activation,
    }

    for f in FEATURES:
        rec[f] = r[f]

    records.append(rec)


# ============================================================
# TOKEN SEQUENCES
# ============================================================

by_token = defaultdict(list)

for r in records:
    by_token[r["token_mint"]].append(r)


for tok, rr in by_token.items():
    rr.sort(key=lambda x: (x["timestamp"], x["id"]))

    # Full-token summaries are used ONLY for descriptive
    # cross-token decomposition, not as prospective features.
    full_means = {}

    for f in FEATURES:
        vals = [x[f] for x in rr if valid(x[f])]
        full_means[f] = mean(vals)

    prior_values = defaultdict(list)

    for idx, r in enumerate(rr, start=1):

        r["event_number"] = idx
        r["events_total_token"] = len(rr)

        for f in FEATURES:

            r[f"{f}_token_mean_full"] = full_means[f]

            pv = [
                x for x in prior_values[f]
                if valid(x)
            ]

            prior_mean = mean(pv)

            r[f"{f}_prior_mean"] = prior_mean

            if valid(r[f]) and valid(prior_mean):
                r[f"{f}_innovation"] = (
                    r[f] - prior_mean
                )
            else:
                r[f"{f}_innovation"] = None

            if valid(r[f]):
                prior_values[f].append(r[f])


# ============================================================
# SPLITS
# ============================================================

hist = [r for r in records if r["historical"]]
pros = [r for r in records if not r["historical"]]

first = [r for r in records if r["event_number"] == 1]
repeat = [r for r in records if r["event_number"] >= 2]

hist_repeat = [
    r for r in repeat
    if r["historical"]
]

pros_repeat = [
    r for r in repeat
    if not r["historical"]
]


# ============================================================
# HEADER
# ============================================================

print("=" * 180)
print(
    "MEMECOIN LAB — T92 INTRA-TOKEN vs CROSS-TOKEN DECOMPOSITION AUDIT"
)
print("=" * 180)

print("MODE                  : READ-ONLY")
print("MODEL FITTING         : NONE")
print("THRESHOLD SEARCH      : NONE")
print("DB WRITES             : NONE")
print("T59/T78/T82/T86       : UNTOUCHED")
print()
print(f"T59 BOUNDARY          : {boundary}")
print(f"TARGET                : |R30| >= {TARGET_THRESHOLD:.1f}%")
print(f"PRIMARY               : {PRIMARY}")
print()
print(
    "NOTE: full-token means are DESCRIPTIVE ONLY and contain future "
    "events relative to early observations."
)
print(
    "Prospective-safe intra-token analysis uses PRIOR observations only."
)


# ============================================================
# A) TOKEN REPETITION STRUCTURE
# ============================================================

print()
print("=" * 180)
print("A) TOKEN REPETITION STRUCTURE")
print("=" * 180)

counts = [len(rr) for rr in by_token.values()]

print(f"EVENTS               : {len(records)}")
print(f"TOKENS               : {len(by_token)}")
print(f"EVENTS/TOKEN MEAN    : {fmt(mean(counts))}")
print(f"EVENTS/TOKEN MEDIAN  : {fmt(med(counts))}")

for n in [1, 2, 3, 4, 5]:

    if n < 5:
        c = sum(x == n for x in counts)
        label = f"EXACTLY {n}"
    else:
        c = sum(x >= n for x in counts)
        label = "5+"

    print(
        f"{label:12} | TOKENS={c:4d} "
        f"| SHARE={pct(c,len(counts)):>6}"
    )


# ============================================================
# B) ACTIVATION BY EVENT NUMBER
# ============================================================

print()
print("=" * 180)
print("B) ACTIVATION BY EVENT NUMBER")
print("=" * 180)

groups = [
    ("EVENT #1", lambda r: r["event_number"] == 1),
    ("EVENT #2", lambda r: r["event_number"] == 2),
    ("EVENT #3", lambda r: r["event_number"] == 3),
    ("EVENT #4", lambda r: r["event_number"] == 4),
    ("EVENT #5+", lambda r: r["event_number"] >= 5),
]

for label, fn in groups:

    print()
    print(label)

    for regime_name, base in [
        ("ALL", records),
        ("HIST", hist),
        ("PROS", pros),
    ]:

        rr = [r for r in base if fn(r)]

        yes = sum(r["activation"] for r in rr)

        print(
            f"  {regime_name:5} "
            f"| N={len(rr):4d} "
            f"| YES={yes:4d} "
            f"| RATE={pct(yes,len(rr)):>6}"
        )


# ============================================================
# C) RAW FEATURE — FIRST vs REPEAT EVENTS
# ============================================================

print()
print("=" * 180)
print("C) RAW FEATURE DISCRIMINATION — FIRST vs REPEAT EVENTS")
print("=" * 180)

for f in FEATURES:

    print()
    print(f)

    for name, rr in [
        ("FIRST", first),
        ("REPEAT", repeat),
        ("HIST_REPEAT", hist_repeat),
        ("PROS_REPEAT", pros_repeat),
    ]:

        pairs = [
            (r["activation"], r[f])
            for r in rr
            if valid(r[f])
        ]

        a, direction = directional_auc(pairs)

        print(
            f"  {name:12} "
            f"| N={len(pairs):4d} "
            f"| DIR={str(direction):6} "
            f"| AUC={fmt(a)}"
        )


# ============================================================
# D) CROSS-TOKEN DESCRIPTIVE COMPONENT
# ============================================================

print()
print("=" * 180)
print("D) CROSS-TOKEN COMPONENT — DESCRIPTIVE TOKEN MEAN")
print("=" * 180)

# One row per token.
token_rows = []

for tok, rr in by_token.items():

    token_activation_rate = activation_rate(rr)

    row = {
        "token_mint": tok,
        "n": len(rr),
        "activation_rate": token_activation_rate,
        "ever_activated": int(
            any(r["activation"] == 1 for r in rr)
        )
    }

    for f in FEATURES:
        row[f] = rr[0][f"{f}_token_mean_full"]

    token_rows.append(row)


for f in FEATURES:

    pairs = [
        (r["ever_activated"], r[f])
        for r in token_rows
        if valid(r[f])
    ]

    a, direction = directional_auc(pairs)

    print(
        f"{f:20} "
        f"| TOKENS={len(pairs):4d} "
        f"| EVER-ACT DIR={str(direction):6} "
        f"| AUC={fmt(a)}"
    )


# ============================================================
# E) PROSPECTIVE-SAFE INTRA-TOKEN INNOVATION
# ============================================================

print()
print("=" * 180)
print("E) INTRA-TOKEN INNOVATION — CURRENT VALUE MINUS PRIOR TOKEN MEAN")
print("=" * 180)

for f in FEATURES:

    print()
    print(f)

    key = f"{f}_innovation"

    for name, rr in [
        ("ALL_REPEAT", repeat),
        ("HIST_REPEAT", hist_repeat),
        ("PROS_REPEAT", pros_repeat),
    ]:

        pairs = [
            (r["activation"], r[key])
            for r in rr
            if valid(r[key])
        ]

        a, direction = directional_auc(pairs)

        pos = [
            x for y, x in pairs
            if y == 1
        ]

        neg = [
            x for y, x in pairs
            if y == 0
        ]

        print(
            f"  {name:12} "
            f"| N={len(pairs):4d} "
            f"| YES_MED={fmt(med(pos)):>7} "
            f"| NO_MED={fmt(med(neg)):>7} "
            f"| DIR={str(direction):6} "
            f"| AUC={fmt(a)}"
        )


# ============================================================
# F) PRIMARY FEATURE — PRIOR LEVEL vs INNOVATION
# ============================================================

print()
print("=" * 180)
print("F) PRIMARY FEATURE DECOMPOSITION")
print("=" * 180)

for name, rr in [
    ("ALL_REPEAT", repeat),
    ("HIST_REPEAT", hist_repeat),
    ("PROS_REPEAT", pros_repeat),
]:

    print()
    print(name)

    raw_pairs = [
        (r["activation"], r[PRIMARY])
        for r in rr
        if valid(r[PRIMARY])
    ]

    prior_pairs = [
        (
            r["activation"],
            r[f"{PRIMARY}_prior_mean"]
        )
        for r in rr
        if valid(r[f"{PRIMARY}_prior_mean"])
    ]

    innovation_pairs = [
        (
            r["activation"],
            r[f"{PRIMARY}_innovation"]
        )
        for r in rr
        if valid(r[f"{PRIMARY}_innovation"])
    ]

    for label, pairs in [
        ("RAW CURRENT", raw_pairs),
        ("PRIOR LEVEL", prior_pairs),
        ("INNOVATION", innovation_pairs),
    ]:

        a, direction = directional_auc(pairs)

        print(
            f"  {label:14} "
            f"| N={len(pairs):4d} "
            f"| DIR={str(direction):6} "
            f"| AUC={fmt(a)}"
        )


# ============================================================
# G) PRIMARY INNOVATION BY EVENT NUMBER
# ============================================================

print()
print("=" * 180)
print("G) NEW_WALLETS10 INNOVATION BY EVENT NUMBER")
print("=" * 180)

key = f"{PRIMARY}_innovation"

for label, fn in [
    ("#2", lambda r: r["event_number"] == 2),
    ("#3", lambda r: r["event_number"] == 3),
    ("#4", lambda r: r["event_number"] == 4),
    ("#5+", lambda r: r["event_number"] >= 5),
]:

    print()
    print(label)

    for regime_name, base in [
        ("ALL", records),
        ("HIST", hist),
        ("PROS", pros),
    ]:

        rr = [
            r for r in base
            if fn(r) and valid(r[key])
        ]

        pairs = [
            (r["activation"], r[key])
            for r in rr
        ]

        a, direction = directional_auc(pairs)

        print(
            f"  {regime_name:5} "
            f"| N={len(rr):4d} "
            f"| DIR={str(direction):6} "
            f"| AUC={fmt(a)}"
        )


# ============================================================
# H) CHANGE SIGN — NO THRESHOLD OPTIMIZATION
# ============================================================

print()
print("=" * 180)
print("H) PRIMARY INNOVATION SIGN — DESCRIPTIVE ONLY")
print("=" * 180)

for name, base in [
    ("ALL_REPEAT", repeat),
    ("HIST_REPEAT", hist_repeat),
    ("PROS_REPEAT", pros_repeat),
]:

    print()
    print(name)

    buckets = [
        (
            "INNOV < 0",
            lambda x: x < 0
        ),
        (
            "INNOV = 0",
            lambda x: x == 0
        ),
        (
            "INNOV > 0",
            lambda x: x > 0
        ),
    ]

    for label, fn in buckets:

        rr = [
            r for r in base
            if (
                valid(r[key])
                and fn(r[key])
            )
        ]

        yes = sum(
            r["activation"]
            for r in rr
        )

        print(
            f"  {label:10} "
            f"| N={len(rr):4d} "
            f"| YES={yes:3d} "
            f"| ACT_RATE={pct(yes,len(rr)):>6}"
        )


# ============================================================
# I) WITHIN-TOKEN PAIRWISE TEST
# ============================================================

print()
print("=" * 180)
print("I) WITHIN-TOKEN PAIRWISE ORDERING TEST")
print("=" * 180)

# For each token with both activated and non-activated repeat events,
# compare the feature values within that SAME token.
# This removes between-token level differences.

for f in FEATURES:

    key = f"{f}_innovation"

    wins = 0.0
    total = 0
    eligible_tokens = 0

    for tok, rr0 in by_token.items():

        rr = [
            r for r in rr0
            if (
                r["event_number"] >= 2
                and valid(r[key])
            )
        ]

        pos = [
            r[key]
            for r in rr
            if r["activation"] == 1
        ]

        neg = [
            r[key]
            for r in rr
            if r["activation"] == 0
        ]

        if not pos or not neg:
            continue

        eligible_tokens += 1

        for a in pos:
            for b in neg:

                total += 1

                if a > b:
                    wins += 1

                elif a == b:
                    wins += 0.5

    raw = (
        wins / total
        if total
        else None
    )

    if raw is None:
        da = None
        direction = None

    elif raw >= 0.5:
        da = raw
        direction = "HIGHER"

    else:
        da = 1-raw
        direction = "LOWER"

    print(
        f"{f:20} "
        f"| TOKENS={eligible_tokens:3d} "
        f"| PAIRS={total:5d} "
        f"| DIR={str(direction):6} "
        f"| WITHIN_AUC={fmt(da)}"
    )


# ============================================================
# J) TOKEN EVENT-COUNT CONFOUNDING
# ============================================================

print()
print("=" * 180)
print("J) TOKEN EVENT-COUNT / ACTIVITY CONFOUNDING")
print("=" * 180)

token_n = []
token_primary = []
token_act = []

for r in token_rows:

    if valid(r[PRIMARY]):

        token_n.append(r["n"])
        token_primary.append(r[PRIMARY])
        token_act.append(r["activation_rate"])

print(
    f"CORR(token mean {PRIMARY}, event count) "
    f"= {fmt(corr(token_primary, token_n))}"
)

print(
    f"CORR(token mean {PRIMARY}, activation rate) "
    f"= {fmt(corr(token_primary, token_act))}"
)

print(
    f"CORR(event count, activation rate) "
    f"= {fmt(corr(token_n, token_act))}"
)


# ============================================================
# K) REGIME-SPECIFIC INTRA-TOKEN PRIMARY TEST
# ============================================================

print()
print("=" * 180)
print("K) FROZEN PRIMARY INTRA-TOKEN REGIME CHECK")
print("=" * 180)

for name, rr in [
    ("HIST", hist_repeat),
    ("PROS", pros_repeat),
]:

    pairs = [
        (
            r["activation"],
            r[f"{PRIMARY}_innovation"]
        )
        for r in rr
        if valid(r[f"{PRIMARY}_innovation"])
    ]

    raw = auc_from_pairs(pairs)

    if raw is None:
        direction = None
        directional = None

    elif raw >= 0.5:
        direction = "HIGHER"
        directional = raw

    else:
        direction = "LOWER"
        directional = 1-raw

    print(
        f"{name:5} "
        f"| N={len(pairs):4d} "
        f"| RAW_HIGHER_AUC={fmt(raw)} "
        f"| DIR={str(direction):6} "
        f"| DIR_AUC={fmt(directional)}"
    )


# ============================================================
# L) DECISION SUPPORT
# ============================================================

print()
print("=" * 180)
print("L) DECISION SUPPORT")
print("=" * 180)


def raw_innovation_auc(rr):

    return auc_from_pairs([
        (
            r["activation"],
            r[f"{PRIMARY}_innovation"]
        )
        for r in rr
        if valid(r[f"{PRIMARY}_innovation"])
    ])


all_intra = raw_innovation_auc(repeat)
hist_intra = raw_innovation_auc(hist_repeat)
pros_intra = raw_innovation_auc(pros_repeat)

first_auc = auc_from_pairs([
    (r["activation"], r[PRIMARY])
    for r in first
    if valid(r[PRIMARY])
])


print(
    f"FIRST-EVENT RAW AUC              = {fmt(first_auc)}"
)

print(
    f"ALL REPEAT INNOVATION AUC        = {fmt(all_intra)}"
)

print(
    f"HIST REPEAT INNOVATION AUC       = {fmt(hist_intra)}"
)

print(
    f"PROS REPEAT INNOVATION AUC       = {fmt(pros_intra)}"
)

print()


same_positive_direction = (
    hist_intra is not None
    and pros_intra is not None
    and hist_intra > 0.50
    and pros_intra > 0.50
)

same_negative_direction = (
    hist_intra is not None
    and pros_intra is not None
    and hist_intra < 0.50
    and pros_intra < 0.50
)


if (
    same_positive_direction
    and min(hist_intra, pros_intra) >= 0.56
):

    print(
        "🟢 NEW_WALLETS10 APPEARS TO CONTAIN A REPEAT-EVENT "
        "INTRA-TOKEN ACTIVATION SIGNAL."
    )

    print(
        "T91 failed cross-token gating, but the family may survive "
        "as a token-state / acceleration feature."
    )

    print(
        "Next = dedicated prospective-safe intra-token robustness audit."
    )

elif (
    same_negative_direction
    and max(hist_intra, pros_intra) <= 0.44
):

    print(
        "🟡 NEW_WALLETS10 INNOVATION IS CONSISTENT BUT REVERSED."
    )

    print(
        "Do not reuse the T90 HIGHER interpretation."
    )

    print(
        "A separate hypothesis would be required before further testing."
    )

else:

    print(
        "🔴 NO ROBUST INTRA-TOKEN RESCUE FOR NEW_WALLETS10."
    )

    print(
        "T91 failure is not explained by a clean prospective-safe "
        "within-token acceleration effect."
    )

    print(
        "Close this feature family for activation."
    )


print()
print("IMPORTANT:")
print("• T92 does NOT rehabilitate T91 automatically.")
print("• Full-token means are descriptive only.")
print("• Operationally relevant innovation uses PRIOR events only.")
print("• No target threshold search.")
print("• No feature threshold search.")
print("• No model fitting.")
print("• No DB writes.")
print("• Frozen prospective branches remain untouched.")

db.close()
