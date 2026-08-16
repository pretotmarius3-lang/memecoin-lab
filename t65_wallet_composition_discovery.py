import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
PRE_EVENT_SEC = 30.0

FAST_FLIP_SEC = 60.0


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


def stdev(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.pstdev(xs) if len(xs) >= 2 else None


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def sdiv(a, b):
    if not valid(a) or not valid(b) or b == 0:
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


def auc_directional(y, x):

    pairs = [
        (yy, xx)
        for yy, xx in zip(y, x)
        if yy is not None and valid(xx)
    ]

    pos = [
        xx for yy, xx in pairs
        if yy == 1
    ]

    neg = [
        xx for yy, xx in pairs
        if yy == 0
    ]

    if not pos or not neg:
        return None, None, None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:
            total += 1

            if a > b:
                wins += 1.0
            elif a == b:
                wins += 0.5

    raw = wins / total

    if raw >= 0.5:
        return raw, raw, "HIGHER"

    return raw, 1.0-raw, "LOWER"


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

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s,

    fa,
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

completed_trades = defaultdict(int)
fast_flips = defaultdict(int)
tokens_traded = defaultdict(set)

open_positions = {}

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

        if key not in open_positions:
            open_positions[key] = ts


    elif side == "SELL":

        if key not in open_positions:
            return

        entry_ts = open_positions[key]
        hold = ts-entry_ts

        if hold >= 0:

            completed_trades[wallet] += 1
            tokens_traded[wallet].add(token)

            if hold <= FAST_FLIP_SEC:
                fast_flips[wallet] += 1

        open_positions.pop(
            key,
            None
        )


# ============================================================
# BUILD EVENT COHORTS
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


    histories = []


    for w in buyers:

        trades = completed_trades[w]
        fast = fast_flips[w]

        ff = (
            fast/trades
            if trades > 0
            else None
        )

        histories.append({
            "wallet": w,
            "trades": trades,
            "fast_flips": fast,
            "fast_flip_rate": ff,
            "unique_tokens_prior":
                len(tokens_traded[w]),
        })


    n_buyers = len(histories)

    exp1 = [
        h for h in histories
        if h["trades"] >= 1
    ]

    exp2 = [
        h for h in histories
        if h["trades"] >= 2
    ]

    exp3 = [
        h for h in histories
        if h["trades"] >= 3
    ]

    repeat = [
        h for h in histories
        if h["unique_tokens_prior"] >= 2
    ]


    trade_counts = [
        h["trades"]
        for h in histories
    ]

    token_counts = [
        h["unique_tokens_prior"]
        for h in histories
    ]

    ff_rates = [
        h["fast_flip_rate"]
        for h in histories
        if valid(h["fast_flip_rate"])
    ]


    experienced_trade_counts = [
        h["trades"]
        for h in exp1
    ]


    # concentration of prior experience
    total_prior_trades = sum(
        trade_counts
    )

    max_prior_trades = max(
        trade_counts
    ) if trade_counts else 0


    experience_concentration = (
        max_prior_trades / total_prior_trades
        if total_prior_trades > 0
        else None
    )


    # top-2 experience concentration
    sorted_trades = sorted(
        trade_counts,
        reverse=True
    )

    top2 = sum(
        sorted_trades[:2]
    )

    top2_experience_concentration = (
        top2 / total_prior_trades
        if total_prior_trades > 0
        else None
    )


    features = {

        # -------------------------------
        # Experience coverage
        # -------------------------------

        "experienced_share_1":
            len(exp1)/n_buyers,

        "experienced_share_2":
            len(exp2)/n_buyers,

        "experienced_share_3":
            len(exp3)/n_buyers,

        "repeat_buyer_share":
            len(repeat)/n_buyers,


        # -------------------------------
        # Cohort experience level
        # -------------------------------

        "mean_prior_trades":
            avg(trade_counts),

        "median_prior_trades":
            med(trade_counts),

        "max_prior_trades":
            max_prior_trades,

        "mean_prior_tokens":
            avg(token_counts),

        "median_prior_tokens":
            med(token_counts),

        "max_prior_tokens":
            max(token_counts)
            if token_counts
            else None,


        # -------------------------------
        # Dispersion
        # -------------------------------

        "prior_trades_std":
            stdev(trade_counts),

        "prior_tokens_std":
            stdev(token_counts),


        # -------------------------------
        # Experience concentration
        # -------------------------------

        "experience_concentration":
            experience_concentration,

        "top2_experience_concentration":
            top2_experience_concentration,


        # -------------------------------
        # Fast-flip behavior composition
        # -------------------------------

        "fast_flip_mean":
            avg(ff_rates),

        "fast_flip_median":
            med(ff_rates),

        "fast_flip_std":
            stdev(ff_rates),

        "fast_flip_coverage":
            len(ff_rates)/n_buyers,


        # -------------------------------
        # Experienced-only profile
        # -------------------------------

        "experienced_mean_trades":
            avg(experienced_trade_counts),

        "experienced_median_trades":
            med(experienced_trade_counts),
    }


    records.append({
        "id": e["id"],
        "timestamp": e["timestamp"],
        "token_mint": e["token_mint"],
        "y": y,

        "fa": e["fa"],
        "new_wallets30":
            e["new_wallets30"],

        "features": features,
    })


# ============================================================
# FEATURE LIST
# ============================================================

feature_names = sorted(
    set(
        k
        for r in records
        for k in r["features"]
    )
)


# ============================================================
# GLOBAL
# ============================================================

results = []


for feature in feature_names:

    rr = [
        r for r in records
        if valid(
            r["features"].get(feature)
        )
    ]

    y = [
        r["y"]
        for r in rr
    ]

    x = [
        r["features"][feature]
        for r in rr
    ]

    raw, da, direction = auc_directional(
        y,
        x
    )


    run = [
        xx
        for yy, xx in zip(y,x)
        if yy == 1
    ]

    dump = [
        xx
        for yy, xx in zip(y,x)
        if yy == 0
    ]


    results.append({
        "feature": feature,
        "n": len(rr),

        "run_n":
            len(run),

        "dump_n":
            len(dump),

        "run_med":
            med(run),

        "dump_med":
            med(dump),

        "diff":
            (
                med(run)-med(dump)
                if run and dump
                else None
            ),

        "auc":
            da,

        "direction":
            direction,
    })


results.sort(
    key=lambda r: (
        -(r["auc"] or 0),
        -r["n"]
    )
)


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

seen = set()
first = []


for r in records:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


first_results = {}


for feature in feature_names:

    rr = [
        r for r in first
        if valid(
            r["features"].get(feature)
        )
    ]

    y = [
        r["y"]
        for r in rr
    ]

    x = [
        r["features"][feature]
        for r in rr
    ]

    _, da, direction = auc_directional(
        y,
        x
    )

    first_results[
        feature
    ] = {
        "n":
            len(rr),

        "auc":
            da,

        "direction":
            direction,
    }


# ============================================================
# CHRONOLOGICAL HALVES
# ============================================================

midpoint = len(records)//2

halves = {
    "EARLY":
        records[:midpoint],

    "LATE":
        records[midpoint:],
}


half_results = defaultdict(dict)


for hname, rr0 in halves.items():

    for feature in feature_names:

        rr = [
            r for r in rr0
            if valid(
                r["features"].get(feature)
            )
        ]

        y = [
            r["y"]
            for r in rr
        ]

        x = [
            r["features"][feature]
            for r in rr
        ]

        _, da, direction = auc_directional(
            y,
            x
        )

        half_results[
            feature
        ][
            hname
        ] = {
            "n":
                len(rr),

            "auc":
                da,

            "direction":
                direction,
        }


# ============================================================
# REDUNDANCY AGAINST SIMPLE CONTEXT
# ============================================================

for r in records:

    r["context"] = {
        "fa":
            r["fa"],

        "new_wallets30":
            r["new_wallets30"],
    }


redundancy = {}


for feature in feature_names:

    redundancy[
        feature
    ] = {}

    for ctx in [
        "fa",
        "new_wallets30"
    ]:

        xs = []
        ys = []

        for r in records:

            x = r[
                "features"
            ].get(
                feature
            )

            y = r[
                "context"
            ][
                ctx
            ]

            if valid(x) and valid(y):
                xs.append(x)
                ys.append(y)

        redundancy[
            feature
        ][
            ctx
        ] = pearson(
            xs,
            ys
        )


# ============================================================
# OUTPUT
# ============================================================

print("=" * 185)
print("MEMECOIN LAB — T65 WALLET COMPOSITION / BUYER QUALITY MIX DISCOVERY")
print("=" * 185)

print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in records))}"
)

print(
    f"FEATURES       : {len(feature_names)}"
)

print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


# ============================================================
# A
# ============================================================

print()
print("=" * 185)
print("A) GLOBAL UNIVARIATE RANKING")
print("=" * 185)


for r in results:

    print(
        f"{r['feature']:34} "
        f"N={r['n']:3d} "
        f"RUN={r['run_n']:3d} "
        f"DUMP={r['dump_n']:3d} "
        f"RUN_MED={fmt(r['run_med']):>8} "
        f"DUMP_MED={fmt(r['dump_med']):>8} "
        f"DIFF={fmt(r['diff']):>8} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['auc'])}"
    )


# ============================================================
# B
# ============================================================

top15 = [
    r["feature"]
    for r in results[:15]
]


print()
print("=" * 185)
print("B) TOP 15 — FIRST EVENT / TOKEN")
print("=" * 185)


for feature in top15:

    r = first_results[
        feature
    ]

    print(
        f"{feature:34} "
        f"N={r['n']:3d} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['auc'])}"
    )


# ============================================================
# C
# ============================================================

print()
print("=" * 185)
print("C) TOP 15 — CHRONOLOGICAL HALF STABILITY")
print("=" * 185)


for feature in top15:

    a = half_results[
        feature
    ][
        "EARLY"
    ]

    b = half_results[
        feature
    ][
        "LATE"
    ]

    print(
        f"{feature:34} "
        f"| EARLY N={a['n']:3d} "
        f"DIR={str(a['direction']):6} "
        f"AUC={fmt(a['auc'])} "
        f"| LATE N={b['n']:3d} "
        f"DIR={str(b['direction']):6} "
        f"AUC={fmt(b['auc'])}"
    )


# ============================================================
# D
# ============================================================

print()
print("=" * 185)
print("D) TOP 15 — CONTEXT REDUNDANCY")
print("=" * 185)


for feature in top15:

    vals = [
        (
            ctx,
            corr
        )
        for ctx, corr
        in redundancy[
            feature
        ].items()
        if valid(corr)
    ]

    if vals:

        ctx, corr = max(
            vals,
            key=lambda x:
                abs(x[1])
        )

        print(
            f"{feature:34} "
            f"MAX|CORR|={abs(corr):.3f} "
            f"| WITH={ctx:15} "
            f"| CORR={corr:+.3f}"
        )

    else:

        print(
            f"{feature:34} "
            f"MAX|CORR|=NA"
        )


# ============================================================
# E) CONSERVATIVE GATE
# ============================================================

print()
print("=" * 185)
print("E) CONSERVATIVE DISCOVERY GATE")
print("=" * 185)


survivors = []


for g in results:

    feature = g[
        "feature"
    ]

    f = first_results[
        feature
    ]

    eh = half_results[
        feature
    ][
        "EARLY"
    ]

    lh = half_results[
        feature
    ][
        "LATE"
    ]


    corrs = [
        abs(x)
        for x in redundancy[
            feature
        ].values()
        if valid(x)
    ]

    maxcorr = (
        max(corrs)
        if corrs
        else None
    )


    same_direction = (
        g["direction"] is not None
        and f["direction"] is not None
        and eh["direction"] is not None
        and lh["direction"] is not None
        and g["direction"]
            == f["direction"]
            == eh["direction"]
            == lh["direction"]
    )


    passes = (
        g["n"] >= 70
        and g["auc"] is not None
        and g["auc"] >= 0.57

        and f["auc"] is not None
        and f["auc"] >= 0.55

        and eh["auc"] is not None
        and eh["auc"] >= 0.53

        and lh["auc"] is not None
        and lh["auc"] >= 0.53

        and same_direction

        and (
            maxcorr is None
            or maxcorr <= 0.75
        )
    )


    if passes:

        survivors.append(
            (
                feature,
                g["direction"],
                g["auc"],
                f["auc"],
                eh["auc"],
                lh["auc"],
                maxcorr,
            )
        )


survivors.sort(
    key=lambda x: (
        -min(
            x[2],
            x[3],
            x[4],
            x[5]
        ),
        x[6] if x[6] is not None else 0
    )
)


if survivors:

    for s in survivors:

        print(
            f"{s[0]:34} "
            f"| DIR={s[1]:6} "
            f"| GLOBAL={s[2]:.3f} "
            f"| FIRST={s[3]:.3f} "
            f"| EARLY={s[4]:.3f} "
            f"| LATE={s[5]:.3f} "
            f"| MAXCORR={fmt(s[6])}"
        )

else:

    print(
        "No wallet-composition feature passes the gate."
    )


# ============================================================
# F
# ============================================================

print()
print("=" * 185)
print("F) DECISION SUPPORT")
print("=" * 185)


if survivors:

    best = survivors[0]

    print(
        "🟡 WALLET-COMPOSITION FAMILY CONTAINS "
        "A ROBUSTNESS CANDIDATE."
    )

    print(
        f"PRIMARY T66 CANDIDATE = {best[0]}"
    )

    print(
        f"FROZEN DIRECTION      = {best[1]}"
    )

    print(
        "Next = T66 robustness audit."
    )

    print(
        "No threshold optimization."
    )

else:

    print(
        "🔴 NO WALLET-COMPOSITION FEATURE "
        "SURVIVES THE DISCOVERY GATE."
    )

    print(
        "Do not force this family."
    )

    print(
        "T59 continues untouched."
    )


print()
print("IMPORTANT:")
print("• Wallet history is strictly chronological.")
print("• Only swaps before event timestamp affect wallet history.")
print("• Buyer cohort uses BUY wallets from the 30s pre-event window.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No DB writes.")
print("• T59 remains frozen and untouched.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• Research discovery only.")

db.close()
