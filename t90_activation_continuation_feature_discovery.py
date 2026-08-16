#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

ACTIVATION_THRESHOLD = 3.0
CONTINUATION_THRESHOLD = 10.0


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


def auc_directional(y, x):
    pairs = [
        (yy, xx)
        for yy, xx in zip(y, x)
        if yy is not None and valid(xx)
    ]

    pos = [xx for yy, xx in pairs if yy == 1]
    neg = [xx for yy, xx in pairs if yy == 0]

    if not pos or not neg:
        return None, None

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
        return raw, "HIGHER"

    return 1.0-raw, "LOWER"


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
    e.id,
    e.timestamp,
    e.token_mint,

    e.fa,
    e.nf30,
    e.imbalance30,
    e.price_change30,

    e.fa90,
    e.fa95,
    e.fpa,
    e.extreme,

    e.swaps5,
    e.swaps10,
    e.swaps30,
    e.swaps60,

    e.buyers5,
    e.buyers10,
    e.buyers30,
    e.buyers60,

    e.wallets30,
    e.wallets60,

    e.new_wallets10,
    e.new_wallets30,

    e.buyer_growth,
    e.wallet_growth,

    e.buy_volume30,
    e.sell_volume30,
    e.largest_buy30,
    e.buy_concentration30,

    e.dex_return_30s,
    e.dex_return_300s,

    s.recent_buy_share,
    s.recent_net_share,
    s.breadth_score,
    s.late_chase_score,

    s.early_price_return,
    s.early_net_sol

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL
    AND e.dex_return_30s IS NOT NULL
    AND e.dex_return_300s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


records = []


for r in rows:

    early_div = None

    if (
        valid(r["early_price_return"])
        and valid(r["early_net_sol"])
    ):
        early_div = (
            r["early_price_return"]
            - r["early_net_sol"]
        )


    activation = (
        1
        if abs(r["dex_return_30s"]) >= ACTIVATION_THRESHOLD
        else 0
    )


    continuation = None

    if activation == 1:
        continuation = (
            1
            if abs(r["dex_return_300s"]) >= CONTINUATION_THRESHOLD
            else 0
        )


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],

        "historical": r["id"] <= boundary,

        "activation": activation,
        "continuation": continuation,

        "fa": r["fa"],
        "nf30": r["nf30"],
        "imbalance30": r["imbalance30"],
        "price_change30": r["price_change30"],

        "fa90": r["fa90"],
        "fa95": r["fa95"],
        "fpa": r["fpa"],
        "extreme": r["extreme"],

        "swaps5": r["swaps5"],
        "swaps10": r["swaps10"],
        "swaps30": r["swaps30"],
        "swaps60": r["swaps60"],

        "buyers5": r["buyers5"],
        "buyers10": r["buyers10"],
        "buyers30": r["buyers30"],
        "buyers60": r["buyers60"],

        "wallets30": r["wallets30"],
        "wallets60": r["wallets60"],

        "new_wallets10": r["new_wallets10"],
        "new_wallets30": r["new_wallets30"],

        "buyer_growth": r["buyer_growth"],
        "wallet_growth": r["wallet_growth"],

        "buy_volume30": r["buy_volume30"],
        "sell_volume30": r["sell_volume30"],
        "largest_buy30": r["largest_buy30"],
        "buy_concentration30": r["buy_concentration30"],

        "recent_buy_share": r["recent_buy_share"],
        "recent_net_share": r["recent_net_share"],
        "breadth_score": r["breadth_score"],
        "late_chase_score": r["late_chase_score"],
        "early_div": early_div,
    })


FEATURES = [
    "fa",
    "nf30",
    "imbalance30",
    "price_change30",

    "fa90",
    "fa95",
    "fpa",
    "extreme",

    "swaps5",
    "swaps10",
    "swaps30",
    "swaps60",

    "buyers5",
    "buyers10",
    "buyers30",
    "buyers60",

    "wallets30",
    "wallets60",

    "new_wallets10",
    "new_wallets30",

    "buyer_growth",
    "wallet_growth",

    "buy_volume30",
    "sell_volume30",
    "largest_buy30",
    "buy_concentration30",

    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
    "early_div",
]


# ============================================================
# SPLITS
# ============================================================

hist = [
    r for r in records
    if r["historical"]
]

pros = [
    r for r in records
    if not r["historical"]
]


# ============================================================
# RANKING FUNCTION
# ============================================================

def rank_features(rows, target_name):

    out = []

    for f in FEATURES:

        rr = [
            r for r in rows
            if (
                r[target_name] is not None
                and valid(r[f])
            )
        ]

        y = [
            r[target_name]
            for r in rr
        ]

        x = [
            r[f]
            for r in rr
        ]

        aucv, direction = auc_directional(
            y,
            x
        )

        pos = [
            xx
            for yy, xx in zip(y, x)
            if yy == 1
        ]

        neg = [
            xx
            for yy, xx in zip(y, x)
            if yy == 0
        ]

        out.append({
            "feature": f,
            "n": len(rr),
            "pos": len(pos),
            "neg": len(neg),
            "pos_med": med(pos),
            "neg_med": med(neg),
            "diff": (
                med(pos)-med(neg)
                if pos and neg
                else None
            ),
            "auc": aucv,
            "direction": direction,
        })

    out.sort(
        key=lambda r: (
            -(r["auc"] or 0),
            -r["n"]
        )
    )

    return out


# ============================================================
# RESULTS
# ============================================================

hist_act = rank_features(
    hist,
    "activation"
)

pros_act = rank_features(
    pros,
    "activation"
)

hist_cont = rank_features(
    hist,
    "continuation"
)

pros_cont = rank_features(
    pros,
    "continuation"
)


# ============================================================
# OUTPUT
# ============================================================

print("=" * 180)
print(
    "MEMECOIN LAB — T90 ACTIVATION / CONTINUATION CONDITIONAL FEATURE DISCOVERY"
)
print("=" * 180)

print("MODE                  : READ-ONLY")
print("MODEL FITTING         : NONE")
print("THRESHOLD SEARCH      : NONE")
print("DB WRITES             : NONE")
print("T59/T78/T82/T86       : UNTOUCHED")
print()
print(f"T59 BOUNDARY          : {boundary}")
print(
    f"ACTIVATION TARGET     : |R30| >= {ACTIVATION_THRESHOLD:.1f}%"
)
print(
    f"CONTINUATION TARGET   : |R300| >= {CONTINUATION_THRESHOLD:.1f}% among activated events"
)


# ============================================================
# A) TARGET DENSITY
# ============================================================

print()
print("=" * 180)
print("A) TARGET DENSITY")
print("=" * 180)

for name, rr in [
    ("HIST", hist),
    ("PROS", pros),
]:

    act1 = sum(
        r["activation"] == 1
        for r in rr
    )

    act0 = sum(
        r["activation"] == 0
        for r in rr
    )

    activated = [
        r for r in rr
        if r["activation"] == 1
    ]

    cont1 = sum(
        r["continuation"] == 1
        for r in activated
    )

    cont0 = sum(
        r["continuation"] == 0
        for r in activated
    )

    print(
        f"{name:5} "
        f"| ACTIVATION YES={act1:4d} NO={act0:4d} "
        f"| CONT YES={cont1:4d} NO={cont0:4d}"
    )


# ============================================================
# B) HIST ACTIVATION
# ============================================================

print()
print("=" * 180)
print("B) HISTORICAL — ACTIVATION FEATURE RANKING")
print("=" * 180)

for r in hist_act:

    print(
        f"{r['feature']:24} "
        f"N={r['n']:4d} "
        f"YES={r['pos']:4d} "
        f"NO={r['neg']:4d} "
        f"| YES_MED={fmt(r['pos_med']):>8} "
        f"NO_MED={fmt(r['neg_med']):>8} "
        f"| DIFF={fmt(r['diff']):>8} "
        f"| DIR={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])}"
    )


# ============================================================
# C) PROS ACTIVATION
# ============================================================

print()
print("=" * 180)
print("C) PROSPECTIVE — ACTIVATION FEATURE RANKING")
print("=" * 180)

for r in pros_act:

    print(
        f"{r['feature']:24} "
        f"N={r['n']:4d} "
        f"YES={r['pos']:4d} "
        f"NO={r['neg']:4d} "
        f"| YES_MED={fmt(r['pos_med']):>8} "
        f"NO_MED={fmt(r['neg_med']):>8} "
        f"| DIFF={fmt(r['diff']):>8} "
        f"| DIR={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])}"
    )


# ============================================================
# D) HIST CONTINUATION
# ============================================================

print()
print("=" * 180)
print("D) HISTORICAL — CONTINUATION FEATURE RANKING")
print("=" * 180)

for r in hist_cont:

    print(
        f"{r['feature']:24} "
        f"N={r['n']:4d} "
        f"YES={r['pos']:4d} "
        f"NO={r['neg']:4d} "
        f"| YES_MED={fmt(r['pos_med']):>8} "
        f"NO_MED={fmt(r['neg_med']):>8} "
        f"| DIFF={fmt(r['diff']):>8} "
        f"| DIR={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])}"
    )


# ============================================================
# E) PROS CONTINUATION
# ============================================================

print()
print("=" * 180)
print("E) PROSPECTIVE — CONTINUATION FEATURE RANKING")
print("=" * 180)

for r in pros_cont:

    print(
        f"{r['feature']:24} "
        f"N={r['n']:4d} "
        f"YES={r['pos']:4d} "
        f"NO={r['neg']:4d} "
        f"| YES_MED={fmt(r['pos_med']):>8} "
        f"NO_MED={fmt(r['neg_med']):>8} "
        f"| DIFF={fmt(r['diff']):>8} "
        f"| DIR={str(r['direction']):6} "
        f"| AUC={fmt(r['auc'])}"
    )


# ============================================================
# F) CROSS-REGIME STABILITY — ACTIVATION
# ============================================================

print()
print("=" * 180)
print("F) CROSS-REGIME STABILITY — ACTIVATION")
print("=" * 180)

hist_map = {
    r["feature"]: r
    for r in hist_act
}

pros_map = {
    r["feature"]: r
    for r in pros_act
}

stable_act = []

for f in FEATURES:

    h = hist_map[f]
    p = pros_map[f]

    same_dir = (
        h["direction"] is not None
        and p["direction"] is not None
        and h["direction"] == p["direction"]
    )

    if (
        h["auc"] is not None
        and p["auc"] is not None
        and same_dir
        and h["auc"] >= 0.55
        and p["auc"] >= 0.55
    ):
        stable_act.append(
            (
                min(h["auc"], p["auc"]),
                f,
                h,
                p
            )
        )

stable_act.sort(
    reverse=True
)

if not stable_act:
    print(
        "No activation feature is >=0.55 AUC in the same direction "
        "historically and prospectively."
    )
else:
    for _, f, h, p in stable_act:
        print(
            f"{f:24} "
            f"| DIR={h['direction']:6} "
            f"| HIST={h['auc']:.3f} "
            f"| PROS={p['auc']:.3f}"
        )


# ============================================================
# G) CROSS-REGIME STABILITY — CONTINUATION
# ============================================================

print()
print("=" * 180)
print("G) CROSS-REGIME STABILITY — CONTINUATION")
print("=" * 180)

hist_map_c = {
    r["feature"]: r
    for r in hist_cont
}

pros_map_c = {
    r["feature"]: r
    for r in pros_cont
}

stable_cont = []

for f in FEATURES:

    h = hist_map_c[f]
    p = pros_map_c[f]

    same_dir = (
        h["direction"] is not None
        and p["direction"] is not None
        and h["direction"] == p["direction"]
    )

    if (
        h["auc"] is not None
        and p["auc"] is not None
        and same_dir
        and h["auc"] >= 0.55
        and p["auc"] >= 0.55
    ):
        stable_cont.append(
            (
                min(h["auc"], p["auc"]),
                f,
                h,
                p
            )
        )

stable_cont.sort(
    reverse=True
)

if not stable_cont:
    print(
        "No continuation feature is >=0.55 AUC in the same direction "
        "historically and prospectively."
    )
else:
    for _, f, h, p in stable_cont:
        print(
            f"{f:24} "
            f"| DIR={h['direction']:6} "
            f"| HIST={h['auc']:.3f} "
            f"| PROS={p['auc']:.3f}"
        )


# ============================================================
# H) DECISION SUPPORT
# ============================================================

print()
print("=" * 180)
print("H) DECISION SUPPORT")
print("=" * 180)

if stable_act and stable_cont:

    print(
        "🟢 BOTH ACTIVATION AND CONTINUATION HAVE "
        "CROSS-REGIME FEATURE CANDIDATES."
    )

    print(
        f"TOP ACTIVATION CANDIDATE   = {stable_act[0][1]}"
    )

    print(
        f"TOP CONTINUATION CANDIDATE = {stable_cont[0][1]}"
    )

elif stable_act:

    print(
        "🟡 ACTIVATION HAS A CROSS-REGIME FEATURE CANDIDATE."
    )

    print(
        f"TOP ACTIVATION CANDIDATE = {stable_act[0][1]}"
    )

    print(
        "Continuation remains unresolved."
    )

elif stable_cont:

    print(
        "🟡 CONTINUATION HAS A CROSS-REGIME FEATURE CANDIDATE."
    )

    print(
        f"TOP CONTINUATION CANDIDATE = {stable_cont[0][1]}"
    )

    print(
        "Activation remains unresolved."
    )

else:

    print(
        "🔴 NO CROSS-REGIME PRE-EVENT FEATURE SURVIVES "
        "FOR ACTIVATION OR CONTINUATION."
    )

    print(
        "Current pre-event feature set may not explain the regime failure."
    )


print()
print("IMPORTANT:")
print("• Activation and continuation are separate targets.")
print("• Only pre-event/event-time features are tested.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• T90 writes nothing to DB.")
print("• All frozen prospective branches remain untouched.")

db.close()
