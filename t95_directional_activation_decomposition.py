#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

UP30 = 3.0
DOWN30 = -3.0

UP300 = 10.0
DOWN300 = -10.0


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    n = len(xs)

    if n % 2:
        return xs[n//2]

    return (xs[n//2-1] + xs[n//2]) / 2.0


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def auc_directional(rows, target, feature):

    pairs = [
        (r[target], r[feature])
        for r in rows
        if (
            r[target] is not None
            and valid(r[feature])
        )
    ]

    pos = [x for y, x in pairs if y == 1]
    neg = [x for y, x in pairs if y == 0]

    if not pos or not neg:
        return None, None, len(pairs)

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
        return raw, "HIGHER", len(pairs)

    return 1.0 - raw, "LOWER", len(pairs)


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
    e.dex_done_30s,

    e.dex_return_300s,
    e.dex_done_300s,

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
    AND e.dex_done_30s=1
    AND e.dex_return_30s IS NOT NULL

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


    r30 = r["dex_return_30s"]
    r300 = r["dex_return_300s"]


    # --------------------------------------------------------
    # DIRECTIONAL ACTIVATION TARGETS
    # --------------------------------------------------------

    up_activation = int(
        r30 >= UP30
    )

    down_activation = int(
        r30 <= DOWN30
    )


    # --------------------------------------------------------
    # CONTINUATION TARGETS
    # Only defined inside their activated directional cohort.
    # --------------------------------------------------------

    up_continuation = None

    if (
        r30 >= UP30
        and r["dex_done_300s"] == 1
        and valid(r300)
    ):
        up_continuation = int(
            r300 >= UP300
        )


    down_continuation = None

    if (
        r30 <= DOWN30
        and r["dex_done_300s"] == 1
        and valid(r300)
    ):
        down_continuation = int(
            r300 <= DOWN300
        )


    rec = {
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],

        "historical":
            r["id"] <= boundary,

        "r30": r30,
        "r300": r300,

        "up_activation":
            up_activation,

        "down_activation":
            down_activation,

        "up_continuation":
            up_continuation,

        "down_continuation":
            down_continuation,

        "early_div":
            early_div,
    }


    for f in [
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
    ]:
        rec[f] = r[f]


    records.append(rec)


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


hist = [
    r for r in records
    if r["historical"]
]

pros = [
    r for r in records
    if not r["historical"]
]


# ============================================================
# HEADER
# ============================================================

print("=" * 190)
print(
    "MEMECOIN LAB — T95 DIRECTIONAL ACTIVATION / CONTINUATION DECOMPOSITION"
)
print("=" * 190)

print("MODE                 : READ-ONLY")
print("MODEL FITTING        : NONE")
print("THRESHOLD SEARCH     : NONE")
print("DB WRITES            : NONE")
print("T59/T78/T82/T86      : UNTOUCHED")
print()
print(f"T59 BOUNDARY         : {boundary}")
print()
print(f"UP ACTIVATION        : R30 >= +{UP30:.1f}%")
print(f"DOWN ACTIVATION      : R30 <= {DOWN30:.1f}%")
print(f"UP CONTINUATION      : R300 >= +{UP300:.1f}% among UP activated")
print(f"DOWN CONTINUATION    : R300 <= {DOWN300:.1f}% among DOWN activated")


# ============================================================
# A) TARGET DENSITY
# ============================================================

print()
print("=" * 190)
print("A) TARGET DENSITY")
print("=" * 190)


for name, rr in [
    ("HIST", hist),
    ("PROS", pros),
]:

    up = sum(
        r["up_activation"]
        for r in rr
    )

    down = sum(
        r["down_activation"]
        for r in rr
    )

    quiet = len(rr) - up - down

    upc = [
        r for r in rr
        if r["up_continuation"] is not None
    ]

    downc = [
        r for r in rr
        if r["down_continuation"] is not None
    ]

    print(
        f"{name:5} "
        f"| N={len(rr):4d} "
        f"| UP={up:4d} "
        f"| DOWN={down:4d} "
        f"| QUIET={quiet:4d} "
        f"| UPCONT={sum(r['up_continuation'] for r in upc):3d}/{len(upc):3d} "
        f"| DOWNCONT={sum(r['down_continuation'] for r in downc):3d}/{len(downc):3d}"
    )


# ============================================================
# GENERIC RANKING
# ============================================================

def rank(rows, target):

    out = []

    for f in FEATURES:

        a, d, n = auc_directional(
            rows,
            target,
            f
        )

        pos = [
            r[f]
            for r in rows
            if (
                r[target] == 1
                and valid(r[f])
            )
        ]

        neg = [
            r[f]
            for r in rows
            if (
                r[target] == 0
                and valid(r[f])
            )
        ]

        diff = None

        if pos and neg:
            diff = med(pos)-med(neg)

        out.append({
            "feature": f,
            "auc": a,
            "direction": d,
            "n": n,
            "pos": len(pos),
            "neg": len(neg),
            "diff": diff,
        })

    out.sort(
        key=lambda x: (
            -(x["auc"] or 0),
            -x["n"]
        )
    )

    return out


targets = [
    ("UP_ACTIVATION", "up_activation"),
    ("DOWN_ACTIVATION", "down_activation"),
    ("UP_CONTINUATION", "up_continuation"),
    ("DOWN_CONTINUATION", "down_continuation"),
]


results = {}


for label, target in targets:

    results[label] = {
        "HIST":
            rank(hist, target),

        "PROS":
            rank(pros, target),
    }


# ============================================================
# B-E) RANKINGS
# ============================================================

for label, target in targets:

    for regime in [
        "HIST",
        "PROS",
    ]:

        print()
        print("=" * 190)
        print(
            f"{label} — {regime}"
        )
        print("=" * 190)

        for r in results[label][regime]:

            print(
                f"{r['feature']:24} "
                f"| N={r['n']:4d} "
                f"| YES={r['pos']:3d} "
                f"| NO={r['neg']:4d} "
                f"| DIFF={fmt(r['diff']):>8} "
                f"| DIR={str(r['direction']):6} "
                f"| AUC={fmt(r['auc'])}"
            )


# ============================================================
# F) CROSS-REGIME STABILITY
# ============================================================

print()
print("=" * 190)
print(
    "F) CROSS-REGIME DIRECTIONAL STABILITY"
)
print("=" * 190)


stable = {}


for label, target in targets:

    hm = {
        r["feature"]: r
        for r in results[label]["HIST"]
    }

    pm = {
        r["feature"]: r
        for r in results[label]["PROS"]
    }

    candidates = []

    for f in FEATURES:

        h = hm[f]
        p = pm[f]

        if (
            h["auc"] is not None
            and p["auc"] is not None
            and h["direction"] == p["direction"]
            and h["auc"] >= 0.55
            and p["auc"] >= 0.55
        ):

            candidates.append(
                (
                    min(
                        h["auc"],
                        p["auc"]
                    ),
                    f,
                    h["direction"],
                    h["auc"],
                    p["auc"],
                    h["n"],
                    p["n"],
                )
            )

    candidates.sort(
        reverse=True
    )

    stable[label] = candidates

    print()
    print(label)

    if not candidates:

        print(
            "  No same-direction cross-regime candidate >=0.55."
        )

    else:

        for (
            score,
            f,
            d,
            ha,
            pa,
            hn,
            pn
        ) in candidates:

            print(
                f"  {f:24} "
                f"| DIR={d:6} "
                f"| HIST={ha:.3f} N={hn:4d} "
                f"| PROS={pa:.3f} N={pn:4d}"
            )


# ============================================================
# G) SAME FEATURE OPPOSITE MECHANISMS?
# ============================================================

print()
print("=" * 190)
print(
    "G) FEATURE DIRECTION — UP vs DOWN ACTIVATION"
)
print("=" * 190)


for f in FEATURES:

    hup = next(
        r
        for r in results[
            "UP_ACTIVATION"
        ][
            "HIST"
        ]
        if r["feature"] == f
    )

    hdown = next(
        r
        for r in results[
            "DOWN_ACTIVATION"
        ][
            "HIST"
        ]
        if r["feature"] == f
    )

    pup = next(
        r
        for r in results[
            "UP_ACTIVATION"
        ][
            "PROS"
        ]
        if r["feature"] == f
    )

    pdown = next(
        r
        for r in results[
            "DOWN_ACTIVATION"
        ][
            "PROS"
        ]
        if r["feature"] == f
    )


    print(
        f"{f:24} "
        f"| HIST UP={str(hup['direction']):6}/{fmt(hup['auc'])} "
        f"DOWN={str(hdown['direction']):6}/{fmt(hdown['auc'])} "
        f"| PROS UP={str(pup['direction']):6}/{fmt(pup['auc'])} "
        f"DOWN={str(pdown['direction']):6}/{fmt(pdown['auc'])}"
    )


# ============================================================
# H) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 190)
print("H) FIRST-EVENT/TOKEN DIRECTIONAL ACTIVATION")
print("=" * 190)


def first_token(rr):

    seen = set()
    out = []

    for r in rr:

        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


hf = first_token(hist)
pf = first_token(pros)


for target_label, target in [
    ("UP", "up_activation"),
    ("DOWN", "down_activation"),
]:

    print()
    print(target_label)

    for f in FEATURES:

        ha, hd, hn = auc_directional(
            hf,
            target,
            f
        )

        pa, pd, pn = auc_directional(
            pf,
            target,
            f
        )

        if (
            ha is not None
            and pa is not None
            and hd == pd
            and ha >= 0.55
            and pa >= 0.55
        ):

            print(
                f"  {f:24} "
                f"| DIR={hd:6} "
                f"| HIST={ha:.3f} N={hn:3d} "
                f"| PROS={pa:.3f} N={pn:3d}"
            )


# ============================================================
# I) DECISION SUPPORT
# ============================================================

print()
print("=" * 190)
print("I) DECISION SUPPORT")
print("=" * 190)


up = stable[
    "UP_ACTIVATION"
]

down = stable[
    "DOWN_ACTIVATION"
]

upc = stable[
    "UP_CONTINUATION"
]

downc = stable[
    "DOWN_CONTINUATION"
]


if up or down:

    print(
        "🟡 DIRECTIONAL DECOMPOSITION RECOVERS "
        "CROSS-REGIME ACTIVATION SIGNAL."
    )

    if up:

        print(
            f"TOP UP ACTIVATION   = {up[0][1]} "
            f"| DIR={up[0][2]} "
            f"| HIST={up[0][3]:.3f} "
            f"| PROS={up[0][4]:.3f}"
        )

    else:

        print(
            "UP ACTIVATION       = unresolved"
        )


    if down:

        print(
            f"TOP DOWN ACTIVATION = {down[0][1]} "
            f"| DIR={down[0][2]} "
            f"| HIST={down[0][3]:.3f} "
            f"| PROS={down[0][4]:.3f}"
        )

    else:

        print(
            "DOWN ACTIVATION     = unresolved"
        )

else:

    print(
        "🔴 DIRECTIONAL DECOMPOSITION DOES NOT "
        "RESCUE ACTIVATION."
    )


print()

if upc or downc:

    print(
        "Directional continuation also contains candidate signal."
    )

    if upc:

        print(
            f"TOP UP CONTINUATION   = {upc[0][1]} "
            f"| {upc[0][2]}"
        )

    if downc:

        print(
            f"TOP DOWN CONTINUATION = {downc[0][1]} "
            f"| {downc[0][2]}"
        )

else:

    print(
        "Continuation remains unresolved after directional split."
    )


print()
print("IMPORTANT:")
print("• ±3% activation thresholds were pre-declared.")
print("• ±10% continuation thresholds were pre-declared.")
print("• UP and DOWN are analyzed separately.")
print("• No feature threshold optimization.")
print("• No model fitting.")
print("• No interaction search.")
print("• T95 writes nothing to DB.")
print("• Frozen prospective branches remain untouched.")

db.close()
