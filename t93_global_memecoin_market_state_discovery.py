#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

ACTIVATION_THRESHOLD = 3.0
CONTINUATION_THRESHOLD = 10.0

WINDOW_5M = 300.0
WINDOW_15M = 900.0
WINDOW_30M = 1800.0

# Minimum completed observations required for outcome-history features.
MIN_PRIOR_OUTCOMES = 5


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


def auc_directional(rows, target, feature):

    pairs = [
        (r[target], r[feature])
        for r in rows
        if (
            r[target] is not None
            and valid(r[feature])
        )
    ]

    pos = [
        x for y, x in pairs
        if y == 1
    ]

    neg = [
        x for y, x in pairs
        if y == 0
    ]

    if not pos or not neg:
        return None, None, len(pairs)

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:
            total += 1

            if a > b:
                wins += 1
            elif a == b:
                wins += 0.5

    raw = wins / total

    if raw >= 0.5:
        return raw, "HIGHER", len(pairs)

    return 1.0-raw, "LOWER", len(pairs)


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


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    swaps5,
    swaps10,
    swaps30,
    swaps60,

    buyers5,
    buyers10,
    buyers30,
    buyers60,

    new_wallets10,
    new_wallets30,

    dex_return_30s,
    dex_done_30s,

    dex_return_300s,
    dex_done_300s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


# ============================================================
# BUILD BASE TARGETS
# ============================================================

base = []

for e in events:

    activation = None

    if (
        e["dex_done_30s"] == 1
        and valid(e["dex_return_30s"])
    ):
        activation = int(
            abs(e["dex_return_30s"])
            >= ACTIVATION_THRESHOLD
        )


    continuation = None

    if (
        activation == 1
        and e["dex_done_300s"] == 1
        and valid(e["dex_return_300s"])
    ):
        continuation = int(
            abs(e["dex_return_300s"])
            >= CONTINUATION_THRESHOLD
        )


    base.append({
        "id": e["id"],
        "timestamp": e["timestamp"],
        "token_mint": e["token_mint"],

        "historical":
            e["id"] <= boundary,

        "activation":
            activation,

        "continuation":
            continuation,

        "swaps5":
            e["swaps5"],

        "swaps10":
            e["swaps10"],

        "swaps30":
            e["swaps30"],

        "swaps60":
            e["swaps60"],

        "buyers5":
            e["buyers5"],

        "buyers10":
            e["buyers10"],

        "buyers30":
            e["buyers30"],

        "buyers60":
            e["buyers60"],

        "new_wallets10":
            e["new_wallets10"],

        "new_wallets30":
            e["new_wallets30"],

        "r30":
            e["dex_return_30s"],

        "r300":
            e["dex_return_300s"],
    })


# ============================================================
# GLOBAL MARKET-STATE FEATURES
# ============================================================

records = []

for i, e in enumerate(base):

    t = e["timestamp"]

    prior = base[:i]


    # --------------------------------------------------------
    # EVENT DENSITY
    # --------------------------------------------------------

    prior_5m = [
        r for r in prior
        if r["timestamp"] >= t-WINDOW_5M
    ]

    prior_15m = [
        r for r in prior
        if r["timestamp"] >= t-WINDOW_15M
    ]

    prior_30m = [
        r for r in prior
        if r["timestamp"] >= t-WINDOW_30M
    ]


    events_5m = len(prior_5m)
    events_15m = len(prior_15m)
    events_30m = len(prior_30m)


    unique_tokens_5m = len({
        r["token_mint"]
        for r in prior_5m
    })

    unique_tokens_15m = len({
        r["token_mint"]
        for r in prior_15m
    })

    unique_tokens_30m = len({
        r["token_mint"]
        for r in prior_30m
    })


    # --------------------------------------------------------
    # AGGREGATED CURRENT MARKET ACTIVITY
    # These fields are already event-time/pre-event snapshots.
    # --------------------------------------------------------

    total_swaps30_5m = sum(
        r["swaps30"]
        for r in prior_5m
        if valid(r["swaps30"])
    )

    total_buyers30_5m = sum(
        r["buyers30"]
        for r in prior_5m
        if valid(r["buyers30"])
    )

    total_newwallets30_5m = sum(
        r["new_wallets30"]
        for r in prior_5m
        if valid(r["new_wallets30"])
    )


    total_swaps30_15m = sum(
        r["swaps30"]
        for r in prior_15m
        if valid(r["swaps30"])
    )

    total_buyers30_15m = sum(
        r["buyers30"]
        for r in prior_15m
        if valid(r["buyers30"])
    )

    total_newwallets30_15m = sum(
        r["new_wallets30"]
        for r in prior_15m
        if valid(r["new_wallets30"])
    )


    # --------------------------------------------------------
    # PROSPECTIVE-SAFE OUTCOME HISTORY
    #
    # R30 known only after +30s.
    # R300 known only after +300s.
    # --------------------------------------------------------

    completed_r30_15m = [
        r
        for r in prior
        if (
            r["timestamp"] >= t-WINDOW_15M
            and r["timestamp"] <= t-30.0
            and valid(r["r30"])
        )
    ]


    completed_r300_30m = [
        r
        for r in prior
        if (
            r["timestamp"] >= t-WINDOW_30M
            and r["timestamp"] <= t-300.0
            and valid(r["r300"])
        )
    ]


    activation_rate_prior_15m = None
    median_abs_r30_prior_15m = None

    if len(completed_r30_15m) >= MIN_PRIOR_OUTCOMES:

        activation_rate_prior_15m = (
            sum(
                abs(r["r30"])
                >= ACTIVATION_THRESHOLD
                for r in completed_r30_15m
            )
            / len(completed_r30_15m)
        )

        median_abs_r30_prior_15m = med([
            abs(r["r30"])
            for r in completed_r30_15m
        ])


    tail300_rate_prior_30m = None
    median_abs_r300_prior_30m = None

    if len(completed_r300_30m) >= MIN_PRIOR_OUTCOMES:

        tail300_rate_prior_30m = (
            sum(
                abs(r["r300"])
                >= CONTINUATION_THRESHOLD
                for r in completed_r300_30m
            )
            / len(completed_r300_30m)
        )

        median_abs_r300_prior_30m = med([
            abs(r["r300"])
            for r in completed_r300_30m
        ])


    # --------------------------------------------------------
    # MARKET CONCENTRATION
    # share of recent events belonging to most active token
    # --------------------------------------------------------

    token_counts = {}

    for r in prior_15m:

        tok = r["token_mint"]

        token_counts[tok] = (
            token_counts.get(tok, 0)
            + 1
        )


    top_token_event_share_15m = None

    if prior_15m:

        top_token_event_share_15m = (
            max(token_counts.values())
            / len(prior_15m)
        )


    # --------------------------------------------------------
    # EVENT/TOKEN DENSITY RATIO
    # More repeated events per currently active token.
    # --------------------------------------------------------

    events_per_token_15m = None

    if unique_tokens_15m > 0:

        events_per_token_15m = (
            events_15m
            / unique_tokens_15m
        )


    records.append({
        **e,

        "events_5m":
            events_5m,

        "events_15m":
            events_15m,

        "events_30m":
            events_30m,

        "unique_tokens_5m":
            unique_tokens_5m,

        "unique_tokens_15m":
            unique_tokens_15m,

        "unique_tokens_30m":
            unique_tokens_30m,

        "total_swaps30_5m":
            total_swaps30_5m,

        "total_buyers30_5m":
            total_buyers30_5m,

        "total_newwallets30_5m":
            total_newwallets30_5m,

        "total_swaps30_15m":
            total_swaps30_15m,

        "total_buyers30_15m":
            total_buyers30_15m,

        "total_newwallets30_15m":
            total_newwallets30_15m,

        "activation_rate_prior_15m":
            activation_rate_prior_15m,

        "median_abs_r30_prior_15m":
            median_abs_r30_prior_15m,

        "tail300_rate_prior_30m":
            tail300_rate_prior_30m,

        "median_abs_r300_prior_30m":
            median_abs_r300_prior_30m,

        "top_token_event_share_15m":
            top_token_event_share_15m,

        "events_per_token_15m":
            events_per_token_15m,
    })


# ============================================================
# FEATURES
# ============================================================

FEATURES = [
    "events_5m",
    "events_15m",
    "events_30m",

    "unique_tokens_5m",
    "unique_tokens_15m",
    "unique_tokens_30m",

    "total_swaps30_5m",
    "total_buyers30_5m",
    "total_newwallets30_5m",

    "total_swaps30_15m",
    "total_buyers30_15m",
    "total_newwallets30_15m",

    "activation_rate_prior_15m",
    "median_abs_r30_prior_15m",

    "tail300_rate_prior_30m",
    "median_abs_r300_prior_30m",

    "top_token_event_share_15m",
    "events_per_token_15m",
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
    "MEMECOIN LAB — T93 GLOBAL MEMECOIN MARKET-STATE DISCOVERY"
)
print("=" * 190)

print("MODE                    : READ-ONLY")
print("MODEL FITTING           : NONE")
print("THRESHOLD SEARCH        : NONE")
print("DB WRITES               : NONE")
print("T59/T78/T82/T86         : UNTOUCHED")
print()
print(f"T59 BOUNDARY            : {boundary}")
print(
    f"ACTIVATION              : |R30| >= {ACTIVATION_THRESHOLD:.1f}%"
)
print(
    f"CONTINUATION            : |R300| >= {CONTINUATION_THRESHOLD:.1f}% among activated"
)
print()
print(
    "OUTCOME-BASED MARKET STATE IS STRICTLY DELAYED:"
)
print(
    "R30 features require source event <= current_time - 30s."
)
print(
    "R300 features require source event <= current_time - 300s."
)


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

    act = [
        r for r in rr
        if r["activation"] is not None
    ]

    cont = [
        r for r in rr
        if r["continuation"] is not None
    ]

    act_yes = sum(
        r["activation"] == 1
        for r in act
    )

    cont_yes = sum(
        r["continuation"] == 1
        for r in cont
    )

    print(
        f"{name:5} "
        f"| ACT N={len(act):4d} YES={act_yes:4d} "
        f"| CONT N={len(cont):4d} YES={cont_yes:4d}"
    )


# ============================================================
# B) ACTIVATION RANKING — HIST
# ============================================================

print()
print("=" * 190)
print("B) HISTORICAL — MARKET STATE vs ACTIVATION")
print("=" * 190)

hist_activation_results = {}

for f in FEATURES:

    a, direction, n = auc_directional(
        hist,
        "activation",
        f
    )

    hist_activation_results[f] = (
        a,
        direction,
        n
    )

    print(
        f"{f:32} "
        f"| N={n:4d} "
        f"| DIR={str(direction):6} "
        f"| AUC={fmt(a)}"
    )


# ============================================================
# C) ACTIVATION RANKING — PROS
# ============================================================

print()
print("=" * 190)
print("C) PROSPECTIVE — MARKET STATE vs ACTIVATION")
print("=" * 190)

pros_activation_results = {}

for f in FEATURES:

    a, direction, n = auc_directional(
        pros,
        "activation",
        f
    )

    pros_activation_results[f] = (
        a,
        direction,
        n
    )

    print(
        f"{f:32} "
        f"| N={n:4d} "
        f"| DIR={str(direction):6} "
        f"| AUC={fmt(a)}"
    )


# ============================================================
# D) CROSS-REGIME ACTIVATION
# ============================================================

print()
print("=" * 190)
print("D) CROSS-REGIME STABILITY — ACTIVATION")
print("=" * 190)

stable_activation = []

for f in FEATURES:

    ha, hd, hn = hist_activation_results[f]
    pa, pd, pn = pros_activation_results[f]

    if (
        ha is not None
        and pa is not None
        and hd == pd
        and ha >= 0.55
        and pa >= 0.55
    ):

        stable_activation.append(
            (
                min(ha, pa),
                f,
                hd,
                ha,
                pa,
                hn,
                pn
            )
        )


stable_activation.sort(
    reverse=True
)


if not stable_activation:

    print(
        "No market-state activation feature reaches "
        "AUC >=0.55 in the same direction across both regimes."
    )

else:

    for _, f, d, ha, pa, hn, pn in stable_activation:

        print(
            f"{f:32} "
            f"| DIR={d:6} "
            f"| HIST={ha:.3f} N={hn:4d} "
            f"| PROS={pa:.3f} N={pn:4d}"
        )


# ============================================================
# E) CONTINUATION RANKING — HIST
# ============================================================

print()
print("=" * 190)
print("E) HISTORICAL — MARKET STATE vs CONTINUATION")
print("=" * 190)

hist_cont_results = {}

for f in FEATURES:

    a, direction, n = auc_directional(
        hist,
        "continuation",
        f
    )

    hist_cont_results[f] = (
        a,
        direction,
        n
    )

    print(
        f"{f:32} "
        f"| N={n:4d} "
        f"| DIR={str(direction):6} "
        f"| AUC={fmt(a)}"
    )


# ============================================================
# F) CONTINUATION RANKING — PROS
# ============================================================

print()
print("=" * 190)
print("F) PROSPECTIVE — MARKET STATE vs CONTINUATION")
print("=" * 190)

pros_cont_results = {}

for f in FEATURES:

    a, direction, n = auc_directional(
        pros,
        "continuation",
        f
    )

    pros_cont_results[f] = (
        a,
        direction,
        n
    )

    print(
        f"{f:32} "
        f"| N={n:4d} "
        f"| DIR={str(direction):6} "
        f"| AUC={fmt(a)}"
    )


# ============================================================
# G) CROSS-REGIME CONTINUATION
# ============================================================

print()
print("=" * 190)
print("G) CROSS-REGIME STABILITY — CONTINUATION")
print("=" * 190)

stable_continuation = []

for f in FEATURES:

    ha, hd, hn = hist_cont_results[f]
    pa, pd, pn = pros_cont_results[f]

    if (
        ha is not None
        and pa is not None
        and hd == pd
        and ha >= 0.55
        and pa >= 0.55
    ):

        stable_continuation.append(
            (
                min(ha, pa),
                f,
                hd,
                ha,
                pa,
                hn,
                pn
            )
        )


stable_continuation.sort(
    reverse=True
)


if not stable_continuation:

    print(
        "No market-state continuation feature reaches "
        "AUC >=0.55 in the same direction across both regimes."
    )

else:

    for _, f, d, ha, pa, hn, pn in stable_continuation:

        print(
            f"{f:32} "
            f"| DIR={d:6} "
            f"| HIST={ha:.3f} N={hn:4d} "
            f"| PROS={pa:.3f} N={pn:4d}"
        )


# ============================================================
# H) MARKET-STATE DISTRIBUTION SHIFT
# ============================================================

print()
print("=" * 190)
print("H) MARKET-STATE MEDIANS — HIST vs PROS")
print("=" * 190)

for f in FEATURES:

    hv = [
        r[f]
        for r in hist
        if valid(r[f])
    ]

    pv = [
        r[f]
        for r in pros
        if valid(r[f])
    ]

    print(
        f"{f:32} "
        f"| HIST N={len(hv):4d} MED={fmt(med(hv)):>8} "
        f"| PROS N={len(pv):4d} MED={fmt(med(pv)):>8}"
    )


# ============================================================
# I) FIRST-EVENT/TOKEN ACTIVATION
# ============================================================

print()
print("=" * 190)
print("I) FIRST-EVENT/TOKEN MARKET-STATE ACTIVATION")
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


for f in FEATURES:

    ha, hd, hn = auc_directional(
        hf,
        "activation",
        f
    )

    pa, pd, pn = auc_directional(
        pf,
        "activation",
        f
    )

    print(
        f"{f:32} "
        f"| HIST DIR={str(hd):6} AUC={fmt(ha)} N={hn:3d} "
        f"| PROS DIR={str(pd):6} AUC={fmt(pa)} N={pn:3d}"
    )


# ============================================================
# J) DECISION SUPPORT
# ============================================================

print()
print("=" * 190)
print("J) DECISION SUPPORT")
print("=" * 190)


if stable_activation and stable_continuation:

    print(
        "🟢 GLOBAL MARKET STATE HAS CROSS-REGIME SIGNAL "
        "FOR BOTH ACTIVATION AND CONTINUATION."
    )

    print(
        f"TOP ACTIVATION     = {stable_activation[0][1]}"
    )

    print(
        f"TOP CONTINUATION   = {stable_continuation[0][1]}"
    )

    print(
        "Next = freeze directions and robustness-audit each target separately."
    )


elif stable_activation:

    print(
        "🟡 GLOBAL MARKET STATE EXPLAINS ACTIVATION "
        "BETTER THAN CURRENT TOKEN FEATURES."
    )

    print(
        f"TOP ACTIVATION CANDIDATE = {stable_activation[0][1]}"
    )

    print(
        "Continuation still requires another mechanism."
    )


elif stable_continuation:

    print(
        "🟡 GLOBAL MARKET STATE EXPLAINS CONTINUATION "
        "BUT NOT ACTIVATION."
    )

    print(
        f"TOP CONTINUATION CANDIDATE = {stable_continuation[0][1]}"
    )


else:

    print(
        "🔴 NO CROSS-REGIME GLOBAL MARKET-STATE FEATURE SURVIVES."
    )

    print(
        "The activation/continuation collapse is not explained "
        "by these ecosystem-level state variables."
    )


print()
print("IMPORTANT:")
print("• Market-state features use only prior events.")
print("• Outcome-derived R30 stats are delayed by >=30s.")
print("• Outcome-derived R300 stats are delayed by >=300s.")
print("• No future outcomes enter current-event features.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No interaction search.")
print("• T93 writes nothing to DB.")
print("• All frozen prospective branches remain untouched.")

db.close()
