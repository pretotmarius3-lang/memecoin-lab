import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

EPS = 0.05


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


def sdiv(a, b, eps=EPS):
    if not valid(a) or not valid(b):
        return None

    if abs(b) < eps:
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
    """
    Returns:
      raw_auc    : higher feature => RUN
      dir_auc    : max(raw_auc, 1-raw_auc)
      direction  : HIGHER or LOWER for RUN
    """

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

    return raw, 1.0 - raw, "LOWER"


def pearson(xs, ys):
    pairs = [
        (x, y)
        for x, y in zip(xs, ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    x = [a for a, _ in pairs]
    y = [b for _, b in pairs]

    mx = avg(x)
    my = avg(y)

    vx = sum((a-mx)**2 for a in x)
    vy = sum((b-my)**2 for b in y)

    if vx <= 0 or vy <= 0:
        return None

    cov = sum(
        (a-mx)*(b-my)
        for a, b in zip(x, y)
    )

    return cov / math.sqrt(vx*vy)


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
    e.dex_return_60s,

    s.early_buy_count,
    s.mid_buy_count,
    s.recent_buy_count,

    s.early_sell_count,
    s.mid_sell_count,
    s.recent_sell_count,

    s.early_buy_sol,
    s.mid_buy_sol,
    s.recent_buy_sol,

    s.early_sell_sol,
    s.mid_sell_sol,
    s.recent_sell_sol,

    s.early_net_sol,
    s.mid_net_sol,
    s.recent_net_sol,

    s.early_unique_buyers,
    s.mid_unique_buyers,
    s.recent_unique_buyers,

    s.early_unique_sellers,
    s.mid_unique_sellers,
    s.recent_unique_sellers,

    s.early_price_return,
    s.mid_price_return,
    s.recent_price_return,

    s.early_swaps_per_sec,
    s.mid_swaps_per_sec,
    s.recent_swaps_per_sec,

    s.early_duration,
    s.mid_duration,
    s.recent_duration,

    s.early_buy_concentration,
    s.mid_buy_concentration,
    s.recent_buy_concentration,

    s.early_median_buy,
    s.mid_median_buy,
    s.recent_median_buy,

    s.buy_count_trend,
    s.buy_sol_trend,
    s.net_sol_trend,
    s.buyer_diversity_trend,
    s.buy_concentration_trend,
    s.frequency_trend,

    s.recent_buy_share,
    s.recent_net_share,
    s.price_response_recent,
    s.late_chase_score,
    s.breadth_score

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL
    AND e.dex_return_60s IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


# ============================================================
# FEATURE ENGINEERING
# ============================================================

records = []


for r in rows:

    y = label_r60(
        r["dex_return_60s"]
    )

    if y is None:
        continue


    f = {}


    # --------------------------------------------------------
    # 1) BUY FLOW RETENTION
    # --------------------------------------------------------

    f["buy_sol_recent_vs_early"] = sdiv(
        r["recent_buy_sol"],
        r["early_buy_sol"]
    )

    f["buy_sol_mid_vs_early"] = sdiv(
        r["mid_buy_sol"],
        r["early_buy_sol"]
    )

    f["buy_sol_recent_vs_mid"] = sdiv(
        r["recent_buy_sol"],
        r["mid_buy_sol"]
    )


    # --------------------------------------------------------
    # 2) NET FLOW RETENTION
    # --------------------------------------------------------

    f["net_sol_recent_vs_early"] = sdiv(
        r["recent_net_sol"],
        r["early_net_sol"]
    )

    f["net_sol_mid_vs_early"] = sdiv(
        r["mid_net_sol"],
        r["early_net_sol"]
    )

    f["net_sol_recent_vs_mid"] = sdiv(
        r["recent_net_sol"],
        r["mid_net_sol"]
    )


    # --------------------------------------------------------
    # 3) BUYER RETENTION / EXPANSION
    # --------------------------------------------------------

    f["buyers_recent_vs_early"] = sdiv(
        r["recent_unique_buyers"],
        r["early_unique_buyers"],
        eps=1.0
    )

    f["buyers_mid_vs_early"] = sdiv(
        r["mid_unique_buyers"],
        r["early_unique_buyers"],
        eps=1.0
    )

    f["buyers_recent_vs_mid"] = sdiv(
        r["recent_unique_buyers"],
        r["mid_unique_buyers"],
        eps=1.0
    )


    # --------------------------------------------------------
    # 4) ACTIVITY / FREQUENCY RETENTION
    # --------------------------------------------------------

    f["freq_recent_vs_early"] = sdiv(
        r["recent_swaps_per_sec"],
        r["early_swaps_per_sec"],
        eps=1e-6
    )

    f["freq_mid_vs_early"] = sdiv(
        r["mid_swaps_per_sec"],
        r["early_swaps_per_sec"],
        eps=1e-6
    )

    f["freq_recent_vs_mid"] = sdiv(
        r["recent_swaps_per_sec"],
        r["mid_swaps_per_sec"],
        eps=1e-6
    )


    # --------------------------------------------------------
    # 5) BUY COUNT RETENTION
    # --------------------------------------------------------

    f["buy_count_recent_vs_early"] = sdiv(
        r["recent_buy_count"],
        r["early_buy_count"],
        eps=1.0
    )

    f["buy_count_mid_vs_early"] = sdiv(
        r["mid_buy_count"],
        r["early_buy_count"],
        eps=1.0
    )

    f["buy_count_recent_vs_mid"] = sdiv(
        r["recent_buy_count"],
        r["mid_buy_count"],
        eps=1.0
    )


    # --------------------------------------------------------
    # 6) BREADTH / BUYER DIVERSITY
    # unique buyers per buy
    # --------------------------------------------------------

    early_breadth = sdiv(
        r["early_unique_buyers"],
        r["early_buy_count"],
        eps=1.0
    )

    mid_breadth = sdiv(
        r["mid_unique_buyers"],
        r["mid_buy_count"],
        eps=1.0
    )

    recent_breadth = sdiv(
        r["recent_unique_buyers"],
        r["recent_buy_count"],
        eps=1.0
    )


    f["breadth_early"] = early_breadth
    f["breadth_mid"] = mid_breadth
    f["breadth_recent"] = recent_breadth


    f["breadth_recent_minus_early"] = (
        recent_breadth - early_breadth
        if valid(recent_breadth)
        and valid(early_breadth)
        else None
    )


    f["breadth_recent_vs_early"] = sdiv(
        recent_breadth,
        early_breadth,
        eps=0.01
    )


    # --------------------------------------------------------
    # 7) CONCENTRATION DECAY
    # Lower concentration later may indicate broader flow.
    # --------------------------------------------------------

    f["concentration_recent_minus_early"] = (
        r["recent_buy_concentration"]
        - r["early_buy_concentration"]
        if valid(r["recent_buy_concentration"])
        and valid(r["early_buy_concentration"])
        else None
    )


    f["concentration_recent_vs_early"] = sdiv(
        r["recent_buy_concentration"],
        r["early_buy_concentration"],
        eps=0.01
    )


    # --------------------------------------------------------
    # 8) TICKET SIZE EVOLUTION
    # --------------------------------------------------------

    f["median_buy_recent_vs_early"] = sdiv(
        r["recent_median_buy"],
        r["early_median_buy"]
    )

    f["median_buy_recent_minus_early"] = (
        r["recent_median_buy"]
        - r["early_median_buy"]
        if valid(r["recent_median_buy"])
        and valid(r["early_median_buy"])
        else None
    )


    # --------------------------------------------------------
    # 9) PRICE MOMENTUM PERSISTENCE
    # --------------------------------------------------------

    f["price_recent_minus_early"] = (
        r["recent_price_return"]
        - r["early_price_return"]
        if valid(r["recent_price_return"])
        and valid(r["early_price_return"])
        else None
    )


    f["price_mid_minus_early"] = (
        r["mid_price_return"]
        - r["early_price_return"]
        if valid(r["mid_price_return"])
        and valid(r["early_price_return"])
        else None
    )


    f["price_recent_minus_mid"] = (
        r["recent_price_return"]
        - r["mid_price_return"]
        if valid(r["recent_price_return"])
        and valid(r["mid_price_return"])
        else None
    )


    # --------------------------------------------------------
    # 10) FLOW ACCELERATION
    # Difference of changes instead of ratios.
    # --------------------------------------------------------

    if (
        valid(r["early_net_sol"])
        and valid(r["mid_net_sol"])
        and valid(r["recent_net_sol"])
    ):
        f["net_flow_acceleration"] = (
            (r["recent_net_sol"] - r["mid_net_sol"])
            -
            (r["mid_net_sol"] - r["early_net_sol"])
        )
    else:
        f["net_flow_acceleration"] = None


    if (
        valid(r["early_buy_sol"])
        and valid(r["mid_buy_sol"])
        and valid(r["recent_buy_sol"])
    ):
        f["buy_flow_acceleration"] = (
            (r["recent_buy_sol"] - r["mid_buy_sol"])
            -
            (r["mid_buy_sol"] - r["early_buy_sol"])
        )
    else:
        f["buy_flow_acceleration"] = None


    if (
        valid(r["early_unique_buyers"])
        and valid(r["mid_unique_buyers"])
        and valid(r["recent_unique_buyers"])
    ):
        f["buyer_acceleration"] = (
            (r["recent_unique_buyers"] - r["mid_unique_buyers"])
            -
            (r["mid_unique_buyers"] - r["early_unique_buyers"])
        )
    else:
        f["buyer_acceleration"] = None


    # --------------------------------------------------------
    # 11) EXISTING TREND FEATURES
    # Included as reference / sanity check.
    # --------------------------------------------------------

    for name in [
        "buy_count_trend",
        "buy_sol_trend",
        "net_sol_trend",
        "buyer_diversity_trend",
        "buy_concentration_trend",
        "frequency_trend",
    ]:
        f[name] = r[name]


    # --------------------------------------------------------
    # 12) RAW CONTEXT REFERENCES
    # Not candidates for promotion here.
    # --------------------------------------------------------

    context = {
        "recent_buy_share":
            r["recent_buy_share"],

        "recent_net_share":
            r["recent_net_share"],

        "price_response_recent":
            r["price_response_recent"],

        "late_chase_score":
            r["late_chase_score"],

        "breadth_score":
            r["breadth_score"],
    }


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],
        "y": y,
        "features": f,
        "context": context,
    })


# ============================================================
# FEATURE LIST
# ============================================================

feature_names = sorted(
    set(
        k
        for r in records
        for k in r["features"].keys()
    )
)


# ============================================================
# GLOBAL AUDIT
# ============================================================

global_results = []


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


    raw_auc, dir_auc, direction = auc_directional(
        y,
        x
    )


    run_vals = [
        xx
        for yy, xx in zip(y, x)
        if yy == 1
    ]

    dump_vals = [
        xx
        for yy, xx in zip(y, x)
        if yy == 0
    ]


    global_results.append({
        "feature": feature,
        "n": len(rr),
        "run_n": len(run_vals),
        "dump_n": len(dump_vals),
        "run_med": med(run_vals),
        "dump_med": med(dump_vals),
        "diff": (
            med(run_vals) - med(dump_vals)
            if run_vals and dump_vals
            else None
        ),
        "raw_auc": raw_auc,
        "dir_auc": dir_auc,
        "direction": direction,
    })


global_results.sort(
    key=lambda r: (
        -(r["dir_auc"] or 0),
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

    raw_auc, dir_auc, direction = auc_directional(
        y,
        x
    )

    first_results[feature] = {
        "n": len(rr),
        "dir_auc": dir_auc,
        "direction": direction,
    }


# ============================================================
# CHRONOLOGICAL HALF STABILITY
# ============================================================

midpoint = len(records) // 2

halves = {
    "EARLY_HALF":
        records[:midpoint],

    "LATE_HALF":
        records[midpoint:],
}


half_results = defaultdict(dict)


for half_name, rr0 in halves.items():

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
            half_name
        ] = {
            "n": len(rr),
            "auc": da,
            "direction": direction,
        }


# ============================================================
# REDUNDANCY AGAINST EXISTING CONTEXT
# ============================================================

context_names = [
    "recent_buy_share",
    "recent_net_share",
    "price_response_recent",
    "late_chase_score",
    "breadth_score",
]


redundancy = {}


for feature in feature_names:

    redundancy[feature] = {}

    for ctx in context_names:

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
            ].get(
                ctx
            )

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

print("=" * 190)
print("MEMECOIN LAB — T60 FLOW PERSISTENCE / DECAY DISCOVERY")
print("=" * 190)

print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in records))}"
)

print(
    f"CANDIDATES     : {len(feature_names)}"
)

print()
print(
    "DISCOVERY ONLY — NO MODEL FITTING / NO THRESHOLD SEARCH"
)


# ============================================================
# A
# ============================================================

print()
print("=" * 190)
print("A) GLOBAL UNIVARIATE RANKING")
print("=" * 190)

for r in global_results:

    print(
        f"{r['feature']:36} "
        f"N={r['n']:3d} "
        f"RUN={r['run_n']:3d} "
        f"DUMP={r['dump_n']:3d} "
        f"RUN_MED={fmt(r['run_med']):>9} "
        f"DUMP_MED={fmt(r['dump_med']):>9} "
        f"DIFF={fmt(r['diff']):>9} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['dir_auc'])}"
    )


# ============================================================
# B
# ============================================================

print()
print("=" * 190)
print("B) TOP 15 — FIRST EVENT / TOKEN")
print("=" * 190)

top15 = [
    r["feature"]
    for r in global_results[:15]
]


for feature in top15:

    r = first_results[
        feature
    ]

    print(
        f"{feature:36} "
        f"N={r['n']:3d} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['dir_auc'])}"
    )


# ============================================================
# C
# ============================================================

print()
print("=" * 190)
print("C) TOP 15 — CHRONOLOGICAL HALF STABILITY")
print("=" * 190)

for feature in top15:

    a = half_results[
        feature
    ][
        "EARLY_HALF"
    ]

    b = half_results[
        feature
    ][
        "LATE_HALF"
    ]

    print(
        f"{feature:36} "
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
print("=" * 190)
print("D) TOP 15 — MAX ABS CORRELATION WITH EXISTING CONTEXT")
print("=" * 190)

for feature in top15:

    vals = [
        abs(x)
        for x in redundancy[
            feature
        ].values()
        if valid(x)
    ]

    maxcorr = (
        max(vals)
        if vals
        else None
    )

    strongest_ctx = None
    strongest_val = None

    for ctx, corr in redundancy[
        feature
    ].items():

        if not valid(corr):
            continue

        if (
            strongest_val is None
            or abs(corr) > abs(strongest_val)
        ):
            strongest_val = corr
            strongest_ctx = ctx

    print(
        f"{feature:36} "
        f"MAX|CORR|={fmt(maxcorr)} "
        f"| WITH={str(strongest_ctx):24} "
        f"| CORR={fmt(strongest_val)}"
    )


# ============================================================
# E — CONSERVATIVE DISCOVERY SCORECARD
# ============================================================

print()
print("=" * 190)
print("E) CONSERVATIVE DISCOVERY SCORECARD")
print("=" * 190)


candidates = []


for g in global_results:

    feature = g[
        "feature"
    ]

    fr = first_results[
        feature
    ]

    eh = half_results[
        feature
    ][
        "EARLY_HALF"
    ]

    lh = half_results[
        feature
    ][
        "LATE_HALF"
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


    same_half_direction = (
        eh["direction"] is not None
        and lh["direction"] is not None
        and eh["direction"] == lh["direction"]
    )


    same_global_first_direction = (
        g["direction"] is not None
        and fr["direction"] is not None
        and g["direction"] == fr["direction"]
    )


    passes = (
        g["n"] >= 80
        and g["dir_auc"] is not None
        and g["dir_auc"] >= 0.57
        and fr["dir_auc"] is not None
        and fr["dir_auc"] >= 0.55
        and eh["auc"] is not None
        and eh["auc"] >= 0.53
        and lh["auc"] is not None
        and lh["auc"] >= 0.53
        and same_half_direction
        and same_global_first_direction
        and (
            maxcorr is None
            or maxcorr <= 0.75
        )
    )


    if passes:

        candidates.append(
            (
                feature,
                g["direction"],
                g["dir_auc"],
                fr["dir_auc"],
                eh["auc"],
                lh["auc"],
                maxcorr,
            )
        )


if candidates:

    candidates.sort(
        key=lambda x: (
            -min(
                x[2],
                x[3],
                x[4],
                x[5],
            ),
            x[6] if x[6] is not None else 0
        )
    )


    for x in candidates:

        print(
            f"{x[0]:36} "
            f"| DIR={x[1]:6} "
            f"| GLOBAL={x[2]:.3f} "
            f"| FIRST={x[3]:.3f} "
            f"| EARLY={x[4]:.3f} "
            f"| LATE={x[5]:.3f} "
            f"| MAXCORR={fmt(x[6])}"
        )

else:

    print(
        "No candidate passes the conservative discovery gate."
    )


# ============================================================
# F
# ============================================================

print()
print("=" * 190)
print("F) DECISION SUPPORT")
print("=" * 190)


if not candidates:

    print(
        "🔴 NO FLOW-PERSISTENCE FEATURE IS STRONG ENOUGH "
        "FOR ROBUSTNESS PROMOTION."
    )

    print(
        "Do not manufacture a candidate from the ranking."
    )

    print(
        "T59 continues untouched."
    )


else:

    best = candidates[0]

    print(
        "🟡 FLOW-PERSISTENCE FAMILY CONTAINS AT LEAST "
        "ONE ROBUSTNESS CANDIDATE."
    )

    print(
        f"PRIMARY T61 CANDIDATE = {best[0]}"
    )

    print(
        f"FROZEN DIRECTION      = {best[1]}"
    )

    print(
        "Next = T61 robustness audit on this exact "
        "feature and direction."
    )

    print(
        "Do NOT optimize a threshold."
    )


print()
print("IMPORTANT:")
print("• Discovery only.")
print("• Labels remain RUN >= +10%, DUMP <= -10%.")
print("• No logistic model.")
print("• No threshold optimization.")
print("• Ratios use fixed denominator guards.")
print("• First-event/token is diagnostic.")
print("• Chronological halves are diagnostic.")
print("• Existing trend columns are reference candidates only.")
print("• T59 is completely untouched.")
print("• T23/T31/T32/T47 remain untouched.")
print("• T60 writes nothing to DB.")
print("• Any winner must pass T61 before incremental testing.")

db.close()
