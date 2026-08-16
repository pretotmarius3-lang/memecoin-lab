import sqlite3
import math
import statistics
from collections import Counter

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

N_SWAPS = 12


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


def safe_div(a, b):
    if not valid(a) or not valid(b) or abs(b) < 1e-12:
        return None
    return a / b


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
                wins += 1

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


def longest_streak(seq, value):

    best = 0
    cur = 0

    for x in seq:

        if x == value:
            cur += 1
            best = max(best, cur)

        else:
            cur = 0

    return best


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
    dex_return_60s,
    fa,
    new_wallets30

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


# ============================================================
# BUILD
# ============================================================

records = []


for e in events:

    y = label_r60(
        e["dex_return_60s"]
    )

    if y is None:
        continue


    rows = db.execute("""
    SELECT
        timestamp,
        side,
        ABS(sol_delta) AS sol

    FROM swaps

    WHERE
        token_mint=?
        AND price_valid=1
        AND timestamp < ?
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


    sides = [
        r["side"]
        for r in rows
    ]

    sols = [
        r["sol"]
        if valid(r["sol"])
        else 0.0
        for r in rows
    ]

    times = [
        r["timestamp"]
        for r in rows
    ]


    # --------------------------------------------------------
    # Direction switching / streak structure
    # --------------------------------------------------------

    switches = sum(
        sides[i] != sides[i-1]
        for i in range(1, len(sides))
    )

    alternation_rate = safe_div(
        switches,
        len(sides)-1
    )

    longest_buy_streak = longest_streak(
        sides,
        "BUY"
    )

    longest_sell_streak = longest_streak(
        sides,
        "SELL"
    )

    buy_count = sum(
        s == "BUY"
        for s in sides
    )

    sell_count = sum(
        s == "SELL"
        for s in sides
    )


    # --------------------------------------------------------
    # Last 4 dominance
    # --------------------------------------------------------

    last4 = rows[-4:]

    last4_buy_count = sum(
        r["side"] == "BUY"
        for r in last4
    )

    last4_buy_share = (
        last4_buy_count / 4
    )


    # --------------------------------------------------------
    # Volume geometry
    # --------------------------------------------------------

    buy_sols = [
        sol
        for sol,side
        in zip(sols,sides)
        if side == "BUY"
    ]

    sell_sols = [
        sol
        for sol,side
        in zip(sols,sides)
        if side == "SELL"
    ]

    total_buy = sum(buy_sols)
    total_sell = sum(sell_sols)
    total_flow = total_buy + total_sell

    buy_volume_share = safe_div(
        total_buy,
        total_flow
    )

    avg_buy_size = avg(
        buy_sols
    )

    avg_sell_size = avg(
        sell_sols
    )

    size_asymmetry = (
        avg_buy_size - avg_sell_size
        if valid(avg_buy_size)
        and valid(avg_sell_size)
        else None
    )

    size_ratio = safe_div(
        avg_buy_size,
        avg_sell_size
    )


    # --------------------------------------------------------
    # Timing geometry
    # --------------------------------------------------------

    gaps = [
        times[i] - times[i-1]
        for i in range(1, len(times))
        if times[i] >= times[i-1]
    ]

    gap_mean = avg(gaps)
    gap_std = stdev(gaps)

    gap_cv = safe_div(
        gap_std,
        gap_mean
    )

    early_gaps = gaps[:4]
    recent_gaps = gaps[-4:]

    early_gap_mean = avg(
        early_gaps
    )

    recent_gap_mean = avg(
        recent_gaps
    )

    rhythm_acceleration = (
        early_gap_mean - recent_gap_mean
        if valid(early_gap_mean)
        and valid(recent_gap_mean)
        else None
    )

    recent_vs_early_gap = safe_div(
        recent_gap_mean,
        early_gap_mean
    )


    # --------------------------------------------------------
    # Last swap dominance
    # --------------------------------------------------------

    last_sol = sols[-1]

    last_swap_flow_share = safe_div(
        last_sol,
        total_flow
    )


    # --------------------------------------------------------
    # Entropy / balance
    # --------------------------------------------------------

    p_buy = buy_count / len(sides)
    p_sell = sell_count / len(sides)

    entropy = 0.0

    for p in [p_buy, p_sell]:

        if p > 0:
            entropy -= p * math.log(p)


    # --------------------------------------------------------
    # Pattern counts
    # --------------------------------------------------------

    trigrams = Counter(
        "".join(
            "B" if x == "BUY" else "S"
            for x in sides[i:i+3]
        )
        for i in range(len(sides)-2)
    )


    features = {
        "switch_count":
            switches,

        "alternation_rate":
            alternation_rate,

        "longest_buy_streak":
            longest_buy_streak,

        "longest_sell_streak":
            longest_sell_streak,

        "buy_count":
            buy_count,

        "sell_count":
            sell_count,

        "last4_buy_share":
            last4_buy_share,

        "buy_volume_share":
            buy_volume_share,

        "avg_buy_size":
            avg_buy_size,

        "avg_sell_size":
            avg_sell_size,

        "size_asymmetry":
            size_asymmetry,

        "size_ratio":
            size_ratio,

        "gap_mean":
            gap_mean,

        "gap_std":
            gap_std,

        "gap_cv":
            gap_cv,

        "rhythm_acceleration":
            rhythm_acceleration,

        "recent_vs_early_gap":
            recent_vs_early_gap,

        "last_swap_flow_share":
            last_swap_flow_share,

        "side_entropy":
            entropy,

        "BBB_count":
            trigrams.get("BBB",0),

        "SSS_count":
            trigrams.get("SSS",0),

        "BSB_count":
            trigrams.get("BSB",0),

        "SBS_count":
            trigrams.get("SBS",0),
    }


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "y":
            y,

        "fa":
            e["fa"],

        "new_wallets30":
            e["new_wallets30"],

        "features":
            features,
    })


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

    _, da, direction = auc_directional(
        y,
        x
    )

    run = [
        xx
        for yy,xx in zip(y,x)
        if yy == 1
    ]

    dump = [
        xx
        for yy,xx in zip(y,x)
        if yy == 0
    ]

    results.append({
        "feature":
            feature,

        "n":
            len(rr),

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
    key=lambda r:
        (
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


half_results = {}


for feature in feature_names:

    half_results[
        feature
    ] = {}

    for name, rr0 in halves.items():

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
            name
        ] = {
            "n":
                len(rr),

            "auc":
                da,

            "direction":
                direction,
        }


# ============================================================
# REDUNDANCY WITH SIMPLE CONTEXT
# ============================================================

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
print("MEMECOIN LAB — T68 MICROSTRUCTURE SEQUENCE DISCOVERY")
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


print()
print("=" * 185)
print("A) GLOBAL UNIVARIATE RANKING")
print("=" * 185)

for r in results:

    print(
        f"{r['feature']:30} "
        f"N={r['n']:3d} "
        f"RUN={r['run_n']:3d} "
        f"DUMP={r['dump_n']:3d} "
        f"RUN_MED={fmt(r['run_med']):>8} "
        f"DUMP_MED={fmt(r['dump_med']):>8} "
        f"DIFF={fmt(r['diff']):>8} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['auc'])}"
    )


top15 = [
    r["feature"]
    for r in results[:15]
]


print()
print("=" * 185)
print("B) TOP 15 — FIRST EVENT / TOKEN")
print("=" * 185)

for f in top15:

    r = first_results[
        f
    ]

    print(
        f"{f:30} "
        f"N={r['n']:3d} "
        f"DIR={str(r['direction']):6} "
        f"AUC={fmt(r['auc'])}"
    )


print()
print("=" * 185)
print("C) TOP 15 — CHRONOLOGICAL HALF STABILITY")
print("=" * 185)

for f in top15:

    a = half_results[
        f
    ][
        "EARLY"
    ]

    b = half_results[
        f
    ][
        "LATE"
    ]

    print(
        f"{f:30} "
        f"| EARLY N={a['n']:3d} "
        f"DIR={str(a['direction']):6} "
        f"AUC={fmt(a['auc'])} "
        f"| LATE N={b['n']:3d} "
        f"DIR={str(b['direction']):6} "
        f"AUC={fmt(b['auc'])}"
    )


print()
print("=" * 185)
print("D) TOP 15 — CONTEXT REDUNDANCY")
print("=" * 185)

for f in top15:

    vals = [
        (ctx,c)
        for ctx,c
        in redundancy[
            f
        ].items()
        if valid(c)
    ]

    if vals:

        ctx,c = max(
            vals,
            key=lambda x:
                abs(x[1])
        )

        print(
            f"{f:30} "
            f"MAX|CORR|={abs(c):.3f} "
            f"| WITH={ctx:15} "
            f"| CORR={c:+.3f}"
        )


print()
print("=" * 185)
print("E) CONSERVATIVE DISCOVERY GATE")
print("=" * 185)

survivors = []


for g in results:

    f = g[
        "feature"
    ]

    fr = first_results[
        f
    ]

    eh = half_results[
        f
    ][
        "EARLY"
    ]

    lh = half_results[
        f
    ][
        "LATE"
    ]

    corrs = [
        abs(c)
        for c in redundancy[
            f
        ].values()
        if valid(c)
    ]

    maxcorr = (
        max(corrs)
        if corrs
        else None
    )


    same_dir = (
        g["direction"] is not None
        and fr["direction"] is not None
        and eh["direction"] is not None
        and lh["direction"] is not None
        and g["direction"]
        == fr["direction"]
        == eh["direction"]
        == lh["direction"]
    )


    passes = (
        g["n"] >= 80
        and g["auc"] is not None
        and g["auc"] >= 0.57

        and fr["auc"] is not None
        and fr["auc"] >= 0.55

        and eh["auc"] is not None
        and eh["auc"] >= 0.53

        and lh["auc"] is not None
        and lh["auc"] >= 0.53

        and same_dir

        and (
            maxcorr is None
            or maxcorr <= 0.75
        )
    )


    if passes:

        survivors.append(
            (
                f,
                g["direction"],
                g["auc"],
                fr["auc"],
                eh["auc"],
                lh["auc"],
                maxcorr,
            )
        )


survivors.sort(
    key=lambda x:
        (
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
            f"{s[0]:30} "
            f"| DIR={s[1]:6} "
            f"| GLOBAL={s[2]:.3f} "
            f"| FIRST={s[3]:.3f} "
            f"| EARLY={s[4]:.3f} "
            f"| LATE={s[5]:.3f} "
            f"| MAXCORR={fmt(s[6])}"
        )

else:

    print(
        "No microstructure feature passes the gate."
    )


print()
print("=" * 185)
print("F) DECISION SUPPORT")
print("=" * 185)

if survivors:

    best = survivors[0]

    print(
        "🟡 MICROSTRUCTURE FAMILY CONTAINS A ROBUSTNESS CANDIDATE."
    )

    print(
        f"PRIMARY T69 CANDIDATE = {best[0]}"
    )

    print(
        f"FROZEN DIRECTION      = {best[1]}"
    )

    print(
        "Next = T69 robustness audit."
    )

else:

    print(
        "🔴 NO MICROSTRUCTURE FEATURE SURVIVES."
    )

    print(
        "Do not force this family."
    )


print()
print("IMPORTANT:")
print("• Uses exactly the 12 last valid swaps before each event.")
print("• No future swaps.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No DB writes.")
print("• T59 remains frozen.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• Research discovery only.")

db.close()
