import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

HORIZONS = [20, 30, 45, 60, 90, 120]

MAX_BEFORE = 20.0
MAX_AFTER = 20.0

MAX_LOCAL_RATIO = 5.0
MAX_ABS_RETURN_FOR_ROBUST = 1000.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def percentile(vals, q):
    vals = sorted(x for x in vals if valid(x))

    if not vals:
        return None

    idx = int(round(
        (q / 100.0) * (len(vals) - 1)
    ))

    return vals[idx]


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# LOAD SAME-UNIT SWAPS
# ============================================================

rows = db.execute("""
    SELECT
        signature,
        timestamp,
        wallet,
        side,
        token_mint,
        clean_price,
        program,
        price_valid
    FROM swaps
    WHERE
        token_mint IS NOT NULL
        AND timestamp IS NOT NULL
        AND clean_price IS NOT NULL
        AND clean_price > 0
        AND (
            price_valid IS NULL
            OR price_valid = 1
        )
    ORDER BY token_mint, timestamp
""").fetchall()


by_token = defaultdict(list)

for r in rows:
    by_token[r["token_mint"]].append(r)


buys = [
    r for r in rows
    if str(r["side"]).upper() == "BUY"
]


print("=" * 165)
print("MEMECOIN LAB — T41.5 MULTI-HORIZON SAME-UNIT COVERAGE LAB")
print("=" * 165)

print("ENTRY/EXIT UNIT : swaps.clean_price -> swaps.clean_price")
print("HORIZONS        :", HORIZONS)
print()

print(f"VALID SWAPS : {len(rows)}")
print(f"BUY ENTRIES : {len(buys)}")
print(f"TOKENS      : {len(by_token)}")


# ============================================================
# HELPERS
# ============================================================

def bracket(token, target_ts):
    arr = by_token.get(token, [])

    before = None
    after = None

    for r in arr:

        ts = r["timestamp"]

        if ts <= target_ts:
            before = r
            continue

        after = r
        break

    return before, after


def local_continuity_ok(before, after):

    if before is None or after is None:
        return False

    p0 = before["clean_price"]
    p1 = after["clean_price"]

    if (
        not valid(p0)
        or not valid(p1)
        or p0 <= 0
        or p1 <= 0
    ):
        return False

    ratio = max(
        p0 / p1,
        p1 / p0
    )

    return ratio <= MAX_LOCAL_RATIO


def interpolated_price(token, target_ts):

    before, after = bracket(
        token,
        target_ts
    )

    if before is None or after is None:
        return None

    dbefore = (
        target_ts
        - before["timestamp"]
    )

    dafter = (
        after["timestamp"]
        - target_ts
    )

    if (
        dbefore < 0
        or dafter < 0
        or dbefore > MAX_BEFORE
        or dafter > MAX_AFTER
    ):
        return None

    if not local_continuity_ok(
        before,
        after
    ):
        return None

    t0 = before["timestamp"]
    t1 = after["timestamp"]

    if t1 <= t0:
        return None

    p0 = before["clean_price"]
    p1 = after["clean_price"]

    weight = (
        target_ts - t0
    ) / (
        t1 - t0
    )

    # log-price interpolation
    logp = (
        math.log(p0)
        + weight
        * (
            math.log(p1)
            - math.log(p0)
        )
    )

    return {
        "price":
            math.exp(logp),

        "before_delay":
            dbefore,

        "after_delay":
            dafter,

        "gap":
            t1 - t0,
    }


def nearest_price(token, target_ts):

    before, after = bracket(
        token,
        target_ts
    )

    candidates = []

    if before is not None:

        d = (
            target_ts
            - before["timestamp"]
        )

        if 0 <= d <= MAX_BEFORE:
            candidates.append(
                (
                    d,
                    before["clean_price"]
                )
            )

    if after is not None:

        d = (
            after["timestamp"]
            - target_ts
        )

        if 0 <= d <= MAX_AFTER:
            candidates.append(
                (
                    d,
                    after["clean_price"]
                )
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: x[0]
    )

    return {
        "delay":
            candidates[0][0],

        "price":
            candidates[0][1],
    }


# ============================================================
# BUILD ALL HORIZON RESULTS
# ============================================================

results = {}

for horizon in HORIZONS:

    interp_records = []
    nearest_records = []

    for b in buys:

        p0 = b["clean_price"]

        if (
            not valid(p0)
            or p0 <= 0
        ):
            continue

        target = (
            b["timestamp"]
            + horizon
        )

        # INTERPOLATED
        ip = interpolated_price(
            b["token_mint"],
            target
        )

        if ip is not None:

            ret = (
                ip["price"] / p0
                - 1.0
            ) * 100.0

            interp_records.append({
                "token":
                    b["token_mint"],

                "wallet":
                    b["wallet"],

                "entry_program":
                    b["program"],

                "ret":
                    ret,

                "before_delay":
                    ip["before_delay"],

                "after_delay":
                    ip["after_delay"],

                "gap":
                    ip["gap"],
            })

        # NEAREST
        np_ = nearest_price(
            b["token_mint"],
            target
        )

        if np_ is not None:

            ret = (
                np_["price"] / p0
                - 1.0
            ) * 100.0

            nearest_records.append({
                "token":
                    b["token_mint"],

                "wallet":
                    b["wallet"],

                "entry_program":
                    b["program"],

                "ret":
                    ret,

                "delay":
                    np_["delay"],
            })

    results[horizon] = {
        "interp":
            interp_records,

        "nearest":
            nearest_records,
    }


# ============================================================
# A) COVERAGE TABLE
# ============================================================

print()
print("=" * 165)
print("A) COVERAGE BY HORIZON")
print("=" * 165)

print(
    f"{'H':>5} "
    f"{'INTERP N':>10} "
    f"{'INTERP %':>10} "
    f"{'ROBUST N':>10} "
    f"{'ROBUST %':>10} "
    f"{'NEAR N':>10} "
    f"{'NEAR %':>10}"
)

print("-" * 80)

summary = []

for h in HORIZONS:

    interp = results[h]["interp"]
    nearest = results[h]["nearest"]

    robust = [
        r for r in interp
        if (
            valid(r["ret"])
            and abs(r["ret"])
            < MAX_ABS_RETURN_FOR_ROBUST
        )
    ]

    row = {
        "h":
            h,

        "interp_n":
            len(interp),

        "interp_cov":
            len(interp) / len(buys),

        "robust_n":
            len(robust),

        "robust_cov":
            len(robust) / len(buys),

        "nearest_n":
            len(nearest),

        "nearest_cov":
            len(nearest) / len(buys),
    }

    summary.append(row)

    print(
        f"{h:5d} "
        f"{len(interp):10d} "
        f"{100*len(interp)/len(buys):9.2f}% "
        f"{len(robust):10d} "
        f"{100*len(robust)/len(buys):9.2f}% "
        f"{len(nearest):10d} "
        f"{100*len(nearest)/len(buys):9.2f}%"
    )


# ============================================================
# B) ROBUST RETURN DISTRIBUTION BY HORIZON
# ============================================================

print()
print("=" * 165)
print("B) ROBUST INTERPOLATED RETURN DISTRIBUTION")
print("=" * 165)

print(
    f"{'H':>5} "
    f"{'N':>8} "
    f"{'AVG':>10} "
    f"{'MED':>10} "
    f"{'WIN':>8} "
    f"{'P10':>10} "
    f"{'P90':>10} "
    f"{'>=1000':>9}"
)

print("-" * 95)

for h in HORIZONS:

    vals_all = [
        r["ret"]
        for r in results[h]["interp"]
        if valid(r["ret"])
    ]

    vals = [
        x for x in vals_all
        if abs(x)
        < MAX_ABS_RETURN_FOR_ROBUST
    ]

    if not vals:
        continue

    huge = sum(
        abs(x) >= 1000
        for x in vals_all
    )

    print(
        f"{h:5d} "
        f"{len(vals):8d} "
        f"{avg(vals):+9.2f}% "
        f"{med(vals):+9.2f}% "
        f"{100*sum(x>0 for x in vals)/len(vals):7.1f}% "
        f"{percentile(vals,10):+9.2f}% "
        f"{percentile(vals,90):+9.2f}% "
        f"{huge:9d}"
    )


# ============================================================
# C) TIME QUALITY
# ============================================================

print()
print("=" * 165)
print("C) INTERPOLATION TIME QUALITY")
print("=" * 165)

print(
    f"{'H':>5} "
    f"{'N':>8} "
    f"{'MED BEFORE':>12} "
    f"{'MED AFTER':>11} "
    f"{'MED GAP':>10} "
    f"{'P90 GAP':>10}"
)

print("-" * 80)

for h in HORIZONS:

    rr = results[h]["interp"]

    if not rr:
        continue

    before = [
        r["before_delay"]
        for r in rr
    ]

    after = [
        r["after_delay"]
        for r in rr
    ]

    gap = [
        r["gap"]
        for r in rr
    ]

    print(
        f"{h:5d} "
        f"{len(rr):8d} "
        f"{med(before):11.2f}s "
        f"{med(after):10.2f}s "
        f"{med(gap):9.2f}s "
        f"{percentile(gap,90):9.2f}s"
    )


# ============================================================
# D) HORIZON-TO-HORIZON RETURN CORRELATION
# ============================================================

print()
print("=" * 165)
print("D) TOKEN/WALLET RETURN AGREEMENT ACROSS HORIZONS")
print("=" * 165)

# key by token+wallet and compare only common observations
maps = {}

for h in HORIZONS:

    m = {}

    for r in results[h]["interp"]:

        if (
            valid(r["ret"])
            and abs(r["ret"]) < 1000
        ):
            key = (
                r["token"],
                r["wallet"]
            )

            # retain first observed for that wallet-token key
            if key not in m:
                m[key] = r["ret"]

    maps[h] = m


for h1, h2 in zip(
    HORIZONS[:-1],
    HORIZONS[1:]
):

    common = sorted(
        set(maps[h1])
        & set(maps[h2])
    )

    if len(common) < 3:
        continue

    x = [
        maps[h1][k]
        for k in common
    ]

    y = [
        maps[h2][k]
        for k in common
    ]

    mx = avg(x)
    my = avg(y)

    num = sum(
        (a-mx)*(b-my)
        for a,b in zip(x,y)
    )

    denx = math.sqrt(
        sum(
            (a-mx)**2
            for a in x
        )
    )

    deny = math.sqrt(
        sum(
            (b-my)**2
            for b in y
        )
    )

    corr = (
        num / (denx*deny)
        if denx > 0 and deny > 0
        else None
    )

    diffs = [
        abs(a-b)
        for a,b in zip(x,y)
    ]

    print(
        f"{h1:>3}s -> {h2:>3}s "
        f"| N={len(common):5d} "
        f"| CORR="
        f"{corr:+.3f} "
        f"| MED |Δ|={med(diffs):6.2f} pts"
        if corr is not None
        else
        f"{h1:>3}s -> {h2:>3}s | CORR=NA"
    )


# ============================================================
# E) EXTREME RATE
# ============================================================

print()
print("=" * 165)
print("E) RAW EXTREME RATE BY HORIZON")
print("=" * 165)

for h in HORIZONS:

    vals = [
        r["ret"]
        for r in results[h]["interp"]
        if valid(r["ret"])
    ]

    if not vals:
        continue

    print()
    print(f"H={h}s | N={len(vals)}")

    for t in [
        100,
        250,
        500,
        1000
    ]:

        n = sum(
            abs(x) >= t
            for x in vals
        )

        print(
            f"  |RET|>={t:4}% "
            f": {n:4d}/{len(vals)} "
            f"({100*n/len(vals):5.2f}%)"
        )


# ============================================================
# F) PROGRAM BREAKDOWN FOR EACH HORIZON
# ============================================================

print()
print("=" * 165)
print("F) ROBUST RETURN BY PROGRAM / HORIZON")
print("=" * 165)

for h in HORIZONS:

    by_program = defaultdict(list)

    for r in results[h]["interp"]:

        if (
            valid(r["ret"])
            and abs(r["ret"]) < 1000
        ):
            by_program[
                r["entry_program"] or "NA"
            ].append(
                r["ret"]
            )

    print()
    print(f"H={h}s")

    for program, vals in sorted(
        by_program.items(),
        key=lambda x: -len(x[1])
    ):

        print(
            f"  {program:15} "
            f"| N={len(vals):5d} "
            f"| AVG={avg(vals):+8.2f}% "
            f"| MED={med(vals):+8.2f}% "
            f"| WIN={100*sum(x>0 for x in vals)/len(vals):5.1f}%"
        )


# ============================================================
# G) HORIZON QUALITY SCORE
# ============================================================

print()
print("=" * 165)
print("G) HORIZON QUALITY SCORE")
print("=" * 165)

print(
    "Score rewards robust coverage and penalizes "
    "wide interpolation gaps / extreme-return contamination."
)

ranking = []

for h in HORIZONS:

    interp = results[h]["interp"]

    if not interp:
        continue

    all_rets = [
        r["ret"]
        for r in interp
        if valid(r["ret"])
    ]

    robust = [
        r for r in interp
        if (
            valid(r["ret"])
            and abs(r["ret"]) < 1000
        )
    ]

    robust_cov = (
        len(robust) / len(buys)
    )

    huge_rate = (
        sum(
            abs(x) >= 1000
            for x in all_rets
        )
        / len(all_rets)
    )

    med_gap = med([
        r["gap"]
        for r in interp
    ])

    # Heuristic QA score only — not a trading score.
    quality = (
        100 * robust_cov
        - 100 * huge_rate
        - 0.30 * med_gap
    )

    ranking.append({
        "h":
            h,

        "quality":
            quality,

        "coverage":
            robust_cov,

        "huge":
            huge_rate,

        "gap":
            med_gap,
    })


ranking.sort(
    key=lambda x:
        x["quality"],
    reverse=True
)


for x in ranking:

    print(
        f"H={x['h']:3d}s "
        f"| QUALITY={x['quality']:7.2f} "
        f"| ROBUST COV={100*x['coverage']:6.2f}% "
        f"| >=1000={100*x['huge']:5.2f}% "
        f"| MED GAP={x['gap']:5.2f}s"
    )


# ============================================================
# H) RECOMMENDATION
# ============================================================

print()
print("=" * 165)
print("H) DECISION SUPPORT")
print("=" * 165)

best = ranking[0]

print(
    f"BEST QA HORIZON = "
    f"{best['h']}s"
)

print(
    f"ROBUST COVERAGE = "
    f"{100*best['coverage']:.2f}%"
)

print(
    f">=1000% RATE    = "
    f"{100*best['huge']:.3f}%"
)

print(
    f"MED BRACKET GAP = "
    f"{best['gap']:.2f}s"
)

print()

if (
    best["coverage"] >= 0.60
    and best["huge"] <= 0.01
):

    print(
        "✅ A SAME-UNIT HORIZON IS DENSE ENOUGH "
        "FOR T40B."
    )

    print(
        f"Primary wallet behavior horizon candidate: "
        f"{best['h']}s"
    )

elif (
    best["coverage"] >= 0.45
    and best["huge"] <= 0.01
):

    print(
        "🟡 A CAUTIOUS SAME-UNIT WALLET HORIZON EXISTS."
    )

    print(
        "T40B can be run as an audit, but history coverage "
        "must remain an explicit feature/filter."
    )

else:

    print(
        "⚠️ NO FIXED HORIZON HAS STRONG ENOUGH "
        "SAME-UNIT COVERAGE YET."
    )

    print(
        "Next direction should be an event-driven wallet metric "
        "rather than forcing a fixed-time post-buy return."
    )


print()
print("IMPORTANT:")
print("• Same-unit SOL/token prices only.")
print("• No dex_prices.price_usd.")
print("• No hardcoded SOL/USD conversion.")
print("• HORIZON QUALITY is a data-quality score, not a trading score.")
print("• T41.5 writes nothing to DB.")
print("• T23/T31/T32 remain untouched.")

db.close()
