import sqlite3
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

HORIZON = 60.0

# Search window around the +60s target.
MAX_BEFORE = 30.0
MAX_AFTER = 30.0

# A local jump larger than this is flagged as a continuity issue.
# This is diagnostic only; we report both raw and filtered coverage.
MAX_LOCAL_RATIO = 5.0


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
    vals = sorted(
        x for x in vals
        if valid(x)
    )

    if not vals:
        return None

    idx = int(
        round(
            (q / 100.0)
            * (len(vals) - 1)
        )
    )

    return vals[idx]


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute(
    "PRAGMA busy_timeout=5000"
)


# ============================================================
# LOAD VALID SWAPS
# ============================================================

rows = db.execute("""
    SELECT
        signature,
        timestamp,
        wallet,
        side,
        token_mint,
        clean_price,
        raw_price,
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
    by_token[
        r["token_mint"]
    ].append(r)


buys = [
    r for r in rows
    if str(r["side"]).upper() == "BUY"
]


print("=" * 160)
print(
    "MEMECOIN LAB — "
    "T41.4 SAME-UNIT PRICE PATH RECONSTRUCTION"
)
print("=" * 160)

print(
    "ENTRY = swaps.clean_price"
)

print(
    "TARGET = entry timestamp +60s"
)

print(
    "EXIT PRICE FAMILY = swaps.clean_price only"
)

print(
    "METHODS = NEAREST / PREVIOUS / INTERPOLATED"
)

print()

print(
    f"VALID SWAPS : {len(rows)}"
)

print(
    f"BUY ENTRIES : {len(buys)}"
)

print(
    f"TOKENS      : {len(by_token)}"
)


# ============================================================
# FIND BEFORE / AFTER
# ============================================================

def bracket(token, target_ts):

    arr = by_token.get(
        token,
        []
    )

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


def continuity_ok(before, after):

    if before is None or after is None:
        return None

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


def reconstruct(
    token,
    target_ts
):

    before, after = bracket(
        token,
        target_ts
    )

    result = {
        "before": before,
        "after": after,

        "previous_price": None,
        "nearest_price": None,
        "interp_price": None,

        "previous_delay": None,
        "nearest_delay": None,

        "continuity_ok": None,
    }

    # --------------------------------------------------------
    # PREVIOUS
    # --------------------------------------------------------

    if before is not None:

        delay_before = (
            target_ts
            - before["timestamp"]
        )

        if (
            delay_before >= 0
            and delay_before <= MAX_BEFORE
        ):
            result[
                "previous_price"
            ] = before["clean_price"]

            result[
                "previous_delay"
            ] = delay_before

    # --------------------------------------------------------
    # AFTER ELIGIBILITY
    # --------------------------------------------------------

    after_ok = False
    delay_after = None

    if after is not None:

        delay_after = (
            after["timestamp"]
            - target_ts
        )

        after_ok = (
            delay_after >= 0
            and delay_after <= MAX_AFTER
        )

    # --------------------------------------------------------
    # NEAREST
    # --------------------------------------------------------

    candidates = []

    if (
        before is not None
        and result["previous_price"]
        is not None
    ):
        candidates.append(
            (
                abs(
                    before["timestamp"]
                    - target_ts
                ),
                before
            )
        )

    if after_ok:
        candidates.append(
            (
                abs(
                    after["timestamp"]
                    - target_ts
                ),
                after
            )
        )

    if candidates:

        candidates.sort(
            key=lambda x: x[0]
        )

        err, chosen = candidates[0]

        result[
            "nearest_price"
        ] = chosen["clean_price"]

        result[
            "nearest_delay"
        ] = (
            chosen["timestamp"]
            - target_ts
        )

    # --------------------------------------------------------
    # INTERPOLATION
    # --------------------------------------------------------

    if (
        before is not None
        and after is not None
    ):

        delay_before = (
            target_ts
            - before["timestamp"]
        )

        delay_after = (
            after["timestamp"]
            - target_ts
        )

        bracket_ok = (
            delay_before >= 0
            and delay_before <= MAX_BEFORE
            and delay_after >= 0
            and delay_after <= MAX_AFTER
        )

        cont = continuity_ok(
            before,
            after
        )

        result[
            "continuity_ok"
        ] = cont

        if (
            bracket_ok
            and cont
        ):

            t0 = before["timestamp"]
            t1 = after["timestamp"]

            p0 = before["clean_price"]
            p1 = after["clean_price"]

            if t1 > t0:

                weight = (
                    target_ts - t0
                ) / (
                    t1 - t0
                )

                # log interpolation is more appropriate for price
                logp = (
                    math.log(p0)
                    + weight
                    * (
                        math.log(p1)
                        - math.log(p0)
                    )
                )

                result[
                    "interp_price"
                ] = math.exp(logp)

    return result


# ============================================================
# BUILD RECORDS
# ============================================================

records = []

for b in buys:

    entry_price = b["clean_price"]

    if (
        not valid(entry_price)
        or entry_price <= 0
    ):
        continue

    target = (
        b["timestamp"]
        + HORIZON
    )

    rec = reconstruct(
        b["token_mint"],
        target
    )

    out = {
        "token":
            b["token_mint"],

        "wallet":
            b["wallet"],

        "entry_program":
            b["program"],

        "entry_ts":
            b["timestamp"],

        "entry_price":
            entry_price,

        "target_ts":
            target,

        "previous_price":
            rec["previous_price"],

        "nearest_price":
            rec["nearest_price"],

        "interp_price":
            rec["interp_price"],

        "previous_delay":
            rec["previous_delay"],

        "nearest_delay":
            rec["nearest_delay"],

        "continuity_ok":
            rec["continuity_ok"],
    }

    for method in [
        "previous",
        "nearest",
        "interp"
    ]:

        px = out[
            f"{method}_price"
        ]

        if (
            valid(px)
            and px > 0
        ):
            out[
                f"{method}_ret"
            ] = (
                px
                / entry_price
                - 1.0
            ) * 100.0
        else:
            out[
                f"{method}_ret"
            ] = None

    records.append(out)


# ============================================================
# A) METHOD COVERAGE
# ============================================================

print()
print("=" * 160)
print("A) METHOD COVERAGE")
print("=" * 160)

for method in [
    "previous",
    "nearest",
    "interp"
]:

    vals = [
        r for r in records
        if valid(
            r[
                f"{method}_ret"
            ]
        )
    ]

    print(
        f"{method.upper():10} "
        f"| N={len(vals):5d}/{len(buys)} "
        f"| COVERAGE="
        f"{100*len(vals)/len(buys):6.2f}%"
    )


# ============================================================
# B) RETURN DISTRIBUTIONS
# ============================================================

print()
print("=" * 160)
print("B) RECONSTRUCTED +60s RETURN DISTRIBUTIONS")
print("=" * 160)

for method in [
    "previous",
    "nearest",
    "interp"
]:

    vals = [
        r[
            f"{method}_ret"
        ]
        for r in records
        if valid(
            r[
                f"{method}_ret"
            ]
        )
    ]

    print()
    print(method.upper())
    print("-" * 100)

    if not vals:
        print("NO DATA")
        continue

    print(
        f"N={len(vals)} "
        f"| AVG={avg(vals):+.2f}% "
        f"| MED={med(vals):+.2f}% "
        f"| WIN="
        f"{100*sum(x>0 for x in vals)/len(vals):.1f}%"
    )

    for q in [
        1,
        5,
        10,
        25,
        50,
        75,
        90,
        95,
        99
    ]:

        print(
            f"P{q:>2}="
            f"{percentile(vals,q):+9.2f}%"
        )


# ============================================================
# C) EXTREME RATE BY METHOD
# ============================================================

print()
print("=" * 160)
print("C) EXTREME RETURN RATE")
print("=" * 160)

for method in [
    "previous",
    "nearest",
    "interp"
]:

    vals = [
        r[
            f"{method}_ret"
        ]
        for r in records
        if valid(
            r[
                f"{method}_ret"
            ]
        )
    ]

    if not vals:
        continue

    print()
    print(method.upper())

    for threshold in [
        100,
        250,
        500,
        1000
    ]:

        n = sum(
            abs(x) >= threshold
            for x in vals
        )

        print(
            f"  |RET|>={threshold:4}% "
            f": {n:4d}/{len(vals)} "
            f"({100*n/len(vals):5.2f}%)"
        )


# ============================================================
# D) METHOD AGREEMENT
# ============================================================

print()
print("=" * 160)
print("D) METHOD AGREEMENT")
print("=" * 160)

pairs = [
    (
        "previous",
        "nearest"
    ),
    (
        "nearest",
        "interp"
    ),
    (
        "previous",
        "interp"
    ),
]

for a, b in pairs:

    diffs = []

    for r in records:

        x = r[
            f"{a}_ret"
        ]

        y = r[
            f"{b}_ret"
        ]

        if (
            valid(x)
            and valid(y)
        ):
            diffs.append(
                abs(x-y)
            )

    if not diffs:
        continue

    print(
        f"{a.upper():8} vs "
        f"{b.upper():8} "
        f"| N={len(diffs):5d} "
        f"| MED |Δ|={med(diffs):6.2f} pts "
        f"| AVG |Δ|={avg(diffs):6.2f} pts"
    )

    for t in [
        1,
        2,
        5,
        10,
        20
    ]:

        n = sum(
            d <= t
            for d in diffs
        )

        print(
            f"   within {t:>2} pts "
            f": {100*n/len(diffs):5.1f}%"
        )


# ============================================================
# E) CONTINUITY COVERAGE
# ============================================================

print()
print("=" * 160)
print("E) LOCAL PRICE CONTINUITY")
print("=" * 160)

known_cont = [
    r for r in records
    if r[
        "continuity_ok"
    ] is not None
]

good_cont = [
    r for r in known_cont
    if r[
        "continuity_ok"
    ]
]

bad_cont = [
    r for r in known_cont
    if not r[
        "continuity_ok"
    ]
]

print(
    f"BRACKETED PATHS : "
    f"{len(known_cont)}"
)

print(
    f"CONTINUOUS      : "
    f"{len(good_cont)} "
    f"({100*len(good_cont)/len(known_cont):.2f}%)"
    if known_cont
    else "CONTINUOUS      : NA"
)

print(
    f"FLAGGED JUMPS   : "
    f"{len(bad_cont)} "
    f"({100*len(bad_cont)/len(known_cont):.2f}%)"
    if known_cont
    else "FLAGGED JUMPS   : NA"
)


# ============================================================
# F) DELAY QUALITY
# ============================================================

print()
print("=" * 160)
print("F) TIME DISTANCE TO +60s")
print("=" * 160)

for method, field in [
    (
        "PREVIOUS",
        "previous_delay"
    ),
    (
        "NEAREST",
        "nearest_delay"
    )
]:

    vals = [
        abs(r[field])
        for r in records
        if valid(r[field])
    ]

    if not vals:
        continue

    print(
        f"{method:10} "
        f"| N={len(vals):5d} "
        f"| MED={med(vals):5.2f}s "
        f"| AVG={avg(vals):5.2f}s "
        f"| P90={percentile(vals,90):5.2f}s"
    )


# ============================================================
# G) INTERPOLATION EXTREMES
# ============================================================

print()
print("=" * 160)
print("G) TOP INTERPOLATED ABSOLUTE EXTREMES")
print("=" * 160)

interp_rows = [
    r for r in records
    if valid(
        r[
            "interp_ret"
        ]
    )
]

interp_rows.sort(
    key=lambda r:
        abs(
            r[
                "interp_ret"
            ]
        ),
    reverse=True
)

print(
    f"{'RET':>12} "
    f"{'ENTRY':>14} "
    f"{'P60':>14} "
    f"{'TOKEN':22} "
    f"{'PROGRAM':>10}"
)

print("-" * 85)

for r in interp_rows[:30]:

    print(
        f"{r['interp_ret']:+11.2f}% "
        f"{r['entry_price']:13.6g} "
        f"{r['interp_price']:13.6g} "
        f"{r['token'][:22]:22} "
        f"{str(r['entry_program'])[:10]:>10}"
    )


# ============================================================
# H) ROBUST INTERPOLATED SET
# ============================================================

robust = [
    r for r in records
    if (
        valid(
            r[
                "interp_ret"
            ]
        )
        and r[
            "continuity_ok"
        ] is True
        and abs(
            r[
                "interp_ret"
            ]
        ) < 1000
    )
]

robust_rets = [
    r[
        "interp_ret"
    ]
    for r in robust
]


print()
print("=" * 160)
print("H) ROBUST INTERPOLATED SAMPLE")
print("=" * 160)

print(
    f"N={len(robust)} "
    f"| COVERAGE="
    f"{100*len(robust)/len(buys):.2f}%"
)

if robust_rets:

    print(
        f"AVG={avg(robust_rets):+.2f}% "
        f"| MED={med(robust_rets):+.2f}% "
        f"| WIN="
        f"{100*sum(x>0 for x in robust_rets)/len(robust_rets):.1f}%"
    )

    print(
        f"P10="
        f"{percentile(robust_rets,10):+.2f}% "
        f"| P90="
        f"{percentile(robust_rets,90):+.2f}%"
    )


# ============================================================
# I) DECISION SUPPORT
# ============================================================

interp_n = len(
    [
        r for r in records
        if valid(
            r[
                "interp_ret"
            ]
        )
    ]
)

interp_coverage = (
    interp_n / len(buys)
    if buys else 0
)

robust_coverage = (
    len(robust) / len(buys)
    if buys else 0
)

interp_huge = (
    sum(
        abs(r["interp_ret"]) >= 1000
        for r in interp_rows
    )
    / len(interp_rows)
    if interp_rows
    else 1
)


print()
print("=" * 160)
print("I) DECISION SUPPORT")
print("=" * 160)

print(
    f"INTERP COVERAGE       = "
    f"{100*interp_coverage:.2f}%"
)

print(
    f"ROBUST INTERP COVERAGE= "
    f"{100*robust_coverage:.2f}%"
)

print(
    f"INTERP >=1000% RATE  = "
    f"{100*interp_huge:.3f}%"
)

print()

if (
    robust_coverage >= 0.60
    and interp_huge <= 0.01
):

    print(
        "✅ SAME-UNIT PATH RECONSTRUCTION IS "
        "GOOD ENOUGH FOR T40B."
    )

    print(
        "Use robust interpolated +60s return "
        "for wallet behavioral history."
    )

elif (
    robust_coverage >= 0.40
    and interp_huge <= 0.01
):

    print(
        "🟡 SAME-UNIT RECONSTRUCTION IS USABLE "
        "FOR A CAUTIOUS T40B AUDIT."
    )

    print(
        "Coverage is not ideal, so T40B should "
        "report missing-history coverage explicitly."
    )

else:

    print(
        "⚠️ SAME-UNIT +60s RECONSTRUCTION "
        "IS STILL TOO SPARSE OR UNSTABLE."
    )

    print(
        "Do not rebuild wallet skill scoring yet."
    )


print()
print("IMPORTANT:")
print("• No dex_prices.price_usd is used.")
print("• No SOL/USD conversion factor is used.")
print("• Interpolation is within swaps.clean_price only.")
print("• Local discontinuities are excluded from robust set.")
print("• T41.4 writes nothing to DB.")
print("• T23/T31/T32 remain untouched.")

db.close()
