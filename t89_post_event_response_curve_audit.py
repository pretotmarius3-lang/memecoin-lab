#!/usr/bin/env python3

import sqlite3
import math
import statistics

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

HORIZONS = [5, 10, 20, 30, 60, 300]
ACTIVATION_THRESHOLDS = [1.0, 2.0, 3.0, 5.0]
FINAL_THRESHOLDS = [5.0, 10.0]


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


def quantile(xs, q):
    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    p = (len(xs)-1)*q
    lo = int(math.floor(p))
    hi = int(math.ceil(p))

    if lo == hi:
        return xs[lo]

    w = p-lo
    return xs[lo]*(1-w) + xs[hi]*w


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def same_sign(a, b):
    if not valid(a) or not valid(b):
        return None

    if a == 0 or b == 0:
        return None

    return (a > 0 and b > 0) or (a < 0 and b < 0)


# ============================================================
# DB READ ONLY
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
    raise RuntimeError("Cannot determine T59 boundary.")

boundary = int(boundary)


rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    dex_return_5s,
    dex_return_10s,
    dex_return_20s,
    dex_return_30s,
    dex_return_60s,
    dex_return_300s,

    dex_done_5s,
    dex_done_10s,
    dex_done_20s,
    dex_done_30s,
    dex_done_60s,
    dex_done_300s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


def row_value(r, h):
    return r[f"dex_return_{h}s"]


def row_done(r, h):
    return r[f"dex_done_{h}s"]


def complete_all(r):
    for h in HORIZONS:
        if (
            row_done(r, h) != 1
            or not valid(row_value(r, h))
        ):
            return False
    return True


historical = [
    r for r in rows
    if r["id"] <= boundary
]

prospective = [
    r for r in rows
    if r["id"] > boundary
]

hist_cc = [
    r for r in historical
    if complete_all(r)
]

pros_cc = [
    r for r in prospective
    if complete_all(r)
]


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

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


hist_first = first_token(hist_cc)
pros_first = first_token(pros_cc)


# ============================================================
# HEADER
# ============================================================

print("=" * 170)
print(
    "MEMECOIN LAB — T89 POST-EVENT RESPONSE CURVE / CONTINUATION AUDIT"
)
print("=" * 170)

print("MODE              : READ-ONLY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")
print("DB WRITES         : NONE")
print("T59/T78/T82/T86   : UNTOUCHED")
print()
print(f"T59 BOUNDARY      : {boundary}")


# ============================================================
# A) COVERAGE
# ============================================================

print()
print("=" * 170)
print("A) COMPLETE RESPONSE-CURVE COVERAGE")
print("=" * 170)

print(
    f"HISTORICAL | N={len(hist_cc):4d} "
    f"| TOKENS={len(set(r['token_mint'] for r in hist_cc)):4d}"
)

print(
    f"PROSPECTIVE | N={len(pros_cc):4d} "
    f"| TOKENS={len(set(r['token_mint'] for r in pros_cc)):4d}"
)

print(
    f"HIST FIRST | N={len(hist_first):4d}"
)

print(
    f"PROS FIRST | N={len(pros_first):4d}"
)


# ============================================================
# B) RESPONSE CURVE DISTRIBUTIONS
# ============================================================

print()
print("=" * 170)
print("B) RESPONSE CURVE DISTRIBUTIONS")
print("=" * 170)

for h in HORIZONS:

    hx = [
        row_value(r, h)
        for r in hist_cc
    ]

    px = [
        row_value(r, h)
        for r in pros_cc
    ]

    print(
        f"{h:3d}s "
        f"| HIST MED={fmt(med(hx)):>8} "
        f"P10={fmt(quantile(hx,0.10)):>8} "
        f"P90={fmt(quantile(hx,0.90)):>8} "
        f"| PROS MED={fmt(med(px)):>8} "
        f"P10={fmt(quantile(px,0.10)):>8} "
        f"P90={fmt(quantile(px,0.90)):>8}"
    )


# ============================================================
# C) ACTIVATION PROBABILITY
# ============================================================

print()
print("=" * 170)
print("C) ACTIVATION — P(|RETURN| >= THRESHOLD)")
print("=" * 170)

for t in ACTIVATION_THRESHOLDS:

    print()
    print(f"THRESHOLD ±{t:.1f}%")

    for h in HORIZONS:

        hx = [
            row_value(r, h)
            for r in hist_cc
        ]

        px = [
            row_value(r, h)
            for r in pros_cc
        ]

        hn = sum(
            abs(x) >= t
            for x in hx
        )

        pn = sum(
            abs(x) >= t
            for x in px
        )

        print(
            f"  {h:3d}s "
            f"| HIST={hn:4d}/{len(hx):4d} "
            f"({pct(hn,len(hx)):>6}) "
            f"| PROS={pn:4d}/{len(px):4d} "
            f"({pct(pn,len(px)):>6})"
        )


# ============================================================
# D) EARLY MOVE -> FINAL MOVE
# ============================================================

print()
print("=" * 170)
print("D) CONTINUATION — EARLY MOVE → LARGE 300s MOVE")
print("=" * 170)

early_horizons = [10, 20, 30, 60]

for early_h in early_horizons:

    print()
    print(f"EARLY HORIZON = {early_h}s")

    for early_t in [1.0, 2.0, 3.0, 5.0]:

        for final_t in FINAL_THRESHOLDS:

            print(
                f"  |R{early_h}| >= {early_t:.1f}% "
                f"→ |R300| >= {final_t:.1f}%"
            )

            for name, rr in [
                ("HIST", hist_cc),
                ("PROS", pros_cc),
            ]:

                eligible = [
                    r
                    for r in rr
                    if abs(
                        row_value(r, early_h)
                    ) >= early_t
                ]

                success = sum(
                    abs(
                        row_value(r, 300)
                    ) >= final_t
                    for r in eligible
                )

                print(
                    f"    {name:4} "
                    f"| N={len(eligible):4d} "
                    f"| SUCCESS={success:4d} "
                    f"| RATE={pct(success,len(eligible)):>6}"
                )


# ============================================================
# E) DIRECTIONAL PERSISTENCE
# ============================================================

print()
print("=" * 170)
print("E) DIRECTIONAL PERSISTENCE TO 300s")
print("=" * 170)

for h in [10, 20, 30, 60]:

    print()
    print(f"{h}s → 300s")

    for threshold in [1.0, 2.0, 3.0]:

        for name, rr in [
            ("HIST", hist_cc),
            ("PROS", pros_cc),
        ]:

            eligible = [
                r
                for r in rr
                if abs(
                    row_value(r, h)
                ) >= threshold
            ]

            persistent = 0
            usable = 0

            for r in eligible:

                s = same_sign(
                    row_value(r, h),
                    row_value(r, 300)
                )

                if s is None:
                    continue

                usable += 1

                if s:
                    persistent += 1

            print(
                f"  TH={threshold:.1f}% "
                f"| {name:4} "
                f"N={usable:4d} "
                f"| SAME_SIGN={persistent:4d} "
                f"| RATE={pct(persistent,usable):>6}"
            )


# ============================================================
# F) EXPANSION / DECAY RATIOS
# ============================================================

print()
print("=" * 170)
print("F) MAGNITUDE EXPANSION / DECAY TO 300s")
print("=" * 170)

for h in [10, 20, 30, 60]:

    print()
    print(f"|R300| / |R{h}|")

    for name, rr in [
        ("HIST", hist_cc),
        ("PROS", pros_cc),
    ]:

        ratios = []

        for r in rr:

            early = abs(
                row_value(r, h)
            )

            final = abs(
                row_value(r, 300)
            )

            if (
                valid(early)
                and valid(final)
                and early >= 0.5
            ):
                ratios.append(
                    final / early
                )

        print(
            f"  {name:4} "
            f"| N={len(ratios):4d} "
            f"| MED={fmt(med(ratios)):>7} "
            f"| P25={fmt(quantile(ratios,0.25)):>7} "
            f"| P75={fmt(quantile(ratios,0.75)):>7}"
        )


# ============================================================
# G) MEAN REVERSION / FAILURE
# ============================================================

print()
print("=" * 170)
print("G) EARLY ACTIVATION FAILURE / MEAN REVERSION")
print("=" * 170)

for h in [10, 20, 30, 60]:

    print()
    print(f"EARLY = {h}s")

    for threshold in [2.0, 3.0, 5.0]:

        for name, rr in [
            ("HIST", hist_cc),
            ("PROS", pros_cc),
        ]:

            eligible = [
                r
                for r in rr
                if abs(
                    row_value(r, h)
                ) >= threshold
            ]

            collapse = sum(
                abs(
                    row_value(r, 300)
                ) < 1.0
                for r in eligible
            )

            reversal = sum(
                (
                    row_value(r, h) > 0
                    and row_value(r, 300) < 0
                )
                or (
                    row_value(r, h) < 0
                    and row_value(r, 300) > 0
                )
                for r in eligible
            )

            print(
                f"  TH={threshold:.1f}% "
                f"| {name:4} "
                f"N={len(eligible):4d} "
                f"| COLLAPSE<1%={collapse:3d} "
                f"({pct(collapse,len(eligible)):>6}) "
                f"| REVERSAL={reversal:3d} "
                f"({pct(reversal,len(eligible)):>6})"
            )


# ============================================================
# H) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 170)
print("H) FIRST-EVENT/TOKEN RESPONSE CURVE")
print("=" * 170)

for h in HORIZONS:

    hx = [
        row_value(r, h)
        for r in hist_first
    ]

    px = [
        row_value(r, h)
        for r in pros_first
    ]

    h3 = sum(
        abs(x) >= 3.0
        for x in hx
    )

    p3 = sum(
        abs(x) >= 3.0
        for x in px
    )

    h10 = sum(
        abs(x) >= 10.0
        for x in hx
    )

    p10 = sum(
        abs(x) >= 10.0
        for x in px
    )

    print(
        f"{h:3d}s "
        f"| HIST ±3={pct(h3,len(hx)):>6} "
        f"±10={pct(h10,len(hx)):>6} "
        f"| PROS ±3={pct(p3,len(px)):>6} "
        f"±10={pct(p10,len(px)):>6}"
    )


# ============================================================
# I) RESPONSE-CURVE SHAPE
# ============================================================

print()
print("=" * 170)
print("I) MEDIAN ABSOLUTE RETURN CURVE")
print("=" * 170)

for name, rr in [
    ("HISTORICAL", hist_cc),
    ("PROSPECTIVE", pros_cc),
]:

    print()
    print(name)

    for h in HORIZONS:

        xs = [
            abs(
                row_value(r, h)
            )
            for r in rr
        ]

        print(
            f"  {h:3d}s "
            f"| MED|R|={fmt(med(xs)):>8} "
            f"| P75|R|={fmt(quantile(xs,0.75)):>8} "
            f"| P90|R|={fmt(quantile(xs,0.90)):>8}"
        )


# ============================================================
# J) DECISION SUPPORT
# ============================================================

print()
print("=" * 170)
print("J) DECISION SUPPORT")
print("=" * 170)

# Focus on 30s as a reasonable early activation checkpoint.

hist_active30 = [
    r
    for r in hist_cc
    if abs(
        row_value(r, 30)
    ) >= 3.0
]

pros_active30 = [
    r
    for r in pros_cc
    if abs(
        row_value(r, 30)
    ) >= 3.0
]


hist_cont = (
    sum(
        abs(
            row_value(r, 300)
        ) >= 10.0
        for r in hist_active30
    )
    / len(hist_active30)
    if hist_active30
    else None
)

pros_cont = (
    sum(
        abs(
            row_value(r, 300)
        ) >= 10.0
        for r in pros_active30
    )
    / len(pros_active30)
    if pros_active30
    else None
)


hist_activation = (
    len(hist_active30)
    / len(hist_cc)
    if hist_cc
    else None
)

pros_activation = (
    len(pros_active30)
    / len(pros_cc)
    if pros_cc
    else None
)


print(
    f"HIST P(|R30|>=3%)        = {fmt(hist_activation,3)}"
)

print(
    f"PROS P(|R30|>=3%)        = {fmt(pros_activation,3)}"
)

print(
    f"HIST P(|R300|>=10 | act) = {fmt(hist_cont,3)}"
)

print(
    f"PROS P(|R300|>=10 | act) = {fmt(pros_cont,3)}"
)

print()


activation_collapse = (
    hist_activation is not None
    and pros_activation is not None
    and hist_activation > 0
    and pros_activation <= 0.5 * hist_activation
)

continuation_collapse = (
    hist_cont is not None
    and pros_cont is not None
    and hist_cont > 0
    and pros_cont <= 0.5 * hist_cont
)


if (
    activation_collapse
    and continuation_collapse
):

    print(
        "🟠 BOTH ACTIVATION AND CONTINUATION HAVE COLLAPSED."
    )

    print(
        "Prospective events move less often, and early movers also follow through less."
    )

elif activation_collapse:

    print(
        "🟡 PRIMARY FAILURE MODE = ACTIVATION."
    )

    print(
        "Prospective events rarely start moving strongly after trigger."
    )

elif continuation_collapse:

    print(
        "🟡 PRIMARY FAILURE MODE = CONTINUATION."
    )

    print(
        "Early prospective moves occur, but they fail to expand into 300s tails."
    )

else:

    print(
        "🟢 NO SINGLE DOMINANT ACTIVATION/CONTINUATION FAILURE DETECTED."
    )

    print(
        "The regime difference may be distributed across the whole response curve."
    )


print()
print("IMPORTANT:")
print("• Same complete-case horizons for every response curve.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Thresholds are diagnostic only.")
print("• T89 writes nothing to DB.")
print("• All frozen prospective experiments remain untouched.")

db.close()
