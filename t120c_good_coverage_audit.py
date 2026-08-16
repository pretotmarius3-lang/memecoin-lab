#!/usr/bin/env python3

import sqlite3
import math
import statistics
import os

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

db = sqlite3.connect(DB)
db.row_factory = sqlite3.Row


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    return statistics.median(xs)


def percentile(xs, q):
    xs = sorted(x for x in xs if valid(x))

    if not xs:
        return None

    if len(xs) == 1:
        return xs[0]

    pos = (len(xs) - 1) * q

    lo = math.floor(pos)
    hi = math.ceil(pos)

    if lo == hi:
        return xs[lo]

    w = pos - lo

    return xs[lo] * (1 - w) + xs[hi] * w


def ranks(values):
    indexed = sorted(
        enumerate(values),
        key=lambda x: x[1]
    )

    out = [0.0] * len(values)

    i = 0

    while i < len(indexed):
        j = i

        while (
            j + 1 < len(indexed)
            and indexed[j + 1][1] == indexed[i][1]
        ):
            j += 1

        rank = (i + j + 2) / 2.0

        for k in range(i, j + 1):
            out[indexed[k][0]] = rank

        i = j + 1

    return out


def pearson(x, y):
    if len(x) < 3:
        return None

    mx = mean(x)
    my = mean(y)

    num = sum(
        (a - mx) * (b - my)
        for a, b in zip(x, y)
    )

    dx = math.sqrt(
        sum((a - mx) ** 2 for a in x)
    )

    dy = math.sqrt(
        sum((b - my) ** 2 for b in y)
    )

    if dx == 0 or dy == 0:
        return None

    return num / (dx * dy)


def spearman(x, y):
    pairs = [
        (a, b)
        for a, b in zip(x, y)
        if valid(a) and valid(b)
    ]

    if len(pairs) < 3:
        return None

    xx = [a for a, _ in pairs]
    yy = [b for _, b in pairs]

    return pearson(
        ranks(xx),
        ranks(yy)
    )


rows = db.execute("""
SELECT
    b.*,
    h.hit20 AS current_t120_hit20,
    h.max300 AS current_t120_max300
FROM t120b_strict_outcomes b

LEFT JOIN t120_holdout h
    ON h.dump_event_id=b.dump_event_id

WHERE
    b.strict_done_300s=1
    AND b.coverage_status='GOOD'
    AND b.strict_hit20 IS NOT NULL
    AND b.strict_max300 IS NOT NULL
    AND b.strict_min300 IS NOT NULL
    AND b.strict_end300 IS NOT NULL

ORDER BY b.frozen_score
""").fetchall()


print("=" * 150)
print("MEMECOIN LAB — T120C GOOD-COVERAGE STRICT AUDIT")
print("=" * 150)

print(f"GOOD ROWS       : {len(rows)}")
print(
    f"STRICT +20      : "
    f"{sum(r['strict_hit20']==1 for r in rows)}/{len(rows)}"
)

print()


scores = [
    r["frozen_score"]
    for r in rows
]

hit20 = [
    r["strict_hit20"]
    for r in rows
]

max300 = [
    r["strict_max300"]
    for r in rows
]

min300 = [
    r["strict_min300"]
    for r in rows
]

end300 = [
    r["strict_end300"]
    for r in rows
]


print("STRICT SCORE RELATION — GOOD ONLY")
print("-" * 70)

print(
    f"SCORE ↔ HIT20   : "
    f"{spearman(scores, hit20):.3f}"
)

print(
    f"SCORE ↔ MAX300  : "
    f"{spearman(scores, max300):.3f}"
)

print(
    f"SCORE ↔ MIN300  : "
    f"{spearman(scores, min300):.3f}"
)

print(
    f"SCORE ↔ END300  : "
    f"{spearman(scores, end300):.3f}"
)


print()
print("GOOD-ONLY QUARTILES")
print("-" * 110)

print(
    f"{'Q':<5}"
    f"{'N':>7}"
    f"{'+20':>10}"
    f"{'MAX MED':>12}"
    f"{'MAX P90':>12}"
    f"{'MIN MED':>12}"
    f"{'END MED':>12}"
)

n = len(rows)

for i in range(4):
    a = int(n * i / 4)
    b = int(n * (i + 1) / 4)

    part = rows[a:b]

    if not part:
        continue

    rate = (
        100
        * sum(
            r["strict_hit20"] == 1
            for r in part
        )
        / len(part)
    )

    print(
        f"Q{i+1:<4}"
        f"{len(part):>7}"
        f"{rate:>9.1f}%"
        f"{median([r['strict_max300'] for r in part]):>12.1f}"
        f"{percentile([r['strict_max300'] for r in part],0.90):>12.1f}"
        f"{median([r['strict_min300'] for r in part]):>12.1f}"
        f"{median([r['strict_end300'] for r in part]):>12.1f}"
    )


print()
print("TOP HALF VS BOTTOM HALF")
print("-" * 70)

mid = len(rows) // 2

bottom = rows[:mid]
top = rows[mid:]


def hitrate(part):
    return (
        100
        * sum(
            r["strict_hit20"] == 1
            for r in part
        )
        / len(part)
    )


print(
    f"BOTTOM 50% | N={len(bottom):3d} "
    f"| +20={hitrate(bottom):5.1f}%"
)

print(
    f"TOP 50%    | N={len(top):3d} "
    f"| +20={hitrate(top):5.1f}%"
)


print()
print("OLD T120 vs STRICT — GOOD ONLY")
print("-" * 90)

comparable = [
    r for r in rows
    if r["current_t120_hit20"] is not None
]

changed = [
    r for r in comparable
    if r["current_t120_hit20"]
       != r["strict_hit20"]
]

print(
    f"CURRENT T120 LABEL AVAILABLE : "
    f"{len(comparable)}/{len(rows)}"
)

print(
    f"CHANGED LABELS               : "
    f"{len(changed)}/{len(comparable)}"
    if comparable
    else "CHANGED LABELS               : 0/0"
)

print()

if len(rows) >= 40:

    rho = spearman(
        scores,
        hit20
    )

    q1 = rows[:max(1, len(rows)//4)]
    q4 = rows[
        int(len(rows)*3/4):
    ]

    q1_rate = hitrate(q1)
    q4_rate = hitrate(q4)

    if (
        rho is not None
        and rho > 0.15
        and q4_rate > q1_rate + 10
    ):

        print(
            "🟢 STRICT SIGNAL SURVIVES ON GOOD COVERAGE"
        )

    else:

        print(
            "🔴 FROZEN SCORE NOT VALIDATED "
            "ON STRICT GOOD-COVERAGE OUTCOMES"
        )

db.close()
