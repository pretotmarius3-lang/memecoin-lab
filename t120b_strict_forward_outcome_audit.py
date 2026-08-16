#!/usr/bin/env python3

import sqlite3
import os
import time
import math
import statistics

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

HOLDOUT = "t120_holdout"
DUMP_EVENTS = "t116_premigration_dump_events"
CLEAN = "t116_clean_swaps"

REFRESH = 10


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


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
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


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


def fmt(x, n=1):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


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

    if mx is None or my is None:
        return None

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


db.execute("""
CREATE TABLE IF NOT EXISTS t120b_strict_outcomes (

    dump_event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,

    frozen_score REAL NOT NULL,

    trigger_timestamp REAL NOT NULL,

    actual_entry_ts REAL,
    actual_entry_price REAL,
    entry_delay_s REAL,

    strict_done_300s INTEGER NOT NULL DEFAULT 0,

    n_forward INTEGER,
    first_forward_delay_s REAL,
    max_gap_s REAL,

    strict_max300 REAL,
    strict_min300 REAL,
    strict_end300 REAL,

    strict_hit10 INTEGER,
    strict_hit20 INTEGER,
    strict_hit30 INTEGER,
    strict_hit50 INTEGER,

    strict_t10 REAL,
    strict_t20 REAL,
    strict_t30 REAL,
    strict_t50 REAL,

    strict_time_to_max REAL,

    coverage_status TEXT,

    old_hit20 INTEGER,
    old_max300 REAL,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


def ensure_rows():
    rows = db.execute("""
    SELECT *
    FROM t120_holdout
    """).fetchall()

    for r in rows:
        db.execute("""
        INSERT OR IGNORE INTO t120b_strict_outcomes (
            dump_event_id,
            token_mint,
            frozen_score,
            trigger_timestamp,
            old_hit20,
            old_max300,
            created_at,
            last_update_at
        )
        VALUES (?,?,?,?,?,?,?,?)
        """, (
            r["dump_event_id"],
            r["token_mint"],
            r["frozen_score"],
            r["trigger_timestamp"],
            r["hit20"],
            r["max300"],
            time.time(),
            time.time(),
        ))

    db.commit()


def first_entry(mint, trigger_ts):
    return db.execute("""
    SELECT
        timestamp,
        clean_price_sol
    FROM t116_clean_swaps
    WHERE
        token_mint=?
        AND price_valid=1
        AND clean_price_sol IS NOT NULL
        AND clean_price_sol > 0
        AND timestamp >= ?
    ORDER BY timestamp ASC
    LIMIT 1
    """, (
        mint,
        trigger_ts
    )).fetchone()


def forward_rows(
    mint,
    entry_ts,
    end_ts
):
    return db.execute("""
    SELECT
        timestamp,
        clean_price_sol
    FROM t116_clean_swaps
    WHERE
        token_mint=?
        AND price_valid=1
        AND clean_price_sol IS NOT NULL
        AND clean_price_sol > 0
        AND timestamp > ?
        AND timestamp <= ?
    ORDER BY timestamp ASC
    """, (
        mint,
        entry_ts,
        end_ts
    )).fetchall()


def max_gap(entry_ts, rows):
    if not rows:
        return None

    times = [entry_ts] + [
        r["timestamp"]
        for r in rows
    ]

    gaps = [
        times[i] - times[i - 1]
        for i in range(1, len(times))
    ]

    return max(gaps) if gaps else None


def update_rows():
    now = time.time()

    rows = db.execute("""
    SELECT *
    FROM t120b_strict_outcomes
    WHERE strict_done_300s=0
    """).fetchall()

    for r in rows:
        entry = first_entry(
            r["token_mint"],
            r["trigger_timestamp"]
        )

        if not entry:
            continue

        entry_ts = entry["timestamp"]
        entry_price = entry["clean_price_sol"]

        if now < entry_ts + 300:
            continue

        fw = forward_rows(
            r["token_mint"],
            entry_ts,
            entry_ts + 300
        )

        if not fw:
            db.execute("""
            UPDATE t120b_strict_outcomes
            SET
                actual_entry_ts=?,
                actual_entry_price=?,
                entry_delay_s=?,
                strict_done_300s=1,
                n_forward=0,
                coverage_status='NO_FORWARD',
                last_update_at=?
            WHERE dump_event_id=?
            """, (
                entry_ts,
                entry_price,
                entry_ts - r["trigger_timestamp"],
                time.time(),
                r["dump_event_id"]
            ))

            continue

        returns = []

        for p in fw:
            ret = 100.0 * (
                p["clean_price_sol"]
                / entry_price
                - 1.0
            )

            returns.append(
                (
                    p["timestamp"],
                    ret
                )
            )

        max_row = max(
            returns,
            key=lambda x: x[1]
        )

        min_row = min(
            returns,
            key=lambda x: x[1]
        )

        end300 = returns[-1][1]

        def first_hit(level):
            for ts, ret in returns:
                if ret >= level:
                    return 1, ts - entry_ts

            return 0, None

        h10, t10 = first_hit(10)
        h20, t20 = first_hit(20)
        h30, t30 = first_hit(30)
        h50, t50 = first_hit(50)

        gap = max_gap(
            entry_ts,
            fw
        )

        if len(fw) >= 3 and (gap is None or gap <= 120):
            coverage = "GOOD"
        else:
            coverage = "SPARSE"

        db.execute("""
        UPDATE t120b_strict_outcomes
        SET
            actual_entry_ts=?,
            actual_entry_price=?,
            entry_delay_s=?,

            strict_done_300s=1,

            n_forward=?,
            first_forward_delay_s=?,
            max_gap_s=?,

            strict_max300=?,
            strict_min300=?,
            strict_end300=?,

            strict_hit10=?,
            strict_hit20=?,
            strict_hit30=?,
            strict_hit50=?,

            strict_t10=?,
            strict_t20=?,
            strict_t30=?,
            strict_t50=?,

            strict_time_to_max=?,

            coverage_status=?,
            last_update_at=?

        WHERE dump_event_id=?
        """, (
            entry_ts,
            entry_price,
            entry_ts - r["trigger_timestamp"],

            len(fw),
            fw[0]["timestamp"] - entry_ts,
            gap,

            max_row[1],
            min_row[1],
            end300,

            h10,
            h20,
            h30,
            h50,

            t10,
            t20,
            t30,
            t50,

            max_row[0] - entry_ts,

            coverage,
            time.time(),

            r["dump_event_id"]
        ))

    db.commit()


def show():
    os.system("clear")

    rows = db.execute("""
    SELECT *
    FROM t120b_strict_outcomes
    ORDER BY trigger_timestamp DESC
    """).fetchall()

    mature = [
        r for r in rows
        if r["strict_done_300s"] == 1
    ]

    observed = [
        r for r in mature
        if r["n_forward"] is not None
        and r["n_forward"] >= 1
    ]

    good = [
        r for r in observed
        if r["coverage_status"] == "GOOD"
    ]

    sparse = [
        r for r in observed
        if r["coverage_status"] == "SPARSE"
    ]

    nodata = [
        r for r in mature
        if r["coverage_status"] == "NO_FORWARD"
    ]

    old_hits = sum(
        r["old_hit20"] == 1
        for r in mature
    )

    strict_hits = sum(
        r["strict_hit20"] == 1
        for r in observed
    )

    changed = sum(
        (
            r["old_hit20"] is not None
            and r["strict_hit20"] is not None
            and r["old_hit20"] != r["strict_hit20"]
        )
        for r in observed
    )

    print("=" * 180)
    print(
        "MEMECOIN LAB — T120B STRICT FORWARD OUTCOME AUDIT"
    )
    print("=" * 180)

    print(
        f"FROZEN HOLDOUT      : {len(rows)}"
    )

    print(
        f"STRICT MATURE       : {len(mature)}"
    )

    print(
        f"OBSERVED FORWARD    : {len(observed)}"
    )

    print(
        f"GOOD COVERAGE       : {len(good)}"
    )

    print(
        f"SPARSE              : {len(sparse)}"
    )

    print(
        f"NO FORWARD DATA     : {len(nodata)}"
    )

    print()

    print(
        f"OLD HIT +20         : {old_hits}/{len(mature)}"
        if mature
        else "OLD HIT +20         : 0/0"
    )

    print(
        f"STRICT HIT +20      : {strict_hits}/{len(observed)}"
        if observed
        else "STRICT HIT +20      : 0/0"
    )

    print(
        f"CHANGED LABELS      : {changed}"
    )

    print()
    print("=" * 180)
    print("ENTRY / COVERAGE AUDIT")
    print("=" * 180)

    print(
        f"ENTRY DELAY MEDIAN  : "
        f"{fmt(median([r['entry_delay_s'] for r in observed]),1)}s"
    )

    print(
        f"ENTRY DELAY P90     : "
        f"{fmt(percentile([r['entry_delay_s'] for r in observed],0.90),1)}s"
    )

    print(
        f"FORWARD N MEDIAN    : "
        f"{fmt(median([r['n_forward'] for r in observed]),1)}"
    )

    print(
        f"MAX GAP MEDIAN      : "
        f"{fmt(median([r['max_gap_s'] for r in observed]),1)}s"
    )

    print(
        f"MAX GAP P90         : "
        f"{fmt(percentile([r['max_gap_s'] for r in observed],0.90),1)}s"
    )

    print()
    print("=" * 180)
    print("STRICT SCORE RELATION")
    print("=" * 180)

    if len(observed) >= 10:
        scores = [
            r["frozen_score"]
            for r in observed
        ]

        hit20 = [
            r["strict_hit20"]
            for r in observed
        ]

        max300 = [
            r["strict_max300"]
            for r in observed
        ]

        min300 = [
            r["strict_min300"]
            for r in observed
        ]

        end300 = [
            r["strict_end300"]
            for r in observed
        ]

        print(
            f"SCORE ↔ HIT20       : "
            f"{fmt(spearman(scores,hit20),3)}"
        )

        print(
            f"SCORE ↔ MAX300      : "
            f"{fmt(spearman(scores,max300),3)}"
        )

        print(
            f"SCORE ↔ MIN300      : "
            f"{fmt(spearman(scores,min300),3)}"
        )

        print(
            f"SCORE ↔ END300      : "
            f"{fmt(spearman(scores,end300),3)}"
        )

    print()
    print("=" * 180)
    print("STRICT SCORE QUARTILES")
    print("=" * 180)

    if len(observed) >= 20:
        ordered = sorted(
            observed,
            key=lambda r:
                r["frozen_score"]
        )

        n = len(ordered)

        print(
            f"{'Q':<5}"
            f"{'N':>7}"
            f"{'+10':>9}"
            f"{'+20':>9}"
            f"{'+30':>9}"
            f"{'+50':>9}"
            f"{'MAX MED':>11}"
            f"{'MAX P90':>11}"
            f"{'MIN MED':>11}"
            f"{'END MED':>11}"
        )

        for i in range(4):
            a = int(n * i / 4)
            b = int(n * (i + 1) / 4)

            part = ordered[a:b]

            if not part:
                continue

            def rate(col):
                return 100.0 * sum(
                    r[col] == 1
                    for r in part
                ) / len(part)

            print(
                f"Q{i+1:<4}"
                f"{len(part):>7}"
                f"{rate('strict_hit10'):>8.1f}%"
                f"{rate('strict_hit20'):>8.1f}%"
                f"{rate('strict_hit30'):>8.1f}%"
                f"{rate('strict_hit50'):>8.1f}%"
                f"{fmt(median([r['strict_max300'] for r in part]),1):>11}"
                f"{fmt(percentile([r['strict_max300'] for r in part],0.90),1):>11}"
                f"{fmt(median([r['strict_min300'] for r in part]),1):>11}"
                f"{fmt(median([r['strict_end300'] for r in part]),1):>11}"
            )

    print()
    print("=" * 180)
    print("OLD → STRICT LABEL CHANGES")
    print("=" * 180)

    changes = [
        r for r in observed
        if (
            r["old_hit20"] is not None
            and r["strict_hit20"] is not None
            and r["old_hit20"] != r["strict_hit20"]
        )
    ]

    if not changes:
        print(
            "No changed +20 labels."
        )

    else:
        for r in changes[:30]:
            print(
                f"{r['token_mint'][:18]:18} "
                f"| SCORE={r['frozen_score']:7.3f} "
                f"| OLD20={r['old_hit20']} "
                f"| STRICT20={r['strict_hit20']} "
                f"| OLD_MAX={fmt(r['old_max300'],1):>7}% "
                f"| STRICT_MAX={fmt(r['strict_max300'],1):>7}% "
                f"| ENTRY_DELAY={fmt(r['entry_delay_s'],1):>6}s "
                f"| N={r['n_forward']:3d} "
                f"| GAP={fmt(r['max_gap_s'],1):>6}s"
            )

    print()
    print("=" * 180)
    print("TOP STRICT CASES")
    print("=" * 180)

    top = sorted(
        observed,
        key=lambda r:
            r["strict_max300"]
            if r["strict_max300"] is not None
            else -999999,
        reverse=True
    )

    for r in top[:25]:
        print(
            f"{r['token_mint'][:18]:18} "
            f"| SCORE={r['frozen_score']:7.3f} "
            f"| MAX={fmt(r['strict_max300'],1):>7}% "
            f"| MIN={fmt(r['strict_min300'],1):>7}% "
            f"| END={fmt(r['strict_end300'],1):>7}% "
            f"| T20={fmt(r['strict_t20'],1):>6}s "
            f"| N={r['n_forward']:3d} "
            f"| GAP={fmt(r['max_gap_s'],1):>6}s "
            f"| {r['coverage_status']}"
        )

    print()
    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T120B only"
    )


try:
    while True:
        ensure_rows()
        update_rows()
        show()
        time.sleep(REFRESH)

except KeyboardInterrupt:
    print()
    print("T120B stopped safely.")

finally:
    db.close()
