#!/usr/bin/env python3

import sqlite3
import time
import os
import math
import statistics

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

STATE = "t116_token_state"
PUMP_EVENTS = "t116_pump_events"
DUMP_EVENTS = "t116_premigration_dump_events"

PUMP_OUT = "t117_pump_outcomes"
DUMP_OUT = "t117_dump_outcomes"

REFRESH = 20

MIGRATION_MATURITY_S = 1800

PUMP_LEVELS = [20, 50, 100, 200]
DUMP_LEVELS = [10, 20, 30, 40, 50]


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# HELPERS
# ============================================================

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


def fmt(x, n=2):
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
            out[
                indexed[k][0]
            ] = rank

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
        sum(
            (a - mx) ** 2
            for a in x
        )
    )

    dy = math.sqrt(
        sum(
            (b - my) ** 2
            for b in y
        )
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

    xs = [a for a, _ in pairs]
    ys = [b for _, b in pairs]

    return pearson(
        ranks(xs),
        ranks(ys)
    )


def table_exists(name):

    return db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE type='table'
      AND name=?
    """, (
        name,
    )).fetchone() is not None


# ============================================================
# BASIC TOKEN UNIVERSE
# ============================================================

def token_states():

    return db.execute(f"""
    SELECT *
    FROM {STATE}
    """).fetchall()


def migration_state(r, now):

    if r["migrated"] == 1:
        return "MIGRATED"

    first_seen = r["first_seen"]

    if first_seen is None:
        return "CENSORED"

    age = (
        now
        - first_seen
    )

    if age >= MIGRATION_MATURITY_S:
        return "NON_MIGRATED_OBSERVED"

    return "CENSORED"


# ============================================================
# PUMP TOKEN-LEVEL
# ============================================================

def pump_level_rows(level):

    return db.execute(f"""
    WITH first_event AS (

        SELECT
            token_mint,
            MIN(trigger_timestamp) AS first_trigger

        FROM {PUMP_EVENTS}

        WHERE pump_level=?

        GROUP BY token_mint
    )

    SELECT
        e.*,
        o.path_done_300s,
        o.path_snapshots_300s,
        o.path_max_return_300s,
        o.path_min_return_300s,
        o.path_end_return_300s,
        o.path_new_high_300s,
        o.migrated_after_event

    FROM first_event f

    JOIN {PUMP_EVENTS} e
      ON e.token_mint=f.token_mint
     AND e.pump_level=?
     AND e.trigger_timestamp=f.first_trigger

    LEFT JOIN {PUMP_OUT} o
      ON o.t116_pump_event_id=e.id
    """, (
        level,
        level
    )).fetchall()


# ============================================================
# DUMP TOKEN-LEVEL
# ============================================================

def dump_level_rows(level):

    return db.execute(f"""
    WITH first_event AS (

        SELECT
            token_mint,
            MIN(trigger_timestamp) AS first_trigger

        FROM {DUMP_EVENTS}

        WHERE dump_level=?

        GROUP BY token_mint
    )

    SELECT
        e.*,
        o.path_done_300s,
        o.path_snapshots_300s,
        o.path_max_return_300s,
        o.path_min_return_300s,
        o.path_end_return_300s,
        o.rebound20_300,
        o.rebound50_300,
        o.reclaim_old_peak_300,
        o.migrated_after_event

    FROM first_event f

    JOIN {DUMP_EVENTS} e
      ON e.token_mint=f.token_mint
     AND e.dump_level=?
     AND e.trigger_timestamp=f.first_trigger

    LEFT JOIN {DUMP_OUT} o
      ON o.t116_dump_event_id=e.id
    """, (
        level,
        level
    )).fetchall()


# ============================================================
# FIRST QUALIFYING DUMP PER TOKEN
# ============================================================

def first_dump_per_token():

    return db.execute(f"""
    WITH first_dump AS (

        SELECT
            token_mint,
            MIN(trigger_timestamp) AS ts

        FROM {DUMP_EVENTS}

        GROUP BY token_mint
    )

    SELECT
        e.*,
        o.path_done_300s,
        o.path_snapshots_300s,
        o.path_max_return_300s,
        o.path_min_return_300s,
        o.path_end_return_300s,
        o.rebound20_300,
        o.rebound50_300,
        o.reclaim_old_peak_300

    FROM first_dump f

    JOIN {DUMP_EVENTS} e
      ON e.token_mint=f.token_mint
     AND e.trigger_timestamp=f.ts

    LEFT JOIN {DUMP_OUT} o
      ON o.t116_dump_event_id=e.id
    """).fetchall()


# ============================================================
# FIRST QUALIFYING PUMP PER TOKEN
# ============================================================

def first_pump_per_token():

    return db.execute(f"""
    WITH first_pump AS (

        SELECT
            token_mint,
            MIN(trigger_timestamp) AS ts

        FROM {PUMP_EVENTS}

        GROUP BY token_mint
    )

    SELECT
        e.*,
        o.path_done_300s,
        o.path_snapshots_300s,
        o.path_max_return_300s,
        o.path_min_return_300s,
        o.path_end_return_300s,
        o.path_new_high_300s

    FROM first_pump f

    JOIN {PUMP_EVENTS} e
      ON e.token_mint=f.token_mint
     AND e.trigger_timestamp=f.ts

    LEFT JOIN {PUMP_OUT} o
      ON o.t116_pump_event_id=e.id
    """).fetchall()


# ============================================================
# STATE AT TOKEN LEVEL
# ============================================================

def state_map():

    rows = token_states()

    return {
        r["token_mint"]: r
        for r in rows
    }


# ============================================================
# EVENT SEQUENCE
# ============================================================

def sequence_for_token(mint):

    rows = db.execute(f"""
    SELECT
        trigger_timestamp AS ts,
        'P' AS kind,
        pump_level AS level

    FROM {PUMP_EVENTS}

    WHERE token_mint=?

    UNION ALL

    SELECT
        trigger_timestamp AS ts,
        'D' AS kind,
        dump_level AS level

    FROM {DUMP_EVENTS}

    WHERE token_mint=?

    ORDER BY ts ASC
    """, (
        mint,
        mint
    )).fetchall()

    if not rows:
        return None

    seq = []

    seen = set()

    for r in rows:

        key = (
            r["kind"],
            r["level"]
        )

        if key in seen:
            continue

        seen.add(key)

        seq.append(
            f"{r['kind']}{r['level']}"
        )

    return "→".join(seq)


# ============================================================
# FEATURE AUDIT
# ============================================================

DUMP_FEATURES = [
    "drawdown_pct",
    "run_from_first_pct",
    "buys_total",
    "sells_total",
    "buy_sol_total",
    "sell_sol_total",
    "net_sol_total",
    "swaps_30s",
    "buys_30s",
    "sells_30s",
    "net_sol_30s",
    "swaps_60s",
    "buys_60s",
    "sells_60s",
    "net_sol_60s",
]


PUMP_FEATURES = [
    "run_from_first_pct",
    "buys_total",
    "sells_total",
    "buy_sol_total",
    "sell_sol_total",
    "net_sol_total",
    "swaps_30s",
    "buys_30s",
    "sells_30s",
    "net_sol_30s",
    "swaps_60s",
    "buys_60s",
    "sells_60s",
    "net_sol_60s",
]


def feature_audit(
    rows,
    features,
    label_col
):

    results = []

    usable = [
        r
        for r in rows
        if (
            r[label_col] is not None
            and r["path_snapshots_300s"] is not None
            and r["path_snapshots_300s"] >= 1
        )
    ]

    for feature in features:

        pos = []
        neg = []

        x = []
        y = []

        for r in usable:

            v = r[feature]
            label = r[label_col]

            if not valid(v):
                continue

            x.append(v)
            y.append(float(label))

            if label == 1:
                pos.append(v)
            else:
                neg.append(v)

        results.append({
            "feature":
                feature,

            "pos_med":
                median(pos),

            "neg_med":
                median(neg),

            "rho":
                spearman(x, y),

            "n":
                len(x),

            "n_pos":
                len(pos),

            "n_neg":
                len(neg),
        })

    results.sort(
        key=lambda r:
            abs(r["rho"])
            if r["rho"] is not None
            else -1,
        reverse=True
    )

    return results


# ============================================================
# ROBUSTNESS
# ============================================================

def loto_spearman(
    rows,
    feature,
    outcome
):

    base = [
        r
        for r in rows
        if valid(r.get(feature))
        and valid(r.get(outcome))
    ]

    tokens = sorted({
        r["token_mint"]
        for r in base
    })

    full = spearman(
        [r[feature] for r in base],
        [r[outcome] for r in base]
    )

    vals = []

    signs = []

    for token in tokens:

        sub = [
            r
            for r in base
            if r["token_mint"] != token
        ]

        rho = spearman(
            [r[feature] for r in sub],
            [r[outcome] for r in sub]
        )

        if rho is None:
            continue

        vals.append(rho)

        if full is not None:
            signs.append(
                int(
                    rho == 0
                    or full == 0
                    or (
                        rho > 0
                        and full > 0
                    )
                    or (
                        rho < 0
                        and full < 0
                    )
                )
            )

    return {
        "full":
            full,

        "loto_median":
            median(vals),

        "sign_consistency":
            (
                sum(signs)
                if signs
                else 0
            ),

        "sign_total":
            len(signs),

        "tokens":
            len(tokens),
    }


# ============================================================
# SHOW
# ============================================================

def show():

    os.system("clear")

    now = time.time()

    states = token_states()
    smap = state_map()

    pump_events_n = db.execute(
        f"SELECT COUNT(*) FROM {PUMP_EVENTS}"
    ).fetchone()[0]

    pump_tokens_n = db.execute(
        f"SELECT COUNT(DISTINCT token_mint) FROM {PUMP_EVENTS}"
    ).fetchone()[0]

    dump_events_n = db.execute(
        f"SELECT COUNT(*) FROM {DUMP_EVENTS}"
    ).fetchone()[0]

    dump_tokens_n = db.execute(
        f"SELECT COUNT(DISTINCT token_mint) FROM {DUMP_EVENTS}"
    ).fetchone()[0]

    migrated = 0
    non_migrated = 0
    censored = 0

    for s in states:

        ms = migration_state(
            s,
            now
        )

        if ms == "MIGRATED":
            migrated += 1

        elif ms == "NON_MIGRATED_OBSERVED":
            non_migrated += 1

        else:
            censored += 1


    print("=" * 180)
    print(
        "MEMECOIN LAB — T118 PRE-MIGRATION TOKEN-LEVEL AUDIT"
    )
    print("=" * 180)

    print(
        "MODE              : RESEARCH / AUDIT ONLY"
    )

    print(
        "UNIT              : UNIQUE TOKEN"
    )

    print(
        "MODEL FITTING     : NONE"
    )

    print(
        "THRESHOLD SEARCH  : NONE"
    )

    print(
        "LIVE RULE         : NONE"
    )


    # ========================================================
    # INTEGRITY
    # ========================================================

    print()
    print("=" * 180)
    print("DATASET INTEGRITY")
    print("=" * 180)

    print(
        f"T116 TOKENS             : {len(states)}"
    )

    print(
        f"TOKENS WITH PUMP EVENT  : {pump_tokens_n}"
    )

    print(
        f"TOKENS WITH DUMP EVENT  : {dump_tokens_n}"
    )

    print(
        f"MIGRATED                : {migrated}"
    )

    print(
        f"NON_MIGRATED OBSERVED   : {non_migrated}"
    )

    print(
        f"CENSORED                 : {censored}"
    )

    print(
        f"PUMP RAW/UNIQUE          : "
        f"{pump_events_n}/{pump_tokens_n} "
        f"= {pump_events_n/max(1,pump_tokens_n):.2f}x"
    )

    print(
        f"DUMP RAW/UNIQUE          : "
        f"{dump_events_n}/{dump_tokens_n} "
        f"= {dump_events_n/max(1,dump_tokens_n):.2f}x"
    )


    # ========================================================
    # PUMP CONTINUATION
    # ========================================================

    print()
    print("=" * 180)
    print("1 — PUMP CONTINUATION")
    print("=" * 180)

    print(
        f"{'LEVEL':<8}"
        f"{'TOKENS':>8}"
        f"{'OBS300':>10}"
        f"{'MAX MED':>12}"
        f"{'MAX AVG':>12}"
        f"{'MIN MED':>12}"
        f"{'END MED':>12}"
        f"{'MIG':>8}"
    )

    for level in PUMP_LEVELS:

        rows = pump_level_rows(
            level
        )

        obs = [
            r
            for r in rows
            if (
                r["path_done_300s"] == 1
                and r["path_snapshots_300s"] is not None
                and r["path_snapshots_300s"] >= 1
            )
        ]

        print(
            f"+{level:<7}"
            f"{len(rows):>8}"
            f"{len(obs):>10}"
            f"{fmt(median([r['path_max_return_300s'] for r in obs]),1):>12}"
            f"{fmt(mean([r['path_max_return_300s'] for r in obs]),1):>12}"
            f"{fmt(median([r['path_min_return_300s'] for r in obs]),1):>12}"
            f"{fmt(median([r['path_end_return_300s'] for r in obs]),1):>12}"
            f"{sum(r['migrated_after_event']==1 for r in rows):>8}"
        )


    # ========================================================
    # DUMP RESURRECTION
    # ========================================================

    print()
    print("=" * 180)
    print("2 — DUMP RESURRECTION")
    print("=" * 180)

    print(
        f"{'LEVEL':<8}"
        f"{'TOKENS':>8}"
        f"{'OBS300':>10}"
        f"{'+20':>8}"
        f"{'+50':>8}"
        f"{'PEAK':>8}"
        f"{'MIG':>8}"
    )

    for level in DUMP_LEVELS:

        rows = dump_level_rows(
            level
        )

        obs = [
            r
            for r in rows
            if (
                r["path_done_300s"] == 1
                and r["path_snapshots_300s"] is not None
                and r["path_snapshots_300s"] >= 1
            )
        ]

        print(
            f"-{level:<7}"
            f"{len(rows):>8}"
            f"{len(obs):>10}"
            f"{sum(r['rebound20_300']==1 for r in obs):>8}"
            f"{sum(r['rebound50_300']==1 for r in obs):>8}"
            f"{sum(r['reclaim_old_peak_300']==1 for r in obs):>8}"
            f"{sum(r['migrated_after_event']==1 for r in rows):>8}"
        )

    print()

    print(
        f"{'LEVEL':<8}"
        f"{'MAX MED':>12}"
        f"{'MAX AVG':>12}"
        f"{'MIN MED':>12}"
        f"{'END MED':>12}"
    )

    for level in DUMP_LEVELS:

        rows = [
            r
            for r in dump_level_rows(level)
            if (
                r["path_done_300s"] == 1
                and r["path_snapshots_300s"] is not None
                and r["path_snapshots_300s"] >= 1
            )
        ]

        print(
            f"-{level:<7}"
            f"{fmt(median([r['path_max_return_300s'] for r in rows]),1):>12}"
            f"{fmt(mean([r['path_max_return_300s'] for r in rows]),1):>12}"
            f"{fmt(median([r['path_min_return_300s'] for r in rows]),1):>12}"
            f"{fmt(median([r['path_end_return_300s'] for r in rows]),1):>12}"
        )


    # ========================================================
    # FEATURE AUDIT — DUMP
    # ========================================================

    print()
    print("=" * 180)
    print("4 — FEATURE AUDIT: DUMP RESURRECTION +20")
    print("=" * 180)

    first_dumps = first_dump_per_token()

    dump_audit = feature_audit(
        first_dumps,
        DUMP_FEATURES,
        "rebound20_300"
    )

    print(
        f"{'FEATURE':<28}"
        f"{'POS MED':>12}"
        f"{'NEG MED':>12}"
        f"{'RHO':>10}"
        f"{'N':>8}"
    )

    for r in dump_audit[:15]:

        print(
            f"{r['feature']:<28}"
            f"{fmt(r['pos_med'],3):>12}"
            f"{fmt(r['neg_med'],3):>12}"
            f"{fmt(r['rho'],3):>10}"
            f"{r['n']:>8}"
        )


    # ========================================================
    # FEATURE AUDIT — PUMP
    # ========================================================

    print()
    print("=" * 180)
    print("5 — FEATURE AUDIT: PUMP CONTINUATION")
    print("=" * 180)

    first_pumps = first_pump_per_token()

    for r in first_pumps:
        rdict = dict(r)

    pump_cont_rows = []

    for r in first_pumps:

        d = dict(r)

        mx = d.get(
            "path_max_return_300s"
        )

        d[
            "continued20"
        ] = (
            int(mx >= 20)
            if valid(mx)
            else None
        )

        pump_cont_rows.append(
            d
        )

    pump_audit = []

    usable = [
        r
        for r in pump_cont_rows
        if (
            r["continued20"] is not None
            and r["path_snapshots_300s"] is not None
            and r["path_snapshots_300s"] >= 1
        )
    ]

    for feature in PUMP_FEATURES:

        pos = []
        neg = []
        x = []
        y = []

        for r in usable:

            v = r.get(
                feature
            )

            if not valid(v):
                continue

            label = r[
                "continued20"
            ]

            x.append(v)
            y.append(label)

            if label:
                pos.append(v)
            else:
                neg.append(v)

        pump_audit.append({
            "feature":
                feature,

            "pos_med":
                median(pos),

            "neg_med":
                median(neg),

            "rho":
                spearman(x, y),

            "n":
                len(x),
        })

    pump_audit.sort(
        key=lambda r:
            abs(r["rho"])
            if r["rho"] is not None
            else -1,
        reverse=True
    )

    print(
        f"{'FEATURE':<28}"
        f"{'POS MED':>12}"
        f"{'NEG MED':>12}"
        f"{'RHO':>10}"
        f"{'N':>8}"
    )

    for r in pump_audit[:15]:

        print(
            f"{r['feature']:<28}"
            f"{fmt(r['pos_med'],3):>12}"
            f"{fmt(r['neg_med'],3):>12}"
            f"{fmt(r['rho'],3):>10}"
            f"{r['n']:>8}"
        )


    # ========================================================
    # MIGRATION
    # ========================================================

    print()
    print("=" * 180)
    print("6 — PRE-MIGRATION → MIGRATION")
    print("=" * 180)

    print(
        f"MIGRATED UNIQUE TOKENS       : {migrated}"
    )

    print(
        f"NON-MIGRATED OBSERVED TOKENS : {non_migrated}"
    )

    print(
        f"CENSORED                      : {censored}"
    )

    if migrated < 30:

        print()
        print(
            "⚠ MIGRATION SAMPLE TOO SMALL FOR MODEL"
        )

        print(
            "No classifier. No threshold search."
        )


    # ========================================================
    # EVENT SEQUENCES
    # ========================================================

    print()
    print("=" * 180)
    print("7 — EVENT SEQUENCE")
    print("=" * 180)

    sequence_stats = {}

    for s in states:

        mint = s[
            "token_mint"
        ]

        seq = sequence_for_token(
            mint
        )

        if not seq:
            continue

        item = sequence_stats.setdefault(
            seq,
            {
                "tokens": 0,
                "migrated": 0,
            }
        )

        item[
            "tokens"
        ] += 1

        if s[
            "migrated"
        ] == 1:

            item[
                "migrated"
            ] += 1

    ordered = sorted(
        sequence_stats.items(),
        key=lambda kv:
            kv[1]["tokens"],
        reverse=True
    )

    print(
        f"{'SEQUENCE':<50}"
        f"{'TOKENS':>8}"
        f"{'MIG%':>10}"
    )

    for seq, st in ordered[:20]:

        mig_pct = (
            100.0
            * st["migrated"]
            / st["tokens"]
        )

        print(
            f"{seq[:49]:<50}"
            f"{st['tokens']:>8}"
            f"{mig_pct:>9.1f}%"
        )


    # ========================================================
    # ROBUSTNESS
    # ========================================================

    print()
    print("=" * 180)
    print("8 — TOKEN-LEVEL ROBUSTNESS")
    print("=" * 180)

    robust_rows = []

    for r in first_dumps:

        d = dict(r)

        if (
            d.get(
                "path_snapshots_300s"
            ) is None
            or d[
                "path_snapshots_300s"
            ] < 1
        ):
            continue

        robust_rows.append(
            d
        )

    candidates = [
        "net_sol_60s",
        "buys_60s",
        "sells_60s",
        "net_sol_total",
        "run_from_first_pct",
        "drawdown_pct",
    ]

    print(
        f"{'FEATURE':<24}"
        f"{'FULL':>10}"
        f"{'LOTO MED':>12}"
        f"{'SIGN':>12}"
        f"{'TOKENS':>10}"
    )

    for feature in candidates:

        res = loto_spearman(
            robust_rows,
            feature,
            "path_max_return_300s"
        )

        print(
            f"{feature:<24}"
            f"{fmt(res['full'],3):>10}"
            f"{fmt(res['loto_median'],3):>12}"
            f"{res['sign_consistency']:>5}/"
            f"{res['sign_total']:<5}"
            f"{res['tokens']:>10}"
        )


    # ========================================================
    # FINAL INTERPRETATION
    # ========================================================

    print()
    print("=" * 180)
    print("FINAL INTERPRETATION")
    print("=" * 180)

    best_dump = (
        dump_audit[0]
        if dump_audit
        else None
    )

    best_pump = (
        pump_audit[0]
        if pump_audit
        else None
    )

    print(
        "Best dump-resurrection signal : "
        + (
            f"{best_dump['feature']} "
            f"(rho={fmt(best_dump['rho'],3)})"
            if best_dump
            else "NA"
        )
    )

    print(
        "Best pump-continuation signal  : "
        + (
            f"{best_pump['feature']} "
            f"(rho={fmt(best_pump['rho'],3)})"
            if best_pump
            else "NA"
        )
    )

    if ordered:

        seq, st = ordered[0]

        print(
            f"Most common sequence           : "
            f"{seq} "
            f"(N={st['tokens']})"
        )

    print(
        f"Migration sample size          : {migrated}"
    )

    first_dump_obs = [
        r
        for r in first_dumps
        if (
            r["path_snapshots_300s"] is not None
            and r["path_snapshots_300s"] >= 1
            and r["rebound20_300"] is not None
        )
    ]

    positives = sum(
        r[
            "rebound20_300"
        ] == 1
        for r in first_dump_obs
    )

    negatives = (
        len(first_dump_obs)
        - positives
    )

    ready_t119 = (
        dump_tokens_n >= 300
        and positives >= 50
        and negatives >= 100
    )

    print()

    if ready_t119:

        print(
            "NEXT ACTION: 🟢 READY FOR T119 CONTROLLED MULTIVARIATE AUDIT"
        )

    else:

        print(
            "NEXT ACTION: 🔵 COLLECT MORE"
        )

        print(
            f"Current first-dump sample: "
            f"N={len(first_dump_obs)} "
            f"| +20={positives} "
            f"| no+20={negatives}"
        )

        print(
            "Target: dump tokens>=300, "
            "+20>=50, no+20>=100"
        )

    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T118 only"
    )


try:

    while True:

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T118 stopped safely."
    )


finally:

    db.close()
