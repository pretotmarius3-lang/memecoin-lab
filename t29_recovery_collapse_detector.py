import sqlite3
import statistics
import math

DB = "validation_v090.db"

MAX_ID = 545
VOLUME_CUT = 8837.925

TARGET = 5.0
HORIZON = 120

CHECKPOINTS = [30, 45, 60, 75]

DISCOVERY_FRAC = 0.60


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def percentile(vals, p):
    vals = sorted(x for x in vals if valid(x))

    if not vals:
        return None

    k = (len(vals)-1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return vals[lo]*(hi-k) + vals[hi]*(k-lo)


def ret(entry, price):
    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    return (price/entry - 1) * 100


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# FIRST V2 SIGNAL PER TOKEN
# ============================================================

signals = db.execute("""
WITH first_dex AS (
    SELECT d.*
    FROM dex_prices d
    JOIN (
        SELECT event_id, MIN(timestamp) AS first_time
        FROM dex_prices
        GROUP BY event_id
    ) x
      ON d.event_id=x.event_id
     AND d.timestamp=x.first_time
)

SELECT
    e.id,
    e.token_mint,

    d.timestamp AS entry_timestamp,
    d.price_usd AS entry_price,
    d.volume_m5

FROM events e

JOIN first_dex d
ON d.event_id=e.id

WHERE
    e.id <= ?
    AND e.fa95=1
    AND e.new_wallets30 >= 2
    AND d.volume_m5 >= ?

ORDER BY e.id
""", (
    MAX_ID,
    VOLUME_CUT
)).fetchall()


first = []
seen = set()

for r in signals:
    token = r["token_mint"]

    if token in seen:
        continue

    seen.add(token)
    first.append(r)


cut = int(
    len(first) * DISCOVERY_FRAC
)

disc_tokens = set(
    r["token_mint"]
    for r in first[:cut]
)

valid_tokens = set(
    r["token_mint"]
    for r in first[cut:]
)


# ============================================================
# PATH
# ============================================================

def load_path(signal):
    return db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND timestamp >= ?
            AND timestamp <= ?
            AND price_usd IS NOT NULL
            AND price_usd > 0

        ORDER BY timestamp
    """, (
        signal["id"],
        signal["entry_timestamp"],
        signal["entry_timestamp"] + HORIZON,
    )).fetchall()


def nearest_before(path, target):
    eligible = [
        r for r in path
        if r["timestamp"] <= target
    ]

    if not eligible:
        return None

    return eligible[-1]


def slope_between(path_returns, now_ts, seconds):
    start_ts = now_ts - seconds

    before = [
        (ts, r)
        for ts, r in path_returns
        if ts <= start_ts
    ]

    after = [
        (ts, r)
        for ts, r in path_returns
        if ts <= now_ts
    ]

    if not after:
        return None

    current_ts, current_ret = after[-1]

    if not before:
        return None

    old_ts, old_ret = before[-1]

    dt = current_ts - old_ts

    if dt <= 0:
        return None

    return (
        current_ret - old_ret
    ) / dt


# ============================================================
# BUILD RECORDS
# ============================================================

records = []

for s in first:

    path = load_path(s)

    if not path:
        continue

    entry = s["entry_price"]
    start = s["entry_timestamp"]

    path_returns = []

    for row in path:
        r = ret(
            entry,
            row["price_usd"]
        )

        if valid(r):
            path_returns.append(
                (
                    row["timestamp"],
                    r
                )
            )

    if not path_returns:
        continue

    # --------------------------------------------------------
    # CLASS
    # --------------------------------------------------------

    first_hit_5 = None

    for ts, r in path_returns:
        if r >= TARGET:
            first_hit_5 = ts - start
            break

    if (
        first_hit_5 is not None
        and first_hit_5 <= 45
    ):
        cls = "EARLY_RUNNER"

    elif (
        first_hit_5 is not None
        and first_hit_5 <= HORIZON
    ):
        cls = "RECOVERY_RUNNER"

    else:
        cls = "TRUE_FAILURE"

    rec = {
        "token":
            s["token_mint"],

        "event_id":
            s["id"],

        "split":
            (
                "DISC"
                if s["token_mint"]
                in disc_tokens
                else "VALID"
            ),

        "class":
            cls,

        "first_hit_5":
            first_hit_5,
    }

    # --------------------------------------------------------
    # CHECKPOINT FEATURES
    # --------------------------------------------------------

    for cp in CHECKPOINTS:

        target_ts = (
            start + cp
        )

        snap = nearest_before(
            path,
            target_ts
        )

        prior = [
            (ts, r)
            for ts, r in path_returns
            if ts <= target_ts
        ]

        if (
            snap is None
            or not prior
        ):
            for name in [
                "ret",
                "max",
                "min",
                "drawdown",
                "recovery",
                "slope10",
                "slope15",
                "time_since_low",
                "time_since_high",
                "new_highs",
                "new_lows",
                "positive_share",
                "negative_share",
                "points",
            ]:
                rec[f"{name}_{cp}"] = None

            continue

        current = ret(
            entry,
            snap["price_usd"]
        )

        vals = [
            r for _, r in prior
        ]

        peak = max(vals)
        trough = min(vals)

        # last time peak/trough occurred
        peak_times = [
            ts for ts, r in prior
            if r == peak
        ]

        trough_times = [
            ts for ts, r in prior
            if r == trough
        ]

        last_peak_ts = (
            peak_times[-1]
            if peak_times
            else None
        )

        last_trough_ts = (
            trough_times[-1]
            if trough_times
            else None
        )

        # count incremental highs/lows
        running_high = None
        running_low = None

        new_highs = 0
        new_lows = 0

        for _, r in prior:

            if (
                running_high is None
                or r > running_high
            ):
                running_high = r
                new_highs += 1

            if (
                running_low is None
                or r < running_low
            ):
                running_low = r
                new_lows += 1

        positive_share = (
            sum(r > 0 for r in vals)
            / len(vals)
        )

        negative_share = (
            sum(r < 0 for r in vals)
            / len(vals)
        )

        rec[f"ret_{cp}"] = current

        rec[f"max_{cp}"] = peak

        rec[f"min_{cp}"] = trough

        rec[f"drawdown_{cp}"] = (
            current - peak
            if valid(current)
            else None
        )

        rec[f"recovery_{cp}"] = (
            current - trough
            if valid(current)
            else None
        )

        rec[f"slope10_{cp}"] = slope_between(
            path_returns,
            snap["timestamp"],
            10
        )

        rec[f"slope15_{cp}"] = slope_between(
            path_returns,
            snap["timestamp"],
            15
        )

        rec[f"time_since_low_{cp}"] = (
            snap["timestamp"]
            - last_trough_ts
            if last_trough_ts is not None
            else None
        )

        rec[f"time_since_high_{cp}"] = (
            snap["timestamp"]
            - last_peak_ts
            if last_peak_ts is not None
            else None
        )

        rec[f"new_highs_{cp}"] = new_highs
        rec[f"new_lows_{cp}"] = new_lows

        rec[f"positive_share_{cp}"] = positive_share
        rec[f"negative_share_{cp}"] = negative_share

        rec[f"points_{cp}"] = len(prior)

    records.append(rec)


# ============================================================
# DESCRIPTIVE COMPARISON
# ============================================================

def group_stats(rows, field):
    vals = [
        r[field]
        for r in rows
        if valid(r[field])
    ]

    return {
        "n": len(vals),
        "med": med(vals),
        "avg": avg(vals),
    }


def print_comparison(
    split_name,
    rows
):

    recovery = [
        r for r in rows
        if r["class"] == "RECOVERY_RUNNER"
    ]

    failures = [
        r for r in rows
        if r["class"] == "TRUE_FAILURE"
    ]

    early = [
        r for r in rows
        if r["class"] == "EARLY_RUNNER"
    ]

    print()
    print("="*175)
    print(
        f"{split_name} — "
        f"RECOVERY RUNNER VS TRUE FAILURE"
    )
    print("="*175)

    print(
        f"TOKENS={len(rows)} | "
        f"EARLY={len(early)} | "
        f"RECOVERY={len(recovery)} | "
        f"FAILURE={len(failures)}"
    )

    FEATURES = [
        "ret",
        "max",
        "min",
        "drawdown",
        "recovery",
        "slope10",
        "slope15",
        "time_since_low",
        "time_since_high",
        "new_highs",
        "new_lows",
        "positive_share",
        "negative_share",
        "points",
    ]

    for cp in CHECKPOINTS:

        print()
        print(
            f"CHECKPOINT {cp}s"
        )
        print("-"*145)

        print(
            f"{'FEATURE':22} "
            f"{'REC MED':>14} "
            f"{'FAIL MED':>14} "
            f"{'DIFF':>14} "
            f"{'REC N':>8} "
            f"{'FAIL N':>8}"
        )

        for feature in FEATURES:

            field = (
                f"{feature}_{cp}"
            )

            rs = group_stats(
                recovery,
                field
            )

            fs = group_stats(
                failures,
                field
            )

            diff = (
                rs["med"] - fs["med"]
                if (
                    valid(rs["med"])
                    and valid(fs["med"])
                )
                else None
            )

            print(
                f"{feature:22} "
                f"{(rs['med'] if valid(rs['med']) else 0):+13.3f} "
                f"{(fs['med'] if valid(fs['med']) else 0):+13.3f} "
                f"{(diff if valid(diff) else 0):+13.3f} "
                f"{rs['n']:8d} "
                f"{fs['n']:8d}"
            )


disc = [
    r for r in records
    if r["split"] == "DISC"
]

valid_rows = [
    r for r in records
    if r["split"] == "VALID"
]


print("="*175)
print(
    "MEMECOIN LAB — "
    "T29 EARLY PATH RECOVERY / COLLAPSE DETECTOR"
)
print("="*175)

print(
    f"FIRST V2 TOKENS : "
    f"{len(records)}"
)

print(
    f"EARLY_RUNNER    : "
    f"hits +5% by 45s"
)

print(
    f"RECOVERY_RUNNER : "
    f"hits +5% after 45s but by 120s"
)

print(
    f"TRUE_FAILURE    : "
    f"never hits +5% by 120s"
)

print(
    "NO FUTURE OUTCOME USED "
    "IN CHECKPOINT FEATURES."
)

print_comparison(
    "DISCOVERY",
    disc
)

print_comparison(
    "VALIDATION",
    valid_rows
)


# ============================================================
# RULE TEST ENGINE
# ============================================================

def evaluate_rule(
    rows,
    cp,
    rule
):

    usable = [
        r for r in rows
        if (
            r["class"]
            in [
                "EARLY_RUNNER",
                "RECOVERY_RUNNER",
                "TRUE_FAILURE"
            ]
        )
    ]

    if not usable:
        return None

    kept = []
    aborted = []

    for r in usable:

        try:
            keep = rule(r)
        except:
            keep = True

        if keep:
            kept.append(r)
        else:
            aborted.append(r)

    rec_total = sum(
        r["class"] == "RECOVERY_RUNNER"
        for r in usable
    )

    rec_kept = sum(
        r["class"] == "RECOVERY_RUNNER"
        for r in kept
    )

    all_runner_total = sum(
        r["class"]
        in [
            "EARLY_RUNNER",
            "RECOVERY_RUNNER"
        ]
        for r in usable
    )

    all_runner_kept = sum(
        r["class"]
        in [
            "EARLY_RUNNER",
            "RECOVERY_RUNNER"
        ]
        for r in kept
    )

    fail_total = sum(
        r["class"] == "TRUE_FAILURE"
        for r in usable
    )

    fail_aborted = sum(
        r["class"] == "TRUE_FAILURE"
        for r in aborted
    )

    return {
        "kept":
            len(kept),

        "aborted":
            len(aborted),

        "recovery_recall":
            (
                100
                * rec_kept
                / rec_total
                if rec_total
                else None
            ),

        "all_runner_recall":
            (
                100
                * all_runner_kept
                / all_runner_total
                if all_runner_total
                else None
            ),

        "failure_kill":
            (
                100
                * fail_aborted
                / fail_total
                if fail_total
                else None
            ),
    }


# ============================================================
# COARSE DISCOVERY RULES
# ============================================================

candidate_rules = []

for cp in [45, 60, 75]:

    # recovery_from_low
    for x in [0.0, 1.0, 2.0, 3.0, 5.0]:

        candidate_rules.append((
            cp,
            f"recovery>={x:.1f}",

            lambda r,cp=cp,x=x:
                (
                    valid(r[f"recovery_{cp}"])
                    and r[f"recovery_{cp}"] >= x
                )
        ))

    # slope 10s
    for x in [-0.20, -0.10, 0.0, 0.10, 0.20]:

        candidate_rules.append((
            cp,
            f"slope10>={x:.2f}",

            lambda r,cp=cp,x=x:
                (
                    valid(r[f"slope10_{cp}"])
                    and r[f"slope10_{cp}"] >= x
                )
        ))

    # slope 15s
    for x in [-0.15, -0.05, 0.0, 0.05, 0.15]:

        candidate_rules.append((
            cp,
            f"slope15>={x:.2f}",

            lambda r,cp=cp,x=x:
                (
                    valid(r[f"slope15_{cp}"])
                    and r[f"slope15_{cp}"] >= x
                )
        ))

    # current return + recovery
    for rx in [-10.0, -5.0, 0.0]:

        for recx in [0.0, 2.0, 5.0]:

            candidate_rules.append((
                cp,
                f"ret>={rx:.0f}&rec>={recx:.0f}",

                lambda r,cp=cp,rx=rx,recx=recx:
                    (
                        valid(r[f"ret_{cp}"])
                        and valid(r[f"recovery_{cp}"])
                        and r[f"ret_{cp}"] >= rx
                        and r[f"recovery_{cp}"] >= recx
                    )
            ))

    # time since low + recovery
    for tx in [10.0, 20.0, 30.0]:

        for recx in [1.0, 2.0, 5.0]:

            candidate_rules.append((
                cp,
                f"TSLOW<={tx:.0f}&REC>={recx:.0f}",

                lambda r,cp=cp,tx=tx,recx=recx:
                    (
                        valid(r[f"time_since_low_{cp}"])
                        and valid(r[f"recovery_{cp}"])
                        and r[f"time_since_low_{cp}"] <= tx
                        and r[f"recovery_{cp}"] >= recx
                    )
            ))


# ============================================================
# SELECT ON DISCOVERY ONLY
# ============================================================

evaluated = []

for cp, name, rule in candidate_rules:

    d = evaluate_rule(
        disc,
        cp,
        rule
    )

    v = evaluate_rule(
        valid_rows,
        cp,
        rule
    )

    if not d or not v:
        continue

    # Require discovery recovery recall >=80 if possible.
    rec_recall = (
        d["recovery_recall"]
        if d["recovery_recall"] is not None
        else 100.0
    )

    all_recall = (
        d["all_runner_recall"]
        if d["all_runner_recall"] is not None
        else 0.0
    )

    fail_kill = (
        d["failure_kill"]
        if d["failure_kill"] is not None
        else 0.0
    )

    score = (
        fail_kill
        + 0.5 * all_recall
        + 0.5 * rec_recall
    )

    if rec_recall < 80:
        score -= 100

    evaluated.append({
        "cp":
            cp,

        "name":
            name,

        "score":
            score,

        "disc":
            d,

        "valid":
            v,
    })


evaluated.sort(
    key=lambda x:
        x["score"],
    reverse=True
)


# ============================================================
# OUTPUT RULES
# ============================================================

print()
print("="*175)
print(
    "DISCOVERY-SELECTED EMERGENCY EXIT CANDIDATES"
)
print("="*175)

print(
    f"{'CP':>4} "
    f"{'RULE':30} | "
    f"{'D FAIL KILL':>11} "
    f"{'D REC RECALL':>12} "
    f"{'D ALL RECALL':>12} | "
    f"{'V FAIL KILL':>11} "
    f"{'V REC RECALL':>12} "
    f"{'V ALL RECALL':>12}"
)

print("-"*125)

for x in evaluated[:20]:

    d = x["disc"]
    v = x["valid"]

    print(
        f"{x['cp']:4d} "
        f"{x['name'][:30]:30} | "
        f"{(d['failure_kill'] if d['failure_kill'] is not None else 0):10.1f}% "
        f"{(d['recovery_recall'] if d['recovery_recall'] is not None else 0):11.1f}% "
        f"{(d['all_runner_recall'] if d['all_runner_recall'] is not None else 0):11.1f}% | "
        f"{(v['failure_kill'] if v['failure_kill'] is not None else 0):10.1f}% "
        f"{(v['recovery_recall'] if v['recovery_recall'] is not None else 0):11.1f}% "
        f"{(v['all_runner_recall'] if v['all_runner_recall'] is not None else 0):11.1f}%"
    )


# ============================================================
# TOKEN DETAIL
# ============================================================

print()
print("="*175)
print(
    "TOKEN DETAIL"
)
print("="*175)

print(
    f"{'SPLIT':7} "
    f"{'TOKEN':20} "
    f"{'CLASS':16} "
    f"{'HIT5':>7} "
    f"{'RET45':>8} "
    f"{'REC45':>8} "
    f"{'SL10_45':>9} "
    f"{'RET60':>8} "
    f"{'REC60':>8} "
    f"{'SL10_60':>9}"
)

print("-"*120)

for r in records:

    def fp(x):
        return (
            f"{x:+7.2f}"
            if valid(x)
            else "     NA"
        )

    hit = (
        f"{r['first_hit_5']:.1f}s"
        if valid(r["first_hit_5"])
        else "NEVER"
    )

    print(
        f"{r['split']:7} "
        f"{r['token'][:20]:20} "
        f"{r['class']:16} "
        f"{hit:>7} "
        f"{fp(r['ret_45'])} "
        f"{fp(r['recovery_45'])} "
        f"{fp(r['slope10_45'])} "
        f"{fp(r['ret_60'])} "
        f"{fp(r['recovery_60'])} "
        f"{fp(r['slope10_60'])}"
    )


# ============================================================
# DECISION
# ============================================================

print()
print("="*175)
print("DECISION SUPPORT")
print("="*175)

survivors = []

for x in evaluated[:20]:

    d = x["disc"]
    v = x["valid"]

    d_rec = (
        d["recovery_recall"]
        if d["recovery_recall"] is not None
        else 100
    )

    v_rec = (
        v["recovery_recall"]
        if v["recovery_recall"] is not None
        else 100
    )

    d_fail = (
        d["failure_kill"]
        if d["failure_kill"] is not None
        else 0
    )

    v_fail = (
        v["failure_kill"]
        if v["failure_kill"] is not None
        else 0
    )

    if (
        d_rec >= 80
        and v_rec >= 80
        and d_fail >= 25
        and v_fail >= 25
    ):
        survivors.append(x)


if survivors:

    best = survivors[0]

    print(
        "CANDIDATE EMERGENCY EXIT"
    )

    print(
        f"CP={best['cp']}s "
        f"| RULE={best['name']}"
    )

    print(
        "This still needs a separate P&L simulation."
    )

else:

    print(
        "NO EARLY EXIT"
    )

    print(
        "No coarse recovery/collapse rule "
        "survived both Discovery and Validation "
        "with acceptable recovery-runner preservation."
    )


print()
print(
    "IMPORTANT:"
)

print(
    "• Do NOT modify V2 Frozen."
)

print(
    "• Do NOT modify T23."
)

print(
    "• Current frozen execution remains "
    "+5% activation / 3% trail / 120s."
)

print(
    "• T29 is diagnostic only."
)

db.close()
