import sqlite3
import statistics
import math

DB = "validation_v090.db"

MAX_ID = 545
VOLUME_CUT = 8837.925

TARGET_ACTIVATION = 5.0
HORIZON = 120

CHECKPOINTS = [10, 20, 30, 45]

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
# FIRST V2 SIGNAL / TOKEN
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

discovery_tokens = {
    r["token_mint"]
    for r in first[:cut]
}

validation_tokens = {
    r["token_mint"]
    for r in first[cut:]
}


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


def nearest_before(path, start, seconds):

    target = start + seconds

    eligible = [
        r for r in path
        if r["timestamp"] <= target
    ]

    if not eligible:
        return None

    return eligible[-1]


# ============================================================
# BUILD EARLY-PATH RECORD
# ============================================================

records = []

for s in first:

    path = load_path(s)

    if not path:
        continue

    entry = s["entry_price"]

    path_returns = []

    for r in path:
        x = ret(
            entry,
            r["price_usd"]
        )

        if valid(x):
            path_returns.append(
                (
                    r["timestamp"],
                    x
                )
            )

    if not path_returns:
        continue

    ever_hit_5 = any(
        x >= TARGET_ACTIVATION
        for _,x in path_returns
    )

    rec = {
        "token":
            s["token_mint"],

        "event_id":
            s["id"],

        "split":
            (
                "DISC"
                if s["token_mint"]
                in discovery_tokens
                else "VALID"
            ),

        "success":
            1 if ever_hit_5 else 0,
    }

    for cp in CHECKPOINTS:

        snap = nearest_before(
            path,
            s["entry_timestamp"],
            cp
        )

        prior = [
            x
            for ts,x in path_returns
            if ts <= (
                s["entry_timestamp"]
                + cp
            )
        ]

        if (
            snap is None
            or not prior
        ):
            rec[
                f"ret_{cp}"
            ] = None

            rec[
                f"max_{cp}"
            ] = None

            rec[
                f"min_{cp}"
            ] = None

            rec[
                f"drawdown_{cp}"
            ] = None

            rec[
                f"velocity_{cp}"
            ] = None

            rec[
                f"points_{cp}"
            ] = len(prior)

            continue

        current_ret = ret(
            entry,
            snap["price_usd"]
        )

        peak = max(prior)
        trough = min(prior)

        drawdown = (
            current_ret - peak
            if valid(current_ret)
            else None
        )

        velocity = (
            current_ret / cp
            if valid(current_ret)
            and cp > 0
            else None
        )

        rec[
            f"ret_{cp}"
        ] = current_ret

        rec[
            f"max_{cp}"
        ] = peak

        rec[
            f"min_{cp}"
        ] = trough

        rec[
            f"drawdown_{cp}"
        ] = drawdown

        rec[
            f"velocity_{cp}"
        ] = velocity

        rec[
            f"points_{cp}"
        ] = len(prior)

    records.append(rec)


# ============================================================
# HELPERS
# ============================================================

def describe_group(rows, cp):

    fields = [
        f"ret_{cp}",
        f"max_{cp}",
        f"min_{cp}",
        f"drawdown_{cp}",
        f"velocity_{cp}",
        f"points_{cp}",
    ]

    out = {}

    for field in fields:

        vals = [
            r[field]
            for r in rows
            if valid(r[field])
        ]

        out[field] = {
            "n":
                len(vals),

            "med":
                med(vals),

            "avg":
                avg(vals),

            "p25":
                percentile(vals,.25),

            "p75":
                percentile(vals,.75),
        }

    return out


def print_split(split_name, split_rows):

    successes = [
        r for r in split_rows
        if r["success"] == 1
    ]

    failures = [
        r for r in split_rows
        if r["success"] == 0
    ]

    print()
    print("="*160)
    print(
        f"{split_name} — "
        f"SUCCESS (+5% HIT) VS FAILURE (NEVER +5%)"
    )
    print("="*160)

    print(
        f"TOKENS={len(split_rows)} | "
        f"SUCCESS={len(successes)} | "
        f"FAILURE={len(failures)}"
    )

    for cp in CHECKPOINTS:

        S = describe_group(
            successes,
            cp
        )

        F = describe_group(
            failures,
            cp
        )

        print()
        print(
            f"CHECKPOINT {cp}s"
        )

        print("-"*130)

        print(
            f"{'FEATURE':22} "
            f"{'SUCCESS MED':>14} "
            f"{'FAIL MED':>14} "
            f"{'DIFF':>14} "
            f"{'SUCCESS N':>10} "
            f"{'FAIL N':>8}"
        )

        fields = [
            (
                "current_return",
                f"ret_{cp}"
            ),

            (
                "max_return",
                f"max_{cp}"
            ),

            (
                "min_return",
                f"min_{cp}"
            ),

            (
                "drawdown_from_peak",
                f"drawdown_{cp}"
            ),

            (
                "return_per_sec",
                f"velocity_{cp}"
            ),

            (
                "snapshot_count",
                f"points_{cp}"
            ),
        ]

        for label,field in fields:

            sm = S[field]["med"]
            fm = F[field]["med"]

            diff = (
                sm-fm
                if valid(sm)
                and valid(fm)
                else None
            )

            if field.startswith(
                "points_"
            ):

                print(
                    f"{label:22} "
                    f"{(sm if sm is not None else 0):14.1f} "
                    f"{(fm if fm is not None else 0):14.1f} "
                    f"{(diff if diff is not None else 0):+14.1f} "
                    f"{S[field]['n']:10d} "
                    f"{F[field]['n']:8d}"
                )

            else:

                print(
                    f"{label:22} "
                    f"{(sm if sm is not None else 0):+13.3f}% "
                    f"{(fm if fm is not None else 0):+13.3f}% "
                    f"{(diff if diff is not None else 0):+13.3f} "
                    f"{S[field]['n']:10d} "
                    f"{F[field]['n']:8d}"
                )


# ============================================================
# SIMPLE EARLY ABORT HYPOTHESES
#
# Discovery descriptive only.
# Validation shown separately.
# No threshold tuning from validation.
# ============================================================

def abort_test(rows, cp, threshold):

    usable = [
        r for r in rows
        if valid(
            r[f"max_{cp}"]
        )
    ]

    if not usable:
        return None

    kept = [
        r for r in usable
        if r[f"max_{cp}"] >= threshold
    ]

    aborted = [
        r for r in usable
        if r[f"max_{cp}"] < threshold
    ]

    success_total = sum(
        r["success"] == 1
        for r in usable
    )

    success_kept = sum(
        r["success"] == 1
        for r in kept
    )

    failures_removed = sum(
        r["success"] == 0
        for r in aborted
    )

    total_failures = sum(
        r["success"] == 0
        for r in usable
    )

    return {
        "n":
            len(usable),

        "kept":
            len(kept),

        "aborted":
            len(aborted),

        "success_recall":
            (
                100
                * success_kept
                / success_total
                if success_total
                else None
            ),

        "failure_removed":
            (
                100
                * failures_removed
                / total_failures
                if total_failures
                else None
            ),

        "kept_success_rate":
            (
                100
                * success_kept
                / len(kept)
                if kept
                else None
            ),
    }


# ============================================================
# OUTPUT
# ============================================================

print("="*160)
print(
    "MEMECOIN LAB — "
    "T28 EARLY-PATH FAILURE DETECTOR"
)
print("="*160)

print(
    f"V2 FIRST-SIGNAL TOKENS : "
    f"{len(records)}"
)

print(
    f"SUCCESS DEFINITION     : "
    f"reaches +{TARGET_ACTIVATION:.0f}% "
    f"within {HORIZON}s"
)

print(
    f"FAILURE DEFINITION     : "
    f"never reaches +{TARGET_ACTIVATION:.0f}% "
    f"within {HORIZON}s"
)

print(
    "NO FUTURE RETURN USED "
    "IN EARLY FEATURES."
)

disc = [
    r for r in records
    if r["split"] == "DISC"
]

valid_rows = [
    r for r in records
    if r["split"] == "VALID"
]

print_split(
    "DISCOVERY",
    disc
)

print_split(
    "VALIDATION",
    valid_rows
)


# ============================================================
# DISCOVERY-ONLY SIMPLE MAX-RETURN CHECK
# ============================================================

print()
print("="*160)
print(
    "DISCOVERY-ONLY EARLY MAX-RETURN SCREEN"
)
print("="*160)

print(
    "Question: if the token has not shown "
    "enough positive excursion yet, "
    "can we abort failures without killing future +5% hits?"
)

print()

print(
    f"{'CP':>4} "
    f"{'MIN MAX':>8} | "
    f"{'D KEEP':>6} "
    f"{'D ABRT':>6} "
    f"{'D SUCC RECALL':>14} "
    f"{'D FAIL REM':>12} | "
    f"{'V KEEP':>6} "
    f"{'V ABRT':>6} "
    f"{'V SUCC RECALL':>14} "
    f"{'V FAIL REM':>12}"
)

print("-"*115)

# Very small, coarse grid.
# Not a trading rule yet.
for cp in [20,30,45]:

    for threshold in [
        0.0,
        1.0,
        2.0,
        3.0,
    ]:

        d = abort_test(
            disc,
            cp,
            threshold
        )

        v = abort_test(
            valid_rows,
            cp,
            threshold
        )

        if not d or not v:
            continue

        print(
            f"{cp:4d} "
            f"{threshold:7.1f}% | "
            f"{d['kept']:6d} "
            f"{d['aborted']:6d} "
            f"{(d['success_recall'] if d['success_recall'] is not None else 0):13.1f}% "
            f"{(d['failure_removed'] if d['failure_removed'] is not None else 0):11.1f}% | "
            f"{v['kept']:6d} "
            f"{v['aborted']:6d} "
            f"{(v['success_recall'] if v['success_recall'] is not None else 0):13.1f}% "
            f"{(v['failure_removed'] if v['failure_removed'] is not None else 0):11.1f}%"
        )


# ============================================================
# TOKEN DETAIL
# ============================================================

print()
print("="*160)
print(
    "TOKEN DETAIL"
)
print("="*160)

print(
    f"{'SPLIT':7} "
    f"{'TOKEN':20} "
    f"{'LABEL':8} "
    f"{'MAX20':>8} "
    f"{'RET20':>8} "
    f"{'MAX30':>8} "
    f"{'RET30':>8} "
    f"{'MAX45':>8} "
    f"{'RET45':>8}"
)

print("-"*105)

for r in records:

    label = (
        "SUCCESS"
        if r["success"] == 1
        else "FAIL"
    )

    def fp(x):
        return (
            f"{x:+7.2f}%"
            if valid(x)
            else "     NA"
        )

    print(
        f"{r['split']:7} "
        f"{r['token'][:20]:20} "
        f"{label:8} "
        f"{fp(r['max_20'])} "
        f"{fp(r['ret_20'])} "
        f"{fp(r['max_30'])} "
        f"{fp(r['ret_30'])} "
        f"{fp(r['max_45'])} "
        f"{fp(r['ret_45'])}"
    )


print()
print("="*160)
print("INTERPRETATION")
print("="*160)

print("""
What we want:

• failures already have weak max excursion by 20-30s
• successes show materially stronger early max/return
• a coarse discovery rule preserves most future +5% hits
• the same direction survives validation

Bad result:

• successful runners often look dead at 20-30s
• early abort would kill too many future runners
• discovery separation disappears in validation

This is descriptive research only.
Do NOT modify T23 or the frozen V2 filter.
""")

db.close()
