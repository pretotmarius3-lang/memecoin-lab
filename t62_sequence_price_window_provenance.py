import sqlite3
import math
import statistics

DB = "validation_v090.db"

MAX_NEAREST_DELAY = 5.0


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


def fmt(x, n=4):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def pearson(xs, ys):

    pairs = [
        (x,y)
        for x,y in zip(xs,ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 5:
        return None

    xx = [x for x,_ in pairs]
    yy = [y for _,y in pairs]

    mx = avg(xx)
    my = avg(yy)

    dx = math.sqrt(
        sum(
            (x-mx)**2
            for x in xx
        )
    )

    dy = math.sqrt(
        sum(
            (y-my)**2
            for y in yy
        )
    )

    if dx == 0 or dy == 0:
        return None

    return sum(
        (x-mx)*(y-my)
        for x,y in pairs
    ) / (dx*dy)


def mae(xs, ys):

    vals = [
        abs(x-y)
        for x,y in zip(xs,ys)
        if valid(x) and valid(y)
    ]

    return avg(vals)


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# EVENTS
# ============================================================

seq_cols = [
    r["name"]
    for r in db.execute(
        "PRAGMA table_info(event_sequence_features_v340)"
    ).fetchall()
]


updated_exists = (
    "updated_at" in seq_cols
)


updated_select = (
    ", s.updated_at"
    if updated_exists
    else ""
)


events = db.execute(f"""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,

    s.event_timestamp,

    s.early_price_return,
    s.mid_price_return,
    s.recent_price_return

    {updated_select}

FROM events e

JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.timestamp IS NOT NULL
    AND e.token_mint IS NOT NULL

ORDER BY
    e.timestamp,
    e.id
""").fetchall()


# ============================================================
# SWAPS CACHE BY TOKEN
# ============================================================

swap_rows = db.execute("""
SELECT
    token_mint,
    timestamp,
    clean_price
FROM swaps
WHERE
    token_mint IS NOT NULL
    AND timestamp IS NOT NULL
    AND clean_price IS NOT NULL
    AND clean_price > 0
    AND (
        price_valid IS NULL
        OR price_valid=1
    )
ORDER BY
    token_mint,
    timestamp
""").fetchall()


by_token = {}

for r in swap_rows:

    tok = r["token_mint"]

    by_token.setdefault(
        tok,
        []
    ).append(
        (
            float(r["timestamp"]),
            float(r["clean_price"])
        )
    )


# ============================================================
# NEAREST PRICE
# ============================================================

def nearest_price(
    token,
    target_ts,
    require_before=None
):

    arr = by_token.get(
        token
    )

    if not arr:
        return None, None


    best_price = None
    best_delay = None


    # Dataset is modest enough for QA.
    for ts, price in arr:

        if (
            require_before is True
            and ts > target_ts
        ):
            continue

        if (
            require_before is False
            and ts < target_ts
        ):
            continue


        delay = abs(
            ts-target_ts
        )


        if (
            best_delay is None
            or delay < best_delay
        ):

            best_delay = delay
            best_price = price


    if (
        best_delay is None
        or best_delay > MAX_NEAREST_DELAY
    ):
        return None, None


    return (
        best_price,
        best_delay
    )


# ============================================================
# RETURN FOR ARBITRARY EVENT-RELATIVE WINDOW
# ============================================================

def window_return(
    token,
    event_ts,
    start_offset,
    end_offset
):

    ts1 = (
        event_ts
        + start_offset
    )

    ts2 = (
        event_ts
        + end_offset
    )


    p1, d1 = nearest_price(
        token,
        ts1
    )

    p2, d2 = nearest_price(
        token,
        ts2
    )


    if (
        not valid(p1)
        or not valid(p2)
        or p1 <= 0
    ):
        return None, None


    ret = (
        p2/p1 - 1
    ) * 100


    return (
        ret,
        max(d1,d2)
    )


# ============================================================
# CANDIDATE WINDOWS
#
# IMPORTANT:
# These are QA windows only.
# No outcome labels are used.
# ============================================================

WINDOWS = [

    # strict pre-event
    ("PRE_-60_-40", -60, -40),
    ("PRE_-60_-30", -60, -30),
    ("PRE_-60_0",   -60,   0),

    ("PRE_-45_-30", -45, -30),
    ("PRE_-45_-15", -45, -15),
    ("PRE_-45_0",   -45,   0),

    ("PRE_-30_-20", -30, -20),
    ("PRE_-30_-15", -30, -15),
    ("PRE_-30_-10", -30, -10),
    ("PRE_-30_0",   -30,   0),

    ("PRE_-20_-10", -20, -10),
    ("PRE_-20_0",   -20,   0),

    ("PRE_-15_-10", -15, -10),
    ("PRE_-15_-5",  -15,  -5),
    ("PRE_-15_0",   -15,   0),

    ("PRE_-10_-5",  -10,  -5),
    ("PRE_-10_0",   -10,   0),

    ("PRE_-5_0",     -5,   0),

    # post-event diagnostics
    ("POST_0_5",       0,   5),
    ("POST_0_10",      0,  10),
    ("POST_0_15",      0,  15),
    ("POST_0_20",      0,  20),
    ("POST_0_30",      0,  30),

    ("POST_5_10",      5,  10),
    ("POST_5_15",      5,  15),

    ("POST_10_20",    10,  20),
    ("POST_10_30",    10,  30),

    ("POST_20_30",    20,  30),

    # crossing event timestamp — suspicious if this wins
    ("CROSS_-10_5",  -10,   5),
    ("CROSS_-10_10", -10,  10),
    ("CROSS_-15_15", -15,  15),
    ("CROSS_-30_30", -30,  30),
]


TARGETS = [
    "early_price_return",
    "mid_price_return",
    "recent_price_return",
]


# ============================================================
# RECONSTRUCTION
# ============================================================

results = {
    target: []
    for target in TARGETS
}


for target in TARGETS:

    for (
        name,
        start_offset,
        end_offset
    ) in WINDOWS:

        actual = []
        recon = []
        delays = []


        for e in events:

            target_value = e[
                target
            ]


            if not valid(
                target_value
            ):
                continue


            ret, delay = window_return(
                e["token_mint"],
                e["timestamp"],
                start_offset,
                end_offset
            )


            if not valid(ret):
                continue


            actual.append(
                target_value
            )

            recon.append(
                ret
            )

            delays.append(
                delay
            )


        c = pearson(
            actual,
            recon
        )


        results[
            target
        ].append({
            "name":
                name,

            "start":
                start_offset,

            "end":
                end_offset,

            "n":
                len(actual),

            "corr":
                c,

            "mae":
                mae(
                    actual,
                    recon
                ),

            "med_delay":
                med(
                    delays
                ),
        })


# ============================================================
# OUTPUT
# ============================================================

print("=" * 185)
print("MEMECOIN LAB — T62 SEQUENCE PRICE-WINDOW PROVENANCE AUDIT")
print("=" * 185)

print(
    f"EVENTS CHECKED : {len(events)}"
)

print(
    f"TOKENS         : "
    f"{len(set(e['token_mint'] for e in events))}"
)

print(
    f"WINDOWS TESTED : {len(WINDOWS)}"
)

print(
    "OUTCOME LABELS : NOT USED"
)

print()


# ============================================================
# A) BASIC TIMESTAMP PROVENANCE
# ============================================================

print("=" * 185)
print("A) EVENT / SEQUENCE TIMESTAMP")
print("=" * 185)


deltas = [
    e["event_timestamp"]
    - e["timestamp"]
    for e in events
    if (
        valid(
            e["event_timestamp"]
        )
        and valid(
            e["timestamp"]
        )
    )
]


if deltas:

    print(
        f"N={len(deltas)} "
        f"| MED={med(deltas):+.6f}s "
        f"| MIN={min(deltas):+.6f}s "
        f"| MAX={max(deltas):+.6f}s"
    )


# ============================================================
# B) UPDATED_AT
# ============================================================

print()
print("=" * 185)
print("B) SEQUENCE FEATURE AVAILABILITY / UPDATED_AT")
print("=" * 185)


if updated_exists:

    update_deltas = []

    for e in events:

        u = e[
            "updated_at"
        ]

        ts = e[
            "timestamp"
        ]


        try:
            u = float(u)
        except Exception:
            continue


        if valid(ts):
            update_deltas.append(
                u-ts
            )


    if update_deltas:

        print(
            f"N={len(update_deltas)}"
        )

        print(
            f"MED UPDATED-EVENT = "
            f"{med(update_deltas):+.3f}s"
        )

        print(
            f"P10 = "
            f"{sorted(update_deltas)[int(.10*(len(update_deltas)-1))]:+.3f}s"
        )

        print(
            f"P90 = "
            f"{sorted(update_deltas)[int(.90*(len(update_deltas)-1))]:+.3f}s"
        )

        print(
            f"UPDATED AFTER EVENT = "
            f"{sum(x>0 for x in update_deltas)}/{len(update_deltas)}"
        )

    else:

        print(
            "updated_at exists but could not be interpreted as epoch seconds."
        )

else:

    print(
        "No updated_at column."
    )


# ============================================================
# C/D/E — TARGET RECONSTRUCTION
# ============================================================

for section, target in zip(
    ["C","D","E"],
    TARGETS
):

    print()
    print("=" * 185)

    print(
        f"{section}) RECONSTRUCTION — {target}"
    )

    print("=" * 185)


    rr = sorted(
        results[target],
        key=lambda x: (
            -(abs(x["corr"])
              if x["corr"] is not None
              else -1),
            -x["n"]
        )
    )


    print(
        f"{'WINDOW':22} "
        f"{'N':>5} "
        f"{'CORR':>9} "
        f"{'MAE':>12} "
        f"{'MED DELAY':>12} "
        f"CLASS"
    )

    print("-" * 90)


    for x in rr[:15]:

        if (
            x["end"] <= 0
        ):
            cls = "PRE"

        elif (
            x["start"] >= 0
        ):
            cls = "POST"

        else:
            cls = "CROSS"


        print(
            f"{x['name']:22} "
            f"{x['n']:5d} "
            f"{fmt(x['corr']):>9} "
            f"{fmt(x['mae']):>12} "
            f"{fmt(x['med_delay'],2):>12} "
            f"{cls}"
        )


# ============================================================
# F) BEST PRE vs POST vs CROSS
# ============================================================

print()
print("=" * 185)
print("F) BEST TEMPORAL SOURCE BY V340 PRICE FEATURE")
print("=" * 185)


summary = {}


for target in TARGETS:

    groups = {
        "PRE": [],
        "POST": [],
        "CROSS": [],
    }


    for x in results[
        target
    ]:

        if x["corr"] is None:
            continue


        if x["end"] <= 0:
            cls = "PRE"

        elif x["start"] >= 0:
            cls = "POST"

        else:
            cls = "CROSS"


        groups[
            cls
        ].append(x)


    summary[target] = {}


    print()
    print(target)
    print("-" * 100)


    for cls in [
        "PRE",
        "POST",
        "CROSS"
    ]:

        if not groups[cls]:

            print(
                f"{cls:6} | NONE"
            )

            continue


        best = max(
            groups[cls],
            key=lambda x:
                abs(
                    x["corr"]
                )
        )


        summary[
            target
        ][
            cls
        ] = best


        print(
            f"{cls:6} "
            f"| WINDOW={best['name']:20} "
            f"| N={best['n']:3d} "
            f"| CORR={best['corr']:+.4f} "
            f"| MAE={fmt(best['mae'])}"
        )


# ============================================================
# G) PRIMARY PROVENANCE
#
# Reconstruct price_recent_minus_early using combinations
# of independently best PRE / POST mappings.
# ============================================================

print()
print("=" * 185)
print("G) PRIMARY FEATURE PROVENANCE")
print("=" * 185)


early_pre = summary[
    "early_price_return"
].get(
    "PRE"
)

recent_pre = summary[
    "recent_price_return"
].get(
    "PRE"
)

early_post = summary[
    "early_price_return"
].get(
    "POST"
)

recent_post = summary[
    "recent_price_return"
].get(
    "POST"
)


def reconstruct_primary(
    early_window,
    recent_window
):

    actual = []
    proxy = []


    if (
        early_window is None
        or recent_window is None
    ):
        return None


    for e in events:

        ep = e[
            "early_price_return"
        ]

        rp = e[
            "recent_price_return"
        ]


        if not (
            valid(ep)
            and valid(rp)
        ):
            continue


        early_recon, _ = window_return(
            e["token_mint"],
            e["timestamp"],
            early_window["start"],
            early_window["end"]
        )


        recent_recon, _ = window_return(
            e["token_mint"],
            e["timestamp"],
            recent_window["start"],
            recent_window["end"]
        )


        if not (
            valid(early_recon)
            and valid(recent_recon)
        ):
            continue


        actual.append(
            rp-ep
        )

        proxy.append(
            recent_recon
            - early_recon
        )


    return {
        "n": len(actual),
        "corr": pearson(
            actual,
            proxy
        ),
        "mae": mae(
            actual,
            proxy
        ),
    }


pre_primary = reconstruct_primary(
    early_pre,
    recent_pre
)

post_primary = reconstruct_primary(
    early_post,
    recent_post
)


print(
    "STRICT PRE-EVENT BEST-MATCH PRIMARY"
)

if pre_primary:

    print(
        f"N={pre_primary['n']} "
        f"| CORR={fmt(pre_primary['corr'])} "
        f"| MAE={fmt(pre_primary['mae'])}"
    )

else:

    print(
        "Unavailable."
    )


print()

print(
    "POST-EVENT BEST-MATCH PRIMARY"
)

if post_primary:

    print(
        f"N={post_primary['n']} "
        f"| CORR={fmt(post_primary['corr'])} "
        f"| MAE={fmt(post_primary['mae'])}"
    )

else:

    print(
        "Unavailable."
    )


# ============================================================
# H) DECISION
# ============================================================

print()
print("=" * 185)
print("H) DECISION SUPPORT")
print("=" * 185)


pre_recent = summary[
    "recent_price_return"
].get(
    "PRE"
)

post_recent = summary[
    "recent_price_return"
].get(
    "POST"
)


pre_corr = (
    abs(
        pre_recent[
            "corr"
        ]
    )
    if pre_recent
    else 0
)


post_corr = (
    abs(
        post_recent[
            "corr"
        ]
    )
    if post_recent
    else 0
)


primary_pre_corr = (
    abs(
        pre_primary[
            "corr"
        ]
    )
    if (
        pre_primary
        and pre_primary[
            "corr"
        ] is not None
    )
    else 0
)


primary_post_corr = (
    abs(
        post_primary[
            "corr"
        ]
    )
    if (
        post_primary
        and post_primary[
            "corr"
        ] is not None
    )
    else 0
)


print(
    f"RECENT BEST PRE CORR   = {pre_corr:.4f}"
)

print(
    f"RECENT BEST POST CORR  = {post_corr:.4f}"
)

print(
    f"PRIMARY BEST PRE CORR  = {primary_pre_corr:.4f}"
)

print(
    f"PRIMARY BEST POST CORR = {primary_post_corr:.4f}"
)

print()


if (
    primary_pre_corr >= 0.70
    and primary_pre_corr
        > primary_post_corr
):

    print(
        "🟢 V340 PRICE-PERSISTENCE IS STRONGLY "
        "RECONSTRUCTIBLE FROM PRE-EVENT SWAPS."
    )

    print(
        "Temporal provenance is compatible with prospective use."
    )

    print(
        "Next = T63 incremental audit vs frozen CAP-v2."
    )


elif (
    primary_post_corr
    > primary_pre_corr + 0.15
):

    print(
        "🔴 POST-EVENT WINDOWS MATCH V340 PRIMARY "
        "BETTER THAN PRE-EVENT WINDOWS."
    )

    print(
        "Possible temporal contamination / delayed feature construction."
    )

    print(
        "DO NOT integrate price-persistence."
    )


else:

    print(
        "🟠 PRICE-PERSISTENCE TEMPORAL PROVENANCE "
        "REMAINS UNRESOLVED."
    )

    print(
        "Do not integrate it yet."
    )

    print(
        "Next step = inspect event_sequence_v340.py "
        "window construction directly."
    )


print()
print("IMPORTANT:")
print("• T62 uses NO RUN/DUMP labels.")
print("• No predictive feature selection occurs here.")
print("• Window search is data-provenance QA only.")
print("• All reconstructed returns use swaps.clean_price.")
print("• PRE windows end at or before the event timestamp.")
print("• POST windows begin at or after the event timestamp.")
print("• CROSS windows are diagnostic only.")
print("• T59 remains frozen and untouched.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T62 writes nothing to DB.")

db.close()
