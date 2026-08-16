import sqlite3
import math
import statistics

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


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


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

ORDER BY
    timestamp,
    id
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
        wallet,
        side

    FROM swaps

    WHERE
        token_mint=?
        AND price_valid=1
        AND timestamp < ?
        AND wallet IS NOT NULL
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


    early = rows[0:4]
    mid = rows[4:8]
    recent = rows[8:12]


    def wallets(block, side=None):

        return set(
            r["wallet"]
            for r in block
            if (
                r["wallet"]
                and (
                    side is None
                    or r["side"] == side
                )
            )
        )


    eb = wallets(
        early,
        "BUY"
    )

    mb = wallets(
        mid,
        "BUY"
    )

    rb = wallets(
        recent,
        "BUY"
    )


    es = wallets(
        early,
        "SELL"
    )

    ms = wallets(
        mid,
        "SELL"
    )

    rs = wallets(
        recent,
        "SELL"
    )


    ea = wallets(
        early
    )

    ma = wallets(
        mid
    )

    ra = wallets(
        recent
    )


    # ========================================================
    # BUYER RETENTION
    # ========================================================

    early_recent_buy_overlap = (
        eb & rb
    )

    mid_recent_buy_overlap = (
        mb & rb
    )

    early_mid_buy_overlap = (
        eb & mb
    )


    buyer_retention_early_recent = safe_div(
        len(
            early_recent_buy_overlap
        ),
        len(eb)
    )


    buyer_retention_mid_recent = safe_div(
        len(
            mid_recent_buy_overlap
        ),
        len(mb)
    )


    buyer_retention_early_mid = safe_div(
        len(
            early_mid_buy_overlap
        ),
        len(eb)
    )


    # ========================================================
    # NEW / LATE BUYERS
    # ========================================================

    prior_buyers = (
        eb | mb
    )


    recent_new_buyers = (
        rb - prior_buyers
    )


    recent_new_buyer_share = safe_div(
        len(
            recent_new_buyers
        ),
        len(rb)
    )


    recent_returning_buyer_share = safe_div(
        len(
            rb & prior_buyers
        ),
        len(rb)
    )


    # ========================================================
    # CHURN
    # ========================================================

    early_churn = (
        eb - rb
    )


    early_buyer_churn_share = safe_div(
        len(
            early_churn
        ),
        len(eb)
    )


    mid_churn = (
        mb - rb
    )


    mid_buyer_churn_share = safe_div(
        len(
            mid_churn
        ),
        len(mb)
    )


    # ========================================================
    # MULTI-BLOCK PERSISTENCE
    # ========================================================

    buy_presence = {}

    for wallet in (
        eb | mb | rb
    ):

        buy_presence[
            wallet
        ] = sum([
            wallet in eb,
            wallet in mb,
            wallet in rb,
        ])


    buy_union = (
        eb | mb | rb
    )


    persistent_2block_buyers = {
        w
        for w,n in buy_presence.items()
        if n >= 2
    }


    persistent_3block_buyers = {
        w
        for w,n in buy_presence.items()
        if n == 3
    }


    persistent_2block_share = safe_div(
        len(
            persistent_2block_buyers
        ),
        len(
            buy_union
        )
    )


    persistent_3block_share = safe_div(
        len(
            persistent_3block_buyers
        ),
        len(
            buy_union
        )
    )


    # ========================================================
    # SIDE CONVERSION
    # ========================================================

    prior_buyers_to_recent_sellers = (
        prior_buyers & rs
    )


    buyer_to_seller_conversion = safe_div(
        len(
            prior_buyers_to_recent_sellers
        ),
        len(
            prior_buyers
        )
    )


    prior_sellers = (
        es | ms
    )


    prior_sellers_to_recent_buyers = (
        prior_sellers & rb
    )


    seller_to_buyer_conversion = safe_div(
        len(
            prior_sellers_to_recent_buyers
        ),
        len(
            prior_sellers
        )
    )


    # ========================================================
    # ALL-WALLET OVERLAP
    # ========================================================

    early_recent_wallet_overlap = safe_div(
        len(
            ea & ra
        ),
        len(
            ea | ra
        )
    )


    mid_recent_wallet_overlap = safe_div(
        len(
            ma & ra
        ),
        len(
            ma | ra
        )
    )


    # ========================================================
    # WALLET NOVELTY
    # ========================================================

    previous_wallets = (
        ea | ma
    )


    recent_new_wallets = (
        ra - previous_wallets
    )


    recent_new_wallet_share = safe_div(
        len(
            recent_new_wallets
        ),
        len(
            ra
        )
    )


    # ========================================================
    # COHORT SIZE EVOLUTION
    # ========================================================

    buyer_count_change = (
        len(rb)
        - len(eb)
    )


    buyer_count_ratio = safe_div(
        len(rb),
        len(eb)
    )


    wallet_count_change = (
        len(ra)
        - len(ea)
    )


    wallet_count_ratio = safe_div(
        len(ra),
        len(ea)
    )


    # ========================================================
    # CONCENTRATION OF PARTICIPATION ACROSS BLOCKS
    # ========================================================

    total_buy_appearances = (
        len(eb)
        + len(mb)
        + len(rb)
    )


    unique_buyers_all = len(
        buy_union
    )


    buyer_reuse_intensity = (
        safe_div(
            total_buy_appearances,
            unique_buyers_all
        )
    )


    all_union = (
        ea | ma | ra
    )


    total_wallet_appearances = (
        len(ea)
        + len(ma)
        + len(ra)
    )


    wallet_reuse_intensity = safe_div(
        total_wallet_appearances,
        len(all_union)
    )


    features = {
        "buyer_retention_early_recent":
            buyer_retention_early_recent,

        "buyer_retention_mid_recent":
            buyer_retention_mid_recent,

        "buyer_retention_early_mid":
            buyer_retention_early_mid,

        "recent_new_buyer_share":
            recent_new_buyer_share,

        "recent_returning_buyer_share":
            recent_returning_buyer_share,

        "early_buyer_churn_share":
            early_buyer_churn_share,

        "mid_buyer_churn_share":
            mid_buyer_churn_share,

        "persistent_2block_share":
            persistent_2block_share,

        "persistent_3block_share":
            persistent_3block_share,

        "buyer_to_seller_conversion":
            buyer_to_seller_conversion,

        "seller_to_buyer_conversion":
            seller_to_buyer_conversion,

        "early_recent_wallet_overlap":
            early_recent_wallet_overlap,

        "mid_recent_wallet_overlap":
            mid_recent_wallet_overlap,

        "recent_new_wallet_share":
            recent_new_wallet_share,

        "buyer_count_change":
            buyer_count_change,

        "buyer_count_ratio":
            buyer_count_ratio,

        "wallet_count_change":
            wallet_count_change,

        "wallet_count_ratio":
            wallet_count_ratio,

        "buyer_reuse_intensity":
            buyer_reuse_intensity,

        "wallet_reuse_intensity":
            wallet_reuse_intensity,
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


# ============================================================
# FEATURE LIST
# ============================================================

feature_names = sorted(
    set(
        k
        for r in records
        for k in r["features"]
    )
)


# ============================================================
# GLOBAL RANKING
# ============================================================

results = []


for feature in feature_names:

    rr = [
        r for r in records
        if valid(
            r["features"].get(
                feature
            )
        )
    ]


    y = [
        r["y"]
        for r in rr
    ]


    x = [
        r["features"][
            feature
        ]
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
    key=lambda r: (
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

    tok = r[
        "token_mint"
    ]

    if tok in seen:
        continue

    seen.add(
        tok
    )

    first.append(
        r
    )


first_results = {}


for feature in feature_names:

    rr = [
        r for r in first
        if valid(
            r["features"].get(
                feature
            )
        )
    ]


    y = [
        r["y"]
        for r in rr
    ]

    x = [
        r["features"][
            feature
        ]
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

midpoint = (
    len(records)//2
)


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


    for hname, rr0 in halves.items():

        rr = [
            r for r in rr0
            if valid(
                r["features"].get(
                    feature
                )
            )
        ]


        y = [
            r["y"]
            for r in rr
        ]

        x = [
            r["features"][
                feature
            ]
            for r in rr
        ]


        _, da, direction = auc_directional(
            y,
            x
        )


        half_results[
            feature
        ][
            hname
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

                xs.append(
                    x
                )

                ys.append(
                    y
                )


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
print("MEMECOIN LAB — T69 WALLET COHORT RETENTION / CHURN DISCOVERY")
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
        f"{r['feature']:34} "
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
    r[
        "feature"
    ]
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
        f"{f:34} "
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
        f"{f:34} "
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
        for ctx,c in redundancy[
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
            f"{f:34} "
            f"MAX|CORR|={abs(c):.3f} "
            f"| WITH={ctx:15} "
            f"| CORR={c:+.3f}"
        )


# ============================================================
# CONSERVATIVE GATE
# ============================================================

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
    key=lambda x: (
        -min(
            x[2],
            x[3],
            x[4],
            x[5]
        ),
        x[6]
        if x[6] is not None
        else 0
    )
)


if survivors:

    for s in survivors:

        print(
            f"{s[0]:34} "
            f"| DIR={s[1]:6} "
            f"| GLOBAL={s[2]:.3f} "
            f"| FIRST={s[3]:.3f} "
            f"| EARLY={s[4]:.3f} "
            f"| LATE={s[5]:.3f} "
            f"| MAXCORR={fmt(s[6])}"
        )

else:

    print(
        "No wallet-cohort feature passes the gate."
    )


# ============================================================
# DECISION
# ============================================================

print()
print("=" * 185)
print("F) DECISION SUPPORT")
print("=" * 185)


if survivors:

    best = survivors[0]

    print(
        "🟡 WALLET-COHORT FAMILY CONTAINS "
        "A ROBUSTNESS CANDIDATE."
    )

    print(
        f"PRIMARY T70 CANDIDATE = {best[0]}"
    )

    print(
        f"FROZEN DIRECTION      = {best[1]}"
    )

    print(
        "Next = T70 robustness audit."
    )

    print(
        "No threshold optimization."
    )

else:

    print(
        "🔴 NO WALLET-COHORT FEATURE SURVIVES."
    )

    print(
        "Do not force this family."
    )

    print(
        "T59 continues untouched."
    )


print()
print("IMPORTANT:")
print("• Uses exactly the 12 last valid swaps before each event.")
print("• Blocks are exactly EARLY=0:4, MID=4:8, RECENT=8:12.")
print("• Wallet overlap uses actual wallet IDs.")
print("• No future swaps.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• No DB writes.")
print("• T59 remains frozen.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• Research discovery only.")

db.close()
