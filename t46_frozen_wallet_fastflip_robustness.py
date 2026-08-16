import sqlite3
import math
import random
import statistics
from collections import defaultdict

from sklearn.metrics import roc_auc_score


DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0

# FROZEN FROM T45
MIN_PRIOR_TRADES = 1

PRIMARY = "buyer_fast_mean"
SANITY = "buyer_fast_weighted"
CONTEXT = "cohort_token_count"

# Direction is frozen independently of this script:
# lower fast-flip => more RUN-like
PRIMARY_SIGN = -1

BOOTSTRAPS = 5000
RANDOM_SEED = 42


# ============================================================
# HELPERS
# ============================================================

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


def percentile(vals, q):
    vals = sorted(x for x in vals if valid(x))

    if not vals:
        return None

    idx = int(round(
        (q / 100.0) * (len(vals)-1)
    ))

    return vals[idx]


def safe_div(a, b):
    if b is None or b == 0:
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


def auc_safe(y, x):
    pairs = [
        (yy, xx)
        for yy, xx in zip(y, x)
        if valid(xx)
    ]

    if len(pairs) < 4:
        return None

    yy = [p[0] for p in pairs]
    xx = [p[1] for p in pairs]

    if len(set(yy)) < 2:
        return None

    if len(set(xx)) < 2:
        return None

    try:
        return roc_auc_score(yy, xx)
    except Exception:
        return None


def directional_auc(rows, feature, sign):
    usable = [
        r for r in rows
        if valid(r[feature])
    ]

    y = [
        r["label"]
        for r in usable
    ]

    x = [
        r[feature] * sign
        for r in usable
    ]

    return auc_safe(y, x)


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


events = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,
    dex_return_60s
FROM events
WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_return_60s IS NOT NULL
ORDER BY timestamp, id
""").fetchall()


events = [
    e for e in events
    if label_r60(e["dex_return_60s"]) is not None
]


swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    clean_price,
    price_valid
FROM swaps
WHERE
    timestamp IS NOT NULL
    AND wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND side IN ('BUY','SELL')
    AND clean_price IS NOT NULL
    AND clean_price > 0
    AND (
        price_valid IS NULL
        OR price_valid = 1
    )
ORDER BY timestamp
""").fetchall()


# ============================================================
# HISTORICAL WALLET STATE
# ============================================================

completed = defaultdict(int)
wallet_tokens = defaultdict(set)
wallet_fast_flips = defaultdict(int)

open_pos = {}

swap_idx = 0


def process_swap(s):
    wallet = s["wallet"]
    token = s["token_mint"]
    side = s["side"]
    price = s["clean_price"]
    ts = s["timestamp"]

    if not valid(price) or price <= 0:
        return

    wallet_tokens[wallet].add(token)

    key = (wallet, token)

    if side == "BUY":

        if key not in open_pos:
            open_pos[key] = {
                "entry_ts": ts,
                "entry_price": price,
            }

    elif side == "SELL":

        if key not in open_pos:
            return

        st = open_pos[key]

        hold = ts - st["entry_ts"]

        if hold < 0:
            open_pos.pop(key, None)
            return

        completed[wallet] += 1

        if hold <= 60:
            wallet_fast_flips[wallet] += 1

        open_pos.pop(key, None)


def wallet_snapshot(wallet):
    n = completed[wallet]

    return {
        "prior_trades":
            n,

        "prior_tokens":
            len(wallet_tokens[wallet]),

        "fast_flip_rate":
            safe_div(
                wallet_fast_flips[wallet],
                n
            ),
    }


# ============================================================
# CHRONOLOGICAL EVENT FEATURES
# ============================================================

records = []


for e in events:

    event_ts = e["timestamp"]

    while (
        swap_idx < len(swaps)
        and swaps[swap_idx]["timestamp"] < event_ts
    ):

        process_swap(
            swaps[swap_idx]
        )

        swap_idx += 1


    pre = db.execute("""
    SELECT
        wallet,
        side
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
    """, (
        e["token_mint"],
        event_ts - PRE_EVENT_SEC,
        event_ts
    )).fetchall()


    if not pre:
        continue


    buyers = sorted(
        set(
            x["wallet"]
            for x in pre
            if x["side"] == "BUY"
        )
    )


    wallets = sorted(
        set(
            x["wallet"]
            for x in pre
        )
    )


    exp_buyers = []

    for w in buyers:

        st = wallet_snapshot(w)

        if st["prior_trades"] >= MIN_PRIOR_TRADES:
            exp_buyers.append(st)


    exp_wallets = []

    for w in wallets:

        st = wallet_snapshot(w)

        if st["prior_trades"] >= MIN_PRIOR_TRADES:
            exp_wallets.append(st)


    fast = [
        x["fast_flip_rate"]
        for x in exp_buyers
        if valid(x["fast_flip_rate"])
    ]


    buyer_fast_mean = avg(fast)


    weighted = None

    if exp_buyers:

        num = 0.0
        den = 0.0

        for st in exp_buyers:

            ff = st["fast_flip_rate"]
            n = st["prior_trades"]

            if valid(ff) and n > 0:

                num += (
                    ff * n
                )

                den += n

        if den > 0:
            weighted = num / den


    cohort_token_count = med([
        x["prior_tokens"]
        for x in exp_wallets
    ])


    records.append({
        "id":
            e["id"],

        "timestamp":
            event_ts,

        "token_mint":
            e["token_mint"],

        "label":
            label_r60(
                e["dex_return_60s"]
            ),

        "r60":
            e["dex_return_60s"],

        "buyer_fast_mean":
            buyer_fast_mean,

        "buyer_fast_weighted":
            weighted,

        "cohort_token_count":
            cohort_token_count,

        "exp_buyer_count":
            len(exp_buyers),

        "exp_wallet_count":
            len(exp_wallets),

        "buyer_count":
            len(buyers),

        "wallet_count":
            len(wallets),
    })


records = sorted(
    records,
    key=lambda r:
        (
            r["timestamp"],
            r["id"]
        )
)


usable = [
    r for r in records
    if valid(
        r[
            PRIMARY
        ]
    )
]


print("=" * 180)
print("MEMECOIN LAB — T46 FROZEN WALLET FAST-FLIP ROBUSTNESS AUDIT")
print("=" * 180)

print(
    f"ALL LABELED EVENTS       : {len(records)}"
)

print(
    f"PRIMARY USABLE EVENTS    : {len(usable)}"
)

print(
    f"PRIMARY UNIQUE TOKENS    : "
    f"{len(set(r['token_mint'] for r in usable))}"
)

print(
    f"EXPERIENCE THRESHOLD     : >= {MIN_PRIOR_TRADES} PRIOR TRADE"
)

print(
    f"PRIMARY FEATURE          : {PRIMARY}"
)

print(
    "FROZEN DIRECTION         : LOWER FAST-FLIP => MORE RUN-LIKE"
)

print(
    "NO MODEL FITTING / NO THRESHOLD SEARCH"
)


# ============================================================
# A) GLOBAL
# ============================================================

print()
print("=" * 180)
print("A) GLOBAL PRIMARY FEATURE")
print("=" * 180)


run = [
    r[PRIMARY]
    for r in usable
    if r["label"] == 1
]

dump = [
    r[PRIMARY]
    for r in usable
    if r["label"] == 0
]


global_auc = directional_auc(
    usable,
    PRIMARY,
    PRIMARY_SIGN
)


print(
    f"N={len(usable)} "
    f"| TOKENS="
    f"{len(set(r['token_mint'] for r in usable))} "
    f"| RUN={len(run)} "
    f"| DUMP={len(dump)}"
)

print(
    f"RUN MED  = {med(run):.4f}"
)

print(
    f"DUMP MED = {med(dump):.4f}"
)

print(
    f"DIFF RUN-DUMP = "
    f"{med(run)-med(dump):+.4f}"
)

print(
    f"DIRECTIONAL AUC = "
    f"{fmt(global_auc)}"
)


# ============================================================
# B) CHRONOLOGICAL BLOCKS
# ============================================================

print()
print("=" * 180)
print("B) CHRONOLOGICAL BLOCK ROBUSTNESS")
print("=" * 180)


def audit_block(name, rr):

    uu = [
        r for r in rr
        if valid(r[PRIMARY])
    ]

    run = [
        r[PRIMARY]
        for r in uu
        if r["label"] == 1
    ]

    dump = [
        r[PRIMARY]
        for r in uu
        if r["label"] == 0
    ]

    auc = directional_auc(
        uu,
        PRIMARY,
        PRIMARY_SIGN
    )

    return {
        "name":
            name,

        "n":
            len(uu),

        "tokens":
            len(
                set(
                    r["token_mint"]
                    for r in uu
                )
            ),

        "run":
            len(run),

        "dump":
            len(dump),

        "run_med":
            med(run),

        "dump_med":
            med(dump),

        "auc":
            auc,
    }


def print_block(a):

    diff = (
        a["run_med"] - a["dump_med"]
        if (
            a["run_med"] is not None
            and a["dump_med"] is not None
        )
        else None
    )

    print(
        f"{a['name']:<16} "
        f"N={a['n']:3d} "
        f"TOK={a['tokens']:3d} "
        f"RUN={a['run']:3d} "
        f"DUMP={a['dump']:3d} "
        f"DIFF={fmt(diff):>7} "
        f"DIR_AUC={fmt(a['auc']):>6}"
    )


n = len(records)


# thirds
thirds = [
    ("T1", records[:n//3]),
    ("T2", records[n//3:(2*n)//3]),
    ("T3", records[(2*n)//3:]),
]


for name, rr in thirds:
    print_block(
        audit_block(
            name,
            rr
        )
    )


# quartiles
print()

bounds = [
    0,
    n//4,
    n//2,
    (3*n)//4,
    n
]


for i in range(4):

    print_block(
        audit_block(
            f"Q{i+1}",
            records[
                bounds[i]:
                bounds[i+1]
            ]
        )
    )


# ============================================================
# C) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 180)
print("C) FIRST-EVENT/TOKEN")
print("=" * 180)


seen = set()
first = []


for r in records:

    tok = r["token_mint"]

    if tok in seen:
        continue

    seen.add(tok)
    first.append(r)


first_audit = audit_block(
    "FIRST",
    first
)

print_block(
    first_audit
)


# ============================================================
# D) TOKEN-BLOCK ROBUSTNESS
# ============================================================

print()
print("=" * 180)
print("D) CHRONOLOGICAL UNIQUE-TOKEN BLOCKS")
print("=" * 180)


first_ts = {}


for r in records:

    first_ts.setdefault(
        r["token_mint"],
        r["timestamp"]
    )


ordered_tokens = sorted(
    first_ts,
    key=lambda t:
        first_ts[t]
)


tb = [
    0,
    len(ordered_tokens)//4,
    len(ordered_tokens)//2,
    (3*len(ordered_tokens))//4,
    len(ordered_tokens)
]


token_block_audits = []


for i in range(4):

    tokset = set(
        ordered_tokens[
            tb[i]:
            tb[i+1]
        ]
    )

    rr = [
        r for r in records
        if r["token_mint"]
        in tokset
    ]

    a = audit_block(
        f"TOK_Q{i+1}",
        rr
    )

    token_block_audits.append(
        a
    )

    print_block(a)


# ============================================================
# E) BOOTSTRAP AUC
#
# Token-level bootstrap to avoid pretending repeated events
# from the same token are independent.
# ============================================================

print()
print("=" * 180)
print("E) TOKEN-LEVEL BOOTSTRAP DIRECTIONAL AUC")
print("=" * 180)


by_token = defaultdict(list)


for r in usable:

    by_token[
        r["token_mint"]
    ].append(r)


tokens = list(
    by_token.keys()
)


rng = random.Random(
    RANDOM_SEED
)


boot = []


for _ in range(
    BOOTSTRAPS
):

    sampled_tokens = [
        rng.choice(tokens)
        for _ in range(
            len(tokens)
        )
    ]

    rr = []

    for tok in sampled_tokens:

        rr.extend(
            by_token[
                tok
            ]
        )

    auc = directional_auc(
        rr,
        PRIMARY,
        PRIMARY_SIGN
    )

    if auc is not None:
        boot.append(
            auc
        )


print(
    f"BOOT N={len(boot)}"
)

if boot:

    print(
        f"MED AUC = "
        f"{med(boot):.3f}"
    )

    print(
        f"95% CI  = "
        f"[{percentile(boot,2.5):.3f}, "
        f"{percentile(boot,97.5):.3f}]"
    )

    print(
        f"P(AUC>0.50) = "
        f"{100*sum(x>0.50 for x in boot)/len(boot):.1f}%"
    )

    print(
        f"P(AUC>0.55) = "
        f"{100*sum(x>0.55 for x in boot)/len(boot):.1f}%"
    )

    print(
        f"P(AUC>0.60) = "
        f"{100*sum(x>0.60 for x in boot)/len(boot):.1f}%"
    )


# ============================================================
# F) LEAVE-ONE-TOKEN-OUT
# ============================================================

print()
print("=" * 180)
print("F) LEAVE-ONE-TOKEN-OUT")
print("=" * 180)


loo = []


for tok in tokens:

    rr = [
        r for r in usable
        if r["token_mint"]
        != tok
    ]

    auc = directional_auc(
        rr,
        PRIMARY,
        PRIMARY_SIGN
    )

    if auc is not None:

        loo.append(
            (
                auc,
                tok,
                len(
                    by_token[
                        tok
                    ]
                )
            )
        )


loo.sort(
    key=lambda x:
        x[0]
)


if loo:

    vals = [
        x[0]
        for x in loo
    ]

    print(
        f"N TOKENS = {len(loo)}"
    )

    print(
        f"MED REMAINING AUC = "
        f"{med(vals):.3f}"
    )

    print(
        f"WORST REMAINING AUC = "
        f"{min(vals):.3f}"
    )

    print(
        f"BEST REMAINING AUC = "
        f"{max(vals):.3f}"
    )

    print()

    print(
        "5 TOKENS WHOSE REMOVAL HURTS MOST:"
    )

    for auc, tok, nrows in loo[:5]:

        print(
            f"{tok[:24]:24} "
            f"| EVENTS={nrows:2d} "
            f"| REMAINING AUC={auc:.3f}"
        )

    print()

    print(
        "5 TOKENS WHOSE REMOVAL HELPS MOST:"
    )

    for auc, tok, nrows in loo[-5:]:

        print(
            f"{tok[:24]:24} "
            f"| EVENTS={nrows:2d} "
            f"| REMAINING AUC={auc:.3f}"
        )


# ============================================================
# G) EXPERIENCED BUYER COUNT
# ============================================================

print()
print("=" * 180)
print("G) EXPERIENCED BUYER COUNT REGIMES")
print("=" * 180)


groups = [
    (
        "EXP=1",
        lambda x:
            x == 1
    ),
    (
        "EXP=2",
        lambda x:
            x == 2
    ),
    (
        "EXP>=3",
        lambda x:
            x >= 3
    ),
]


for name, fn in groups:

    rr = [
        r for r in records
        if fn(
            r[
                "exp_buyer_count"
            ]
        )
    ]

    print_block(
        audit_block(
            name,
            rr
        )
    )


# ============================================================
# H) REPRESENTATION SANITY CHECK
# ============================================================

print()
print("=" * 180)
print("H) MEAN VS WEIGHTED REPRESENTATION")
print("=" * 180)


common = [
    r for r in records
    if (
        valid(
            r[
                PRIMARY
            ]
        )
        and valid(
            r[
                SANITY
            ]
        )
    )
]


x = [
    r[
        PRIMARY
    ]
    for r in common
]

y = [
    r[
        SANITY
    ]
    for r in common
]


mx = avg(x)
my = avg(y)


num = sum(
    (a-mx)*(b-my)
    for a,b in zip(x,y)
)

dx = math.sqrt(
    sum(
        (a-mx)**2
        for a in x
    )
)

dy = math.sqrt(
    sum(
        (b-my)**2
        for b in y
    )
)


corr = (
    num/(dx*dy)
    if dx > 0 and dy > 0
    else None
)


print(
    f"N={len(common)}"
)

print(
    f"CORR(mean, weighted) = "
    f"{fmt(corr)}"
)

print(
    f"MEAN DIR-AUC     = "
    f"{fmt(directional_auc(common, PRIMARY, -1))}"
)

print(
    f"WEIGHTED DIR-AUC = "
    f"{fmt(directional_auc(common, SANITY, -1))}"
)


# ============================================================
# I) CONTEXT CHECK — COHORT TOKEN COUNT
# ============================================================

print()
print("=" * 180)
print("I) COHORT TOKEN COUNT CONTEXT")
print("=" * 180)


ctx = [
    r for r in records
    if valid(
        r[
            CONTEXT
        ]
    )
]


run_ctx = [
    r[
        CONTEXT
    ]
    for r in ctx
    if r[
        "label"
    ] == 1
]

dump_ctx = [
    r[
        CONTEXT
    ]
    for r in ctx
    if r[
        "label"
    ] == 0
]


ctx_auc = directional_auc(
    ctx,
    CONTEXT,
    +1
)


print(
    f"N={len(ctx)} "
    f"| TOKENS="
    f"{len(set(r['token_mint'] for r in ctx))}"
)

print(
    f"RUN MED={fmt(med(run_ctx))} "
    f"| DUMP MED={fmt(med(dump_ctx))}"
)

print(
    f"HIGHER TOKEN-COUNT DIR-AUC = "
    f"{fmt(ctx_auc)}"
)


# ============================================================
# J) ROBUSTNESS SCORECARD
# ============================================================

print()
print("=" * 180)
print("J) ROBUSTNESS SCORECARD")
print("=" * 180)


audits = []


for name, rr in thirds:

    audits.append(
        audit_block(
            name,
            rr
        )
    )


for i in range(4):

    audits.append(
        audit_block(
            f"Q{i+1}",
            records[
                bounds[i]:
                bounds[i+1]
            ]
        )
    )


audits.append(
    first_audit
)

audits.extend(
    token_block_audits
)


usable_audits = [
    a for a in audits
    if (
        a[
            "n"
        ] >= 8
        and a[
            "run"
        ] >= 2
        and a[
            "dump"
        ] >= 2
        and a[
            "auc"
        ] is not None
    )
]


positive_direction = [
    a for a in usable_audits
    if (
        a[
            "run_med"
        ] is not None
        and a[
            "dump_med"
        ] is not None
        and a[
            "run_med"
        ] < a[
            "dump_med"
        ]
    )
]


auc55 = [
    a for a in usable_audits
    if a[
        "auc"
    ] >= 0.55
]


auc60 = [
    a for a in usable_audits
    if a[
        "auc"
    ] >= 0.60
]


print(
    f"USABLE AUDITS         = "
    f"{len(usable_audits)}"
)

print(
    f"LOWER FOR RUN         = "
    f"{len(positive_direction)}/"
    f"{len(usable_audits)}"
)

print(
    f"DIR-AUC >=0.55        = "
    f"{len(auc55)}/"
    f"{len(usable_audits)}"
)

print(
    f"DIR-AUC >=0.60        = "
    f"{len(auc60)}/"
    f"{len(usable_audits)}"
)


if usable_audits:

    vals = [
        a[
            "auc"
        ]
        for a in usable_audits
    ]

    print(
        f"MEDIAN AUDIT AUC      = "
        f"{med(vals):.3f}"
    )

    print(
        f"WORST AUDIT AUC       = "
        f"{min(vals):.3f}"
    )

    print(
        f"BEST AUDIT AUC        = "
        f"{max(vals):.3f}"
    )


# ============================================================
# K) DECISION
# ============================================================

print()
print("=" * 180)
print("K) DECISION SUPPORT")
print("=" * 180)


if usable_audits:

    direction_rate = (
        len(
            positive_direction
        )
        / len(
            usable_audits
        )
    )

    auc55_rate = (
        len(
            auc55
        )
        / len(
            usable_audits
        )
    )

    audit_med = med([
        a[
            "auc"
        ]
        for a in usable_audits
    ])

else:

    direction_rate = 0
    auc55_rate = 0
    audit_med = None


boot_prob = (
    sum(
        x > 0.50
        for x in boot
    ) / len(
        boot
    )
    if boot
    else 0
)


if (
    audit_med is not None
    and audit_med >= 0.57
    and direction_rate >= 0.70
    and auc55_rate >= 0.60
    and boot_prob >= 0.80
):

    print(
        "🟢 FROZEN FAST-FLIP METRIC SURVIVES ROBUSTNESS AUDIT."
    )

    print(
        "Next step = T47 prospective shadow recorder."
    )

    print(
        "Do NOT use for execution yet."
    )


elif (
    audit_med is not None
    and audit_med >= 0.54
    and direction_rate >= 0.60
    and boot_prob >= 0.65
):

    print(
        "🟡 FAST-FLIP METRIC SHOWS PARTIAL ROBUSTNESS."
    )

    print(
        "Keep collecting before prospective promotion."
    )


else:

    print(
        "🔴 FAST-FLIP METRIC DOES NOT PASS ROBUSTNESS GATE."
    )

    print(
        "Do not integrate it into the execution stack."
    )


print()
print("IMPORTANT:")
print("• Experience threshold >=1 is frozen before T46.")
print("• Primary representation buyer_fast_mean is frozen before T46.")
print("• Direction LOWER = RUN-like is frozen before T46.")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Bootstrap resamples entire tokens.")
print("• Same-unit swaps.clean_price only.")
print("• Wallet history is chronological.")
print("• T23/T31/T32 remain untouched.")
print("• T46 writes nothing to DB.")
print("• Research audit only.")

db.close()
