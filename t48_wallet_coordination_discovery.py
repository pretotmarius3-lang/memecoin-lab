import sqlite3
import math
import random
import statistics
from collections import defaultdict, Counter

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

PRE_EVENT_SEC = 30.0
RANDOM_SEED = 42


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


def sign(x):
    if x is None:
        return 0
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def pairwise_gaps(times):
    times = sorted(times)

    if len(times) < 2:
        return []

    return [
        times[i] - times[i-1]
        for i in range(1, len(times))
    ]


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# LOAD LABELED EVENTS
# ============================================================

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


# ============================================================
# HISTORICAL WALLET/TOKEN CO-OCCURRENCE
# STRICTLY PRIOR TO CURRENT EVENT
# ============================================================

wallet_seen_tokens = defaultdict(set)
token_seen_wallets = defaultdict(set)

# pair history: how many prior tokens two wallets shared
pair_shared_tokens = defaultdict(int)

# swaps pointer
all_swaps = db.execute("""
SELECT
    timestamp,
    wallet,
    side,
    token_mint,
    sol_delta,
    clean_price
FROM swaps
WHERE
    timestamp IS NOT NULL
    AND wallet IS NOT NULL
    AND token_mint IS NOT NULL
    AND side IN ('BUY','SELL')
ORDER BY timestamp
""").fetchall()

swap_idx = 0


def pair_key(a, b):
    return tuple(sorted((a, b)))


def update_history_with_swap(s):

    wallet = s["wallet"]
    token = s["token_mint"]

    # If this wallet is first seen on this token, create pair
    # co-occurrence links with wallets already seen on token.
    if token not in wallet_seen_tokens[wallet]:

        others = token_seen_wallets[token]

        for other in others:
            if other == wallet:
                continue

            pair_shared_tokens[
                pair_key(wallet, other)
            ] += 1

        wallet_seen_tokens[wallet].add(token)
        token_seen_wallets[token].add(wallet)


# ============================================================
# BUILD EVENT-LEVEL COORDINATION FEATURES
# ============================================================

records = []

for e in events:

    ts = e["timestamp"]

    # advance only strictly historical swaps
    while (
        swap_idx < len(all_swaps)
        and all_swaps[swap_idx]["timestamp"] < ts
    ):
        update_history_with_swap(
            all_swaps[swap_idx]
        )
        swap_idx += 1


    pre = db.execute("""
    SELECT
        timestamp,
        wallet,
        side,
        sol_delta
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
        AND side IN ('BUY','SELL')
    ORDER BY timestamp
    """, (
        e["token_mint"],
        ts - PRE_EVENT_SEC,
        ts
    )).fetchall()


    if len(pre) < 2:
        continue


    buys = [
        r for r in pre
        if r["side"] == "BUY"
    ]

    sells = [
        r for r in pre
        if r["side"] == "SELL"
    ]


    buy_wallets = sorted(
        set(r["wallet"] for r in buys)
    )

    sell_wallets = sorted(
        set(r["wallet"] for r in sells)
    )

    all_wallets = sorted(
        set(r["wallet"] for r in pre)
    )


    buy_times = [
        r["timestamp"]
        for r in buys
    ]

    distinct_buy_first_times = {}

    for r in buys:
        distinct_buy_first_times.setdefault(
            r["wallet"],
            r["timestamp"]
        )

    wallet_buy_times = sorted(
        distinct_buy_first_times.values()
    )


    # --------------------------------------------------------
    # TEMPORAL SYNCHRONIZATION
    # --------------------------------------------------------

    gaps = pairwise_gaps(
        wallet_buy_times
    )

    median_buy_wallet_gap = med(gaps)

    mean_buy_wallet_gap = avg(gaps)

    burst_1s = (
        safe_div(
            sum(g <= 1 for g in gaps),
            len(gaps)
        )
        if gaps else None
    )

    burst_3s = (
        safe_div(
            sum(g <= 3 for g in gaps),
            len(gaps)
        )
        if gaps else None
    )

    burst_5s = (
        safe_div(
            sum(g <= 5 for g in gaps),
            len(gaps)
        )
        if gaps else None
    )


    # --------------------------------------------------------
    # WALLET COHORT HISTORY
    # --------------------------------------------------------

    pair_hist = []

    for i in range(len(buy_wallets)):
        for j in range(i+1, len(buy_wallets)):

            a = buy_wallets[i]
            b = buy_wallets[j]

            pair_hist.append(
                pair_shared_tokens[
                    pair_key(a, b)
                ]
            )


    pair_prior_shared_mean = avg(pair_hist)
    pair_prior_shared_max = (
        max(pair_hist)
        if pair_hist
        else None
    )

    repeated_pair_share = (
        safe_div(
            sum(x > 0 for x in pair_hist),
            len(pair_hist)
        )
        if pair_hist
        else None
    )


    # --------------------------------------------------------
    # INDIVIDUAL WALLET EXPERIENCE
    # --------------------------------------------------------

    buyer_prior_token_counts = [
        len(wallet_seen_tokens[w])
        for w in buy_wallets
    ]

    buyer_prior_tokens_med = med(
        buyer_prior_token_counts
    )

    buyer_prior_tokens_mean = avg(
        buyer_prior_token_counts
    )

    multi_token_buyer_share = (
        safe_div(
            sum(x >= 2 for x in buyer_prior_token_counts),
            len(buyer_prior_token_counts)
        )
        if buyer_prior_token_counts
        else None
    )


    # --------------------------------------------------------
    # BUY SIZE COORDINATION
    # --------------------------------------------------------

    buy_sizes = [
        abs(r["sol_delta"])
        for r in buys
        if valid(r["sol_delta"])
    ]

    buy_size_mean = avg(buy_sizes)
    buy_size_med = med(buy_sizes)

    buy_size_cv = None

    if len(buy_sizes) >= 2:
        m = avg(buy_sizes)

        if m and m != 0:
            buy_size_cv = (
                statistics.pstdev(buy_sizes)
                / abs(m)
            )


    # --------------------------------------------------------
    # CONCENTRATION / COLLECTIVITY
    # --------------------------------------------------------

    buy_counts_by_wallet = Counter(
        r["wallet"]
        for r in buys
    )

    total_buy_count = len(buys)

    max_buy_count = (
        max(buy_counts_by_wallet.values())
        if buy_counts_by_wallet
        else 0
    )

    dominant_buyer_share = safe_div(
        max_buy_count,
        total_buy_count
    )

    distinct_buyer_ratio = safe_div(
        len(buy_wallets),
        total_buy_count
    )


    # --------------------------------------------------------
    # BUY/SELL COHORT OVERLAP
    # --------------------------------------------------------

    overlap = set(buy_wallets) & set(sell_wallets)

    buy_sell_overlap_ratio = safe_div(
        len(overlap),
        len(set(all_wallets))
    )

    buy_to_sell_wallet_ratio = safe_div(
        len(buy_wallets),
        len(sell_wallets)
    )


    # --------------------------------------------------------
    # SIMPLE FLOW COLLECTIVITY
    # --------------------------------------------------------

    buy_event_share = safe_div(
        len(buys),
        len(pre)
    )

    wallet_buy_share = safe_div(
        len(buy_wallets),
        len(all_wallets)
    )


    records.append({
        "id":
            e["id"],

        "timestamp":
            e["timestamp"],

        "token_mint":
            e["token_mint"],

        "label":
            label_r60(
                e["dex_return_60s"]
            ),

        "r60":
            e["dex_return_60s"],

        "buy_wallet_count":
            len(buy_wallets),

        "sell_wallet_count":
            len(sell_wallets),

        "wallet_count":
            len(all_wallets),

        "median_buy_wallet_gap":
            median_buy_wallet_gap,

        "mean_buy_wallet_gap":
            mean_buy_wallet_gap,

        "burst_1s_share":
            burst_1s,

        "burst_3s_share":
            burst_3s,

        "burst_5s_share":
            burst_5s,

        "pair_prior_shared_mean":
            pair_prior_shared_mean,

        "pair_prior_shared_max":
            pair_prior_shared_max,

        "repeated_pair_share":
            repeated_pair_share,

        "buyer_prior_tokens_med":
            buyer_prior_tokens_med,

        "buyer_prior_tokens_mean":
            buyer_prior_tokens_mean,

        "multi_token_buyer_share":
            multi_token_buyer_share,

        "buy_size_mean":
            buy_size_mean,

        "buy_size_med":
            buy_size_med,

        "buy_size_cv":
            buy_size_cv,

        "dominant_buyer_share":
            dominant_buyer_share,

        "distinct_buyer_ratio":
            distinct_buyer_ratio,

        "buy_sell_overlap_ratio":
            buy_sell_overlap_ratio,

        "buy_to_sell_wallet_ratio":
            buy_to_sell_wallet_ratio,

        "buy_event_share":
            buy_event_share,

        "wallet_buy_share":
            wallet_buy_share,
    })


FEATURES = [
    "buy_wallet_count",
    "sell_wallet_count",
    "wallet_count",

    "median_buy_wallet_gap",
    "mean_buy_wallet_gap",

    "burst_1s_share",
    "burst_3s_share",
    "burst_5s_share",

    "pair_prior_shared_mean",
    "pair_prior_shared_max",
    "repeated_pair_share",

    "buyer_prior_tokens_med",
    "buyer_prior_tokens_mean",
    "multi_token_buyer_share",

    "buy_size_mean",
    "buy_size_med",
    "buy_size_cv",

    "dominant_buyer_share",
    "distinct_buyer_ratio",

    "buy_sell_overlap_ratio",
    "buy_to_sell_wallet_ratio",

    "buy_event_share",
    "wallet_buy_share",
]


# ============================================================
# TOKEN HOLDOUT SPLIT
# ============================================================

tokens = sorted(
    set(
        r["token_mint"]
        for r in records
    )
)

rng = random.Random(
    RANDOM_SEED
)

rng.shuffle(tokens)

n = len(tokens)

n_train = int(
    0.60 * n
)

n_valid = int(
    0.20 * n
)

train_tokens = set(
    tokens[:n_train]
)

valid_tokens = set(
    tokens[
        n_train:
        n_train+n_valid
    ]
)

test_tokens = set(
    tokens[
        n_train+n_valid:
    ]
)


def subset(tokset):
    return [
        r for r in records
        if r["token_mint"]
        in tokset
    ]


train = subset(
    train_tokens
)

valid_set = subset(
    valid_tokens
)

test = subset(
    test_tokens
)


# ============================================================
# OUTPUT
# ============================================================

print("=" * 170)
print("MEMECOIN LAB — T48 WALLET COHORT COORDINATION DISCOVERY LAB")
print("=" * 170)

print(
    f"LABELED EVENTS : {len(records)}"
)

print(
    f"UNIQUE TOKENS  : {len(tokens)}"
)

print(
    f"TRAIN          : {len(train)} events | {len(train_tokens)} tokens"
)

print(
    f"VALID          : {len(valid_set)} events | {len(valid_tokens)} tokens"
)

print(
    f"TEST           : {len(test)} events | {len(test_tokens)} tokens"
)


# ============================================================
# A) COVERAGE
# ============================================================

print()
print("=" * 170)
print("A) FEATURE COVERAGE")
print("=" * 170)

for f in FEATURES:

    vals = [
        r[f]
        for r in records
        if valid(r[f])
    ]

    print(
        f"{f:32} "
        f"| N={len(vals):4d}/{len(records)} "
        f"| COVERAGE="
        f"{100*len(vals)/max(1,len(records)):6.1f}%"
    )


# ============================================================
# B) TRAIN SEPARATION ONLY
# ============================================================

print()
print("=" * 170)
print("B) TRAIN-ONLY FEATURE SEPARATION")
print("=" * 170)

train_scores = []

for f in FEATURES:

    run = [
        r[f]
        for r in train
        if (
            r["label"] == 1
            and valid(r[f])
        )
    ]

    dump = [
        r[f]
        for r in train
        if (
            r["label"] == 0
            and valid(r[f])
        )
    ]

    if len(run) < 2 or len(dump) < 2:
        continue

    rm = med(run)
    dm = med(dump)

    pooled = statistics.pstdev(
        run + dump
    )

    sep = (
        abs(rm-dm) / pooled
        if pooled > 0
        else 0
    )

    train_scores.append({
        "feature":
            f,

        "run_med":
            rm,

        "dump_med":
            dm,

        "diff":
            rm-dm,

        "sep":
            sep,
    })


train_scores.sort(
    key=lambda x:
        x["sep"],
    reverse=True
)

print(
    f"{'FEATURE':34} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-" * 90)

for x in train_scores:

    print(
        f"{x['feature']:34} "
        f"{x['run_med']:+11.4f} "
        f"{x['dump_med']:+11.4f} "
        f"{x['diff']:+11.4f} "
        f"{x['sep']:7.3f}"
    )


# ============================================================
# C) DIRECTION SURVIVAL
# ============================================================

print()
print("=" * 170)
print("C) TRAIN / VALID / TEST DIRECTION SURVIVAL")
print("=" * 170)

survivors = []

for f in FEATURES:

    diffs = []

    ns = []

    for rr in [
        train,
        valid_set,
        test
    ]:

        run = [
            r[f]
            for r in rr
            if (
                r["label"] == 1
                and valid(r[f])
            )
        ]

        dump = [
            r[f]
            for r in rr
            if (
                r["label"] == 0
                and valid(r[f])
            )
        ]

        ns.append(
            (
                len(run),
                len(dump)
            )
        )

        if not run or not dump:
            diffs.append(None)

        else:
            diffs.append(
                med(run)
                - med(dump)
            )


    same = False

    if all(
        valid(x)
        for x in diffs
    ):

        s = [
            sign(x)
            for x in diffs
        ]

        same = (
            s[0] != 0
            and s[0]
            == s[1]
            == s[2]
        )


    if same:
        survivors.append(f)


    print(
        f"{f:34} "
        f"TRAIN={str(diffs[0]):>12} "
        f"VALID={str(diffs[1]):>12} "
        f"TEST={str(diffs[2]):>12} "
        f"SAME={same} "
        f"N={ns}"
    )


# ============================================================
# D) CHRONOLOGICAL THIRDS
# ============================================================

print()
print("=" * 170)
print("D) CHRONOLOGICAL THIRD STABILITY")
print("=" * 170)

ordered = sorted(
    records,
    key=lambda r:
        (
            r["timestamp"],
            r["id"]
        )
)

n = len(ordered)

thirds = [
    ordered[:n//3],
    ordered[n//3:(2*n)//3],
    ordered[(2*n)//3:]
]


for f in survivors:

    print()
    print(f)
    print("-" * 110)

    for i, rr in enumerate(
        thirds,
        start=1
    ):

        run = [
            r[f]
            for r in rr
            if (
                r["label"] == 1
                and valid(r[f])
            )
        ]

        dump = [
            r[f]
            for r in rr
            if (
                r["label"] == 0
                and valid(r[f])
            )
        ]

        diff = (
            med(run)-med(dump)
            if run and dump
            else None
        )

        print(
            f"T{i} | "
            f"N={len(run)+len(dump):3d} "
            f"| RUN_MED={med(run) if run else None} "
            f"| DUMP_MED={med(dump) if dump else None} "
            f"| DIFF={diff}"
        )


# ============================================================
# E) FIRST EVENT / TOKEN
# ============================================================

print()
print("=" * 170)
print("E) FIRST-EVENT/TOKEN DIRECTION")
print("=" * 170)

def first_per_token(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x:
            x["id"]
    ):

        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


for name, rr in [
    (
        "VALID",
        first_per_token(valid_set)
    ),
    (
        "TEST",
        first_per_token(test)
    )
]:

    print()
    print(name)
    print("-" * 110)

    for f in survivors:

        run = [
            r[f]
            for r in rr
            if (
                r["label"] == 1
                and valid(r[f])
            )
        ]

        dump = [
            r[f]
            for r in rr
            if (
                r["label"] == 0
                and valid(r[f])
            )
        ]

        diff = (
            med(run)-med(dump)
            if run and dump
            else None
        )

        print(
            f"{f:34} "
            f"| N={len(run)+len(dump):3d} "
            f"| DIFF={diff}"
        )


# ============================================================
# F) DECISION SUPPORT
# ============================================================

print()
print("=" * 170)
print("F) DECISION SUPPORT")
print("=" * 170)

print(
    f"SAME-DIRECTION FEATURES = {len(survivors)}"
)

for f in survivors:
    print(
        "•",
        f
    )


if len(survivors) >= 3:

    print()
    print(
        "🟢 COORDINATION FAMILY CONTAINS MULTIPLE "
        "CROSS-SPLIT SURVIVORS."
    )

    print(
        "Next step: T49 robustness audit on frozen survivors only."
    )

elif len(survivors) >= 1:

    print()
    print(
        "🟡 A SMALL NUMBER OF COORDINATION METRICS SURVIVE."
    )

    print(
        "Do not model yet. Audit them individually first."
    )

else:

    print()
    print(
        "🔴 NO ROBUST COORDINATION EDGE YET."
    )

    print(
        "Do not force a model from this feature family."
    )


print()
print("IMPORTANT:")
print("• T48 fits no model.")
print("• TRAIN separation is descriptive only.")
print("• VALID/TEST are not used to select thresholds.")
print("• Historical wallet co-occurrence is strictly prior-only.")
print("• Current event outcome never enters historical state.")
print("• T23/T31/T32/T47 remain untouched.")
print("• T48 writes nothing to DB.")
print("• Research discovery only.")

db.close()
