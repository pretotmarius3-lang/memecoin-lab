import sqlite3
import math
import random
import statistics

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

EPS_SOL = 0.05
SEED = 55


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def avg(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def sdiv(a, b, eps=EPS_SOL):
    if not valid(a) or not valid(b):
        return None

    if abs(b) < eps:
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
    if not valid(x) or x == 0:
        return 0
    return 1 if x > 0 else -1


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


rows = db.execute("""
SELECT
    e.id,
    e.timestamp,
    e.token_mint,
    e.dex_return_60s,

    s.early_buy_sol,
    s.mid_buy_sol,
    s.recent_buy_sol,

    s.early_sell_sol,
    s.mid_sell_sol,
    s.recent_sell_sol,

    s.early_net_sol,
    s.mid_net_sol,
    s.recent_net_sol,

    s.early_price_return,
    s.mid_price_return,
    s.recent_price_return,

    s.early_unique_buyers,
    s.mid_unique_buyers,
    s.recent_unique_buyers,

    s.early_unique_sellers,
    s.mid_unique_sellers,
    s.recent_unique_sellers,

    s.early_buy_count,
    s.mid_buy_count,
    s.recent_buy_count,

    s.early_sell_count,
    s.mid_sell_count,
    s.recent_sell_count,

    s.early_duration,
    s.mid_duration,
    s.recent_duration,

    s.early_swaps_per_sec,
    s.mid_swaps_per_sec,
    s.recent_swaps_per_sec

FROM events e
JOIN event_sequence_features_v340 s
    ON s.event_id=e.id

WHERE
    e.dex_return_60s IS NOT NULL

ORDER BY e.timestamp, e.id
""").fetchall()


records = []


for r in rows:

    y = label_r60(
        r["dex_return_60s"]
    )

    if y is None:
        continue


    eb = r["early_buy_sol"]
    mb = r["mid_buy_sol"]
    rb = r["recent_buy_sol"]

    es = r["early_sell_sol"]
    ms = r["mid_sell_sol"]
    rs = r["recent_sell_sol"]

    en = r["early_net_sol"]
    mn = r["mid_net_sol"]
    rn = r["recent_net_sol"]

    ep = r["early_price_return"]
    mp = r["mid_price_return"]
    rp = r["recent_price_return"]


    # ========================================================
    # 1) MARGINAL PRICE RESPONSE TO NET FLOW
    # ========================================================

    early_marginal_response = sdiv(ep, en)
    mid_marginal_response = sdiv(mp, mn)
    recent_marginal_response = sdiv(rp, rn)


    # ========================================================
    # 2) RESPONSE TO BUY CAPITAL ONLY
    # ========================================================

    early_buy_response = sdiv(ep, eb)
    mid_buy_response = sdiv(mp, mb)
    recent_buy_response = sdiv(rp, rb)


    # ========================================================
    # 3) SELL ABSORPTION
    #
    # Positive price despite larger sell pressure => stronger
    # absorption.
    # ========================================================

    early_sell_absorption = sdiv(
        ep,
        abs(es) if valid(es) else None
    )

    mid_sell_absorption = sdiv(
        mp,
        abs(ms) if valid(ms) else None
    )

    recent_sell_absorption = sdiv(
        rp,
        abs(rs) if valid(rs) else None
    )


    # ========================================================
    # 4) CAPITAL EFFICIENCY PER BUYER
    # ========================================================

    early_price_per_buyer = sdiv(
        ep,
        r["early_unique_buyers"],
        eps=0.5
    )

    mid_price_per_buyer = sdiv(
        mp,
        r["mid_unique_buyers"],
        eps=0.5
    )

    recent_price_per_buyer = sdiv(
        rp,
        r["recent_unique_buyers"],
        eps=0.5
    )


    early_sol_per_buyer = sdiv(
        eb,
        r["early_unique_buyers"],
        eps=0.5
    )

    mid_sol_per_buyer = sdiv(
        mb,
        r["mid_unique_buyers"],
        eps=0.5
    )

    recent_sol_per_buyer = sdiv(
        rb,
        r["recent_unique_buyers"],
        eps=0.5
    )


    # ========================================================
    # 5) SELLER RESISTANCE
    #
    # selling intensity relative to buyers / buy flow
    # ========================================================

    early_sell_buy_sol_ratio = sdiv(
        abs(es) if valid(es) else None,
        abs(eb) if valid(eb) else None
    )

    mid_sell_buy_sol_ratio = sdiv(
        abs(ms) if valid(ms) else None,
        abs(mb) if valid(mb) else None
    )

    recent_sell_buy_sol_ratio = sdiv(
        abs(rs) if valid(rs) else None,
        abs(rb) if valid(rb) else None
    )


    early_seller_buyer_ratio = sdiv(
        r["early_unique_sellers"],
        r["early_unique_buyers"],
        eps=0.5
    )

    mid_seller_buyer_ratio = sdiv(
        r["mid_unique_sellers"],
        r["mid_unique_buyers"],
        eps=0.5
    )

    recent_seller_buyer_ratio = sdiv(
        r["recent_unique_sellers"],
        r["recent_unique_buyers"],
        eps=0.5
    )


    # ========================================================
    # 6) GROSS FLOW EFFICIENCY
    # ========================================================

    early_gross = (
        abs(eb) + abs(es)
        if valid(eb) and valid(es)
        else None
    )

    mid_gross = (
        abs(mb) + abs(ms)
        if valid(mb) and valid(ms)
        else None
    )

    recent_gross = (
        abs(rb) + abs(rs)
        if valid(rb) and valid(rs)
        else None
    )


    early_price_per_gross = sdiv(
        ep,
        early_gross
    )

    mid_price_per_gross = sdiv(
        mp,
        mid_gross
    )

    recent_price_per_gross = sdiv(
        rp,
        recent_gross
    )


    # ========================================================
    # 7) DIMINISHING / IMPROVING RESPONSE
    # ========================================================

    delta_net_response_e_m = (
        mid_marginal_response
        - early_marginal_response
        if (
            valid(mid_marginal_response)
            and valid(early_marginal_response)
        )
        else None
    )

    delta_net_response_m_r = (
        recent_marginal_response
        - mid_marginal_response
        if (
            valid(recent_marginal_response)
            and valid(mid_marginal_response)
        )
        else None
    )


    delta_buy_response_e_m = (
        mid_buy_response
        - early_buy_response
        if (
            valid(mid_buy_response)
            and valid(early_buy_response)
        )
        else None
    )

    delta_buy_response_m_r = (
        recent_buy_response
        - mid_buy_response
        if (
            valid(recent_buy_response)
            and valid(mid_buy_response)
        )
        else None
    )


    delta_gross_response_e_m = (
        mid_price_per_gross
        - early_price_per_gross
        if (
            valid(mid_price_per_gross)
            and valid(early_price_per_gross)
        )
        else None
    )

    delta_gross_response_m_r = (
        recent_price_per_gross
        - mid_price_per_gross
        if (
            valid(recent_price_per_gross)
            and valid(mid_price_per_gross)
        )
        else None
    )


    # ========================================================
    # 8) PRICE RESPONSE VELOCITY
    # ========================================================

    early_price_velocity = sdiv(
        ep,
        r["early_duration"],
        eps=0.1
    )

    mid_price_velocity = sdiv(
        mp,
        r["mid_duration"],
        eps=0.1
    )

    recent_price_velocity = sdiv(
        rp,
        r["recent_duration"],
        eps=0.1
    )


    # ========================================================
    # 9) FLOW VELOCITY
    # ========================================================

    early_net_velocity = sdiv(
        en,
        r["early_duration"],
        eps=0.1
    )

    mid_net_velocity = sdiv(
        mn,
        r["mid_duration"],
        eps=0.1
    )

    recent_net_velocity = sdiv(
        rn,
        r["recent_duration"],
        eps=0.1
    )


    # ========================================================
    # 10) RESPONSE PER FLOW VELOCITY
    # ========================================================

    early_response_per_flow_velocity = sdiv(
        early_price_velocity,
        early_net_velocity,
        eps=0.01
    )

    mid_response_per_flow_velocity = sdiv(
        mid_price_velocity,
        mid_net_velocity,
        eps=0.01
    )

    recent_response_per_flow_velocity = sdiv(
        recent_price_velocity,
        recent_net_velocity,
        eps=0.01
    )


    # ========================================================
    # 11) SWAP EFFICIENCY
    # ========================================================

    early_price_per_swap_rate = sdiv(
        ep,
        r["early_swaps_per_sec"],
        eps=0.01
    )

    mid_price_per_swap_rate = sdiv(
        mp,
        r["mid_swaps_per_sec"],
        eps=0.01
    )

    recent_price_per_swap_rate = sdiv(
        rp,
        r["recent_swaps_per_sec"],
        eps=0.01
    )


    # ========================================================
    # 12) FLOW/PRICE SIGN MISMATCH
    # ========================================================

    early_positive_price_negative_flow = (
        1.0
        if (
            valid(ep)
            and valid(en)
            and ep > 0
            and en < 0
        )
        else 0.0
    )

    mid_positive_price_negative_flow = (
        1.0
        if (
            valid(mp)
            and valid(mn)
            and mp > 0
            and mn < 0
        )
        else 0.0
    )

    recent_positive_price_negative_flow = (
        1.0
        if (
            valid(rp)
            and valid(rn)
            and rp > 0
            and rn < 0
        )
        else 0.0
    )


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],
        "label": y,
        "r60": r["dex_return_60s"],

        "early_marginal_response": early_marginal_response,
        "mid_marginal_response": mid_marginal_response,
        "recent_marginal_response": recent_marginal_response,

        "early_buy_response": early_buy_response,
        "mid_buy_response": mid_buy_response,
        "recent_buy_response": recent_buy_response,

        "early_sell_absorption": early_sell_absorption,
        "mid_sell_absorption": mid_sell_absorption,
        "recent_sell_absorption": recent_sell_absorption,

        "early_price_per_buyer": early_price_per_buyer,
        "mid_price_per_buyer": mid_price_per_buyer,
        "recent_price_per_buyer": recent_price_per_buyer,

        "early_sol_per_buyer": early_sol_per_buyer,
        "mid_sol_per_buyer": mid_sol_per_buyer,
        "recent_sol_per_buyer": recent_sol_per_buyer,

        "early_sell_buy_sol_ratio": early_sell_buy_sol_ratio,
        "mid_sell_buy_sol_ratio": mid_sell_buy_sol_ratio,
        "recent_sell_buy_sol_ratio": recent_sell_buy_sol_ratio,

        "early_seller_buyer_ratio": early_seller_buyer_ratio,
        "mid_seller_buyer_ratio": mid_seller_buyer_ratio,
        "recent_seller_buyer_ratio": recent_seller_buyer_ratio,

        "early_price_per_gross": early_price_per_gross,
        "mid_price_per_gross": mid_price_per_gross,
        "recent_price_per_gross": recent_price_per_gross,

        "delta_net_response_e_m": delta_net_response_e_m,
        "delta_net_response_m_r": delta_net_response_m_r,

        "delta_buy_response_e_m": delta_buy_response_e_m,
        "delta_buy_response_m_r": delta_buy_response_m_r,

        "delta_gross_response_e_m": delta_gross_response_e_m,
        "delta_gross_response_m_r": delta_gross_response_m_r,

        "early_price_velocity": early_price_velocity,
        "mid_price_velocity": mid_price_velocity,
        "recent_price_velocity": recent_price_velocity,

        "early_net_velocity": early_net_velocity,
        "mid_net_velocity": mid_net_velocity,
        "recent_net_velocity": recent_net_velocity,

        "early_response_per_flow_velocity": early_response_per_flow_velocity,
        "mid_response_per_flow_velocity": mid_response_per_flow_velocity,
        "recent_response_per_flow_velocity": recent_response_per_flow_velocity,

        "early_price_per_swap_rate": early_price_per_swap_rate,
        "mid_price_per_swap_rate": mid_price_per_swap_rate,
        "recent_price_per_swap_rate": recent_price_per_swap_rate,

        "early_positive_price_negative_flow": early_positive_price_negative_flow,
        "mid_positive_price_negative_flow": mid_positive_price_negative_flow,
        "recent_positive_price_negative_flow": recent_positive_price_negative_flow,
    })


FEATURES = [
    "early_marginal_response",
    "mid_marginal_response",
    "recent_marginal_response",

    "early_buy_response",
    "mid_buy_response",
    "recent_buy_response",

    "early_sell_absorption",
    "mid_sell_absorption",
    "recent_sell_absorption",

    "early_price_per_buyer",
    "mid_price_per_buyer",
    "recent_price_per_buyer",

    "early_sol_per_buyer",
    "mid_sol_per_buyer",
    "recent_sol_per_buyer",

    "early_sell_buy_sol_ratio",
    "mid_sell_buy_sol_ratio",
    "recent_sell_buy_sol_ratio",

    "early_seller_buyer_ratio",
    "mid_seller_buyer_ratio",
    "recent_seller_buyer_ratio",

    "early_price_per_gross",
    "mid_price_per_gross",
    "recent_price_per_gross",

    "delta_net_response_e_m",
    "delta_net_response_m_r",

    "delta_buy_response_e_m",
    "delta_buy_response_m_r",

    "delta_gross_response_e_m",
    "delta_gross_response_m_r",

    "early_price_velocity",
    "mid_price_velocity",
    "recent_price_velocity",

    "early_net_velocity",
    "mid_net_velocity",
    "recent_net_velocity",

    "early_response_per_flow_velocity",
    "mid_response_per_flow_velocity",
    "recent_response_per_flow_velocity",

    "early_price_per_swap_rate",
    "mid_price_per_swap_rate",
    "recent_price_per_swap_rate",

    "early_positive_price_negative_flow",
    "mid_positive_price_negative_flow",
    "recent_positive_price_negative_flow",
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

rng = random.Random(SEED)
rng.shuffle(tokens)

n = len(tokens)

n_train = int(0.60 * n)
n_valid = int(0.20 * n)

train_tokens = set(tokens[:n_train])
valid_tokens = set(tokens[n_train:n_train+n_valid])
test_tokens = set(tokens[n_train+n_valid:])


def subset(tokset):
    return [
        r for r in records
        if r["token_mint"] in tokset
    ]


train = subset(train_tokens)
valid_set = subset(valid_tokens)
test = subset(test_tokens)


def feature_diff(rr, f):

    runs = [
        r[f]
        for r in rr
        if r["label"] == 1 and valid(r[f])
    ]

    dumps = [
        r[f]
        for r in rr
        if r["label"] == 0 and valid(r[f])
    ]

    if not runs or not dumps:
        return None, len(runs), len(dumps)

    return (
        med(runs) - med(dumps),
        len(runs),
        len(dumps)
    )


print("=" * 185)
print("MEMECOIN LAB — T55 FLOW ABSORPTION / MARGINAL PRICE RESPONSE DISCOVERY")
print("=" * 185)

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
print("=" * 185)
print("A) FEATURE COVERAGE")
print("=" * 185)

for f in FEATURES:

    vals = [
        r[f]
        for r in records
        if valid(r[f])
    ]

    print(
        f"{f:40} "
        f"N={len(vals):4d}/{len(records)} "
        f"COV={100*len(vals)/max(1,len(records)):6.1f}%"
    )


# ============================================================
# B) TRAIN SEPARATION
# ============================================================

print()
print("=" * 185)
print("B) TRAIN-ONLY FEATURE SEPARATION")
print("=" * 185)

scores = []

for f in FEATURES:

    runs = [
        r[f]
        for r in train
        if r["label"] == 1 and valid(r[f])
    ]

    dumps = [
        r[f]
        for r in train
        if r["label"] == 0 and valid(r[f])
    ]

    if len(runs) < 2 or len(dumps) < 2:
        continue

    rm = med(runs)
    dm = med(dumps)

    pooled = statistics.pstdev(
        runs + dumps
    )

    sep = (
        abs(rm-dm) / pooled
        if pooled > 0
        else 0
    )

    scores.append(
        (
            f,
            rm,
            dm,
            rm-dm,
            sep
        )
    )


scores.sort(
    key=lambda x: x[4],
    reverse=True
)


print(
    f"{'FEATURE':40} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-" * 95)


for f, rm, dm, diff, sep in scores:

    print(
        f"{f:40} "
        f"{rm:+11.4f} "
        f"{dm:+11.4f} "
        f"{diff:+11.4f} "
        f"{sep:7.3f}"
    )


# ============================================================
# C) DIRECTION SURVIVAL
# ============================================================

print()
print("=" * 185)
print("C) TRAIN / VALID / TEST DIRECTION SURVIVAL")
print("=" * 185)

survivors = []


for f in FEATURES:

    td, tr, tx = feature_diff(train, f)
    vd, vr, vx = feature_diff(valid_set, f)
    xd, xr, xx = feature_diff(test, f)

    diffs = [
        td,
        vd,
        xd
    ]

    same = False

    if all(valid(x) for x in diffs):

        s = [
            sign(x)
            for x in diffs
        ]

        same = (
            s[0] != 0
            and s[0] == s[1] == s[2]
        )


    if same:
        survivors.append(f)


    print(
        f"{f:40} "
        f"TRAIN={str(td):>14} "
        f"VALID={str(vd):>14} "
        f"TEST={str(xd):>14} "
        f"SAME={same} "
        f"N=[({tr},{tx}),({vr},{vx}),({xr},{xx})]"
    )


# ============================================================
# D) CHRONOLOGICAL STABILITY
# ============================================================

print()
print("=" * 185)
print("D) CHRONOLOGICAL STABILITY — SURVIVORS ONLY")
print("=" * 185)


ordered = sorted(
    records,
    key=lambda r: (
        r["timestamp"],
        r["id"]
    )
)

N = len(ordered)

blocks = [
    (
        "Q1",
        ordered[:N//4]
    ),
    (
        "Q2",
        ordered[N//4:N//2]
    ),
    (
        "Q3",
        ordered[N//2:(3*N)//4]
    ),
    (
        "Q4",
        ordered[(3*N)//4:]
    ),
]


for f in survivors:

    print()
    print(f)
    print("-" * 120)

    for name, rr in blocks:

        diff, nr, nd = feature_diff(
            rr,
            f
        )

        print(
            f"{name:4} "
            f"| N={nr+nd:3d} "
            f"| RUN={nr:3d} "
            f"| DUMP={nd:3d} "
            f"| DIFF={diff}"
        )


# ============================================================
# E) FIRST EVENT / TOKEN
# ============================================================

def first_per_token(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x: (
            x["timestamp"],
            x["id"]
        )
    ):

        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


print()
print("=" * 185)
print("E) FIRST-EVENT/TOKEN — SURVIVORS")
print("=" * 185)


for split_name, rr in [
    (
        "TRAIN",
        first_per_token(train)
    ),
    (
        "VALID",
        first_per_token(valid_set)
    ),
    (
        "TEST",
        first_per_token(test)
    ),
]:

    print()
    print(split_name)
    print("-" * 120)

    for f in survivors:

        diff, nr, nd = feature_diff(
            rr,
            f
        )

        print(
            f"{f:40} "
            f"N={nr+nd:3d} "
            f"RUN={nr:3d} "
            f"DUMP={nd:3d} "
            f"DIFF={diff}"
        )


# ============================================================
# F) REDUNDANCY
# ============================================================

print()
print("=" * 185)
print("F) SURVIVOR REDUNDANCY")
print("=" * 185)


def pearson(a, b):

    pairs = [
        (x, y)
        for x, y in zip(a, b)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]

    mx = avg(xs)
    my = avg(ys)

    dx = math.sqrt(
        sum(
            (x-mx)**2
            for x in xs
        )
    )

    dy = math.sqrt(
        sum(
            (y-my)**2
            for y in ys
        )
    )

    if dx == 0 or dy == 0:
        return None

    return sum(
        (x-mx)*(y-my)
        for x, y in pairs
    ) / (dx*dy)


if len(survivors) < 2:

    print(
        "Not enough survivors for redundancy analysis."
    )

else:

    for i in range(len(survivors)):

        for j in range(i+1, len(survivors)):

            a = survivors[i]
            b = survivors[j]

            corr = pearson(
                [r[a] for r in records],
                [r[b] for r in records]
            )

            print(
                f"{a:40} vs "
                f"{b:40} "
                f"CORR={corr}"
            )


# ============================================================
# G) DECISION
# ============================================================

print()
print("=" * 185)
print("G) DECISION SUPPORT")
print("=" * 185)

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
        "🟢 FLOW-ABSORPTION FAMILY CONTAINS "
        "MULTIPLE CROSS-SPLIT SURVIVORS."
    )

    print(
        "Next = freeze only the strongest non-redundant survivors "
        "and run T56 robustness/incremental audit."
    )

elif len(survivors) >= 1:

    print()
    print(
        "🟡 LIMITED FLOW-ABSORPTION SIGNAL."
    )

    print(
        "Audit individual survivors before any model."
    )

else:

    print()
    print(
        "🔴 NO ROBUST FLOW-ABSORPTION EDGE."
    )

    print(
        "Do not force this feature family."
    )


print()
print("IMPORTANT:")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Token identities do not cross splits.")
print("• Ratio denominators near zero are excluded.")
print("• TEST does not change feature definitions.")
print("• T23/T31/T32/T47/T51 remain untouched.")
print("• T55 writes nothing to DB.")
print("• Research discovery only.")

db.close()
