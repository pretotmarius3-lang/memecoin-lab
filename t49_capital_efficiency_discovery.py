import sqlite3
import math
import random
import statistics

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
SEED = 49
EPS = 0.05   # prevents ratios exploding around zero SOL flow


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.median(xs) if xs else None


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return statistics.mean(xs) if xs else None


def sdiv(a, b, eps=EPS):
    if not valid(a) or not valid(b):
        return None

    if abs(b) < eps:
        return None

    return a / b


def label(r):
    if not valid(r):
        return None

    if r >= RUNNER:
        return 1

    if r <= DUMP:
        return 0

    return None


def sgn(x):
    if not valid(x) or x == 0:
        return 0
    return 1 if x > 0 else -1


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# LOAD
# ============================================================

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

    s.early_buy_count,
    s.mid_buy_count,
    s.recent_buy_count,

    s.early_duration,
    s.mid_duration,
    s.recent_duration

FROM events e
JOIN event_sequence_features_v340 s
    ON s.event_id = e.id

WHERE
    e.dex_return_60s IS NOT NULL

ORDER BY e.timestamp, e.id
""").fetchall()


records = []


# ============================================================
# FEATURE ENGINEERING
# ============================================================

for r in rows:

    y = label(r["dex_return_60s"])

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


    # --------------------------------------------------------
    # BASIC CAPITAL EFFICIENCY
    # price response per unit of capital
    # --------------------------------------------------------

    early_price_per_buy_sol = sdiv(ep, eb)
    mid_price_per_buy_sol = sdiv(mp, mb)
    recent_price_per_buy_sol = sdiv(rp, rb)

    early_price_per_net_sol = sdiv(ep, en)
    mid_price_per_net_sol = sdiv(mp, mn)
    recent_price_per_net_sol = sdiv(rp, rn)


    # --------------------------------------------------------
    # BUYER EFFICIENCY
    # price response per unique buyer
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # CAPITAL PER BUYER
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # SELL ABSORPTION
    #
    # Positive price despite selling pressure is potentially
    # interesting.
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # FLOW BALANCE
    # bounded-ish measure: net / gross capital
    # --------------------------------------------------------

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

    early_net_efficiency = sdiv(en, early_gross)
    mid_net_efficiency = sdiv(mn, mid_gross)
    recent_net_efficiency = sdiv(rn, recent_gross)


    # --------------------------------------------------------
    # PRICE / FLOW DIVERGENCE
    #
    # + price with weak/negative net flow = absorption
    # - price despite positive flow = poor response
    # --------------------------------------------------------

    early_flow_price_div = (
        ep - en
        if valid(ep) and valid(en)
        else None
    )

    mid_flow_price_div = (
        mp - mn
        if valid(mp) and valid(mn)
        else None
    )

    recent_flow_price_div = (
        rp - rn
        if valid(rp) and valid(rn)
        else None
    )


    # --------------------------------------------------------
    # DIRECTION AGREEMENT
    # --------------------------------------------------------

    early_flow_price_agree = (
        1.0 if sgn(ep) == sgn(en) and sgn(ep) != 0 else 0.0
        if valid(ep) and valid(en)
        else None
    )

    mid_flow_price_agree = (
        1.0 if sgn(mp) == sgn(mn) and sgn(mp) != 0 else 0.0
        if valid(mp) and valid(mn)
        else None
    )

    recent_flow_price_agree = (
        1.0 if sgn(rp) == sgn(rn) and sgn(rp) != 0 else 0.0
        if valid(rp) and valid(rn)
        else None
    )


    # --------------------------------------------------------
    # EFFICIENCY TRANSITIONS
    # Does capital become MORE or LESS effective?
    # --------------------------------------------------------

    delta_buy_eff_e_m = (
        mid_price_per_buy_sol - early_price_per_buy_sol
        if valid(mid_price_per_buy_sol)
        and valid(early_price_per_buy_sol)
        else None
    )

    delta_buy_eff_m_r = (
        recent_price_per_buy_sol - mid_price_per_buy_sol
        if valid(recent_price_per_buy_sol)
        and valid(mid_price_per_buy_sol)
        else None
    )

    delta_net_eff_e_m = (
        mid_price_per_net_sol - early_price_per_net_sol
        if valid(mid_price_per_net_sol)
        and valid(early_price_per_net_sol)
        else None
    )

    delta_net_eff_m_r = (
        recent_price_per_net_sol - mid_price_per_net_sol
        if valid(recent_price_per_net_sol)
        and valid(mid_price_per_net_sol)
        else None
    )

    delta_buyer_eff_e_m = (
        mid_price_per_buyer - early_price_per_buyer
        if valid(mid_price_per_buyer)
        and valid(early_price_per_buyer)
        else None
    )

    delta_buyer_eff_m_r = (
        recent_price_per_buyer - mid_price_per_buyer
        if valid(recent_price_per_buyer)
        and valid(mid_price_per_buyer)
        else None
    )


    # --------------------------------------------------------
    # FLOW TRANSITIONS
    # --------------------------------------------------------

    net_sol_accel_e_m = (
        mn - en
        if valid(mn) and valid(en)
        else None
    )

    net_sol_accel_m_r = (
        rn - mn
        if valid(rn) and valid(mn)
        else None
    )

    price_accel_e_m = (
        mp - ep
        if valid(mp) and valid(ep)
        else None
    )

    price_accel_m_r = (
        rp - mp
        if valid(rp) and valid(mp)
        else None
    )


    # --------------------------------------------------------
    # RESPONSE VELOCITY
    # --------------------------------------------------------

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


    records.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token_mint": r["token_mint"],
        "label": y,
        "r60": r["dex_return_60s"],

        "early_price_per_buy_sol": early_price_per_buy_sol,
        "mid_price_per_buy_sol": mid_price_per_buy_sol,
        "recent_price_per_buy_sol": recent_price_per_buy_sol,

        "early_price_per_net_sol": early_price_per_net_sol,
        "mid_price_per_net_sol": mid_price_per_net_sol,
        "recent_price_per_net_sol": recent_price_per_net_sol,

        "early_price_per_buyer": early_price_per_buyer,
        "mid_price_per_buyer": mid_price_per_buyer,
        "recent_price_per_buyer": recent_price_per_buyer,

        "early_sol_per_buyer": early_sol_per_buyer,
        "mid_sol_per_buyer": mid_sol_per_buyer,
        "recent_sol_per_buyer": recent_sol_per_buyer,

        "early_sell_absorption": early_sell_absorption,
        "mid_sell_absorption": mid_sell_absorption,
        "recent_sell_absorption": recent_sell_absorption,

        "early_net_efficiency": early_net_efficiency,
        "mid_net_efficiency": mid_net_efficiency,
        "recent_net_efficiency": recent_net_efficiency,

        "early_flow_price_div": early_flow_price_div,
        "mid_flow_price_div": mid_flow_price_div,
        "recent_flow_price_div": recent_flow_price_div,

        "early_flow_price_agree": early_flow_price_agree,
        "mid_flow_price_agree": mid_flow_price_agree,
        "recent_flow_price_agree": recent_flow_price_agree,

        "delta_buy_eff_e_m": delta_buy_eff_e_m,
        "delta_buy_eff_m_r": delta_buy_eff_m_r,

        "delta_net_eff_e_m": delta_net_eff_e_m,
        "delta_net_eff_m_r": delta_net_eff_m_r,

        "delta_buyer_eff_e_m": delta_buyer_eff_e_m,
        "delta_buyer_eff_m_r": delta_buyer_eff_m_r,

        "net_sol_accel_e_m": net_sol_accel_e_m,
        "net_sol_accel_m_r": net_sol_accel_m_r,

        "price_accel_e_m": price_accel_e_m,
        "price_accel_m_r": price_accel_m_r,

        "early_price_velocity": early_price_velocity,
        "mid_price_velocity": mid_price_velocity,
        "recent_price_velocity": recent_price_velocity,
    })


FEATURES = [
    "early_price_per_buy_sol",
    "mid_price_per_buy_sol",
    "recent_price_per_buy_sol",

    "early_price_per_net_sol",
    "mid_price_per_net_sol",
    "recent_price_per_net_sol",

    "early_price_per_buyer",
    "mid_price_per_buyer",
    "recent_price_per_buyer",

    "early_sol_per_buyer",
    "mid_sol_per_buyer",
    "recent_sol_per_buyer",

    "early_sell_absorption",
    "mid_sell_absorption",
    "recent_sell_absorption",

    "early_net_efficiency",
    "mid_net_efficiency",
    "recent_net_efficiency",

    "early_flow_price_div",
    "mid_flow_price_div",
    "recent_flow_price_div",

    "early_flow_price_agree",
    "mid_flow_price_agree",
    "recent_flow_price_agree",

    "delta_buy_eff_e_m",
    "delta_buy_eff_m_r",

    "delta_net_eff_e_m",
    "delta_net_eff_m_r",

    "delta_buyer_eff_e_m",
    "delta_buyer_eff_m_r",

    "net_sol_accel_e_m",
    "net_sol_accel_m_r",

    "price_accel_e_m",
    "price_accel_m_r",

    "early_price_velocity",
    "mid_price_velocity",
    "recent_price_velocity",
]


# ============================================================
# TOKEN HOLDOUT
# ============================================================

tokens = sorted(set(
    r["token_mint"]
    for r in records
))

rng = random.Random(SEED)
rng.shuffle(tokens)

n = len(tokens)

n_train = int(0.60 * n)
n_valid = int(0.20 * n)

train_tokens = set(tokens[:n_train])
valid_tokens = set(tokens[n_train:n_train+n_valid])
test_tokens = set(tokens[n_train+n_valid:])


def get_subset(tokset):
    return [
        r for r in records
        if r["token_mint"] in tokset
    ]


train = get_subset(train_tokens)
valid_set = get_subset(valid_tokens)
test = get_subset(test_tokens)


# ============================================================
# SEPARATION
# ============================================================

def feature_diff(rr, f):

    runs = [
        r[f] for r in rr
        if r["label"] == 1 and valid(r[f])
    ]

    dumps = [
        r[f] for r in rr
        if r["label"] == 0 and valid(r[f])
    ]

    if not runs or not dumps:
        return None, len(runs), len(dumps)

    return (
        med(runs) - med(dumps),
        len(runs),
        len(dumps)
    )


print("=" * 180)
print("MEMECOIN LAB — T49 CAPITAL EFFICIENCY / PRICE RESPONSE DISCOVERY")
print("=" * 180)

print(f"LABELED EVENTS : {len(records)}")
print(f"UNIQUE TOKENS  : {len(tokens)}")
print()
print(f"TRAIN : {len(train)} events | {len(train_tokens)} tokens")
print(f"VALID : {len(valid_set)} events | {len(valid_tokens)} tokens")
print(f"TEST  : {len(test)} events | {len(test_tokens)} tokens")


# ============================================================
# A) COVERAGE
# ============================================================

print()
print("=" * 180)
print("A) FEATURE COVERAGE")
print("=" * 180)

for f in FEATURES:

    vals = [
        r[f] for r in records
        if valid(r[f])
    ]

    print(
        f"{f:34} "
        f"N={len(vals):4d}/{len(records)} "
        f"COV={100*len(vals)/max(1,len(records)):6.1f}%"
    )


# ============================================================
# B) TRAIN SEPARATION
# ============================================================

print()
print("=" * 180)
print("B) TRAIN-ONLY FEATURE SEPARATION")
print("=" * 180)

scores = []

for f in FEATURES:

    runs = [
        r[f] for r in train
        if r["label"] == 1 and valid(r[f])
    ]

    dumps = [
        r[f] for r in train
        if r["label"] == 0 and valid(r[f])
    ]

    if len(runs) < 2 or len(dumps) < 2:
        continue

    rm = med(runs)
    dm = med(dumps)

    pooled = statistics.pstdev(runs + dumps)

    sep = (
        abs(rm-dm) / pooled
        if pooled > 0
        else 0
    )

    scores.append(
        (f, rm, dm, rm-dm, sep)
    )

scores.sort(
    key=lambda x: x[4],
    reverse=True
)

print(
    f"{'FEATURE':34} "
    f"{'RUN MED':>12} "
    f"{'DUMP MED':>12} "
    f"{'DIFF':>12} "
    f"{'SEP':>8}"
)

print("-" * 85)

for f, rm, dm, diff, sep in scores:

    print(
        f"{f:34} "
        f"{rm:+11.4f} "
        f"{dm:+11.4f} "
        f"{diff:+11.4f} "
        f"{sep:7.3f}"
    )


# ============================================================
# C) DIRECTION SURVIVAL
# ============================================================

print()
print("=" * 180)
print("C) TRAIN / VALID / TEST DIRECTION SURVIVAL")
print("=" * 180)

survivors = []

for f in FEATURES:

    td, tr, tx = feature_diff(train, f)
    vd, vr, vx = feature_diff(valid_set, f)
    xd, xr, xx = feature_diff(test, f)

    diffs = [td, vd, xd]

    same = False

    if all(valid(x) for x in diffs):

        signs = [sgn(x) for x in diffs]

        same = (
            signs[0] != 0
            and signs[0] == signs[1] == signs[2]
        )

    if same:
        survivors.append(f)

    print(
        f"{f:34} "
        f"TRAIN={str(td):>13} "
        f"VALID={str(vd):>13} "
        f"TEST={str(xd):>13} "
        f"SAME={same} "
        f"N=[({tr},{tx}),({vr},{vx}),({xr},{xx})]"
    )


# ============================================================
# D) CHRONOLOGICAL BLOCKS FOR SURVIVORS
# ============================================================

print()
print("=" * 180)
print("D) CHRONOLOGICAL STABILITY — SURVIVORS ONLY")
print("=" * 180)

ordered = sorted(
    records,
    key=lambda x: (x["timestamp"], x["id"])
)

N = len(ordered)

blocks = {
    "Q1": ordered[:N//4],
    "Q2": ordered[N//4:N//2],
    "Q3": ordered[N//2:(3*N)//4],
    "Q4": ordered[(3*N)//4:]
}

for f in survivors:

    print()
    print(f)
    print("-" * 120)

    for name, rr in blocks.items():

        diff, nr, nd = feature_diff(rr, f)

        print(
            f"{name:4} | "
            f"N={nr+nd:3d} | "
            f"RUN={nr:3d} | "
            f"DUMP={nd:3d} | "
            f"DIFF={diff}"
        )


# ============================================================
# E) FIRST EVENT / TOKEN
# ============================================================

def first_event(rr):

    seen = set()
    out = []

    for r in sorted(
        rr,
        key=lambda x: (x["timestamp"], x["id"])
    ):

        if r["token_mint"] in seen:
            continue

        seen.add(r["token_mint"])
        out.append(r)

    return out


print()
print("=" * 180)
print("E) FIRST-EVENT/TOKEN — SURVIVORS")
print("=" * 180)

for split_name, rr in [
    ("TRAIN", first_event(train)),
    ("VALID", first_event(valid_set)),
    ("TEST", first_event(test))
]:

    print()
    print(split_name)
    print("-" * 120)

    for f in survivors:

        diff, nr, nd = feature_diff(rr, f)

        print(
            f"{f:34} "
            f"N={nr+nd:3d} "
            f"RUN={nr:3d} "
            f"DUMP={nd:3d} "
            f"DIFF={diff}"
        )


# ============================================================
# F) SURVIVOR CORRELATION
# Avoid mistaking 5 copies of same signal for 5 edges
# ============================================================

print()
print("=" * 180)
print("F) SURVIVOR REDUNDANCY")
print("=" * 180)

if len(survivors) < 2:

    print(
        "Not enough survivors for redundancy analysis."
    )

else:

    def pearson(xs, ys):

        pairs = [
            (x, y)
            for x, y in zip(xs, ys)
            if valid(x) and valid(y)
        ]

        if len(pairs) < 3:
            return None

        x = [p[0] for p in pairs]
        y = [p[1] for p in pairs]

        mx = mean(x)
        my = mean(y)

        num = sum(
            (a-mx)*(b-my)
            for a, b in pairs
        )

        dx = math.sqrt(sum(
            (a-mx)**2
            for a in x
        ))

        dy = math.sqrt(sum(
            (b-my)**2
            for b in y
        ))

        if dx == 0 or dy == 0:
            return None

        return num / (dx*dy)


    for i in range(len(survivors)):

        for j in range(i+1, len(survivors)):

            a = survivors[i]
            b = survivors[j]

            xs = [r[a] for r in records]
            ys = [r[b] for r in records]

            corr = pearson(xs, ys)

            print(
                f"{a:34} vs "
                f"{b:34} "
                f"CORR={corr}"
            )


# ============================================================
# G) DECISION
# ============================================================

print()
print("=" * 180)
print("G) DECISION SUPPORT")
print("=" * 180)

print(
    f"SAME-DIRECTION FEATURES = {len(survivors)}"
)

for f in survivors:
    print("•", f)


if len(survivors) >= 3:

    print()
    print(
        "🟢 CAPITAL-EFFICIENCY FAMILY HAS MULTIPLE "
        "CROSS-SPLIT SURVIVORS."
    )

    print(
        "Next = freeze survivors and run T50 robustness audit."
    )

elif len(survivors) >= 1:

    print()
    print(
        "🟡 LIMITED CAPITAL-EFFICIENCY SIGNAL."
    )

    print(
        "Audit individual survivors before any model."
    )

else:

    print()
    print(
        "🔴 NO ROBUST CAPITAL-EFFICIENCY EDGE."
    )

    print(
        "Do not force this family into a model."
    )


print()
print("IMPORTANT:")
print("• No model fitting.")
print("• No threshold optimization.")
print("• Token identities do not cross splits.")
print("• TEST is diagnostic final holdout only.")
print("• Ratio denominators near zero are excluded.")
print("• T23/T31/T32/T47 remain untouched.")
print("• T49 writes nothing to DB.")
print("• Research discovery only.")

db.close()
