#!/usr/bin/env python3

import sqlite3
import math
import random
import statistics
from collections import defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"

UP_THRESHOLD = 3.0
DOWN_THRESHOLD = -3.0

BOOT_N = 5000
SEED = 96

CANDIDATES = {
    "UP": {
        "feature": "buyers60",
        "direction": "HIGHER",
        "target_name": "up_activation",
    },

    "DOWN": {
        "feature": "new_wallets10",
        "direction": "HIGHER",
        "target_name": "down_activation",
    },
}


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    n = len(xs)

    if n % 2:
        return xs[n//2]

    return (
        xs[n//2 - 1]
        + xs[n//2]
    ) / 2.0


def quantile(xs, q):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    pos = (len(xs)-1)*q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return xs[lo]

    w = pos-lo

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


def raw_auc(rows, feature, target):

    pos = [
        r[feature]
        for r in rows
        if (
            r[target] == 1
            and valid(r[feature])
        )
    ]

    neg = [
        r[feature]
        for r in rows
        if (
            r[target] == 0
            and valid(r[feature])
        )
    ]

    if not pos or not neg:
        return None

    wins = 0.0
    total = 0

    for a in pos:

        for b in neg:

            total += 1

            if a > b:
                wins += 1.0

            elif a == b:
                wins += 0.5

    return wins / total


def directional_auc(raw, direction):

    if raw is None:
        return None

    if direction == "HIGHER":
        return raw

    return 1.0 - raw


def median_diff(rows, feature, target):

    yes = [
        r[feature]
        for r in rows
        if (
            r[target] == 1
            and valid(r[feature])
        )
    ]

    no = [
        r[feature]
        for r in rows
        if (
            r[target] == 0
            and valid(r[feature])
        )
    ]

    if not yes or not no:
        return None

    return med(yes) - med(no)


def counts(rows, target):

    yes = sum(
        r[target] == 1
        for r in rows
    )

    no = sum(
        r[target] == 0
        for r in rows
    )

    return yes, no


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute(
    "PRAGMA busy_timeout=5000"
)


boundary_row = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()

if (
    boundary_row is None
    or boundary_row[0] is None
):

    raise RuntimeError(
        "Cannot determine T59 boundary."
    )

boundary = int(
    boundary_row[0]
)


rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    buyers60,
    new_wallets10,

    dex_return_30s,
    dex_done_30s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL
    AND dex_done_30s = 1
    AND dex_return_30s IS NOT NULL

ORDER BY
    timestamp,
    id
""").fetchall()


records = []


for r in rows:

    r30 = r[
        "dex_return_30s"
    ]

    records.append({

        "id":
            r["id"],

        "timestamp":
            r["timestamp"],

        "token_mint":
            r["token_mint"],

        "historical":
            r["id"] <= boundary,

        "buyers60":
            r["buyers60"],

        "new_wallets10":
            r["new_wallets10"],

        "up_activation":
            int(
                r30 >= UP_THRESHOLD
            ),

        "down_activation":
            int(
                r30 <= DOWN_THRESHOLD
            ),
    })


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

def first_token(rows):

    seen = set()
    out = []

    for r in rows:

        tok = r[
            "token_mint"
        ]

        if tok in seen:
            continue

        seen.add(tok)

        out.append(r)

    return out


# ============================================================
# RUN ONE FROZEN CANDIDATE
# ============================================================

def audit_candidate(label, spec):

    feature = spec[
        "feature"
    ]

    direction = spec[
        "direction"
    ]

    target = spec[
        "target_name"
    ]


    usable = [
        r
        for r in records
        if valid(
            r[feature]
        )
    ]


    hist = [
        r
        for r in usable
        if r["historical"]
    ]


    pros = [
        r
        for r in usable
        if not r["historical"]
    ]


    first = first_token(
        usable
    )


    hist_first = first_token(
        hist
    )


    pros_first = first_token(
        pros
    )


    # ========================================================
    # HEADER
    # ========================================================

    print()
    print("=" * 190)

    print(
        f"T96-{label} — FROZEN DIRECTIONAL ACTIVATION ROBUSTNESS"
    )

    print("=" * 190)

    print(
        f"FEATURE              : {feature}"
    )

    print(
        f"FROZEN DIRECTION     : {direction}"
    )

    if label == "UP":

        print(
            f"TARGET               : R30 >= +{UP_THRESHOLD:.1f}%"
        )

    else:

        print(
            f"TARGET               : R30 <= {DOWN_THRESHOLD:.1f}%"
        )

    print()

    print(
        f"USABLE EVENTS        : {len(usable)}"
    )

    print(
        f"UNIQUE TOKENS        : "
        f"{len(set(r['token_mint'] for r in usable))}"
    )


    # ========================================================
    # A) GLOBAL
    # ========================================================

    print()
    print("-" * 190)
    print("A) GLOBAL")
    print("-" * 190)

    raw = raw_auc(
        usable,
        feature,
        target
    )

    ga = directional_auc(
        raw,
        direction
    )

    yes, no = counts(
        usable,
        target
    )

    print(
        f"N={len(usable)} "
        f"| YES={yes} "
        f"| NO={no} "
        f"| DIFF={fmt(median_diff(usable,feature,target))} "
        f"| DIR_AUC={fmt(ga)}"
    )


    # ========================================================
    # B) HIST / PROS
    # ========================================================

    print()
    print("-" * 190)
    print("B) HISTORICAL / PROSPECTIVE")
    print("-" * 190)

    regime_auc = {}

    for name, rr in [
        ("HISTORICAL", hist),
        ("PROSPECTIVE", pros),
    ]:

        raw = raw_auc(
            rr,
            feature,
            target
        )

        da = directional_auc(
            raw,
            direction
        )

        regime_auc[
            name
        ] = da

        yes, no = counts(
            rr,
            target
        )

        print(
            f"{name:12} "
            f"| N={len(rr):4d} "
            f"| YES={yes:4d} "
            f"| NO={no:4d} "
            f"| DIFF={fmt(median_diff(rr,feature,target)):>8} "
            f"| DIR_AUC={fmt(da)}"
        )


    # ========================================================
    # C) CHRONO BLOCKS
    # ========================================================

    print()
    print("-" * 190)
    print("C) CHRONOLOGICAL BLOCKS")
    print("-" * 190)

    N = len(
        usable
    )

    blocks = [

        (
            "T1",
            usable[:N//3]
        ),

        (
            "T2",
            usable[N//3:(2*N)//3]
        ),

        (
            "T3",
            usable[(2*N)//3:]
        ),

        (
            "Q1",
            usable[:N//4]
        ),

        (
            "Q2",
            usable[N//4:N//2]
        ),

        (
            "Q3",
            usable[N//2:(3*N)//4]
        ),

        (
            "Q4",
            usable[(3*N)//4:]
        ),
    ]


    audit_values = []


    for name, rr in blocks:

        yes, no = counts(
            rr,
            target
        )

        raw = raw_auc(
            rr,
            feature,
            target
        )

        da = directional_auc(
            raw,
            direction
        )

        print(
            f"{name:4} "
            f"| N={len(rr):4d} "
            f"| YES={yes:3d} "
            f"| NO={no:3d} "
            f"| DIFF={fmt(median_diff(rr,feature,target)):>8} "
            f"| DIR_AUC={fmt(da)}"
        )

        if (
            yes >= 3
            and no >= 3
            and da is not None
        ):

            audit_values.append(
                (
                    name,
                    da
                )
            )


    # ========================================================
    # D) FIRST EVENT/TOKEN
    # ========================================================

    print()
    print("-" * 190)
    print("D) FIRST-EVENT/TOKEN")
    print("-" * 190)

    first_auc = directional_auc(
        raw_auc(
            first,
            feature,
            target
        ),
        direction
    )

    yes, no = counts(
        first,
        target
    )

    print(
        f"TOKENS={len(first)} "
        f"| YES={yes} "
        f"| NO={no} "
        f"| DIFF={fmt(median_diff(first,feature,target))} "
        f"| DIR_AUC={fmt(first_auc)}"
    )


    # ========================================================
    # E) FIRST TOKEN BY REGIME
    # ========================================================

    print()
    print("-" * 190)
    print("E) FIRST-EVENT/TOKEN BY REGIME")
    print("-" * 190)

    first_regime = {}


    for name, rr in [

        (
            "HIST",
            hist_first
        ),

        (
            "PROS",
            pros_first
        ),
    ]:

        yes, no = counts(
            rr,
            target
        )

        da = directional_auc(
            raw_auc(
                rr,
                feature,
                target
            ),
            direction
        )

        first_regime[
            name
        ] = da

        print(
            f"{name:5} "
            f"| TOK={len(rr):3d} "
            f"| YES={yes:3d} "
            f"| NO={no:3d} "
            f"| DIR_AUC={fmt(da)}"
        )


    # ========================================================
    # F) TOKEN BOOTSTRAP
    # ========================================================

    print()
    print("-" * 190)
    print("F) TOKEN-LEVEL BOOTSTRAP")
    print("-" * 190)

    by_token = defaultdict(
        list
    )


    for r in usable:

        by_token[
            r["token_mint"]
        ].append(r)


    tokens = list(
        by_token.keys()
    )


    rng = random.Random(
        SEED
        + (
            1
            if label == "UP"
            else 2
        )
    )


    boots = []


    for _ in range(
        BOOT_N
    ):

        sampled = [
            rng.choice(
                tokens
            )
            for _ in range(
                len(tokens)
            )
        ]

        rr = []

        for tok in sampled:

            rr.extend(
                by_token[tok]
            )

        da = directional_auc(
            raw_auc(
                rr,
                feature,
                target
            ),
            direction
        )

        if da is not None:

            boots.append(
                da
            )


    print(
        f"BOOT N={len(boots)}"
    )

    print(
        f"MED AUC={fmt(med(boots))}"
    )

    print(
        f"95% CI=["
        f"{fmt(quantile(boots,0.025))}, "
        f"{fmt(quantile(boots,0.975))}]"
    )

    for threshold in [
        0.50,
        0.55,
        0.60,
    ]:

        p = (
            sum(
                x > threshold
                for x in boots
            )
            / len(boots)
        )

        print(
            f"P(AUC>{threshold:.2f})="
            f"{100*p:.1f}%"
        )


    # ========================================================
    # G) LEAVE ONE TOKEN OUT
    # ========================================================

    print()
    print("-" * 190)
    print("G) LEAVE-ONE-TOKEN-OUT")
    print("-" * 190)

    loo = []


    for tok in tokens:

        rr = [
            r
            for r in usable
            if r["token_mint"] != tok
        ]

        da = directional_auc(
            raw_auc(
                rr,
                feature,
                target
            ),
            direction
        )

        if da is not None:

            loo.append(
                (
                    da,
                    tok,
                    len(
                        by_token[tok]
                    )
                )
            )


    loo.sort()

    vals = [
        x[0]
        for x in loo
    ]


    print(
        f"TOKENS={len(vals)} "
        f"| MED={fmt(med(vals))} "
        f"| WORST={fmt(min(vals))} "
        f"| BEST={fmt(max(vals))}"
    )


    # ========================================================
    # H) EVENT NUMBER
    # ========================================================

    print()
    print("-" * 190)
    print("H) EVENT NUMBER SENSITIVITY")
    print("-" * 190)

    token_seq = defaultdict(
        int
    )


    enriched = []


    for r in usable:

        tok = r[
            "token_mint"
        ]

        token_seq[
            tok
        ] += 1

        x = dict(r)

        x[
            "event_number"
        ] = token_seq[
            tok
        ]

        enriched.append(
            x
        )


    groups = [

        (
            "#1",
            lambda x:
                x["event_number"] == 1
        ),

        (
            "#2",
            lambda x:
                x["event_number"] == 2
        ),

        (
            "#3",
            lambda x:
                x["event_number"] == 3
        ),

        (
            "#4+",
            lambda x:
                x["event_number"] >= 4
        ),
    ]


    for group_name, fn in groups:

        rr = [
            r
            for r in enriched
            if fn(r)
        ]

        yes, no = counts(
            rr,
            target
        )

        da = directional_auc(
            raw_auc(
                rr,
                feature,
                target
            ),
            direction
        )

        print(
            f"{group_name:4} "
            f"| N={len(rr):4d} "
            f"| YES={yes:3d} "
            f"| NO={no:3d} "
            f"| DIR_AUC={fmt(da)}"
        )


    # ========================================================
    # I) ROBUSTNESS SCORECARD
    # ========================================================

    print()
    print("-" * 190)
    print("I) ROBUSTNESS SCORECARD")
    print("-" * 190)


    for name, rr in [

        (
            "HIST",
            hist
        ),

        (
            "PROS",
            pros
        ),

        (
            "FIRST",
            first
        ),
    ]:

        yes, no = counts(
            rr,
            target
        )

        da = directional_auc(
            raw_auc(
                rr,
                feature,
                target
            ),
            direction
        )

        if (
            yes >= 3
            and no >= 3
            and da is not None
        ):

            audit_values.append(
                (
                    name,
                    da
                )
            )


    auc55 = sum(
        a >= 0.55
        for _, a in audit_values
    )


    auc60 = sum(
        a >= 0.60
        for _, a in audit_values
    )


    print(
        f"USABLE AUDITS      = {len(audit_values)}"
    )

    print(
        f"AUC >=0.55         = "
        f"{auc55}/{len(audit_values)}"
    )

    print(
        f"AUC >=0.60         = "
        f"{auc60}/{len(audit_values)}"
    )

    print(
        f"MED AUDIT AUC      = "
        f"{fmt(med([a for _,a in audit_values]))}"
    )

    print(
        f"WORST AUDIT AUC    = "
        f"{fmt(min(a for _,a in audit_values))}"
    )

    print(
        f"BEST AUDIT AUC     = "
        f"{fmt(max(a for _,a in audit_values))}"
    )


    # ========================================================
    # J) DECISION
    # ========================================================

    print()
    print("-" * 190)
    print("J) DECISION SUPPORT")
    print("-" * 190)


    hist_auc = regime_auc[
        "HISTORICAL"
    ]

    pros_auc = regime_auc[
        "PROSPECTIVE"
    ]


    hist_first_auc = first_regime[
        "HIST"
    ]

    pros_first_auc = first_regime[
        "PROS"
    ]


    boot50 = (
        sum(
            x > 0.50
            for x in boots
        )
        / len(boots)
    )


    print(
        f"GLOBAL DIR-AUC      = {fmt(ga)}"
    )

    print(
        f"HIST DIR-AUC        = {fmt(hist_auc)}"
    )

    print(
        f"PROS DIR-AUC        = {fmt(pros_auc)}"
    )

    print(
        f"FIRST ALL AUC       = {fmt(first_auc)}"
    )

    print(
        f"FIRST HIST AUC      = {fmt(hist_first_auc)}"
    )

    print(
        f"FIRST PROS AUC      = {fmt(pros_first_auc)}"
    )

    print(
        f"BOOT P(AUC>0.50)    = "
        f"{100*boot50:.1f}%"
    )

    print()


    robust = (

        ga is not None
        and ga >= 0.58

        and hist_auc is not None
        and hist_auc >= 0.58

        and pros_auc is not None
        and pros_auc >= 0.58

        and first_auc is not None
        and first_auc >= 0.55

        and hist_first_auc is not None
        and hist_first_auc >= 0.55

        and pros_first_auc is not None
        and pros_first_auc >= 0.55

        and len(
            audit_values
        ) >= 7

        and auc55
        / len(audit_values)
        >= 0.70

        and boot50
        >= 0.95
    )


    if robust:

        print(
            f"🟢 T96-{label} FROZEN {feature.upper()} "
            f"SURVIVES DIRECTIONAL ACTIVATION ROBUSTNESS."
        )

        print(
            "Candidate next = staged/incremental directional audit."
        )

        print(
            "Do NOT optimize an operational feature threshold."
        )

    else:

        print(
            f"🔴 T96-{label} {feature.upper()} DOES NOT "
            f"SURVIVE DIRECTIONAL ACTIVATION ROBUSTNESS."
        )

        print(
            "Do not build a directional gate from it."
        )


    return {
        "label":
            label,

        "feature":
            feature,

        "global_auc":
            ga,

        "hist_auc":
            hist_auc,

        "pros_auc":
            pros_auc,

        "first_auc":
            first_auc,

        "hist_first_auc":
            hist_first_auc,

        "pros_first_auc":
            pros_first_auc,

        "boot50":
            boot50,

        "robust":
            robust,
    }


# ============================================================
# MAIN
# ============================================================

print("=" * 190)
print(
    "MEMECOIN LAB — T96 DUAL DIRECTIONAL ACTIVATION ROBUSTNESS AUDIT"
)
print("=" * 190)

print("MODE                 : READ-ONLY")
print("MODEL FITTING        : NONE")
print("THRESHOLD SEARCH     : NONE")
print("DB WRITES            : NONE")
print("T59/T78/T82/T86      : UNTOUCHED")
print()
print(f"T59 BOUNDARY         : {boundary}")
print()
print(
    "T96-UP   : buyers60 HIGHER → R30 >= +3%"
)

print(
    "T96-DOWN : new_wallets10 HIGHER → R30 <= -3%"
)


summaries = []


for label in [
    "UP",
    "DOWN",
]:

    summaries.append(
        audit_candidate(
            label,
            CANDIDATES[label]
        )
    )


# ============================================================
# MASTER DECISION
# ============================================================

print()
print("=" * 190)
print(
    "K) T96 MASTER DECISION"
)
print("=" * 190)


up = summaries[0]
down = summaries[1]


if (
    up["robust"]
    and down["robust"]
):

    print(
        "🟢 BOTH DIRECTIONAL ACTIVATION CANDIDATES SURVIVE."
    )

    print(
        "Next = directional staged architecture audit."
    )


elif up["robust"]:

    print(
        "🟡 ONLY UP ACTIVATION SURVIVES."
    )

    print(
        "Keep buyers60 HIGHER as the sole directional candidate."
    )

    print(
        "DOWN activation remains unresolved."
    )


elif down["robust"]:

    print(
        "🟡 ONLY DOWN ACTIVATION SURVIVES."
    )

    print(
        "Keep new_wallets10 HIGHER as the sole directional candidate."
    )

    print(
        "UP activation remains unresolved."
    )


else:

    print(
        "🔴 NEITHER DIRECTIONAL ACTIVATION CANDIDATE "
        "SURVIVES ROBUSTNESS."
    )

    print(
        "Directional decomposition alone is insufficient."
    )


print()
print("IMPORTANT:")
print("• Candidate features selected in T95.")
print("• Directions frozen HIGHER before T96.")
print("• UP target remains R30 >= +3%.")
print("• DOWN target remains R30 <= -3%.")
print("• No feature threshold optimization.")
print("• No model fitting.")
print("• No interaction search.")
print("• Bootstrap resamples entire tokens.")
print("• First-token tests are mandatory.")
print("• T96 writes nothing to DB.")
print("• Frozen prospective branches remain untouched.")

db.close()
