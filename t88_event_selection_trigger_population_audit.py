#!/usr/bin/env python3

import sqlite3
import math
from collections import Counter, defaultdict

DB = "validation_v090.db"
T59 = "t59_capv2_prospective"


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def pct(n, d):
    if not d:
        return "NA"
    return f"{100*n/d:.1f}%"


def rate_tail(rows, threshold, horizon):
    col = (
        "dex_return_60s"
        if horizon == 60
        else "dex_return_300s"
    )

    xs = [
        r[col]
        for r in rows
        if valid(r[col])
    ]

    if not xs:
        return None, 0

    n = sum(
        abs(x) >= threshold
        for x in xs
    )

    return n / len(xs), len(xs)


db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row

boundary = db.execute(f"""
SELECT MIN(boundary_id)
FROM {T59}
""").fetchone()[0]

boundary = int(boundary)


rows = db.execute("""
SELECT
    id,
    timestamp,
    token_mint,

    fa,
    nf30,
    imbalance30,
    price_change30,

    fa90,
    fa95,
    fpa,
    extreme,
    flow_regime,

    swaps5,
    swaps10,
    swaps30,
    swaps60,

    buyers5,
    buyers10,
    buyers30,
    buyers60,

    new_wallets10,
    new_wallets30,

    buyer_growth,
    wallet_growth,

    dex_return_60s,
    dex_return_300s

FROM events

WHERE
    timestamp IS NOT NULL
    AND token_mint IS NOT NULL

ORDER BY timestamp, id
""").fetchall()


hist = [
    r for r in rows
    if r["id"] <= boundary
]

pros = [
    r for r in rows
    if r["id"] > boundary
]


def trigger_key(r):
    return (
        int(r["fa90"] or 0),
        int(r["fa95"] or 0),
        int(r["fpa"] or 0),
        int(r["extreme"] or 0),
        str(r["flow_regime"] or "NULL")
    )


def simple_flags(r):
    return {
        "fa90": int(r["fa90"] or 0),
        "fa95": int(r["fa95"] or 0),
        "fpa": int(r["fpa"] or 0),
        "extreme": int(r["extreme"] or 0),
    }


print("=" * 165)
print(
    "MEMECOIN LAB — T88 EVENT SELECTION / TRIGGER POPULATION AUDIT"
)
print("=" * 165)

print("MODE              : READ-ONLY")
print("MODEL FITTING     : NONE")
print("THRESHOLD SEARCH  : NONE")
print("DB WRITES         : NONE")
print("T59/T78/T82/T86   : UNTOUCHED")
print()
print(f"T59 BOUNDARY      : {boundary}")


# ============================================================
# A) SAMPLE
# ============================================================

print()
print("=" * 165)
print("A) COHORT SIZES")
print("=" * 165)

print(
    f"HISTORICAL   | EVENTS={len(hist):4d} "
    f"| TOKENS={len(set(r['token_mint'] for r in hist)):4d}"
)

print(
    f"PROSPECTIVE  | EVENTS={len(pros):4d} "
    f"| TOKENS={len(set(r['token_mint'] for r in pros)):4d}"
)


# ============================================================
# B) FLAG FREQUENCIES
# ============================================================

print()
print("=" * 165)
print("B) TRIGGER FLAG FREQUENCIES")
print("=" * 165)

for flag in [
    "fa90",
    "fa95",
    "fpa",
    "extreme"
]:

    h = sum(
        int(r[flag] or 0) == 1
        for r in hist
    )

    p = sum(
        int(r[flag] or 0) == 1
        for r in pros
    )

    print(
        f"{flag:10} "
        f"| HIST={h:4d}/{len(hist):4d} "
        f"({pct(h,len(hist)):>6}) "
        f"| PROS={p:4d}/{len(pros):4d} "
        f"({pct(p,len(pros)):>6})"
    )


# ============================================================
# C) FLOW REGIME
# ============================================================

print()
print("=" * 165)
print("C) FLOW REGIME COMPOSITION")
print("=" * 165)

hist_flow = Counter(
    str(r["flow_regime"] or "NULL")
    for r in hist
)

pros_flow = Counter(
    str(r["flow_regime"] or "NULL")
    for r in pros
)

all_flows = sorted(
    set(hist_flow) | set(pros_flow)
)

for flow in all_flows:

    h = hist_flow[flow]
    p = pros_flow[flow]

    print(
        f"{flow:25} "
        f"| HIST={h:4d} ({pct(h,len(hist)):>6}) "
        f"| PROS={p:4d} ({pct(p,len(pros)):>6})"
    )


# ============================================================
# D) TRIGGER COMBINATIONS
# ============================================================

print()
print("=" * 165)
print("D) TRIGGER COMBINATIONS")
print("=" * 165)

hc = Counter(
    trigger_key(r)
    for r in hist
)

pc = Counter(
    trigger_key(r)
    for r in pros
)

keys = sorted(
    set(hc) | set(pc),
    key=lambda k: (
        -(hc[k] + pc[k]),
        k
    )
)

for k in keys[:40]:

    h = hc[k]
    p = pc[k]

    print(
        f"FA90={k[0]} "
        f"FA95={k[1]} "
        f"FPA={k[2]} "
        f"EXT={k[3]} "
        f"FLOW={k[4]:18} "
        f"| HIST={h:4d} ({pct(h,len(hist)):>6}) "
        f"| PROS={p:4d} ({pct(p,len(pros)):>6})"
    )


# ============================================================
# E) OUTCOME RATE BY FLAG
# ============================================================

print()
print("=" * 165)
print("E) LARGE-MOVE RATE BY INDIVIDUAL FLAG")
print("=" * 165)

for flag in [
    "fa90",
    "fa95",
    "fpa",
    "extreme"
]:

    print()
    print(flag)

    for value in [0, 1]:

        hr = [
            r for r in hist
            if int(r[flag] or 0) == value
        ]

        pr = [
            r for r in pros
            if int(r[flag] or 0) == value
        ]

        h60, hn60 = rate_tail(
            hr,
            10.0,
            60
        )

        p60, pn60 = rate_tail(
            pr,
            10.0,
            60
        )

        h300, hn300 = rate_tail(
            hr,
            10.0,
            300
        )

        p300, pn300 = rate_tail(
            pr,
            10.0,
            300
        )

        print(
            f"  VALUE={value} "
            f"| HIST60={h60 if h60 is not None else 'NA'} "
            f"N={hn60:4d} "
            f"| PROS60={p60 if p60 is not None else 'NA'} "
            f"N={pn60:4d} "
            f"| HIST300={h300 if h300 is not None else 'NA'} "
            f"N={hn300:4d} "
            f"| PROS300={p300 if p300 is not None else 'NA'} "
            f"N={pn300:4d}"
        )


# ============================================================
# F) OUTCOME RATE BY FLOW REGIME
# ============================================================

print()
print("=" * 165)
print("F) LARGE-MOVE RATE BY FLOW REGIME")
print("=" * 165)

for flow in all_flows:

    hr = [
        r for r in hist
        if str(r["flow_regime"] or "NULL") == flow
    ]

    pr = [
        r for r in pros
        if str(r["flow_regime"] or "NULL") == flow
    ]

    h60, hn60 = rate_tail(
        hr,
        10.0,
        60
    )

    p60, pn60 = rate_tail(
        pr,
        10.0,
        60
    )

    h300, hn300 = rate_tail(
        hr,
        10.0,
        300
    )

    p300, pn300 = rate_tail(
        pr,
        10.0,
        300
    )

    print(
        f"{flow:25} "
        f"| H60={h60 if h60 is not None else 'NA'} "
        f"N={hn60:4d} "
        f"| P60={p60 if p60 is not None else 'NA'} "
        f"N={pn60:4d} "
        f"| H300={h300 if h300 is not None else 'NA'} "
        f"N={hn300:4d} "
        f"| P300={p300 if p300 is not None else 'NA'} "
        f"N={pn300:4d}"
    )


# ============================================================
# G) TOP COMBINATION OUTCOME RATES
# ============================================================

print()
print("=" * 165)
print("G) TOP TRIGGER-COMBINATION OUTCOME RATES")
print("=" * 165)

for k in keys[:25]:

    hr = [
        r for r in hist
        if trigger_key(r) == k
    ]

    pr = [
        r for r in pros
        if trigger_key(r) == k
    ]

    h300, hn = rate_tail(
        hr,
        10.0,
        300
    )

    p300, pn = rate_tail(
        pr,
        10.0,
        300
    )

    print(
        f"FA90={k[0]} "
        f"FA95={k[1]} "
        f"FPA={k[2]} "
        f"EXT={k[3]} "
        f"FLOW={k[4]:16} "
        f"| HIST_N={hn:4d} "
        f"H300={h300 if h300 is not None else 'NA'} "
        f"| PROS_N={pn:4d} "
        f"P300={p300 if p300 is not None else 'NA'}"
    )


# ============================================================
# H) ACTIVITY COMPOSITION
# ============================================================

print()
print("=" * 165)
print("H) ACTIVITY / TRIGGER INTENSITY SNAPSHOT")
print("=" * 165)

features = [
    "fa",
    "nf30",
    "imbalance30",
    "price_change30",
    "swaps5",
    "swaps10",
    "swaps30",
    "buyers5",
    "buyers10",
    "buyers30",
    "new_wallets10",
    "new_wallets30",
]


def median(vals):
    vals = sorted(
        x for x in vals
        if valid(x)
    )

    if not vals:
        return None

    n = len(vals)

    if n % 2:
        return vals[n//2]

    return (
        vals[n//2-1] + vals[n//2]
    ) / 2


for f in features:

    hv = [
        r[f]
        for r in hist
        if valid(r[f])
    ]

    pv = [
        r[f]
        for r in pros
        if valid(r[f])
    ]

    print(
        f"{f:18} "
        f"| HIST_MED={median(hv) if hv else 'NA'} "
        f"| PROS_MED={median(pv) if pv else 'NA'}"
    )


# ============================================================
# I) FIRST EVENT / TOKEN COMPOSITION
# ============================================================

print()
print("=" * 165)
print("I) FIRST-EVENT/TOKEN TRIGGER COMPOSITION")
print("=" * 165)


def first_token(rr):

    seen = set()
    out = []

    for r in rr:

        tok = r["token_mint"]

        if tok in seen:
            continue

        seen.add(tok)
        out.append(r)

    return out


hf = first_token(hist)
pf = first_token(pros)

for flag in [
    "fa90",
    "fa95",
    "fpa",
    "extreme"
]:

    h = sum(
        int(r[flag] or 0) == 1
        for r in hf
    )

    p = sum(
        int(r[flag] or 0) == 1
        for r in pf
    )

    print(
        f"{flag:10} "
        f"| HIST={h:3d}/{len(hf):3d} "
        f"({pct(h,len(hf)):>6}) "
        f"| PROS={p:3d}/{len(pf):3d} "
        f"({pct(p,len(pf)):>6})"
    )


# ============================================================
# J) DECISION SUPPORT
# ============================================================

print()
print("=" * 165)
print("J) DECISION SUPPORT")
print("=" * 165)


flag_shifts = 0

for flag in [
    "fa90",
    "fa95",
    "fpa",
    "extreme"
]:

    h = (
        sum(
            int(r[flag] or 0) == 1
            for r in hist
        )
        / len(hist)
        if hist
        else 0
    )

    p = (
        sum(
            int(r[flag] or 0) == 1
            for r in pros
        )
        / len(pros)
        if pros
        else 0
    )

    if abs(p-h) >= 0.10:
        flag_shifts += 1


flow_tv = 0.0

for flow in all_flows:

    hp = (
        hist_flow[flow] / len(hist)
        if hist
        else 0
    )

    pp = (
        pros_flow[flow] / len(pros)
        if pros
        else 0
    )

    flow_tv += abs(hp-pp)

flow_tv *= 0.5


print(
    f"FLAG SHIFTS >=10pp : {flag_shifts}"
)

print(
    f"FLOW REGIME TVD    : {flow_tv:.3f}"
)

print()


if (
    flag_shifts >= 2
    or flow_tv >= 0.20
):

    print(
        "🟠 EVENT POPULATION COMPOSITION HAS SHIFTED."
    )

    print(
        "Prospective events are not being selected from the same trigger mix."
    )

elif (
    flag_shifts >= 1
    or flow_tv >= 0.10
):

    print(
        "🟡 PARTIAL EVENT-SELECTION SHIFT DETECTED."
    )

    print(
        "Trigger composition may explain part of the return-regime change."
    )

else:

    print(
        "🟢 EVENT-SELECTION MIX IS BROADLY STABLE."
    )

    print(
        "Neutral dominance is unlikely to be explained by trigger composition alone."
    )


print()
print("IMPORTANT:")
print("• No model fitting.")
print("• No threshold optimization.")
print("• ±10% rates are descriptive diagnostics.")
print("• T88 writes nothing to DB.")
print("• T59/T78/T82/T86 remain frozen and untouched.")

db.close()
