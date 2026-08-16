import sqlite3
import statistics
import math
import time
import os

DB = "validation_v090.db"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def safe(row, key):
    try:
        return row[key]
    except:
        return None


def valid(x):
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)


def percentile(vals, p):
    vals = sorted(x for x in vals if valid(x))

    if not vals:
        return None

    k = (len(vals) - 1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return vals[lo] * (hi-k) + vals[hi] * (k-lo)


def median(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def fmt(x, pct=False):
    if x is None:
        return "NA"

    if pct:
        return f"{x:+.2f}%"

    return f"{x:+.4f}"


# ------------------------------------------------------------
# DB schema
# ------------------------------------------------------------

def columns(db, table):
    return {
        r[1]
        for r in db.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()
    }


# ------------------------------------------------------------
# Load events
# ------------------------------------------------------------

def load():

    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row

    ec = columns(db, "events")
    dc = columns(db, "dex_prices")

    wanted_event = [
        "id",
        "token_mint",
        "timestamp",

        "fa",
        "fa95",
        "flow_accel",
        "buyer_accel",
        "net_flow",
        "imbalance",

        "new_wallets10",
        "new_wallets30",
        "concentration",

        "dex_return_5s",
        "dex_return_10s",
        "dex_return_20s",
        "dex_return_30s",
        "dex_return_60s",
        "dex_return_300s",
    ]

    event_fields = [
        x for x in wanted_event
        if x in ec
    ]

    dex_fields = [
        x for x in [
            "volume_m5",
            "liquidity_usd",
            "market_cap",
            "fdv",
            "buys_m5",
            "sells_m5",
        ]
        if x in dc
    ]

    event_select = ",\n".join(
        f"e.{x}" for x in event_fields
    )

    dex_select = ""

    if dex_fields:
        dex_select = ",\n" + ",\n".join(
            f"d.{x}" for x in dex_fields
        )

    q = f"""
    WITH first_dex AS (

        SELECT d.*

        FROM dex_prices d

        JOIN (
            SELECT
                event_id,
                MIN(timestamp) AS first_time

            FROM dex_prices

            GROUP BY event_id
        ) x

        ON d.event_id = x.event_id
        AND d.timestamp = x.first_time
    )

    SELECT
        {event_select}
        {dex_select}

    FROM events e

    LEFT JOIN first_dex d
    ON d.event_id = e.id

    WHERE e.dex_return_60s IS NOT NULL

    ORDER BY e.id
    """

    rows = db.execute(q).fetchall()
    db.close()

    return rows


# ------------------------------------------------------------
# Feature engineering
# ------------------------------------------------------------

def feature(row, name):

    if name == "FA":
        return safe(row, "fa")

    if name == "FLOW_ACCEL":
        return safe(row, "flow_accel")

    if name == "BUYER_ACCEL":
        return safe(row, "buyer_accel")

    if name == "NET_FLOW":
        return safe(row, "net_flow")

    if name == "IMBALANCE":
        return safe(row, "imbalance")

    if name == "NEW10":
        return safe(row, "new_wallets10")

    if name == "NEW30":
        return safe(row, "new_wallets30")

    if name == "CONCENTRATION":
        return safe(row, "concentration")

    if name == "VOLUME_M5":
        return safe(row, "volume_m5")

    if name == "LIQUIDITY":
        return safe(row, "liquidity_usd")

    if name == "MARKET_CAP":
        return safe(row, "market_cap")

    if name == "BUY_RATIO":

        b = safe(row, "buys_m5")
        s = safe(row, "sells_m5")

        if not valid(b) or not valid(s):
            return None

        if b+s <= 0:
            return None

        return b/(b+s)

    if name == "VOL_LIQ_RATIO":

        v = safe(row, "volume_m5")
        l = safe(row, "liquidity_usd")

        if not valid(v) or not valid(l) or l <= 0:
            return None

        return v/l

    if name == "NEW_WALLET_SPEED":

        n10 = safe(row, "new_wallets10")
        n30 = safe(row, "new_wallets30")

        if not valid(n10) or not valid(n30):
            return None

        # Compare last 10s rate with previous ~20s rate.
        recent_rate = n10 / 10.0
        previous = max(n30 - n10, 0)
        previous_rate = previous / 20.0

        return recent_rate - previous_rate

    return None


FEATURES = [
    "FA",
    "FLOW_ACCEL",
    "BUYER_ACCEL",
    "NET_FLOW",
    "IMBALANCE",
    "NEW10",
    "NEW30",
    "NEW_WALLET_SPEED",
    "CONCENTRATION",
    "VOLUME_M5",
    "LIQUIDITY",
    "MARKET_CAP",
    "BUY_RATIO",
    "VOL_LIQ_RATIO",
]


# ------------------------------------------------------------
# Outcome classes
# ------------------------------------------------------------

def classify(row):

    r30 = safe(row, "dex_return_30s")
    r60 = safe(row, "dex_return_60s")

    if not valid(r60):
        return None

    if r60 >= 10:
        return "RUNNER_10"

    if r60 >= 5:
        return "WINNER_5"

    if r60 <= -10:
        return "DUMP_10"

    if r60 <= -5:
        return "LOSER_5"

    if valid(r30) and r30 > 0 and r60 <= 0:
        return "FADE"

    return "FLAT"


# ------------------------------------------------------------
# Compare winners vs losers
# ------------------------------------------------------------

def compare_features(rows):

    winners = [
        r for r in rows
        if safe(r, "dex_return_60s") is not None
        and safe(r, "dex_return_60s") >= 5
    ]

    losers = [
        r for r in rows
        if safe(r, "dex_return_60s") is not None
        and safe(r, "dex_return_60s") <= -5
    ]

    results = []

    for f in FEATURES:

        w = [
            feature(r, f)
            for r in winners
            if valid(feature(r, f))
        ]

        l = [
            feature(r, f)
            for r in losers
            if valid(feature(r, f))
        ]

        if len(w) < 3 or len(l) < 3:
            continue

        wm = median(w)
        lm = median(l)

        allv = w + l

        p25 = percentile(allv, .25)
        p75 = percentile(allv, .75)

        spread = p75 - p25 if (
            p25 is not None and p75 is not None
        ) else 0

        if spread == 0:
            score = 0
        else:
            score = abs(wm-lm)/abs(spread)

        results.append(
            (
                score,
                f,
                len(w),
                wm,
                len(l),
                lm,
                wm-lm
            )
        )

    return sorted(
        results,
        reverse=True
    )


# ------------------------------------------------------------
# Quartile analysis
# ------------------------------------------------------------

def quartile_test(rows, fname):

    pairs = []

    for r in rows:

        x = feature(r, fname)
        y = safe(r, "dex_return_60s")

        if valid(x) and valid(y):
            pairs.append((x,y))

    if len(pairs) < 20:
        return None

    xs = [x for x,_ in pairs]

    p25 = percentile(xs,.25)
    p50 = percentile(xs,.50)
    p75 = percentile(xs,.75)

    buckets = [
        ("Q1", lambda x: x <= p25),
        ("Q2", lambda x: p25 < x <= p50),
        ("Q3", lambda x: p50 < x <= p75),
        ("Q4", lambda x: x > p75),
    ]

    out = []

    for name, fn in buckets:

        ys = [
            y for x,y in pairs
            if fn(x)
        ]

        if not ys:
            continue

        out.append({
            "name": name,
            "n": len(ys),
            "med": median(ys),
            "avg": avg(ys),
            "win": 100*sum(y>0 for y in ys)/len(ys),
            "runner": 100*sum(y>=10 for y in ys)/len(ys),
            "dump": 100*sum(y<=-10 for y in ys)/len(ys),
        })

    return out


# ------------------------------------------------------------
# Multi-dimensional acceleration candidate
# ------------------------------------------------------------

def multi_score(row, thresholds):

    score = 0

    fa = feature(row, "FA")
    nw = feature(row, "NEW_WALLET_SPEED")
    vol = feature(row, "VOLUME_M5")
    ratio = feature(row, "VOL_LIQ_RATIO")
    br = feature(row, "BUY_RATIO")

    if valid(fa) and fa >= thresholds["fa"]:
        score += 1

    if valid(nw) and nw > 0:
        score += 1

    if valid(vol) and vol >= thresholds["vol"]:
        score += 1

    if valid(ratio) and ratio >= thresholds["vlr"]:
        score += 1

    if valid(br) and br >= .55:
        score += 1

    return score


def multi_analysis(rows):

    fa = [
        feature(r,"FA")
        for r in rows
        if valid(feature(r,"FA"))
    ]

    vol = [
        feature(r,"VOLUME_M5")
        for r in rows
        if valid(feature(r,"VOLUME_M5"))
    ]

    vlr = [
        feature(r,"VOL_LIQ_RATIO")
        for r in rows
        if valid(feature(r,"VOL_LIQ_RATIO"))
    ]

    if not fa or not vol or not vlr:
        return []

    thresholds = {
        "fa": percentile(fa,.75),
        "vol": percentile(vol,.50),
        "vlr": percentile(vlr,.50),
    }

    output = []

    for minimum in [2,3,4,5]:

        subset = [
            r for r in rows
            if multi_score(r, thresholds) >= minimum
        ]

        outcomes = [
            safe(r,"dex_return_60s")
            for r in subset
            if valid(safe(r,"dex_return_60s"))
        ]

        if len(outcomes) < 5:
            continue

        output.append({
            "score": minimum,
            "n": len(outcomes),
            "tokens": len(set(
                safe(r,"token_mint")
                for r in subset
                if safe(r,"token_mint")
            )),
            "med": median(outcomes),
            "avg": avg(outcomes),
            "win": 100*sum(x>0 for x in outcomes)/len(outcomes),
            "runner": 100*sum(x>=10 for x in outcomes)/len(outcomes),
            "dump": 100*sum(x<=-10 for x in outcomes)/len(outcomes),
        })

    return output


# ------------------------------------------------------------
# Display
# ------------------------------------------------------------

while True:

    try:

        rows = load()

        os.system("clear")

        print("="*120)
        print("MEMECOIN LAB — V3 FEATURE LAB")
        print("="*120)

        print(
            f"USABLE EVENTS : {len(rows)} | "
            f"TOKENS : {len(set(safe(r,'token_mint') for r in rows if safe(r,'token_mint')))}"
        )

        classes = {}

        for r in rows:
            c = classify(r)
            if c:
                classes[c] = classes.get(c,0)+1

        print()
        print("OUTCOME DISTRIBUTION")
        print("-"*120)

        for k,v in sorted(
            classes.items(),
            key=lambda x:x[1],
            reverse=True
        ):
            print(
                f"{k:12} : {v:>4} "
                f"({100*v/len(rows):5.1f}%)"
            )

        print()
        print("="*120)
        print("FEATURE SEPARATION — 60s WINNERS >= +5% vs LOSERS <= -5%")
        print("="*120)

        print(
            f"{'FEATURE':22} "
            f"{'NW':>5} {'WIN MED':>14} | "
            f"{'NL':>5} {'LOSE MED':>14} | "
            f"{'DIFF':>14} {'SEP':>8}"
        )

        print("-"*120)

        comparisons = compare_features(rows)

        for score,f,nw,wm,nl,lm,diff in comparisons:

            print(
                f"{f:22} "
                f"{nw:5} {wm:14.4f} | "
                f"{nl:5} {lm:14.4f} | "
                f"{diff:+14.4f} {score:8.3f}"
            )

        print()
        print("="*120)
        print("TOP FEATURE QUARTILES")
        print("="*120)

        for _,f,*_ in comparisons[:6]:

            q = quartile_test(rows,f)

            if not q:
                continue

            print()
            print(f)
            print("-"*90)

            for x in q:

                print(
                    f"{x['name']} | "
                    f"N={x['n']:>3} | "
                    f"MED60={x['med']:+7.2f}% | "
                    f"AVG60={x['avg']:+7.2f}% | "
                    f"WIN={x['win']:5.1f}% | "
                    f"RUNNER10={x['runner']:5.1f}% | "
                    f"DUMP10={x['dump']:5.1f}%"
                )

        print()
        print("="*120)
        print("V3 MULTI-DIMENSIONAL ACCELERATION — DISCOVERY ONLY")
        print("="*120)

        print(
            "Components: FA strength + accelerating new wallets + "
            "volume + volume/liquidity + buy ratio."
        )

        print()

        multi = multi_analysis(rows)

        for x in multi:

            print(
                f"SCORE >= {x['score']} | "
                f"N={x['n']:>3} | "
                f"TOK={x['tokens']:>3} | "
                f"MED60={x['med']:+7.2f}% | "
                f"AVG60={x['avg']:+7.2f}% | "
                f"WIN={x['win']:5.1f}% | "
                f"RUNNER10={x['runner']:5.1f}% | "
                f"DUMP10={x['dump']:5.1f}%"
            )

        print()
        print("="*120)
        print("INTERPRETATION")
        print("="*120)

        print(
            "SEP = ability of a feature to separate >=+5% runners "
            "from <=-5% losers."
        )

        print(
            "Do NOT modify V2 using these results. "
            "V3 is independent discovery."
        )

        print(
            "Refresh every 30 seconds."
        )

        time.sleep(30)

    except KeyboardInterrupt:
        print("\nV3 Feature Lab stopped.")
        break

    except Exception as e:
        print("ERROR:", repr(e))
        time.sleep(5)
