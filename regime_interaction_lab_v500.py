import sqlite3
import statistics
import math
import os
import time
from itertools import combinations

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

# Discovery only.
# Last 30% remains recent control.
CONTROL_FRAC = 0.30

MIN_N_DISCOVERY = 10
MIN_N_CONTROL = 5


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def median(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def mean(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


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


def safe_div(a,b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a/b


def fmt(x, d=3):
    if x is None:
        return "NA"
    return f"{x:+.{d}f}"


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


# ============================================================
# LOAD
# ============================================================

def load():

    db = connect()

    rows = db.execute("""
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

            ON d.event_id=x.event_id
            AND d.timestamp=x.first_time
        )

        SELECT
            e.id,
            e.token_mint,
            e.timestamp,

            e.dex_return_30s,
            e.dex_return_60s,

            e.fa,
            e.nf30,
            e.new_wallets10,
            e.new_wallets30,

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,
            d.buys_m5,
            d.sells_m5,

            s.mid_buy_count,
            s.mid_sell_count,
            s.recent_unique_buyers,

            s.early_swaps_per_sec,
            s.mid_swaps_per_sec,
            s.recent_swaps_per_sec,

            s.buy_concentration_trend,

            s.recent_price_return,
            s.mid_price_return,

            s.recent_sell_sol,
            s.recent_net_sol,
            s.recent_buy_share,

            s.late_chase_score,
            s.breadth_score

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        LEFT JOIN first_dex d
        ON d.event_id=e.id

        WHERE
            e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()

    db.close()

    return rows


# ============================================================
# DERIVED FEATURES
# ============================================================

def feature(r, name):

    if name in r.keys():
        return r[name]

    if name == "vol_liq":
        return safe_div(
            r["volume_m5"],
            r["liquidity_usd"]
        )

    if name == "buy_ratio_m5":
        b = r["buys_m5"]
        s = r["sells_m5"]

        if not valid(b) or not valid(s):
            return None

        return safe_div(
            b,
            b+s
        )

    if name == "mid_flow_balance":
        b = r["mid_buy_count"]
        s = r["mid_sell_count"]

        if not valid(b) or not valid(s):
            return None

        return b-s

    if name == "frequency_shape":
        e = r["early_swaps_per_sec"]
        m = r["mid_swaps_per_sec"]
        rr = r["recent_swaps_per_sec"]

        if not all(valid(x) for x in [e,m,rr]):
            return None

        return (
            m - e
        ) + (
            rr - m
        )

    if name == "price_flow_tension":
        p = r["recent_price_return"]
        f = r["recent_net_sol"]

        if not valid(p) or not valid(f):
            return None

        # Positive flow with weak/negative price response
        # => possible absorption/distribution.
        return f - p

    if name == "late_chase_x_conc":
        l = r["late_chase_score"]
        c = r["buy_concentration_trend"]

        if not valid(l) or not valid(c):
            return None

        return l * (1+c)

    return None


FEATURES = [
    "fa",
    "nf30",
    "new_wallets10",
    "new_wallets30",

    "volume_m5",
    "liquidity_usd",
    "market_cap",

    "vol_liq",
    "buy_ratio_m5",

    "mid_buy_count",
    "mid_sell_count",
    "mid_flow_balance",

    "recent_unique_buyers",

    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",
    "frequency_shape",

    "buy_concentration_trend",

    "recent_price_return",
    "mid_price_return",

    "recent_sell_sol",
    "recent_net_sol",
    "recent_buy_share",

    "late_chase_score",
    "breadth_score",

    "price_flow_tension",
    "late_chase_x_conc",
]


# ============================================================
# OUTCOME STATS
# ============================================================

def outcome_stats(rows):

    vals = [
        r["dex_return_60s"]
        for r in rows
        if valid(r["dex_return_60s"])
    ]

    if not vals:
        return None

    runners = sum(x >= RUNNER for x in vals)
    dumps = sum(x <= DUMP for x in vals)

    return {
        "n": len(vals),
        "tokens": len(set(
            r["token_mint"]
            for r in rows
        )),
        "med": median(vals),
        "avg": mean(vals),
        "win": 100*sum(x > 0 for x in vals)/len(vals),
        "runner": 100*runners/len(vals),
        "dump": 100*dumps/len(vals),
        "edge": 100*(runners-dumps)/len(vals),
        "p10": percentile(vals,.10),
    }


# ============================================================
# REGIMES
# ============================================================

def thresholds(rows):

    out = {}

    for f in [
        "volume_m5",
        "liquidity_usd",
        "market_cap",
        "vol_liq",
        "early_swaps_per_sec",
        "mid_swaps_per_sec",
        "recent_swaps_per_sec",
    ]:

        vals = [
            feature(r,f)
            for r in rows
            if valid(feature(r,f))
        ]

        if not vals:
            continue

        out[f] = {
            "p25": percentile(vals,.25),
            "p50": percentile(vals,.50),
            "p75": percentile(vals,.75),
        }

    return out


def build_regimes(t):

    regimes = []

    def add(name, fn):
        regimes.append((name,fn))

    if "liquidity_usd" in t:

        q = t["liquidity_usd"]

        add(
            "LIQ LOW",
            lambda r,q=q:
                valid(feature(r,"liquidity_usd"))
                and feature(r,"liquidity_usd") < q["p25"]
        )

        add(
            "LIQ MID",
            lambda r,q=q:
                valid(feature(r,"liquidity_usd"))
                and q["p25"] <= feature(r,"liquidity_usd") <= q["p75"]
        )

        add(
            "LIQ HIGH",
            lambda r,q=q:
                valid(feature(r,"liquidity_usd"))
                and feature(r,"liquidity_usd") > q["p75"]
        )

    if "vol_liq" in t:

        q = t["vol_liq"]

        add(
            "VOL/LIQ LOW",
            lambda r,q=q:
                valid(feature(r,"vol_liq"))
                and feature(r,"vol_liq") < q["p50"]
        )

        add(
            "VOL/LIQ HIGH",
            lambda r,q=q:
                valid(feature(r,"vol_liq"))
                and feature(r,"vol_liq") >= q["p50"]
        )

    if "market_cap" in t:

        q = t["market_cap"]

        add(
            "MCAP LOW",
            lambda r,q=q:
                valid(feature(r,"market_cap"))
                and feature(r,"market_cap") < q["p50"]
        )

        add(
            "MCAP HIGH",
            lambda r,q=q:
                valid(feature(r,"market_cap"))
                and feature(r,"market_cap") >= q["p50"]
        )

    if "early_swaps_per_sec" in t:

        q = t["early_swaps_per_sec"]

        add(
            "EARLY FAST",
            lambda r,q=q:
                valid(feature(r,"early_swaps_per_sec"))
                and feature(r,"early_swaps_per_sec") >= q["p50"]
        )

        add(
            "EARLY SLOW",
            lambda r,q=q:
                valid(feature(r,"early_swaps_per_sec"))
                and feature(r,"early_swaps_per_sec") < q["p50"]
        )

    return regimes


# ============================================================
# FEATURE DIRECTION INSIDE EACH REGIME
# ============================================================

def feature_direction(rows, f):

    vals = [
        feature(r,f)
        for r in rows
        if valid(feature(r,f))
    ]

    if len(vals) < 12:
        return None

    cut = percentile(vals,.50)

    high = [
        r for r in rows
        if valid(feature(r,f))
        and feature(r,f) >= cut
    ]

    low = [
        r for r in rows
        if valid(feature(r,f))
        and feature(r,f) < cut
    ]

    hs = outcome_stats(high)
    ls = outcome_stats(low)

    if not hs or not ls:
        return None

    return {
        "cut": cut,
        "high": hs,
        "low": ls,

        # positive => HIGH side better
        "direction":
            hs["edge"] - ls["edge"],

        "med_diff":
            hs["med"] - ls["med"],
    }


# ============================================================
# INTERACTIONS
# ============================================================

def interaction_search(rows, min_n):

    cuts = {}

    for f in FEATURES:

        vals = [
            feature(r,f)
            for r in rows
            if valid(feature(r,f))
        ]

        if len(vals) >= 20:
            cuts[f] = percentile(vals,.50)

    out = []

    candidates = [
        "vol_liq",
        "liquidity_usd",
        "market_cap",

        "mid_buy_count",
        "mid_sell_count",
        "mid_flow_balance",

        "recent_unique_buyers",

        "early_swaps_per_sec",
        "mid_swaps_per_sec",
        "recent_swaps_per_sec",

        "buy_concentration_trend",

        "recent_price_return",
        "recent_net_sol",
        "recent_buy_share",

        "late_chase_score",
        "breadth_score",

        "price_flow_tension",
    ]

    candidates = [
        f for f in candidates
        if f in cuts
    ]

    for a,b in combinations(candidates,2):

        for ahigh in [True,False]:

            for bhigh in [True,False]:

                subset = []

                for r in rows:

                    av = feature(r,a)
                    bv = feature(r,b)

                    if not valid(av) or not valid(bv):
                        continue

                    aok = (
                        av >= cuts[a]
                        if ahigh
                        else av < cuts[a]
                    )

                    bok = (
                        bv >= cuts[b]
                        if bhigh
                        else bv < cuts[b]
                    )

                    if aok and bok:
                        subset.append(r)

                s = outcome_stats(subset)

                if not s or s["n"] < min_n:
                    continue

                name = (
                    f"{a} {'HIGH' if ahigh else 'LOW'}"
                    f" + "
                    f"{b} {'HIGH' if bhigh else 'LOW'}"
                )

                # reward runner excess,
                # median and sample diversity
                score = (
                    s["edge"]
                    + s["med"] * 1.5
                    + min(s["tokens"],20)
                    - max(0,-s["p10"]) * .20
                )

                out.append({
                    "name": name,
                    "score": score,
                    **s
                })

    return sorted(
        out,
        key=lambda x:x["score"],
        reverse=True
    )


# ============================================================
# REGIME STABILITY
# ============================================================

def regime_table(
    title,
    discovery,
    control,
    regimes
):

    print()
    print("="*150)
    print(title)
    print("="*150)

    print(
        f"{'REGIME':25} "
        f"{'DISC N':>6} "
        f"{'D EDGE':>8} "
        f"{'D MED':>8} | "
        f"{'CTRL N':>6} "
        f"{'C EDGE':>8} "
        f"{'C MED':>8}"
    )

    print("-"*100)

    for name,fn in regimes:

        d = [
            r for r in discovery
            if fn(r)
        ]

        c = [
            r for r in control
            if fn(r)
        ]

        ds = outcome_stats(d)
        cs = outcome_stats(c)

        if not ds and not cs:
            continue

        print(
            f"{name:25} "
            f"{(ds['n'] if ds else 0):6d} "
            f"{(ds['edge'] if ds else 0):+7.1f}% "
            f"{(ds['med'] if ds else 0):+7.2f}% | "
            f"{(cs['n'] if cs else 0):6d} "
            f"{(cs['edge'] if cs else 0):+7.1f}% "
            f"{(cs['med'] if cs else 0):+7.2f}%"
        )


# ============================================================
# FEATURES WHOSE DIRECTION FLIPS BY REGIME
# ============================================================

def regime_feature_matrix(rows, regimes):

    results = []

    for rname,rfn in regimes:

        subset = [
            r for r in rows
            if rfn(r)
        ]

        if len(subset) < 20:
            continue

        for f in FEATURES:

            d = feature_direction(
                subset,
                f
            )

            if not d:
                continue

            results.append({
                "regime": rname,
                "feature": f,
                **d
            })

    return results


def print_flip_candidates(matrix):

    grouped = {}

    for x in matrix:
        grouped.setdefault(
            x["feature"],
            []
        ).append(x)

    flips = []

    for f,items in grouped.items():

        dirs = [
            x["direction"]
            for x in items
        ]

        if not dirs:
            continue

        if (
            max(dirs) > 15
            and min(dirs) < -15
        ):

            spread = (
                max(dirs)
                - min(dirs)
            )

            flips.append(
                (
                    spread,
                    f,
                    items
                )
            )

    flips.sort(
        reverse=True
    )

    print()
    print("="*150)
    print(
        "REGIME-DEPENDENT FEATURES — "
        "SAME FEATURE CHANGES SIGN"
    )
    print("="*150)

    for spread,f,items in flips[:12]:

        print()
        print(
            f"{f} | regime spread={spread:.1f}"
        )

        print("-"*100)

        for x in sorted(
            items,
            key=lambda z:z["direction"],
            reverse=True
        ):

            print(
                f"{x['regime']:25} | "
                f"HIGH-vs-LOW EDGE={x['direction']:+7.1f} pts | "
                f"MED DIFF={x['med_diff']:+7.2f}%"
            )


# ============================================================
# LOOP
# ============================================================

while True:

    try:

        rows = load()

        if len(rows) < 80:

            print(
                f"Need more events. Current={len(rows)}"
            )

            time.sleep(20)
            continue

        max_id = max(
            r["id"]
            for r in rows
        )

        control_start = int(
            max_id * (
                1-CONTROL_FRAC
            )
        )

        discovery = [
            r for r in rows
            if r["id"] <= control_start
        ]

        control = [
            r for r in rows
            if r["id"] > control_start
        ]

        t = thresholds(
            discovery
        )

        regimes = build_regimes(
            t
        )

        matrix = regime_feature_matrix(
            discovery,
            regimes
        )

        disc_interactions = (
            interaction_search(
                discovery,
                MIN_N_DISCOVERY
            )
        )

        ctrl_interactions = (
            interaction_search(
                control,
                MIN_N_CONTROL
            )
        )

        os.system("clear")

        print("="*150)
        print(
            "MEMECOIN LAB — "
            "V5 REGIME & INTERACTION LAB"
        )
        print("="*150)

        print(
            f"TOTAL EVENTS   : {len(rows)}"
        )

        print(
            f"DISCOVERY      : {len(discovery)}"
        )

        print(
            f"RECENT CONTROL : {len(control)}"
        )

        print(
            f"CONTROL ID     : >{control_start}"
        )

        print()
        print(
            "RUNNER >= +10% | DUMP <= -10%"
        )

        regime_table(
            "REGIME BASELINES",
            discovery,
            control,
            regimes
        )

        print_flip_candidates(
            matrix
        )

        print()
        print("="*150)
        print(
            "DISCOVERY — TOP INTERACTIONS"
        )
        print("="*150)

        print(
            f"{'INTERACTION':72}"
            f"{'N':>5}"
            f"{'TOK':>5}"
            f"{'MED':>9}"
            f"{'AVG':>9}"
            f"{'RUN':>8}"
            f"{'DUMP':>8}"
            f"{'EDGE':>8}"
            f"{'P10':>9}"
        )

        print("-"*140)

        for x in disc_interactions[:25]:

            print(
                f"{x['name'][:72]:72}"
                f"{x['n']:5}"
                f"{x['tokens']:5}"
                f"{x['med']:+8.2f}%"
                f"{x['avg']:+8.2f}%"
                f"{x['runner']:7.1f}%"
                f"{x['dump']:7.1f}%"
                f"{x['edge']:+7.1f}%"
                f"{x['p10']:+8.2f}%"
            )

        print()
        print("="*150)
        print(
            "RECENT CONTROL — TOP INTERACTIONS"
        )
        print("="*150)

        print(
            f"{'INTERACTION':72}"
            f"{'N':>5}"
            f"{'TOK':>5}"
            f"{'MED':>9}"
            f"{'AVG':>9}"
            f"{'RUN':>8}"
            f"{'DUMP':>8}"
            f"{'EDGE':>8}"
            f"{'P10':>9}"
        )

        print("-"*140)

        for x in ctrl_interactions[:25]:

            print(
                f"{x['name'][:72]:72}"
                f"{x['n']:5}"
                f"{x['tokens']:5}"
                f"{x['med']:+8.2f}%"
                f"{x['avg']:+8.2f}%"
                f"{x['runner']:7.1f}%"
                f"{x['dump']:7.1f}%"
                f"{x['edge']:+7.1f}%"
                f"{x['p10']:+8.2f}%"
            )

        print()
        print("="*150)
        print("WHAT TO LOOK FOR")
        print("="*150)

        print(
            "1. Same regime must behave similarly "
            "in Discovery and Recent Control."
        )

        print(
            "2. Interaction is interesting only if "
            "its direction survives in Recent Control."
        )

        print(
            "3. A feature that flips sign between regimes "
            "may explain why V4 changes behaviour."
        )

        print(
            "4. Do NOT modify V4 Frozen or V2 Frozen "
            "from V5 results."
        )

        print(
            "5. V5 is hypothesis generation only."
        )

        print()
        print(
            "Refresh every 30 seconds."
        )

        time.sleep(30)

    except KeyboardInterrupt:

        print(
            "\nV5 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)
