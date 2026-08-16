import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"

# Recent-control split:
# last 30% of event IDs = crude holdout / control sample
CONTROL_FRAC = 0.30

RUNNER_THRESHOLD = 10.0
DUMP_THRESHOLD = -10.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def percentile(vals, p):
    vals = sorted(
        x for x in vals
        if valid(x)
    )

    if not vals:
        return None

    k = (len(vals)-1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return (
        vals[lo] * (hi-k)
        + vals[hi] * (k-lo)
    )


def med(vals):
    vals = [
        x for x in vals
        if valid(x)
    ]

    return (
        statistics.median(vals)
        if vals
        else None
    )


def avg(vals):
    vals = [
        x for x in vals
        if valid(x)
    ]

    return (
        statistics.mean(vals)
        if vals
        else None
    )


def safe(row, key):
    try:
        return row[key]
    except Exception:
        return None


def fmt(x, digits=4):
    if x is None:
        return "NA"

    return f"{x:+.{digits}f}"


def pct(x):
    if x is None:
        return "NA"

    return f"{x:+.2f}%"


def connect():
    db = sqlite3.connect(
        DB,
        timeout=30
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")

    return db


# ============================================================
# LOAD
# ============================================================

def load():

    db = connect()

    rows = db.execute("""
        SELECT
            e.id,
            e.token_mint,

            e.dex_return_30s,
            e.dex_return_60s,
            e.dex_return_300s,

            e.fa,
            e.new_wallets10,
            e.new_wallets30,

            f.*

        FROM events e

        JOIN flow_quality_features f
        ON f.event_id = e.id

        WHERE
            e.dex_return_60s IS NOT NULL

        ORDER BY e.id ASC
    """).fetchall()

    db.close()

    return rows


# ============================================================
# FEATURES
# ============================================================

FEATURES = [
    "buys_10",
    "sells_10",
    "buys_30",
    "sells_30",
    "buys_60",
    "sells_60",

    "buy_wallets_10",
    "sell_wallets_10",
    "buy_wallets_30",
    "sell_wallets_30",
    "buy_wallets_60",
    "sell_wallets_60",

    "buy_sol_10",
    "sell_sol_10",
    "buy_sol_30",
    "sell_sol_30",
    "buy_sol_60",
    "sell_sol_60",

    "median_buy_sol_30",
    "median_sell_sol_30",

    "largest_buy_sol_30",
    "largest_sell_sol_30",

    "buy_sell_sol_ratio_30",
    "net_sol_30",

    "buyer_repeat_ratio_30",
    "seller_repeat_ratio_30",

    "buy_concentration_sol_30",
    "sell_concentration_sol_30",

    "buy_pressure_liq_30",
    "sell_pressure_liq_30",
    "net_pressure_liq_30",

    "return_pre_10s",
    "return_pre_30s",

    "price_response_per_net_sol",
    "absorption_score",

    "dex_liquidity_usd",
]


# ============================================================
# GROUPS
# ============================================================

def runner_rows(rows):
    return [
        r for r in rows
        if (
            valid(safe(r, "dex_return_60s"))
            and safe(r, "dex_return_60s")
            >= RUNNER_THRESHOLD
        )
    ]


def dump_rows(rows):
    return [
        r for r in rows
        if (
            valid(safe(r, "dex_return_60s"))
            and safe(r, "dex_return_60s")
            <= DUMP_THRESHOLD
        )
    ]


# ============================================================
# FEATURE SEPARATION
# ============================================================

def feature_sep(rows):

    runners = runner_rows(rows)
    dumps = dump_rows(rows)

    output = []

    for f in FEATURES:

        rv = [
            safe(r, f)
            for r in runners
            if valid(safe(r, f))
        ]

        dv = [
            safe(r, f)
            for r in dumps
            if valid(safe(r, f))
        ]

        if len(rv) < 4 or len(dv) < 4:
            continue

        rm = med(rv)
        dm = med(dv)

        pooled = rv + dv

        p25 = percentile(
            pooled,
            .25
        )

        p75 = percentile(
            pooled,
            .75
        )

        spread = None

        if (
            p25 is not None
            and p75 is not None
        ):
            spread = (
                p75 - p25
            )

        if (
            spread is None
            or spread == 0
        ):
            sep = 0.0
        else:
            sep = abs(
                rm - dm
            ) / abs(spread)

        output.append({
            "feature": f,
            "runner_n": len(rv),
            "runner_med": rm,
            "dump_n": len(dv),
            "dump_med": dm,
            "diff": rm-dm,
            "sep": sep,
        })

    return sorted(
        output,
        key=lambda x: x["sep"],
        reverse=True
    )


# ============================================================
# QUARTILE TEST
# ============================================================

def quartile_test(rows, feature):

    pairs = []

    for r in rows:

        x = safe(r, feature)
        y = safe(r, "dex_return_60s")

        if (
            valid(x)
            and valid(y)
        ):
            pairs.append(
                (x,y)
            )

    if len(pairs) < 20:
        return []

    xs = [
        x for x,_ in pairs
    ]

    p25 = percentile(xs,.25)
    p50 = percentile(xs,.50)
    p75 = percentile(xs,.75)

    buckets = [
        (
            "Q1",
            lambda x:
                x <= p25
        ),

        (
            "Q2",
            lambda x:
                p25 < x <= p50
        ),

        (
            "Q3",
            lambda x:
                p50 < x <= p75
        ),

        (
            "Q4",
            lambda x:
                x > p75
        ),
    ]

    out = []

    for name, fn in buckets:

        ys = [
            y
            for x,y in pairs
            if fn(x)
        ]

        if not ys:
            continue

        out.append({
            "name": name,
            "n": len(ys),
            "med": med(ys),
            "avg": avg(ys),

            "win":
                100
                * sum(y > 0 for y in ys)
                / len(ys),

            "runner":
                100
                * sum(y >= 10 for y in ys)
                / len(ys),

            "dump":
                100
                * sum(y <= -10 for y in ys)
                / len(ys),
        })

    return out


# ============================================================
# BINARY CONDITIONS
# ============================================================

def make_condition_bank(rows, ranked_features):

    bank = []

    # only top features
    for item in ranked_features[:10]:

        f = item["feature"]

        vals = [
            safe(r,f)
            for r in rows
            if valid(safe(r,f))
        ]

        if len(vals) < 20:
            continue

        p50 = percentile(vals,.50)
        p75 = percentile(vals,.75)

        bank.append((
            f"{f} >= P50",
            lambda r, f=f, p50=p50:
                valid(safe(r,f))
                and safe(r,f) >= p50
        ))

        bank.append((
            f"{f} >= P75",
            lambda r, f=f, p75=p75:
                valid(safe(r,f))
                and safe(r,f) >= p75
        ))

        bank.append((
            f"{f} < P50",
            lambda r, f=f, p50=p50:
                valid(safe(r,f))
                and safe(r,f) < p50
        ))

    return bank


# ============================================================
# CONDITION EVAL
# ============================================================

def eval_subset(rows, rules):

    subset = []

    for r in rows:

        ok = True

        for _,fn in rules:
            try:
                if not fn(r):
                    ok = False
                    break
            except Exception:
                ok = False
                break

        if ok:
            subset.append(r)

    vals = [
        safe(r,"dex_return_60s")
        for r in subset
        if valid(
            safe(r,"dex_return_60s")
        )
    ]

    if len(vals) < 5:
        return None

    tokens = len(set(
        safe(r,"token_mint")
        for r in subset
        if safe(r,"token_mint")
    ))

    return {
        "n": len(vals),
        "tokens": tokens,
        "med": med(vals),
        "avg": avg(vals),

        "win":
            100
            * sum(v > 0 for v in vals)
            / len(vals),

        "runner":
            100
            * sum(v >= 10 for v in vals)
            / len(vals),

        "dump":
            100
            * sum(v <= -10 for v in vals)
            / len(vals),

        "p10": percentile(vals,.10),
    }


# ============================================================
# COMBINATION SEARCH
# ============================================================

def combo_search(rows, bank):

    out = []

    # singles
    for i in range(len(bank)):

        rules = [
            bank[i]
        ]

        s = eval_subset(
            rows,
            rules
        )

        if s:
            out.append((
                bank[i][0],
                s
            ))

    # pairs
    for i in range(len(bank)):

        for j in range(
            i+1,
            len(bank)
        ):

            rules = [
                bank[i],
                bank[j],
            ]

            s = eval_subset(
                rows,
                rules
            )

            if s:
                out.append((
                    (
                        bank[i][0]
                        + " + "
                        + bank[j][0]
                    ),
                    s
                ))

    # score = reward runner / median,
    # penalize dumps and tiny samples
    def score(x):

        _,s = x

        sample_bonus = min(
            s["n"],
            30
        ) / 30 * 5

        return (
            s["med"]
            + (
                s["runner"]
                - s["dump"]
            ) * .25
            + sample_bonus
            + s["win"] * .05
            + s["p10"] * .10
        )

    return sorted(
        out,
        key=score,
        reverse=True
    )[:25]


# ============================================================
# FALSE POSITIVE PROFILE
# ============================================================

def false_positive_profile(rows):

    intense = []

    # crude "high intensity" definition
    buy30_vals = [
        safe(r,"buy_sol_30")
        for r in rows
        if valid(
            safe(r,"buy_sol_30")
        )
    ]

    wallet_vals = [
        safe(r,"buy_wallets_30")
        for r in rows
        if valid(
            safe(r,"buy_wallets_30")
        )
    ]

    if (
        not buy30_vals
        or not wallet_vals
    ):
        return [], []

    buy75 = percentile(
        buy30_vals,
        .75
    )

    wallet75 = percentile(
        wallet_vals,
        .75
    )

    for r in rows:

        if (
            valid(
                safe(r,"buy_sol_30")
            )
            and valid(
                safe(
                    r,
                    "buy_wallets_30"
                )
            )
            and safe(
                r,
                "buy_sol_30"
            ) >= buy75
            and safe(
                r,
                "buy_wallets_30"
            ) >= wallet75
        ):
            intense.append(r)

    good = [
        r for r in intense
        if safe(
            r,
            "dex_return_60s"
        ) >= 10
    ]

    bad = [
        r for r in intense
        if safe(
            r,
            "dex_return_60s"
        ) <= -10
    ]

    return good,bad


# ============================================================
# PROFILE TABLE
# ============================================================

def profile_compare(
    title,
    good,
    bad
):

    print()
    print(title)
    print("-"*120)

    print(
        f"GOOD={len(good)}"
        f" | BAD={len(bad)}"
    )

    if (
        len(good) < 2
        or len(bad) < 2
    ):
        print(
            "Pas assez de cas."
        )
        return

    ranked = []

    for f in FEATURES:

        gv = [
            safe(r,f)
            for r in good
            if valid(
                safe(r,f)
            )
        ]

        bv = [
            safe(r,f)
            for r in bad
            if valid(
                safe(r,f)
            )
        ]

        if (
            len(gv) < 2
            or len(bv) < 2
        ):
            continue

        gm = med(gv)
        bm = med(bv)

        ranked.append(
            (
                abs(gm-bm),
                f,
                gm,
                bm
            )
        )

    ranked.sort(
        reverse=True
    )

    for _,f,gm,bm in ranked[:15]:

        print(
            f"{f:32} "
            f"RUNNER={fmt(gm):>14} | "
            f"DUMP={fmt(bm):>14}"
        )


# ============================================================
# DISPLAY
# ============================================================

while True:

    try:

        rows = load()

        if len(rows) < 30:
            print(
                "Pas assez de data."
            )
            time.sleep(10)
            continue

        max_id = max(
            safe(r,"id")
            for r in rows
        )

        control_start = int(
            max_id
            * (
                1
                - CONTROL_FRAC
            )
        )

        discovery = [
            r for r in rows
            if safe(r,"id")
            <= control_start
        ]

        control = [
            r for r in rows
            if safe(r,"id")
            > control_start
        ]

        sep_disc = feature_sep(
            discovery
        )

        sep_control = feature_sep(
            control
        )

        bank = make_condition_bank(
            discovery,
            sep_disc
        )

        combos = combo_search(
            discovery,
            bank
        )

        good_intense,bad_intense = (
            false_positive_profile(
                rows
            )
        )

        os.system("clear")

        print("="*120)
        print(
            "MEMECOIN LAB — "
            "V3.2 FLOW QUALITY ANALYZER"
        )
        print("="*120)

        print(
            f"TOTAL EVENTS : {len(rows)}"
            f" | MAX ID : {max_id}"
        )

        print(
            f"DISCOVERY    : "
            f"{len(discovery)}"
        )

        print(
            f"RECENT CTRL  : "
            f"{len(control)}"
        )

        print(
            f"CTRL START ID: "
            f">{control_start}"
        )

        print()
        print(
            f"RUNNER >= +{RUNNER_THRESHOLD:.0f}%"
            f" | DUMP <= {DUMP_THRESHOLD:.0f}%"
        )

        # ====================================================
        # DISCOVERY SEPARATION
        # ====================================================

        print()
        print("="*120)
        print(
            "DISCOVERY — RUNNER VS DUMP"
        )
        print("="*120)

        print(
            f"{'FEATURE':32}"
            f"{'RUN N':>7}"
            f"{'RUN MED':>15}"
            f"{'DUMP N':>8}"
            f"{'DUMP MED':>15}"
            f"{'DIFF':>15}"
            f"{'SEP':>9}"
        )

        print("-"*120)

        for x in sep_disc[:20]:

            print(
                f"{x['feature']:32}"
                f"{x['runner_n']:7}"
                f"{fmt(x['runner_med']):>15}"
                f"{x['dump_n']:8}"
                f"{fmt(x['dump_med']):>15}"
                f"{fmt(x['diff']):>15}"
                f"{x['sep']:9.3f}"
            )

        # ====================================================
        # CONTROL SEPARATION
        # ====================================================

        print()
        print("="*120)
        print(
            "RECENT CONTROL — "
            "DO THE SAME FEATURES SURVIVE?"
        )
        print("="*120)

        print(
            f"{'FEATURE':32}"
            f"{'RUN N':>7}"
            f"{'RUN MED':>15}"
            f"{'DUMP N':>8}"
            f"{'DUMP MED':>15}"
            f"{'DIFF':>15}"
            f"{'SEP':>9}"
        )

        print("-"*120)

        for x in sep_control[:20]:

            print(
                f"{x['feature']:32}"
                f"{x['runner_n']:7}"
                f"{fmt(x['runner_med']):>15}"
                f"{x['dump_n']:8}"
                f"{fmt(x['dump_med']):>15}"
                f"{fmt(x['diff']):>15}"
                f"{x['sep']:9.3f}"
            )

        # ====================================================
        # QUARTILES
        # ====================================================

        print()
        print("="*120)
        print(
            "DISCOVERY — TOP FEATURE QUARTILES"
        )
        print("="*120)

        for x in sep_disc[:6]:

            f = x["feature"]

            q = quartile_test(
                discovery,
                f
            )

            if not q:
                continue

            print()
            print(f)
            print("-"*100)

            for z in q:

                print(
                    f"{z['name']} | "
                    f"N={z['n']:>3} | "
                    f"MED60={z['med']:+7.2f}% | "
                    f"AVG60={z['avg']:+7.2f}% | "
                    f"WIN={z['win']:5.1f}% | "
                    f"RUNNER={z['runner']:5.1f}% | "
                    f"DUMP={z['dump']:5.1f}%"
                )

        # ====================================================
        # COMBOS
        # ====================================================

        print()
        print("="*120)
        print(
            "DISCOVERY — TOP COMBINATIONS"
        )
        print("="*120)

        print(
            f"{'FILTER':65}"
            f"{'N':>5}"
            f"{'TOK':>6}"
            f"{'MED60':>10}"
            f"{'AVG60':>10}"
            f"{'WIN':>8}"
            f"{'RUNNER':>10}"
            f"{'DUMP':>8}"
            f"{'P10':>10}"
        )

        print("-"*140)

        for name,s in combos:

            print(
                f"{name[:65]:65}"
                f"{s['n']:5}"
                f"{s['tokens']:6}"
                f"{s['med']:+9.2f}%"
                f"{s['avg']:+9.2f}%"
                f"{s['win']:7.1f}%"
                f"{s['runner']:9.1f}%"
                f"{s['dump']:7.1f}%"
                f"{s['p10']:+9.2f}%"
            )

        # ====================================================
        # FALSE POSITIVES
        # ====================================================

        print()
        print("="*120)
        print(
            "HIGH-INTENSITY EVENTS — "
            "WHY RUNNER VS WHY DUMP?"
        )
        print("="*120)

        profile_compare(
            "HIGH INTENSITY PROFILE",
            good_intense,
            bad_intense
        )

        print()
        print("="*120)
        print("IMPORTANT")
        print("="*120)

        print(
            "DISCOVERY combinations are hypothesis generation only."
        )

        print(
            "Recent control is more important than the discovery ranking."
        )

        print(
            "Do NOT alter V2 Frozen from this analyzer."
        )

        print(
            "Refresh every 30 seconds."
        )

        time.sleep(30)

    except KeyboardInterrupt:

        print(
            "\nV3.2 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)
