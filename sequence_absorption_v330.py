import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
CONTROL_FRAC = 0.30

WINDOWS = [
    ("W1_-30_-20", -30, -20),
    ("W2_-20_-10", -20, -10),
    ("W3_-10_0",   -10,   0),
]


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


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


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def fmt(x, digits=4):
    if x is None:
        return "NA"

    return f"{x:+.{digits}f}"


def connect():

    db = sqlite3.connect(
        DB,
        timeout=30
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")

    return db


db = connect()


# ============================================================
# OUTPUT TABLE
# ============================================================

db.executescript("""
CREATE TABLE IF NOT EXISTS sequence_features_v330 (

    event_id INTEGER PRIMARY KEY,
    token_mint TEXT,
    event_timestamp REAL,

    -- W1 = -30 -> -20
    w1_buys INTEGER,
    w1_sells INTEGER,
    w1_buyers INTEGER,
    w1_sellers INTEGER,
    w1_buy_sol REAL,
    w1_sell_sol REAL,
    w1_net_sol REAL,
    w1_med_buy REAL,
    w1_med_sell REAL,
    w1_largest_buy REAL,
    w1_largest_sell REAL,
    w1_price_return REAL,

    -- W2 = -20 -> -10
    w2_buys INTEGER,
    w2_sells INTEGER,
    w2_buyers INTEGER,
    w2_sellers INTEGER,
    w2_buy_sol REAL,
    w2_sell_sol REAL,
    w2_net_sol REAL,
    w2_med_buy REAL,
    w2_med_sell REAL,
    w2_largest_buy REAL,
    w2_largest_sell REAL,
    w2_price_return REAL,

    -- W3 = -10 -> 0
    w3_buys INTEGER,
    w3_sells INTEGER,
    w3_buyers INTEGER,
    w3_sellers INTEGER,
    w3_buy_sol REAL,
    w3_sell_sol REAL,
    w3_net_sol REAL,
    w3_med_buy REAL,
    w3_med_sell REAL,
    w3_largest_buy REAL,
    w3_largest_sell REAL,
    w3_price_return REAL,

    -- dynamics
    buy_count_accel REAL,
    buyer_accel REAL,
    buy_sol_accel REAL,
    sell_sol_accel REAL,
    net_sol_accel REAL,

    recent_buy_share REAL,

    seller_absorption_ratio REAL,
    buy_efficiency REAL,

    flow_price_divergence REAL,
    absorption_proxy REAL,

    updated_at REAL
);
""")

db.commit()


# ============================================================
# SWAPS
# ============================================================

def get_swaps(token, start, end):

    return db.execute("""
        SELECT
            timestamp,
            wallet,
            side,
            ABS(sol_delta) AS sol,
            clean_price

        FROM swaps

        WHERE
            token_mint=?
            AND price_valid=1
            AND timestamp >= ?
            AND timestamp < ?

        ORDER BY timestamp ASC
    """, (
        token,
        start,
        end
    )).fetchall()


def window_stats(rows):

    buys = [
        r for r in rows
        if r["side"] == "BUY"
    ]

    sells = [
        r for r in rows
        if r["side"] == "SELL"
    ]

    buy_sol = [
        r["sol"]
        for r in buys
        if valid(r["sol"])
    ]

    sell_sol = [
        r["sol"]
        for r in sells
        if valid(r["sol"])
    ]

    buyers = {
        r["wallet"]
        for r in buys
        if r["wallet"]
    }

    sellers = {
        r["wallet"]
        for r in sells
        if r["wallet"]
    }

    tb = sum(buy_sol)
    ts = sum(sell_sol)

    prices = [
        r["clean_price"]
        for r in rows
        if valid(r["clean_price"])
        and r["clean_price"] > 0
    ]

    price_return = None

    if len(prices) >= 2:
        price_return = (
            prices[-1] / prices[0] - 1
        ) * 100

    return {
        "buys": len(buys),
        "sells": len(sells),

        "buyers": len(buyers),
        "sellers": len(sellers),

        "buy_sol": tb,
        "sell_sol": ts,
        "net_sol": tb-ts,

        "med_buy": med(buy_sol),
        "med_sell": med(sell_sol),

        "largest_buy":
            max(buy_sol)
            if buy_sol else 0.0,

        "largest_sell":
            max(sell_sol)
            if sell_sol else 0.0,

        "price_return":
            price_return,
    }


# ============================================================
# BUILD ONE EVENT
# ============================================================

def build_event(event):

    eid = event["id"]
    token = event["token_mint"]
    ts = event["timestamp"]

    ws = []

    for _, a, b in WINDOWS:

        rows = get_swaps(
            token,
            ts+a,
            ts+b
        )

        ws.append(
            window_stats(rows)
        )

    w1, w2, w3 = ws

    # --------------------------------------------------------
    # ACCELERATION
    #
    # "second derivative":
    # (W3-W2) - (W2-W1)
    # --------------------------------------------------------

    buy_count_accel = (
        (w3["buys"] - w2["buys"])
        -
        (w2["buys"] - w1["buys"])
    )

    buyer_accel = (
        (w3["buyers"] - w2["buyers"])
        -
        (w2["buyers"] - w1["buyers"])
    )

    buy_sol_accel = (
        (w3["buy_sol"] - w2["buy_sol"])
        -
        (w2["buy_sol"] - w1["buy_sol"])
    )

    sell_sol_accel = (
        (w3["sell_sol"] - w2["sell_sol"])
        -
        (w2["sell_sol"] - w1["sell_sol"])
    )

    net_sol_accel = (
        (w3["net_sol"] - w2["net_sol"])
        -
        (w2["net_sol"] - w1["net_sol"])
    )

    # --------------------------------------------------------
    # RECENT SHARE OF BUYING
    # --------------------------------------------------------

    total_buy_30 = (
        w1["buy_sol"]
        + w2["buy_sol"]
        + w3["buy_sol"]
    )

    recent_buy_share = (
        safe_div(
            w3["buy_sol"],
            total_buy_30
        )
        if total_buy_30 > 0
        else None
    )

    # --------------------------------------------------------
    # ABSORPTION
    #
    # High sell flow + positive/flat price response
    # may indicate sellers are being absorbed.
    # --------------------------------------------------------

    recent_price = w3["price_return"]

    seller_absorption_ratio = None

    if (
        valid(w3["sell_sol"])
        and valid(w3["buy_sol"])
    ):

        seller_absorption_ratio = (
            safe_div(
                w3["sell_sol"],
                w3["buy_sol"] + 1e-9
            )
        )

    # Price movement per net SOL in recent window
    buy_efficiency = None

    if (
        valid(recent_price)
        and abs(w3["net_sol"]) > 1e-9
    ):
        buy_efficiency = (
            recent_price
            / w3["net_sol"]
        )

    # --------------------------------------------------------
    # FLOW / PRICE DIVERGENCE
    #
    # + flow but <= price response:
    # possible absorption/distribution
    # --------------------------------------------------------

    flow_price_divergence = None

    if (
        valid(recent_price)
        and valid(w3["net_sol"])
    ):

        flow_price_divergence = (
            w3["net_sol"]
            - recent_price
        )

    absorption_proxy = None

    if (
        valid(recent_price)
        and w3["sell_sol"] > 0
    ):

        # Large selling while price holds/up
        if recent_price >= 0:

            absorption_proxy = (
                w3["sell_sol"]
                * (1 + recent_price)
            )

        else:

            absorption_proxy = (
                w3["sell_sol"]
                / (
                    1 + abs(recent_price)
                )
            )

    values = [
        eid,
        token,
        ts,
    ]

    for w in [w1,w2,w3]:

        values += [
            w["buys"],
            w["sells"],
            w["buyers"],
            w["sellers"],

            w["buy_sol"],
            w["sell_sol"],
            w["net_sol"],

            w["med_buy"],
            w["med_sell"],

            w["largest_buy"],
            w["largest_sell"],

            w["price_return"],
        ]

    values += [
        buy_count_accel,
        buyer_accel,
        buy_sol_accel,
        sell_sol_accel,
        net_sol_accel,

        recent_buy_share,

        seller_absorption_ratio,
        buy_efficiency,

        flow_price_divergence,
        absorption_proxy,

        time.time(),
    ]

    db.execute("""
        INSERT OR REPLACE INTO sequence_features_v330 (

            event_id,
            token_mint,
            event_timestamp,

            w1_buys,
            w1_sells,
            w1_buyers,
            w1_sellers,
            w1_buy_sol,
            w1_sell_sol,
            w1_net_sol,
            w1_med_buy,
            w1_med_sell,
            w1_largest_buy,
            w1_largest_sell,
            w1_price_return,

            w2_buys,
            w2_sells,
            w2_buyers,
            w2_sellers,
            w2_buy_sol,
            w2_sell_sol,
            w2_net_sol,
            w2_med_buy,
            w2_med_sell,
            w2_largest_buy,
            w2_largest_sell,
            w2_price_return,

            w3_buys,
            w3_sells,
            w3_buyers,
            w3_sellers,
            w3_buy_sol,
            w3_sell_sol,
            w3_net_sol,
            w3_med_buy,
            w3_med_sell,
            w3_largest_buy,
            w3_largest_sell,
            w3_price_return,

            buy_count_accel,
            buyer_accel,
            buy_sol_accel,
            sell_sol_accel,
            net_sol_accel,

            recent_buy_share,

            seller_absorption_ratio,
            buy_efficiency,

            flow_price_divergence,
            absorption_proxy,

            updated_at
        )

        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, values)

    db.commit()


# ============================================================
# ANALYSIS
# ============================================================

FEATURES = [

    "w1_buys",
    "w2_buys",
    "w3_buys",

    "w1_sells",
    "w2_sells",
    "w3_sells",

    "w1_buyers",
    "w2_buyers",
    "w3_buyers",

    "w1_sellers",
    "w2_sellers",
    "w3_sellers",

    "w1_buy_sol",
    "w2_buy_sol",
    "w3_buy_sol",

    "w1_sell_sol",
    "w2_sell_sol",
    "w3_sell_sol",

    "w1_net_sol",
    "w2_net_sol",
    "w3_net_sol",

    "w1_price_return",
    "w2_price_return",
    "w3_price_return",

    "buy_count_accel",
    "buyer_accel",

    "buy_sol_accel",
    "sell_sol_accel",
    "net_sol_accel",

    "recent_buy_share",

    "seller_absorption_ratio",
    "buy_efficiency",

    "flow_price_divergence",
    "absorption_proxy",
]


def load_analysis():

    return db.execute("""
        SELECT
            e.id,
            e.token_mint,

            e.dex_return_30s,
            e.dex_return_60s,
            e.dex_return_300s,

            s.*

        FROM events e

        JOIN sequence_features_v330 s
        ON s.event_id = e.id

        WHERE
            e.dex_return_60s
            IS NOT NULL

        ORDER BY e.id
    """).fetchall()


def separation(rows):

    runners = [
        r for r in rows
        if r["dex_return_60s"]
        >= RUNNER
    ]

    dumps = [
        r for r in rows
        if r["dex_return_60s"]
        <= DUMP
    ]

    out = []

    for f in FEATURES:

        rv = [
            r[f]
            for r in runners
            if valid(r[f])
        ]

        dv = [
            r[f]
            for r in dumps
            if valid(r[f])
        ]

        if (
            len(rv) < 4
            or len(dv) < 4
        ):
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

        spread = (
            p75-p25
            if (
                p25 is not None
                and p75 is not None
            )
            else 0
        )

        sep = (
            abs(rm-dm)
            / abs(spread)
            if spread != 0
            else 0
        )

        out.append({
            "feature": f,

            "rn": len(rv),
            "rm": rm,

            "dn": len(dv),
            "dm": dm,

            "diff": rm-dm,
            "sep": sep,
        })

    return sorted(
        out,
        key=lambda x:
            x["sep"],
        reverse=True
    )


# ============================================================
# LIVE LOOP
# ============================================================

while True:

    try:

        events = db.execute("""
            SELECT
                id,
                token_mint,
                timestamp

            FROM events

            ORDER BY id
        """).fetchall()

        existing = {
            r["event_id"]
            for r in db.execute("""
                SELECT event_id
                FROM sequence_features_v330
            """).fetchall()
        }

        pending = [
            e for e in events
            if e["id"] not in existing
        ]

        for i,e in enumerate(pending):

            build_event(e)

            if i % 50 == 0:

                print(
                    f"Building "
                    f"{i+1}/"
                    f"{len(pending)}"
                )

        rows = load_analysis()

        max_id = max(
            r["id"]
            for r in rows
        ) if rows else 0

        control_start = int(
            max_id
            * (
                1-CONTROL_FRAC
            )
        )

        discovery = [
            r for r in rows
            if r["id"]
            <= control_start
        ]

        control = [
            r for r in rows
            if r["id"]
            > control_start
        ]

        disc_sep = separation(
            discovery
        )

        ctrl_sep = separation(
            control
        )

        os.system("clear")

        print("="*125)
        print(
            "MEMECOIN LAB — "
            "V3.3 SEQUENCE / ABSORPTION ANALYZER"
        )
        print("="*125)

        print(
            f"TOTAL ANALYZED : "
            f"{len(rows)}"
        )

        print(
            f"DISCOVERY      : "
            f"{len(discovery)}"
        )

        print(
            f"RECENT CONTROL : "
            f"{len(control)}"
        )

        print(
            f"CONTROL ID     : "
            f">{control_start}"
        )

        print()

        print(
            "WINDOWS:"
        )

        print(
            "W1 = -30→-20s"
        )

        print(
            "W2 = -20→-10s"
        )

        print(
            "W3 = -10→0s"
        )

        print()

        print("="*125)
        print(
            "DISCOVERY — "
            "RUNNER >= +10% VS DUMP <= -10%"
        )
        print("="*125)

        print(
            f"{'FEATURE':30}"
            f"{'RUN N':>7}"
            f"{'RUN MED':>15}"
            f"{'DUMP N':>8}"
            f"{'DUMP MED':>15}"
            f"{'DIFF':>15}"
            f"{'SEP':>9}"
        )

        print("-"*125)

        for x in disc_sep[:25]:

            print(
                f"{x['feature']:30}"
                f"{x['rn']:7}"
                f"{fmt(x['rm']):>15}"
                f"{x['dn']:8}"
                f"{fmt(x['dm']):>15}"
                f"{fmt(x['diff']):>15}"
                f"{x['sep']:9.3f}"
            )

        print()
        print("="*125)
        print(
            "RECENT CONTROL — "
            "DO SEQUENCE FEATURES SURVIVE?"
        )
        print("="*125)

        print(
            f"{'FEATURE':30}"
            f"{'RUN N':>7}"
            f"{'RUN MED':>15}"
            f"{'DUMP N':>8}"
            f"{'DUMP MED':>15}"
            f"{'DIFF':>15}"
            f"{'SEP':>9}"
        )

        print("-"*125)

        for x in ctrl_sep[:25]:

            print(
                f"{x['feature']:30}"
                f"{x['rn']:7}"
                f"{fmt(x['rm']):>15}"
                f"{x['dn']:8}"
                f"{fmt(x['dm']):>15}"
                f"{fmt(x['diff']):>15}"
                f"{x['sep']:9.3f}"
            )

        # ----------------------------------------------------
        # DIRECT TRAJECTORY PROFILE
        # ----------------------------------------------------

        print()
        print("="*125)
        print(
            "SEQUENCE MEDIANS — "
            "RUNNERS VS DUMPS"
        )
        print("="*125)

        runners = [
            r for r in rows
            if r["dex_return_60s"]
            >= RUNNER
        ]

        dumps = [
            r for r in rows
            if r["dex_return_60s"]
            <= DUMP
        ]

        seq_fields = [
            "w1_buys",
            "w2_buys",
            "w3_buys",

            "w1_sells",
            "w2_sells",
            "w3_sells",

            "w1_buyers",
            "w2_buyers",
            "w3_buyers",

            "w1_buy_sol",
            "w2_buy_sol",
            "w3_buy_sol",

            "w1_sell_sol",
            "w2_sell_sol",
            "w3_sell_sol",

            "w1_net_sol",
            "w2_net_sol",
            "w3_net_sol",
        ]

        for f in seq_fields:

            rm = med([
                r[f]
                for r in runners
                if valid(r[f])
            ])

            dm = med([
                r[f]
                for r in dumps
                if valid(r[f])
            ])

            print(
                f"{f:28} "
                f"RUNNER={fmt(rm):>14} | "
                f"DUMP={fmt(dm):>14}"
            )

        print()
        print("="*125)
        print("IMPORTANT")
        print("="*125)

        print(
            "V3.3 is DISCOVERY ONLY."
        )

        print(
            "Pre-signal price comes from sampled on-chain swaps, "
            "not continuous DEX ticks."
        )

        print(
            "We care most about features that keep the SAME direction "
            "in DISCOVERY and RECENT CONTROL."
        )

        print(
            "Refresh every 20 seconds."
        )

        time.sleep(20)

    except KeyboardInterrupt:

        print(
            "\nV3.3 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)

db.close()
