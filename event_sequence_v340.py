import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
CONTROL_FRAC = 0.30

N_SWAPS = 12
BLOCK = 4


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


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


def fmt(x):
    if x is None:
        return "NA"
    return f"{x:+.4f}"


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


db = connect()


# ============================================================
# OUTPUT TABLE
# ============================================================

db.executescript("""
CREATE TABLE IF NOT EXISTS event_sequence_features_v340 (

    event_id INTEGER PRIMARY KEY,
    token_mint TEXT,
    event_timestamp REAL,

    available_swaps INTEGER,

    early_buy_count INTEGER,
    mid_buy_count INTEGER,
    recent_buy_count INTEGER,

    early_sell_count INTEGER,
    mid_sell_count INTEGER,
    recent_sell_count INTEGER,

    early_buy_sol REAL,
    mid_buy_sol REAL,
    recent_buy_sol REAL,

    early_sell_sol REAL,
    mid_sell_sol REAL,
    recent_sell_sol REAL,

    early_net_sol REAL,
    mid_net_sol REAL,
    recent_net_sol REAL,

    early_unique_buyers INTEGER,
    mid_unique_buyers INTEGER,
    recent_unique_buyers INTEGER,

    early_unique_sellers INTEGER,
    mid_unique_sellers INTEGER,
    recent_unique_sellers INTEGER,

    early_buy_concentration REAL,
    mid_buy_concentration REAL,
    recent_buy_concentration REAL,

    early_sell_concentration REAL,
    mid_sell_concentration REAL,
    recent_sell_concentration REAL,

    early_median_buy REAL,
    mid_median_buy REAL,
    recent_median_buy REAL,

    early_median_sell REAL,
    mid_median_sell REAL,
    recent_median_sell REAL,

    early_price_return REAL,
    mid_price_return REAL,
    recent_price_return REAL,

    early_duration REAL,
    mid_duration REAL,
    recent_duration REAL,

    early_swaps_per_sec REAL,
    mid_swaps_per_sec REAL,
    recent_swaps_per_sec REAL,

    buy_count_trend REAL,
    buy_sol_trend REAL,
    net_sol_trend REAL,
    buyer_diversity_trend REAL,
    buy_concentration_trend REAL,
    frequency_trend REAL,

    recent_buy_share REAL,
    recent_net_share REAL,

    price_response_recent REAL,

    late_chase_score REAL,
    breadth_score REAL,

    updated_at REAL
);
""")

db.commit()


# ============================================================
# LAST SWAPS BEFORE EVENT
# ============================================================

def last_swaps(token, ts):

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
            AND timestamp < ?

        ORDER BY timestamp DESC
        LIMIT ?
    """, (
        token,
        ts,
        N_SWAPS
    )).fetchall()[::-1]


def block_stats(rows):

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

    buy_wallets = set(
        r["wallet"]
        for r in buys
        if r["wallet"]
    )

    sell_wallets = set(
        r["wallet"]
        for r in sells
        if r["wallet"]
    )

    total_buy = sum(buy_sol)
    total_sell = sum(sell_sol)

    largest_buy = max(buy_sol) if buy_sol else 0.0
    largest_sell = max(sell_sol) if sell_sol else 0.0

    prices = [
        r["clean_price"]
        for r in rows
        if valid(r["clean_price"])
        and r["clean_price"] > 0
    ]

    price_return = None

    if len(prices) >= 2:
        price_return = (
            prices[-1] / prices[0]
            - 1
        ) * 100

    duration = None

    if len(rows) >= 2:
        duration = (
            rows[-1]["timestamp"]
            - rows[0]["timestamp"]
        )

    swaps_per_sec = None

    if valid(duration) and duration > 0:
        swaps_per_sec = (
            len(rows) / duration
        )

    return {
        "buy_count": len(buys),
        "sell_count": len(sells),

        "buy_sol": total_buy,
        "sell_sol": total_sell,
        "net_sol": total_buy-total_sell,

        "unique_buyers": len(buy_wallets),
        "unique_sellers": len(sell_wallets),

        "buy_concentration":
            safe_div(largest_buy, total_buy)
            if total_buy > 0 else 0.0,

        "sell_concentration":
            safe_div(largest_sell, total_sell)
            if total_sell > 0 else 0.0,

        "median_buy": med(buy_sol),
        "median_sell": med(sell_sol),

        "price_return": price_return,

        "duration": duration,
        "swaps_per_sec": swaps_per_sec,
    }


# ============================================================
# EVENT BUILD
# ============================================================

def build_event(event):

    eid = event["id"]
    token = event["token_mint"]
    ts = event["timestamp"]

    rows = last_swaps(
        token,
        ts
    )

    if len(rows) < N_SWAPS:
        return False

    early = block_stats(
        rows[0:4]
    )

    mid = block_stats(
        rows[4:8]
    )

    recent = block_stats(
        rows[8:12]
    )

    buy_count_trend = (
        recent["buy_count"]
        - early["buy_count"]
    )

    buy_sol_trend = (
        recent["buy_sol"]
        - early["buy_sol"]
    )

    net_sol_trend = (
        recent["net_sol"]
        - early["net_sol"]
    )

    buyer_diversity_trend = (
        recent["unique_buyers"]
        - early["unique_buyers"]
    )

    buy_concentration_trend = (
        recent["buy_concentration"]
        - early["buy_concentration"]
    )

    frequency_trend = None

    if (
        valid(recent["swaps_per_sec"])
        and valid(early["swaps_per_sec"])
    ):
        frequency_trend = (
            recent["swaps_per_sec"]
            - early["swaps_per_sec"]
        )

    total_buy = (
        early["buy_sol"]
        + mid["buy_sol"]
        + recent["buy_sol"]
    )

    recent_buy_share = (
        safe_div(
            recent["buy_sol"],
            total_buy
        )
        if total_buy > 0
        else None
    )

    total_net_abs = (
        abs(early["net_sol"])
        + abs(mid["net_sol"])
        + abs(recent["net_sol"])
    )

    recent_net_share = (
        safe_div(
            recent["net_sol"],
            total_net_abs
        )
        if total_net_abs > 0
        else None
    )

    price_response_recent = None

    if (
        valid(recent["price_return"])
        and abs(recent["net_sol"]) > 1e-9
    ):
        price_response_recent = (
            recent["price_return"]
            / recent["net_sol"]
        )

    # Late chase:
    # recent buying highly concentrated + big recent share
    late_chase_score = None

    if valid(recent_buy_share):
        late_chase_score = (
            recent_buy_share
            * (
                1
                + recent["buy_concentration"]
            )
        )

    # Breadth:
    # more distinct buyers with lower concentration
    breadth_score = (
        recent["unique_buyers"]
        * (
            1
            - recent["buy_concentration"]
        )
    )

    vals = [
        eid,
        token,
        ts,

        len(rows),

        early["buy_count"],
        mid["buy_count"],
        recent["buy_count"],

        early["sell_count"],
        mid["sell_count"],
        recent["sell_count"],

        early["buy_sol"],
        mid["buy_sol"],
        recent["buy_sol"],

        early["sell_sol"],
        mid["sell_sol"],
        recent["sell_sol"],

        early["net_sol"],
        mid["net_sol"],
        recent["net_sol"],

        early["unique_buyers"],
        mid["unique_buyers"],
        recent["unique_buyers"],

        early["unique_sellers"],
        mid["unique_sellers"],
        recent["unique_sellers"],

        early["buy_concentration"],
        mid["buy_concentration"],
        recent["buy_concentration"],

        early["sell_concentration"],
        mid["sell_concentration"],
        recent["sell_concentration"],

        early["median_buy"],
        mid["median_buy"],
        recent["median_buy"],

        early["median_sell"],
        mid["median_sell"],
        recent["median_sell"],

        early["price_return"],
        mid["price_return"],
        recent["price_return"],

        early["duration"],
        mid["duration"],
        recent["duration"],

        early["swaps_per_sec"],
        mid["swaps_per_sec"],
        recent["swaps_per_sec"],

        buy_count_trend,
        buy_sol_trend,
        net_sol_trend,
        buyer_diversity_trend,
        buy_concentration_trend,
        frequency_trend,

        recent_buy_share,
        recent_net_share,

        price_response_recent,

        late_chase_score,
        breadth_score,

        time.time(),
    ]

    placeholders = ",".join(
        ["?"] * len(vals)
    )

    db.execute(f"""
        INSERT OR REPLACE INTO event_sequence_features_v340
        VALUES ({placeholders})
    """, vals)

    db.commit()

    return True


# ============================================================
# ANALYSIS
# ============================================================

FEATURES = [
    "early_buy_count",
    "mid_buy_count",
    "recent_buy_count",

    "early_sell_count",
    "mid_sell_count",
    "recent_sell_count",

    "early_buy_sol",
    "mid_buy_sol",
    "recent_buy_sol",

    "early_sell_sol",
    "mid_sell_sol",
    "recent_sell_sol",

    "early_net_sol",
    "mid_net_sol",
    "recent_net_sol",

    "early_unique_buyers",
    "mid_unique_buyers",
    "recent_unique_buyers",

    "early_unique_sellers",
    "mid_unique_sellers",
    "recent_unique_sellers",

    "early_buy_concentration",
    "mid_buy_concentration",
    "recent_buy_concentration",

    "early_price_return",
    "mid_price_return",
    "recent_price_return",

    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",

    "buy_count_trend",
    "buy_sol_trend",
    "net_sol_trend",

    "buyer_diversity_trend",
    "buy_concentration_trend",
    "frequency_trend",

    "recent_buy_share",
    "recent_net_share",

    "price_response_recent",

    "late_chase_score",
    "breadth_score",
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

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        WHERE
            e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()


def separation(rows):

    runners = [
        r for r in rows
        if r["dex_return_60s"] >= RUNNER
    ]

    dumps = [
        r for r in rows
        if r["dex_return_60s"] <= DUMP
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
            len(rv) < 5
            or len(dv) < 5
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
        key=lambda x: x["sep"],
        reverse=True
    )


# ============================================================
# LOOP
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
                FROM event_sequence_features_v340
            """).fetchall()
        }

        pending = [
            e for e in events
            if e["id"] not in existing
        ]

        built = 0
        skipped = 0

        for i,e in enumerate(pending):

            ok = build_event(e)

            if ok:
                built += 1
            else:
                skipped += 1

            if i % 50 == 0:
                print(
                    f"Processing "
                    f"{i+1}/{len(pending)}"
                )

        rows = load_analysis()

        if rows:

            max_id = max(
                r["id"]
                for r in rows
            )

        else:
            max_id = 0

        control_start = int(
            max_id
            * (1-CONTROL_FRAC)
        )

        discovery = [
            r for r in rows
            if r["id"] <= control_start
        ]

        control = [
            r for r in rows
            if r["id"] > control_start
        ]

        disc = separation(
            discovery
        )

        ctrl = separation(
            control
        )

        os.system("clear")

        print("="*125)
        print(
            "MEMECOIN LAB — "
            "V3.4 EVENT-SEQUENCE LAB"
        )
        print("="*125)

        print(
            f"SEQUENCE EVENTS : {len(rows)}"
        )

        print(
            f"DISCOVERY       : {len(discovery)}"
        )

        print(
            f"RECENT CONTROL  : {len(control)}"
        )

        print(
            f"CONTROL ID      : >{control_start}"
        )

        print()

        print(
            "Sequence = 12 last swaps "
            "split 4 / 4 / 4."
        )

        print()

        print("="*125)
        print(
            "DISCOVERY — RUNNER VS DUMP"
        )
        print("="*125)

        print(
            f"{'FEATURE':32}"
            f"{'RUN N':>7}"
            f"{'RUN MED':>15}"
            f"{'DUMP N':>8}"
            f"{'DUMP MED':>15}"
            f"{'DIFF':>15}"
            f"{'SEP':>9}"
        )

        print("-"*125)

        for x in disc[:25]:

            print(
                f"{x['feature']:32}"
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
            "DO EVENT-SEQUENCE FEATURES SURVIVE?"
        )
        print("="*125)

        print(
            f"{'FEATURE':32}"
            f"{'RUN N':>7}"
            f"{'RUN MED':>15}"
            f"{'DUMP N':>8}"
            f"{'DUMP MED':>15}"
            f"{'DIFF':>15}"
            f"{'SEP':>9}"
        )

        print("-"*125)

        for x in ctrl[:25]:

            print(
                f"{x['feature']:32}"
                f"{x['rn']:7}"
                f"{fmt(x['rm']):>15}"
                f"{x['dn']:8}"
                f"{fmt(x['dm']):>15}"
                f"{fmt(x['diff']):>15}"
                f"{x['sep']:9.3f}"
            )

        print()
        print("="*125)
        print("KEY DYNAMICS")
        print("="*125)

        runners = [
            r for r in rows
            if r["dex_return_60s"] >= RUNNER
        ]

        dumps = [
            r for r in rows
            if r["dex_return_60s"] <= DUMP
        ]

        key = [
            "buy_count_trend",
            "buy_sol_trend",
            "net_sol_trend",
            "buyer_diversity_trend",
            "buy_concentration_trend",
            "frequency_trend",
            "recent_buy_share",
            "price_response_recent",
            "late_chase_score",
            "breadth_score",
        ]

        for f in key:

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
                f"{f:30} "
                f"RUNNER={fmt(rm):>14} | "
                f"DUMP={fmt(dm):>14}"
            )

        print()
        print("="*125)
        print("IMPORTANT")
        print("="*125)

        print(
            "V3.4 is discovery only."
        )

        print(
            "More useful than fixed-time windows "
            "because every sequence contains actual swaps."
        )

        print(
            "Focus on features that keep the SAME direction "
            "in discovery and recent control."
        )

        print(
            "Refresh every 20 seconds."
        )

        time.sleep(20)

    except KeyboardInterrupt:

        print(
            "\nV3.4 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)

db.close()
