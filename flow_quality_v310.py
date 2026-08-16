import sqlite3
import statistics
import math
import time
import os

DB = "validation_v090.db"

WINDOWS = [10, 30, 60]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def safe_div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


db = connect()

# ============================================================
# FEATURE TABLE
# ============================================================

db.executescript("""
CREATE TABLE IF NOT EXISTS flow_quality_features (

    event_id INTEGER PRIMARY KEY,
    token_mint TEXT,
    event_timestamp REAL,

    dex_liquidity_usd REAL,

    buys_10 INTEGER,
    sells_10 INTEGER,
    buys_30 INTEGER,
    sells_30 INTEGER,
    buys_60 INTEGER,
    sells_60 INTEGER,

    buy_wallets_10 INTEGER,
    sell_wallets_10 INTEGER,
    buy_wallets_30 INTEGER,
    sell_wallets_30 INTEGER,
    buy_wallets_60 INTEGER,
    sell_wallets_60 INTEGER,

    buy_sol_10 REAL,
    sell_sol_10 REAL,
    buy_sol_30 REAL,
    sell_sol_30 REAL,
    buy_sol_60 REAL,
    sell_sol_60 REAL,

    median_buy_sol_30 REAL,
    median_sell_sol_30 REAL,

    largest_buy_sol_30 REAL,
    largest_sell_sol_30 REAL,

    buy_sell_sol_ratio_30 REAL,
    net_sol_30 REAL,

    buyer_repeat_ratio_30 REAL,
    seller_repeat_ratio_30 REAL,

    buy_concentration_sol_30 REAL,
    sell_concentration_sol_30 REAL,

    buy_pressure_liq_30 REAL,
    sell_pressure_liq_30 REAL,
    net_pressure_liq_30 REAL,

    return_pre_10s REAL,
    return_pre_30s REAL,

    price_response_per_net_sol REAL,

    absorption_score REAL,

    updated_at REAL
);
""")

db.commit()


# ============================================================
# FIRST DEX SNAPSHOT
# ============================================================

def first_dex(event_id):

    return db.execute("""
        SELECT
            timestamp,
            liquidity_usd,
            price_usd

        FROM dex_prices

        WHERE event_id=?

        ORDER BY timestamp ASC
        LIMIT 1
    """, (event_id,)).fetchone()


# ============================================================
# PRICE BEFORE EVENT
# ============================================================

def dex_price_before(event_id, target):

    row = db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND timestamp <= ?

        ORDER BY timestamp DESC
        LIMIT 1
    """, (
        event_id,
        target
    )).fetchone()

    if not row:
        return None

    return row["price_usd"]


# ============================================================
# SWAP WINDOW
# ============================================================

def swaps_for(token, ts, seconds):

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
            AND timestamp BETWEEN ? AND ?

        ORDER BY timestamp ASC
    """, (
        token,
        ts-seconds,
        ts
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

    buy_wallets = set(
        r["wallet"] for r in buys
        if r["wallet"]
    )

    sell_wallets = set(
        r["wallet"] for r in sells
        if r["wallet"]
    )

    buy_sol = [
        r["sol"] for r in buys
        if valid(r["sol"])
    ]

    sell_sol = [
        r["sol"] for r in sells
        if valid(r["sol"])
    ]

    total_buy = sum(buy_sol)
    total_sell = sum(sell_sol)

    largest_buy = (
        max(buy_sol)
        if buy_sol
        else 0.0
    )

    largest_sell = (
        max(sell_sol)
        if sell_sol
        else 0.0
    )

    buy_counts = {}
    sell_counts = {}

    for r in buys:
        w = r["wallet"]
        if w:
            buy_counts[w] = buy_counts.get(w,0) + 1

    for r in sells:
        w = r["wallet"]
        if w:
            sell_counts[w] = sell_counts.get(w,0) + 1

    repeat_buyers = sum(
        1 for n in buy_counts.values()
        if n >= 2
    )

    repeat_sellers = sum(
        1 for n in sell_counts.values()
        if n >= 2
    )

    buyer_repeat_ratio = (
        repeat_buyers / len(buy_wallets)
        if buy_wallets
        else 0.0
    )

    seller_repeat_ratio = (
        repeat_sellers / len(sell_wallets)
        if sell_wallets
        else 0.0
    )

    return {
        "buys": len(buys),
        "sells": len(sells),

        "buy_wallets": len(buy_wallets),
        "sell_wallets": len(sell_wallets),

        "buy_sol": total_buy,
        "sell_sol": total_sell,

        "median_buy": med(buy_sol),
        "median_sell": med(sell_sol),

        "largest_buy": largest_buy,
        "largest_sell": largest_sell,

        "buy_sell_ratio":
            safe_div(total_buy, total_sell)
            if total_sell > 0
            else (
                float("inf")
                if total_buy > 0
                else None
            ),

        "net_sol":
            total_buy - total_sell,

        "buyer_repeat_ratio":
            buyer_repeat_ratio,

        "seller_repeat_ratio":
            seller_repeat_ratio,

        "buy_concentration":
            safe_div(largest_buy, total_buy)
            if total_buy > 0
            else 0.0,

        "sell_concentration":
            safe_div(largest_sell, total_sell)
            if total_sell > 0
            else 0.0,
    }


# ============================================================
# EVENT FEATURE BUILDER
# ============================================================

def build_event(event):

    eid = event["id"]
    token = event["token_mint"]
    ts = event["timestamp"]

    dex = first_dex(eid)

    liquidity = (
        dex["liquidity_usd"]
        if dex
        else None
    )

    w10 = window_stats(
        swaps_for(
            token,
            ts,
            10
        )
    )

    w30 = window_stats(
        swaps_for(
            token,
            ts,
            30
        )
    )

    w60 = window_stats(
        swaps_for(
            token,
            ts,
            60
        )
    )

    entry = (
        dex["price_usd"]
        if dex
        else None
    )

    p10 = dex_price_before(
        eid,
        ts - 10
    )

    p30 = dex_price_before(
        eid,
        ts - 30
    )

    rpre10 = None
    rpre30 = None

    if valid(entry) and entry > 0:

        if valid(p10) and p10 > 0:
            rpre10 = (
                entry/p10 - 1
            ) * 100

        if valid(p30) and p30 > 0:
            rpre30 = (
                entry/p30 - 1
            ) * 100

    buy_pressure = None
    sell_pressure = None
    net_pressure = None

    # This is only a rough proxy because SOL volume
    # and USD liquidity are not in the same unit.
    # Useful comparatively, not as absolute impact.
    if valid(liquidity) and liquidity > 0:

        buy_pressure = (
            w30["buy_sol"]
            / liquidity
        )

        sell_pressure = (
            w30["sell_sol"]
            / liquidity
        )

        net_pressure = (
            w30["net_sol"]
            / liquidity
        )

    price_response = None

    if (
        valid(rpre10)
        and valid(w30["net_sol"])
        and abs(w30["net_sol"]) > 1e-9
    ):
        price_response = (
            rpre10
            / w30["net_sol"]
        )

    # High net buying with little positive price response
    # can be a crude absorption warning.
    absorption = None

    if (
        valid(w30["net_sol"])
        and w30["net_sol"] > 0
        and valid(rpre10)
    ):

        absorption = (
            w30["net_sol"]
            / max(
                rpre10,
                0.10
            )
        )

    db.execute("""
        INSERT INTO flow_quality_features (

            event_id,
            token_mint,
            event_timestamp,

            dex_liquidity_usd,

            buys_10,
            sells_10,
            buys_30,
            sells_30,
            buys_60,
            sells_60,

            buy_wallets_10,
            sell_wallets_10,
            buy_wallets_30,
            sell_wallets_30,
            buy_wallets_60,
            sell_wallets_60,

            buy_sol_10,
            sell_sol_10,
            buy_sol_30,
            sell_sol_30,
            buy_sol_60,
            sell_sol_60,

            median_buy_sol_30,
            median_sell_sol_30,

            largest_buy_sol_30,
            largest_sell_sol_30,

            buy_sell_sol_ratio_30,
            net_sol_30,

            buyer_repeat_ratio_30,
            seller_repeat_ratio_30,

            buy_concentration_sol_30,
            sell_concentration_sol_30,

            buy_pressure_liq_30,
            sell_pressure_liq_30,
            net_pressure_liq_30,

            return_pre_10s,
            return_pre_30s,

            price_response_per_net_sol,

            absorption_score,

            updated_at
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?
        )

        ON CONFLICT(event_id)
        DO UPDATE SET

            dex_liquidity_usd=excluded.dex_liquidity_usd,

            buys_10=excluded.buys_10,
            sells_10=excluded.sells_10,

            buys_30=excluded.buys_30,
            sells_30=excluded.sells_30,

            buys_60=excluded.buys_60,
            sells_60=excluded.sells_60,

            buy_wallets_10=excluded.buy_wallets_10,
            sell_wallets_10=excluded.sell_wallets_10,

            buy_wallets_30=excluded.buy_wallets_30,
            sell_wallets_30=excluded.sell_wallets_30,

            buy_wallets_60=excluded.buy_wallets_60,
            sell_wallets_60=excluded.sell_wallets_60,

            buy_sol_10=excluded.buy_sol_10,
            sell_sol_10=excluded.sell_sol_10,

            buy_sol_30=excluded.buy_sol_30,
            sell_sol_30=excluded.sell_sol_30,

            buy_sol_60=excluded.buy_sol_60,
            sell_sol_60=excluded.sell_sol_60,

            median_buy_sol_30=excluded.median_buy_sol_30,
            median_sell_sol_30=excluded.median_sell_sol_30,

            largest_buy_sol_30=excluded.largest_buy_sol_30,
            largest_sell_sol_30=excluded.largest_sell_sol_30,

            buy_sell_sol_ratio_30=excluded.buy_sell_sol_ratio_30,
            net_sol_30=excluded.net_sol_30,

            buyer_repeat_ratio_30=excluded.buyer_repeat_ratio_30,
            seller_repeat_ratio_30=excluded.seller_repeat_ratio_30,

            buy_concentration_sol_30=excluded.buy_concentration_sol_30,
            sell_concentration_sol_30=excluded.sell_concentration_sol_30,

            buy_pressure_liq_30=excluded.buy_pressure_liq_30,
            sell_pressure_liq_30=excluded.sell_pressure_liq_30,
            net_pressure_liq_30=excluded.net_pressure_liq_30,

            return_pre_10s=excluded.return_pre_10s,
            return_pre_30s=excluded.return_pre_30s,

            price_response_per_net_sol=
                excluded.price_response_per_net_sol,

            absorption_score=
                excluded.absorption_score,

            updated_at=
                excluded.updated_at
    """, (

        eid,
        token,
        ts,

        liquidity,

        w10["buys"],
        w10["sells"],
        w30["buys"],
        w30["sells"],
        w60["buys"],
        w60["sells"],

        w10["buy_wallets"],
        w10["sell_wallets"],
        w30["buy_wallets"],
        w30["sell_wallets"],
        w60["buy_wallets"],
        w60["sell_wallets"],

        w10["buy_sol"],
        w10["sell_sol"],
        w30["buy_sol"],
        w30["sell_sol"],
        w60["buy_sol"],
        w60["sell_sol"],

        w30["median_buy"],
        w30["median_sell"],

        w30["largest_buy"],
        w30["largest_sell"],

        w30["buy_sell_ratio"],
        w30["net_sol"],

        w30["buyer_repeat_ratio"],
        w30["seller_repeat_ratio"],

        w30["buy_concentration"],
        w30["sell_concentration"],

        buy_pressure,
        sell_pressure,
        net_pressure,

        rpre10,
        rpre30,

        price_response,

        absorption,

        time.time()
    ))

    db.commit()


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

        done = {
            r["event_id"]
            for r in db.execute("""
                SELECT event_id
                FROM flow_quality_features
            """).fetchall()
        }

        pending = [
            e for e in events
            if e["id"] not in done
        ]

        for i, event in enumerate(pending):

            build_event(event)

            if i % 25 == 0:
                print(
                    f"Building "
                    f"{i+1}/{len(pending)}..."
                )

        total = db.execute("""
            SELECT COUNT(*)
            FROM flow_quality_features
        """).fetchone()[0]

        max_event = db.execute("""
            SELECT COALESCE(MAX(id),0)
            FROM events
        """).fetchone()[0]

        os.system("clear")

        print("=" * 92)
        print(
            "MEMECOIN LAB — "
            "V3.1 FLOW QUALITY COLLECTOR"
        )
        print("=" * 92)

        print(
            f"EVENTS DB       : {max_event}"
        )

        print(
            f"QUALITY FEATURES: {total}"
        )

        print(
            f"PENDING         : "
            f"{max_event-total}"
        )

        print()

        print(
            "Captured:"
        )

        print(
            "• buy/sell count"
        )

        print(
            "• unique buy/sell wallets"
        )

        print(
            "• buy/sell SOL volume"
        )

        print(
            "• median / largest trade"
        )

        print(
            "• repeat buyer/seller ratios"
        )

        print(
            "• buy/sell concentration"
        )

        print(
            "• pressure / liquidity proxy"
        )

        print(
            "• pre-signal price response"
        )

        print(
            "• crude absorption score"
        )

        print()
        print(
            "Refresh every 10 seconds."
        )

        time.sleep(10)

    except KeyboardInterrupt:

        print(
            "\nV3.1 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)

db.close()
