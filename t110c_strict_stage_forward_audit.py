#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

TABLE = "t110c_strict_stage_forward"

REFRESH = 10

STAGES = [30, 60]
FORWARD_HORIZONS = [300, 900]


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def pct(a, b):
    if (
        not valid(a)
        or not valid(b)
        or a <= 0
    ):
        return None

    return 100.0 * (
        b / a - 1.0
    )


def fmt(x, n=1):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# OUTPUT TABLE
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    t108_event_id INTEGER NOT NULL,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    requested_stage_seconds INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,
    trigger_price REAL NOT NULL,

    old_peak_price REAL,
    old_peak_at REAL,

    requested_stage_timestamp REAL,

    actual_stage_timestamp REAL,
    actual_stage_delay_s REAL,

    stage_price REAL,

    stage_return_from_trigger REAL,

    -- Path known BEFORE / AT entry
    pre_stage_new_low INTEGER,
    pre_stage_reclaimed_trigger INTEGER,
    pre_stage_reclaimed_old_peak INTEGER,

    pre_stage_mfe REAL,
    pre_stage_mae REAL,

    pre_stage_high_time_s REAL,
    pre_stage_low_time_s REAL,

    -- Flow known by stage
    pre60_swaps INTEGER,
    pre60_buys INTEGER,
    pre60_sells INTEGER,
    pre60_buy_sol REAL,
    pre60_sell_sol REAL,
    pre60_net_sol REAL,
    pre60_buy_share REAL,

    post_stage_swaps INTEGER,
    post_stage_buys INTEGER,
    post_stage_sells INTEGER,
    post_stage_buy_sol REAL,
    post_stage_sell_sol REAL,
    post_stage_net_sol REAL,
    post_stage_buy_share REAL,

    net_sol_rate_shift REAL,
    buy_share_shift REAL,

    liquidity_at_dump REAL,
    market_cap_at_dump REAL,
    volume_m5_at_dump REAL,

    liquidity_at_stage REAL,
    market_cap_at_stage REAL,
    volume_m5_at_stage REAL,

    liquidity_change_to_stage REAL,
    market_cap_change_to_stage REAL,
    volume_change_to_stage REAL,

    -- =====================================================
    -- STRICTLY FORWARD OUTCOMES FROM ACTUAL ENTRY TIMESTAMP
    -- =====================================================

    end_price_300 REAL,
    end_return_300 REAL,
    max_return_300 REAL,
    min_return_300 REAL,
    high_time_after_entry_300 REAL,
    low_time_after_entry_300 REAL,
    reclaim_old_peak_after_entry_300 INTEGER,
    up10_after_entry_300 INTEGER,
    up20_after_entry_300 INTEGER,
    up30_after_entry_300 INTEGER,
    up50_after_entry_300 INTEGER,
    snapshots_after_entry_300 INTEGER,
    max_gap_after_entry_300 REAL,
    mature_300 INTEGER NOT NULL DEFAULT 0,

    end_price_900 REAL,
    end_return_900 REAL,
    max_return_900 REAL,
    min_return_900 REAL,
    high_time_after_entry_900 REAL,
    low_time_after_entry_900 REAL,
    reclaim_old_peak_after_entry_900 INTEGER,
    up10_after_entry_900 INTEGER,
    up20_after_entry_900 INTEGER,
    up30_after_entry_900 INTEGER,
    up50_after_entry_900 INTEGER,
    snapshots_after_entry_900 INTEGER,
    max_gap_after_entry_900 REAL,
    mature_900 INTEGER NOT NULL DEFAULT 0,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL,

    UNIQUE(
        t108_event_id,
        requested_stage_seconds
    )
)
""")

db.commit()


# ============================================================
# SOURCES
# ============================================================

def events():

    return db.execute("""
    SELECT
        e.*,

        b.pre60_swaps,
        b.pre60_buys,
        b.pre60_sells,
        b.pre60_buy_sol,
        b.pre60_sell_sol,
        b.pre60_net_sol,
        b.pre60_buy_share,

        b.post30_swaps,
        b.post30_buys,
        b.post30_sells,
        b.post30_buy_sol,
        b.post30_sell_sol,
        b.post30_net_sol,
        b.post30_buy_share,

        b.post30_60_swaps,
        b.post30_60_buys,
        b.post30_60_sells,
        b.post30_60_buy_sol,
        b.post30_60_sell_sol,
        b.post30_60_net_sol,
        b.post30_60_buy_share

    FROM t108_dump_events e

    JOIN t109b_dump_features b
      ON b.t108_event_id=e.id

    ORDER BY e.trigger_timestamp
    """).fetchall()


def first_dex_after(mint, target):

    return db.execute("""
    SELECT
        timestamp,
        price_usd,
        liquidity_usd,
        market_cap,
        volume_m5

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp >= ?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp ASC

    LIMIT 1
    """, (
        mint,
        target
    )).fetchone()


def path_between(
    mint,
    start_ts,
    end_ts
):

    return db.execute("""
    SELECT
        timestamp,
        price_usd

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp <= ?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp ASC
    """, (
        mint,
        start_ts,
        end_ts
    )).fetchall()


# ============================================================
# FLOW WINDOW
# ============================================================

def flow_window(
    mint,
    start_ts,
    end_ts
):

    rows = db.execute("""
    SELECT
        side,
        sol_delta

    FROM swaps

    WHERE
        token_mint=?
        AND program='PUMPSWAP'
        AND timestamp >= ?
        AND timestamp < ?

    ORDER BY timestamp
    """, (
        mint,
        start_ts,
        end_ts
    )).fetchall()


    buys = 0
    sells = 0

    buy_sol = 0.0
    sell_sol = 0.0


    for r in rows:

        amount = abs(
            r["sol_delta"] or 0
        )

        if r["side"] == "BUY":
            buys += 1
            buy_sol += amount

        elif r["side"] == "SELL":
            sells += 1
            sell_sol += amount


    total = buys + sells


    return {
        "swaps": len(rows),
        "buys": buys,
        "sells": sells,
        "buy_sol": buy_sol,
        "sell_sol": sell_sol,
        "net_sol": buy_sol - sell_sol,
        "buy_share":
            buys / total
            if total > 0
            else None,
    }


# ============================================================
# PRE-STAGE PATH
# ============================================================

def pre_stage_path(
    e,
    actual_stage_ts
):

    rows = path_between(
        e["token_mint"],
        e["trigger_timestamp"],
        actual_stage_ts
    )


    if not rows:
        return None


    prices = [
        (
            r["timestamp"],
            r["price_usd"]
        )
        for r in rows
    ]


    high_ts, high_price = max(
        prices,
        key=lambda x: x[1]
    )

    low_ts, low_price = min(
        prices,
        key=lambda x: x[1]
    )


    reclaim_trigger = int(
        any(
            ts > e["trigger_timestamp"]
            and price >= e["trigger_price"]

            for ts, price in prices
        )
    )


    reclaim_peak = int(
        any(
            price >= e["peak_price"]
            for _, price in prices
        )
    )


    new_low = int(
        any(
            price < e["trigger_price"]
            for _, price in prices
        )
    )


    return {
        "new_low":
            new_low,

        "reclaim_trigger":
            reclaim_trigger,

        "reclaim_peak":
            reclaim_peak,

        "mfe":
            pct(
                e["trigger_price"],
                high_price
            ),

        "mae":
            pct(
                e["trigger_price"],
                low_price
            ),

        "high_time":
            high_ts
            - e["trigger_timestamp"],

        "low_time":
            low_ts
            - e["trigger_timestamp"],
    }


# ============================================================
# STRICT FORWARD PATH
# ============================================================

def forward_path(
    mint,
    entry_ts,
    entry_price,
    old_peak,
    horizon
):

    target = (
        entry_ts
        + horizon
    )


    if time.time() < target:
        return None


    rows = path_between(
        mint,
        entry_ts,
        target
    )


    if not rows:
        return {
            "mature": 1,
            "snapshots": 0
        }


    prices = [
        (
            r["timestamp"],
            r["price_usd"]
        )
        for r in rows
    ]


    high_ts, high_price = max(
        prices,
        key=lambda x: x[1]
    )

    low_ts, low_price = min(
        prices,
        key=lambda x: x[1]
    )

    end_ts, end_price = prices[-1]


    # Coverage quality
    gaps = []

    prev = entry_ts

    for ts, _ in prices:
        gaps.append(
            ts - prev
        )
        prev = ts

    gaps.append(
        target - prev
    )

    max_gap = (
        max(gaps)
        if gaps
        else None
    )


    max_ret = pct(
        entry_price,
        high_price
    )

    min_ret = pct(
        entry_price,
        low_price
    )

    end_ret = pct(
        entry_price,
        end_price
    )


    return {

        "mature":
            1,

        "snapshots":
            len(prices),

        "max_gap":
            max_gap,

        "end_price":
            end_price,

        "end_return":
            end_ret,

        "max_return":
            max_ret,

        "min_return":
            min_ret,

        "high_time":
            high_ts
            - entry_ts,

        "low_time":
            low_ts
            - entry_ts,

        "reclaim_peak":
            int(
                any(
                    price >= old_peak
                    for _, price in prices
                )
            ),

        "up10":
            int(
                max_ret is not None
                and max_ret >= 10
            ),

        "up20":
            int(
                max_ret is not None
                and max_ret >= 20
            ),

        "up30":
            int(
                max_ret is not None
                and max_ret >= 30
            ),

        "up50":
            int(
                max_ret is not None
                and max_ret >= 50
            ),
    }


# ============================================================
# BUILD STAGE
# ============================================================

def build_stage(
    e,
    stage
):

    requested_ts = (
        e["trigger_timestamp"]
        + stage
    )


    # Need first actual DEX observation at/after requested stage
    stage_snap = first_dex_after(
        e["token_mint"],
        requested_ts
    )


    if not stage_snap:
        return


    actual_ts = stage_snap[
        "timestamp"
    ]

    actual_delay = (
        actual_ts
        - requested_ts
    )


    # Reject absurdly delayed confirmations.
    # We keep the row but clearly expose the delay.
    stage_price = stage_snap[
        "price_usd"
    ]


    prepath = pre_stage_path(
        e,
        actual_ts
    )

    if not prepath:
        return


    # --------------------------------------------------------
    # FLOW KNOWN UP TO ACTUAL STAGE
    # --------------------------------------------------------

    postflow = flow_window(
        e["token_mint"],
        e["trigger_timestamp"],
        actual_ts
    )


    pre_net_rate = (
        e["pre60_net_sol"] / 60
        if e["pre60_net_sol"] is not None
        else None
    )

    post_duration = (
        actual_ts
        - e["trigger_timestamp"]
    )

    post_net_rate = (
        postflow["net_sol"]
        / post_duration
        if post_duration > 0
        else None
    )


    net_shift = (
        post_net_rate - pre_net_rate

        if (
            pre_net_rate is not None
            and post_net_rate is not None
        )

        else None
    )


    buy_share_shift = (
        postflow["buy_share"]
        - e["pre60_buy_share"]

        if (
            postflow["buy_share"] is not None
            and e["pre60_buy_share"] is not None
        )

        else None
    )


    # --------------------------------------------------------
    # STRICT FORWARD OUTCOMES
    # --------------------------------------------------------

    f300 = forward_path(
        e["token_mint"],
        actual_ts,
        stage_price,
        e["peak_price"],
        300
    )


    f900 = forward_path(
        e["token_mint"],
        actual_ts,
        stage_price,
        e["peak_price"],
        900
    )


    # --------------------------------------------------------
    # STORE BASE
    # --------------------------------------------------------

    db.execute(f"""
    INSERT INTO {TABLE} (

        t108_event_id,

        token_mint,

        dump_level,

        requested_stage_seconds,

        trigger_timestamp,
        trigger_price,

        old_peak_price,
        old_peak_at,

        requested_stage_timestamp,

        actual_stage_timestamp,
        actual_stage_delay_s,

        stage_price,

        stage_return_from_trigger,

        pre_stage_new_low,
        pre_stage_reclaimed_trigger,
        pre_stage_reclaimed_old_peak,

        pre_stage_mfe,
        pre_stage_mae,

        pre_stage_high_time_s,
        pre_stage_low_time_s,

        pre60_swaps,
        pre60_buys,
        pre60_sells,
        pre60_buy_sol,
        pre60_sell_sol,
        pre60_net_sol,
        pre60_buy_share,

        post_stage_swaps,
        post_stage_buys,
        post_stage_sells,
        post_stage_buy_sol,
        post_stage_sell_sol,
        post_stage_net_sol,
        post_stage_buy_share,

        net_sol_rate_shift,
        buy_share_shift,

        liquidity_at_dump,
        market_cap_at_dump,
        volume_m5_at_dump,

        liquidity_at_stage,
        market_cap_at_stage,
        volume_m5_at_stage,

        liquidity_change_to_stage,
        market_cap_change_to_stage,
        volume_change_to_stage,

        created_at,
        last_update_at
    )

    VALUES (
        ?,
        ?,
        ?,
        ?,
        ?,?,
        ?,?,
        ?,
        ?,?,
        ?,
        ?,
        ?,?,?,
        ?,?,
        ?,?,
        ?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,
        ?,?,
        ?,?,?,
        ?,?,?,
        ?,?,?,
        ?,?
    )

    ON CONFLICT(
        t108_event_id,
        requested_stage_seconds
    )

    DO UPDATE SET

        actual_stage_timestamp=
            excluded.actual_stage_timestamp,

        actual_stage_delay_s=
            excluded.actual_stage_delay_s,

        stage_price=
            excluded.stage_price,

        stage_return_from_trigger=
            excluded.stage_return_from_trigger,

        pre_stage_new_low=
            excluded.pre_stage_new_low,

        pre_stage_reclaimed_trigger=
            excluded.pre_stage_reclaimed_trigger,

        pre_stage_reclaimed_old_peak=
            excluded.pre_stage_reclaimed_old_peak,

        pre_stage_mfe=
            excluded.pre_stage_mfe,

        pre_stage_mae=
            excluded.pre_stage_mae,

        post_stage_swaps=
            excluded.post_stage_swaps,

        post_stage_buys=
            excluded.post_stage_buys,

        post_stage_sells=
            excluded.post_stage_sells,

        post_stage_buy_sol=
            excluded.post_stage_buy_sol,

        post_stage_sell_sol=
            excluded.post_stage_sell_sol,

        post_stage_net_sol=
            excluded.post_stage_net_sol,

        post_stage_buy_share=
            excluded.post_stage_buy_share,

        net_sol_rate_shift=
            excluded.net_sol_rate_shift,

        buy_share_shift=
            excluded.buy_share_shift,

        liquidity_at_stage=
            excluded.liquidity_at_stage,

        market_cap_at_stage=
            excluded.market_cap_at_stage,

        volume_m5_at_stage=
            excluded.volume_m5_at_stage,

        liquidity_change_to_stage=
            excluded.liquidity_change_to_stage,

        market_cap_change_to_stage=
            excluded.market_cap_change_to_stage,

        volume_change_to_stage=
            excluded.volume_change_to_stage,

        last_update_at=
            excluded.last_update_at
    """, (

        e["id"],

        e["token_mint"],

        e["dump_level"],

        stage,

        e["trigger_timestamp"],
        e["trigger_price"],

        e["peak_price"],
        e["peak_at"],

        requested_ts,

        actual_ts,
        actual_delay,

        stage_price,

        pct(
            e["trigger_price"],
            stage_price
        ),

        prepath["new_low"],
        prepath["reclaim_trigger"],
        prepath["reclaim_peak"],

        prepath["mfe"],
        prepath["mae"],

        prepath["high_time"],
        prepath["low_time"],

        e["pre60_swaps"],
        e["pre60_buys"],
        e["pre60_sells"],
        e["pre60_buy_sol"],
        e["pre60_sell_sol"],
        e["pre60_net_sol"],
        e["pre60_buy_share"],

        postflow["swaps"],
        postflow["buys"],
        postflow["sells"],
        postflow["buy_sol"],
        postflow["sell_sol"],
        postflow["net_sol"],
        postflow["buy_share"],

        net_shift,
        buy_share_shift,

        e["liquidity_usd"],
        e["market_cap"],
        e["volume_m5"],

        stage_snap["liquidity_usd"],
        stage_snap["market_cap"],
        stage_snap["volume_m5"],

        pct(
            e["liquidity_usd"],
            stage_snap["liquidity_usd"]
        ),

        pct(
            e["market_cap"],
            stage_snap["market_cap"]
        ),

        pct(
            e["volume_m5"],
            stage_snap["volume_m5"]
        ),

        time.time(),
        time.time(),
    ))

    db.commit()


    # --------------------------------------------------------
    # UPDATE FORWARD 300
    # --------------------------------------------------------

    if f300:

        db.execute(f"""
        UPDATE {TABLE}

        SET
            end_price_300=?,
            end_return_300=?,
            max_return_300=?,
            min_return_300=?,

            high_time_after_entry_300=?,
            low_time_after_entry_300=?,

            reclaim_old_peak_after_entry_300=?,

            up10_after_entry_300=?,
            up20_after_entry_300=?,
            up30_after_entry_300=?,
            up50_after_entry_300=?,

            snapshots_after_entry_300=?,
            max_gap_after_entry_300=?,

            mature_300=?,

            last_update_at=?

        WHERE
            t108_event_id=?
            AND requested_stage_seconds=?
        """, (

            f300.get("end_price"),
            f300.get("end_return"),
            f300.get("max_return"),
            f300.get("min_return"),

            f300.get("high_time"),
            f300.get("low_time"),

            f300.get("reclaim_peak"),

            f300.get("up10"),
            f300.get("up20"),
            f300.get("up30"),
            f300.get("up50"),

            f300.get("snapshots"),
            f300.get("max_gap"),

            f300.get("mature", 0),

            time.time(),

            e["id"],
            stage,
        ))

        db.commit()


    # --------------------------------------------------------
    # UPDATE FORWARD 900
    # --------------------------------------------------------

    if f900:

        db.execute(f"""
        UPDATE {TABLE}

        SET
            end_price_900=?,
            end_return_900=?,
            max_return_900=?,
            min_return_900=?,

            high_time_after_entry_900=?,
            low_time_after_entry_900=?,

            reclaim_old_peak_after_entry_900=?,

            up10_after_entry_900=?,
            up20_after_entry_900=?,
            up30_after_entry_900=?,
            up50_after_entry_900=?,

            snapshots_after_entry_900=?,
            max_gap_after_entry_900=?,

            mature_900=?,

            last_update_at=?

        WHERE
            t108_event_id=?
            AND requested_stage_seconds=?
        """, (

            f900.get("end_price"),
            f900.get("end_return"),
            f900.get("max_return"),
            f900.get("min_return"),

            f900.get("high_time"),
            f900.get("low_time"),

            f900.get("reclaim_peak"),

            f900.get("up10"),
            f900.get("up20"),
            f900.get("up30"),
            f900.get("up50"),

            f900.get("snapshots"),
            f900.get("max_gap"),

            f900.get("mature", 0),

            time.time(),

            e["id"],
            stage,
        ))

        db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")


    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    ORDER BY
        trigger_timestamp DESC,
        requested_stage_seconds
    """).fetchall()


    print("=" * 195)

    print(
        "MEMECOIN LAB — T110C STRICT STAGE-FORWARD AUDIT"
    )

    print("=" * 195)


    print(
        f"ROWS               : {len(rows)}"
    )

    print(
        f"UNIQUE DUMPS       : "
        f"{len(set(r['t108_event_id'] for r in rows))}"
    )

    print(
        f"UNIQUE TOKENS      : "
        f"{len(set(r['token_mint'] for r in rows))}"
    )


    print()

    print(
        "ENTRY PRICE        : FIRST DEX SNAPSHOT AT/AFTER REQUESTED STAGE"
    )

    print(
        "FORWARD OUTCOMES   : STRICTLY AFTER ACTUAL ENTRY TIMESTAMP"
    )

    print(
        "MODEL FITTING      : NONE"
    )


    print()
    print("=" * 195)
    print("ACTUAL CONFIRMATION TIMING")
    print("=" * 195)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| REQ={r['requested_stage_seconds']:2d}s "
            f"| DELAY={fmt(r['actual_stage_delay_s'],0):>4}s "
            f"| ACTUAL="
            f"{(r['actual_stage_timestamp'] - r['trigger_timestamp']):5.0f}s "
            f"| NOW={fmt(r['stage_return_from_trigger']):>7}% "
            f"| LOW={r['pre_stage_new_low']} "
            f"| RECLAIM={r['pre_stage_reclaimed_trigger']} "
            f"| PEAK={r['pre_stage_reclaimed_old_peak']}"
        )


    print()
    print("=" * 195)
    print("STRICT FUTURE FROM ACTUAL ENTRY — 300s")
    print("=" * 195)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| REQ={r['requested_stage_seconds']:2d}s "
            f"| END={fmt(r['end_return_300']):>8}% "
            f"| MAX={fmt(r['max_return_300']):>8}% "
            f"| MIN={fmt(r['min_return_300']):>8}% "
            f"| +20={str(r['up20_after_entry_300']):>4} "
            f"| +50={str(r['up50_after_entry_300']):>4} "
            f"| PEAK={str(r['reclaim_old_peak_after_entry_300']):>4} "
            f"| N={str(r['snapshots_after_entry_300']):>3} "
            f"| GAP={fmt(r['max_gap_after_entry_300'],0):>4}s"
        )


    print()
    print("=" * 195)
    print("READINESS")
    print("=" * 195)


    mature = [
        r for r in rows
        if r["mature_300"]
    ]


    usable = [
        r for r in mature
        if (
            r["actual_stage_delay_s"] is not None
            and r["actual_stage_delay_s"] <= 35
            and r["max_gap_after_entry_300"] is not None
            and r["max_gap_after_entry_300"] <= 60
        )
    ]


    tokens = {
        r["token_mint"]
        for r in usable
    }


    print(
        f"MATURE 300       : {len(mature)}"
    )

    print(
        f"STRICT USABLE    : {len(usable)}"
    )

    print(
        f"USABLE TOKENS    : {len(tokens)}"
    )


    if len(tokens) >= 50:

        print(
            "🟢 READY FOR CONTROLLED STAGE-B CONFIRMATION DISCOVERY."
        )

    elif len(tokens) >= 20:

        print(
            "🟡 DESCRIPTIVE STAGE-B ANALYSIS BECOMING USEFUL."
        )

    else:

        print(
            "🔵 COLLECTING STRICT CONFIRMED-REVERSAL CASES."
        )


    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T110C only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for e in events():

            for stage in STAGES:

                build_stage(
                    e,
                    stage
                )


        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T110C stopped safely."
    )


finally:

    db.close()
