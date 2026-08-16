#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

SOURCE = "t108_dump_events"
TABLE = "t109b_dump_features"

REFRESH = 10

WINDOWS = {
    "pre60": (-60, 0),
    "post30": (0, 30),
    "post30_60": (30, 60),
    "post60_300": (60, 300),
}

FLOW_SAMPLE_PERCENT = 10.0
FLOW_SAMPLE_RATE = FLOW_SAMPLE_PERCENT / 100.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def div(a, b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a / b


def pct(a, b):
    if not valid(a) or not valid(b) or a <= 0:
        return None
    return 100.0 * (b / a - 1.0)


def median(xs):
    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    n = len(xs)

    if n % 2:
        return xs[n // 2]

    return (
        xs[n // 2 - 1]
        + xs[n // 2]
    ) / 2.0


def concentration(values):
    values = [
        abs(x)
        for x in values
        if valid(x)
        and abs(x) > 0
    ]

    if not values:
        return None

    total = sum(values)

    if total <= 0:
        return None

    return max(values) / total


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLE
# ============================================================

window_cols = []

for name in WINDOWS:

    window_cols.append(f"""
        {name}_done INTEGER NOT NULL DEFAULT 0,

        {name}_swaps INTEGER,
        {name}_estimated_total_swaps REAL,

        {name}_buys INTEGER,
        {name}_sells INTEGER,

        {name}_unique_wallets INTEGER,
        {name}_unique_buyers INTEGER,
        {name}_unique_sellers INTEGER,

        {name}_buy_sol REAL,
        {name}_sell_sol REAL,
        {name}_net_sol REAL,

        {name}_buy_share REAL,
        {name}_net_share REAL,

        {name}_median_buy_sol REAL,
        {name}_median_sell_sol REAL,

        {name}_largest_buy_sol REAL,
        {name}_largest_sell_sol REAL,

        {name}_buy_concentration REAL,
        {name}_sell_concentration REAL,

        {name}_swaps_per_sec REAL
    """)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    t108_event_id INTEGER NOT NULL UNIQUE,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    peak_price REAL,
    peak_at REAL,

    trigger_price REAL,
    drawdown_pct REAL,

    holders_at_trigger INTEGER,

    liquidity_usd REAL,
    market_cap REAL,
    fdv REAL,
    volume_m5 REAL,

    buys_m5 INTEGER,
    sells_m5 INTEGER,

    pair_address TEXT,
    dex_id TEXT,

    flow_source TEXT,
    flow_sample_percent REAL,

    {",".join(window_cols)},

    buyer_accel_post30_vs_pre60 REAL,
    seller_accel_post30_vs_pre60 REAL,
    wallet_accel_post30_vs_pre60 REAL,

    buy_sol_accel_post30_vs_pre60 REAL,
    sell_sol_accel_post30_vs_pre60 REAL,

    net_sol_shift_post30_vs_pre60 REAL,
    buy_share_shift_post30_vs_pre60 REAL,

    liquidity_change_30s REAL,
    market_cap_change_30s REAL,
    volume_change_30s REAL,

    liquidity_change_60s REAL,
    market_cap_change_60s REAL,
    volume_change_60s REAL,

    liquidity_change_300s REAL,
    market_cap_change_300s REAL,
    volume_change_300s REAL,

    return_10s REAL,
    return_30s REAL,
    return_60s REAL,
    return_300s REAL,
    return_900s REAL,

    peak_recovery_10s REAL,
    peak_recovery_30s REAL,
    peak_recovery_60s REAL,
    peak_recovery_300s REAL,
    peak_recovery_900s REAL,

    reclaim_peak_30s INTEGER,
    reclaim_peak_60s INTEGER,
    reclaim_peak_300s INTEGER,
    reclaim_peak_900s INTEGER,

    new_low_30s INTEGER,
    new_low_60s INTEGER,
    new_low_300s INTEGER,
    new_low_900s INTEGER,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# SOURCE
# ============================================================

def dump_events():

    return db.execute(f"""
    SELECT *
    FROM {SOURCE}
    ORDER BY trigger_timestamp
    """).fetchall()


# ============================================================
# FLOW
# ============================================================

def flow_features(
    mint,
    start_ts,
    end_ts
):

    rows = db.execute("""
    SELECT
        wallet,
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


    buys = []
    sells = []

    buyers = set()
    sellers = set()
    wallets = set()


    for r in rows:

        wallet = r["wallet"]
        side = (
            str(r["side"]).upper()
            if r["side"] is not None
            else ""
        )

        sol = r["sol_delta"]

        if wallet:
            wallets.add(wallet)

        if not valid(sol):
            continue

        amount = abs(sol)

        if side == "BUY":

            buys.append(amount)

            if wallet:
                buyers.add(wallet)

        elif side == "SELL":

            sells.append(amount)

            if wallet:
                sellers.add(wallet)


    buy_sol = sum(buys)
    sell_sol = sum(sells)

    total_sol = (
        buy_sol
        + sell_sol
    )

    duration = (
        end_ts
        - start_ts
    )


    return {

        "swaps":
            len(rows),

        "estimated_total_swaps":
            (
                len(rows) / FLOW_SAMPLE_RATE
                if FLOW_SAMPLE_RATE > 0
                else None
            ),

        "buys":
            len(buys),

        "sells":
            len(sells),

        "unique_wallets":
            len(wallets),

        "unique_buyers":
            len(buyers),

        "unique_sellers":
            len(sellers),

        "buy_sol":
            buy_sol,

        "sell_sol":
            sell_sol,

        "net_sol":
            buy_sol - sell_sol,

        "buy_share":
            div(
                len(buys),
                len(buys) + len(sells)
            ),

        "net_share":
            div(
                buy_sol - sell_sol,
                total_sol
            ),

        "median_buy_sol":
            median(buys),

        "median_sell_sol":
            median(sells),

        "largest_buy_sol":
            max(buys)
            if buys else None,

        "largest_sell_sol":
            max(sells)
            if sells else None,

        "buy_concentration":
            concentration(buys),

        "sell_concentration":
            concentration(sells),

        "swaps_per_sec":
            div(
                len(rows),
                duration
            ),
    }


# ============================================================
# MARKET SNAPSHOTS
# ============================================================

def first_dex_after(
    mint,
    target
):

    return db.execute("""
    SELECT
        timestamp,
        price_usd,
        liquidity_usd,
        market_cap,
        fdv,
        volume_m5,
        buys_m5,
        sells_m5

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


# ============================================================
# CREATE BASE ROW
# ============================================================

def ensure_row(e):

    exists = db.execute(f"""
    SELECT id
    FROM {TABLE}
    WHERE t108_event_id=?
    """, (
        e["id"],
    )).fetchone()

    if exists:
        return


    db.execute(f"""
    INSERT INTO {TABLE} (

        t108_event_id,

        token_mint,
        dump_level,

        trigger_timestamp,

        peak_price,
        peak_at,

        trigger_price,
        drawdown_pct,

        holders_at_trigger,

        liquidity_usd,
        market_cap,
        fdv,
        volume_m5,

        buys_m5,
        sells_m5,

        pair_address,
        dex_id,

        flow_source,
        flow_sample_percent,

        return_10s,
        return_30s,
        return_60s,
        return_300s,
        return_900s,

        peak_recovery_10s,
        peak_recovery_30s,
        peak_recovery_60s,
        peak_recovery_300s,
        peak_recovery_900s,

        reclaim_peak_30s,
        reclaim_peak_60s,
        reclaim_peak_300s,
        reclaim_peak_900s,

        created_at,
        last_update_at
    )

    VALUES (
        ?,
        ?,?,
        ?,
        ?,?,
        ?,?,
        ?,
        ?,?,?,?,
        ?,?,
        ?,?,
        ?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,
        ?,?
    )
    """, (

        e["id"],

        e["token_mint"],
        e["dump_level"],

        e["trigger_timestamp"],

        e["peak_price"],
        e["peak_at"],

        e["trigger_price"],
        e["drawdown_pct"],

        e["holders_at_trigger"],

        e["liquidity_usd"],
        e["market_cap"],
        e["fdv"],
        e["volume_m5"],

        e["buys_m5"],
        e["sells_m5"],

        e["pair_address"],
        e["dex_id"],

        "T107_TARGETED_PUMPSWAP",
        FLOW_SAMPLE_PERCENT,

        e["return_10s"],
        e["return_30s"],
        e["return_60s"],
        e["return_300s"],
        e["return_900s"],

        e["peak_recovery_10s"],
        e["peak_recovery_30s"],
        e["peak_recovery_60s"],
        e["peak_recovery_300s"],
        e["peak_recovery_900s"],

        int(
            e["peak_recovery_30s"] is not None
            and e["peak_recovery_30s"] >= 0
        ) if e["done_30s"] else None,

        int(
            e["peak_recovery_60s"] is not None
            and e["peak_recovery_60s"] >= 0
        ) if e["done_60s"] else None,

        int(
            e["peak_recovery_300s"] is not None
            and e["peak_recovery_300s"] >= 0
        ) if e["done_300s"] else None,

        int(
            e["peak_recovery_900s"] is not None
            and e["peak_recovery_900s"] >= 0
        ) if e["done_900s"] else None,

        time.time(),
        time.time(),
    ))

    db.commit()


# ============================================================
# FILL FLOW WINDOWS
# ============================================================

def fill_windows(e):

    row = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE t108_event_id=?
    """, (
        e["id"],
    )).fetchone()


    now = time.time()

    for name, (
        start_offset,
        end_offset
    ) in WINDOWS.items():

        if row[
            f"{name}_done"
        ] == 1:
            continue


        end_ts = (
            e["trigger_timestamp"]
            + end_offset
        )


        if now < end_ts + 30:
            continue


        f = flow_features(
            e["token_mint"],
            e["trigger_timestamp"]
            + start_offset,
            end_ts
        )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            {name}_done=1,

            {name}_swaps=?,
            {name}_estimated_total_swaps=?,

            {name}_buys=?,
            {name}_sells=?,

            {name}_unique_wallets=?,
            {name}_unique_buyers=?,
            {name}_unique_sellers=?,

            {name}_buy_sol=?,
            {name}_sell_sol=?,
            {name}_net_sol=?,

            {name}_buy_share=?,
            {name}_net_share=?,

            {name}_median_buy_sol=?,
            {name}_median_sell_sol=?,

            {name}_largest_buy_sol=?,
            {name}_largest_sell_sol=?,

            {name}_buy_concentration=?,
            {name}_sell_concentration=?,

            {name}_swaps_per_sec=?,

            last_update_at=?

        WHERE t108_event_id=?
        """, (

            f["swaps"],
            f["estimated_total_swaps"],

            f["buys"],
            f["sells"],

            f["unique_wallets"],
            f["unique_buyers"],
            f["unique_sellers"],

            f["buy_sol"],
            f["sell_sol"],
            f["net_sol"],

            f["buy_share"],
            f["net_share"],

            f["median_buy_sol"],
            f["median_sell_sol"],

            f["largest_buy_sol"],
            f["largest_sell_sol"],

            f["buy_concentration"],
            f["sell_concentration"],

            f["swaps_per_sec"],

            time.time(),

            e["id"],
        ))

        db.commit()


# ============================================================
# DERIVED
# ============================================================

def fill_derived(event_id):

    r = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE t108_event_id=?
    """, (
        event_id,
    )).fetchone()


    if not (
        r["pre60_done"]
        and r["post30_done"]
    ):
        return


    def rate(value, seconds):
        return div(value, seconds)


    def ratio_shift(before, after):

        if (
            before is None
            or after is None
            or abs(before) < 1e-12
        ):
            return None

        return (
            after / before
            - 1.0
        )


    buyer_accel = ratio_shift(
        rate(
            r["pre60_unique_buyers"],
            60
        ),
        rate(
            r["post30_unique_buyers"],
            30
        )
    )


    seller_accel = ratio_shift(
        rate(
            r["pre60_unique_sellers"],
            60
        ),
        rate(
            r["post30_unique_sellers"],
            30
        )
    )


    wallet_accel = ratio_shift(
        rate(
            r["pre60_unique_wallets"],
            60
        ),
        rate(
            r["post30_unique_wallets"],
            30
        )
    )


    buy_sol_accel = ratio_shift(
        rate(
            r["pre60_buy_sol"],
            60
        ),
        rate(
            r["post30_buy_sol"],
            30
        )
    )


    sell_sol_accel = ratio_shift(
        rate(
            r["pre60_sell_sol"],
            60
        ),
        rate(
            r["post30_sell_sol"],
            30
        )
    )


    net_shift = (
        rate(
            r["post30_net_sol"],
            30
        )
        - rate(
            r["pre60_net_sol"],
            60
        )
        if (
            rate(
                r["post30_net_sol"],
                30
            ) is not None
            and rate(
                r["pre60_net_sol"],
                60
            ) is not None
        )
        else None
    )


    buy_share_shift = (
        r["post30_buy_share"]
        - r["pre60_buy_share"]
        if (
            r["post30_buy_share"] is not None
            and r["pre60_buy_share"] is not None
        )
        else None
    )


    db.execute(f"""
    UPDATE {TABLE}

    SET
        buyer_accel_post30_vs_pre60=?,
        seller_accel_post30_vs_pre60=?,
        wallet_accel_post30_vs_pre60=?,

        buy_sol_accel_post30_vs_pre60=?,
        sell_sol_accel_post30_vs_pre60=?,

        net_sol_shift_post30_vs_pre60=?,
        buy_share_shift_post30_vs_pre60=?,

        last_update_at=?

    WHERE t108_event_id=?
    """, (

        buyer_accel,
        seller_accel,
        wallet_accel,

        buy_sol_accel,
        sell_sol_accel,

        net_shift,
        buy_share_shift,

        time.time(),
        event_id,
    ))

    db.commit()


# ============================================================
# MARKET CHANGES + OUTCOMES
# ============================================================

def fill_market_and_outcomes(e):

    r = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE t108_event_id=?
    """, (
        e["id"],
    )).fetchone()


    updates = {}


    for h in [
        30,
        60,
        300,
    ]:

        if time.time() < (
            e["trigger_timestamp"]
            + h
        ):
            continue


        snap = first_dex_after(
            e["token_mint"],
            e["trigger_timestamp"] + h
        )

        if not snap:
            continue


        updates[
            f"liquidity_change_{h}s"
        ] = pct(
            e["liquidity_usd"],
            snap["liquidity_usd"]
        )

        updates[
            f"market_cap_change_{h}s"
        ] = pct(
            e["market_cap"],
            snap["market_cap"]
        )

        updates[
            f"volume_change_{h}s"
        ] = pct(
            e["volume_m5"],
            snap["volume_m5"]
        )


    # copy refreshed T108 outcomes
    refreshed = db.execute("""
    SELECT *
    FROM t108_dump_events
    WHERE id=?
    """, (
        e["id"],
    )).fetchone()


    for h in [
        10,
        30,
        60,
        300,
        900,
    ]:

        updates[
            f"return_{h}s"
        ] = refreshed[
            f"return_{h}s"
        ]

        updates[
            f"peak_recovery_{h}s"
        ] = refreshed[
            f"peak_recovery_{h}s"
        ]


    for h in [
        30,
        60,
        300,
        900,
    ]:

        if refreshed[
            f"done_{h}s"
        ]:

            updates[
                f"reclaim_peak_{h}s"
            ] = int(
                refreshed[
                    f"peak_recovery_{h}s"
                ] is not None

                and refreshed[
                    f"peak_recovery_{h}s"
                ] >= 0
            )


            # continuation below the trigger price
            updates[
                f"new_low_{h}s"
            ] = int(
                refreshed[
                    f"return_{h}s"
                ] is not None

                and refreshed[
                    f"return_{h}s"
                ] < 0
            )


    if not updates:
        return


    sql = ", ".join(
        f"{k}=?"
        for k in updates
    )


    db.execute(
        f"""
        UPDATE {TABLE}

        SET
            {sql},
            last_update_at=?

        WHERE t108_event_id=?
        """,
        (
            *updates.values(),
            time.time(),
            e["id"],
        )
    )

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    ORDER BY trigger_timestamp DESC
    """).fetchall()


    print("=" * 190)
    print(
        "MEMECOIN LAB — T109B DUMP-CENTERED FEATURE RECORDER"
    )
    print("=" * 190)

    print(
        f"EVENTS          : {len(rows)}"
    )

    print(
        f"PRE60 READY     : "
        f"{sum(r['pre60_done'] for r in rows)}"
    )

    print(
        f"POST30 READY    : "
        f"{sum(r['post30_done'] for r in rows)}"
    )

    print(
        f"POST300 READY   : "
        f"{sum(r['post60_300_done'] for r in rows)}"
    )

    print()
    print(
        "MODE            : FEATURE COLLECTION ONLY"
    )
    print(
        "MODEL FITTING   : NONE"
    )
    print(
        "THRESH SEARCH   : NONE"
    )
    print(
        "FLOW SAMPLE     : 10% deterministic"
    )


    print()
    print("=" * 190)
    print("DUMP FLOW")
    print("=" * 190)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| PRE B/S="
            f"{str(r['pre60_buys']):>3}/"
            f"{str(r['pre60_sells']):<3} "
            f"| P30 B/S="
            f"{str(r['post30_buys']):>3}/"
            f"{str(r['post30_sells']):<3} "
            f"| PRE NET="
            f"{(r['pre60_net_sol'] or 0):+7.3f} "
            f"| P30 NET="
            f"{(r['post30_net_sol'] or 0):+7.3f} "
            f"| BACC="
            f"{str(round(r['buyer_accel_post30_vs_pre60'],2)) if r['buyer_accel_post30_vs_pre60'] is not None else 'NA':>6} "
            f"| NETΔ="
            f"{str(round(r['net_sol_shift_post30_vs_pre60'],3)) if r['net_sol_shift_post30_vs_pre60'] is not None else 'NA':>7}"
        )


    print()
    print("=" * 190)
    print("FORWARD OUTCOMES")
    print("=" * 190)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| R30="
            f"{str(round(r['return_30s'],1)) if r['return_30s'] is not None else 'NA':>6}% "
            f"| R60="
            f"{str(round(r['return_60s'],1)) if r['return_60s'] is not None else 'NA':>6}% "
            f"| R300="
            f"{str(round(r['return_300s'],1)) if r['return_300s'] is not None else 'NA':>6}% "
            f"| PEAK300="
            f"{str(r['reclaim_peak_300s']):>4} "
            f"| DOWN300="
            f"{str(r['new_low_300s']):>4}"
        )


    print()
    print("=" * 190)
    print("BY DUMP LEVEL")
    print("=" * 190)


    for level in [
        20,
        30,
        40,
        50,
    ]:

        group = [
            r
            for r in rows
            if r["dump_level"] == level
        ]

        if not group:
            continue


        r300 = [
            r["return_300s"]
            for r in group
            if r["return_300s"] is not None
        ]


        reclaim = [
            r["reclaim_peak_300s"]
            for r in group
            if r["reclaim_peak_300s"] is not None
        ]


        print(
            f"-{level:2d}% "
            f"| N={len(group):3d} "
            f"| R300 READY={len(r300):3d} "
            f"| RECLAIM300="
            f"{sum(reclaim) if reclaim else 0}/"
            f"{len(reclaim)}"
        )


    print()
    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T109B only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for e in dump_events():

            ensure_row(e)

            fill_windows(e)

            fill_derived(
                e["id"]
            )

            fill_market_and_outcomes(e)


        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T109B stopped safely."
    )


finally:

    db.close()
