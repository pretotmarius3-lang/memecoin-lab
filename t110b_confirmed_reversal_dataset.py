#!/usr/bin/env python3

import sqlite3
import time
import os
import math

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

TABLE = "t110b_confirmed_reversal_dataset"

REFRESH = 10

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


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
# OUTPUT TABLE
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    t108_event_id INTEGER NOT NULL,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    stage_seconds INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,
    stage_timestamp REAL NOT NULL,

    trigger_price REAL,
    old_peak_price REAL,

    stage_price REAL,

    -- ============================================
    -- INFORMATION AVAILABLE BY STAGE
    -- ============================================

    return_trigger_to_stage REAL,

    path_new_low INTEGER,

    path_mfe REAL,
    path_mae REAL,

    path_reclaimed_trigger INTEGER,
    path_reclaimed_old_peak INTEGER,

    path_first_reclaim_trigger_s REAL,

    pre60_swaps INTEGER,
    pre60_buys INTEGER,
    pre60_sells INTEGER,
    pre60_unique_wallets INTEGER,
    pre60_unique_buyers INTEGER,
    pre60_unique_sellers INTEGER,
    pre60_buy_sol REAL,
    pre60_sell_sol REAL,
    pre60_net_sol REAL,
    pre60_buy_share REAL,

    post_swaps INTEGER,
    post_buys INTEGER,
    post_sells INTEGER,
    post_unique_wallets INTEGER,
    post_unique_buyers INTEGER,
    post_unique_sellers INTEGER,
    post_buy_sol REAL,
    post_sell_sol REAL,
    post_net_sol REAL,
    post_buy_share REAL,

    buyer_rate_shift REAL,
    seller_rate_shift REAL,
    wallet_rate_shift REAL,

    net_sol_rate_shift REAL,
    buy_share_shift REAL,

    liquidity_at_dump REAL,
    market_cap_at_dump REAL,
    volume_m5_at_dump REAL,

    liquidity_change_stage REAL,
    market_cap_change_stage REAL,
    volume_change_stage REAL,

    -- ============================================
    -- FUTURE OUTCOMES — NEVER FEATURES
    -- ============================================

    future_end_return_300 REAL,
    future_max_return_300 REAL,
    future_min_return_300 REAL,

    future_end_return_900 REAL,
    future_max_return_900 REAL,
    future_min_return_900 REAL,

    future_reclaim_old_peak_300 INTEGER,
    future_reclaim_old_peak_900 INTEGER,

    future_up10_300 INTEGER,
    future_up20_300 INTEGER,
    future_up30_300 INTEGER,
    future_up50_300 INTEGER,

    mature_300 INTEGER NOT NULL DEFAULT 0,
    mature_900 INTEGER NOT NULL DEFAULT 0,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL,

    UNIQUE(
        t108_event_id,
        stage_seconds
    )
)
""")

db.commit()


# ============================================================
# SOURCE EVENTS
# ============================================================

def source_events():

    return db.execute("""
    SELECT
        e.*,

        b.pre60_swaps,
        b.pre60_buys,
        b.pre60_sells,
        b.pre60_unique_wallets,
        b.pre60_unique_buyers,
        b.pre60_unique_sellers,
        b.pre60_buy_sol,
        b.pre60_sell_sol,
        b.pre60_net_sol,
        b.pre60_buy_share,

        b.post30_swaps,
        b.post30_buys,
        b.post30_sells,
        b.post30_unique_wallets,
        b.post30_unique_buyers,
        b.post30_unique_sellers,
        b.post30_buy_sol,
        b.post30_sell_sol,
        b.post30_net_sol,
        b.post30_buy_share,

        b.post30_60_swaps,
        b.post30_60_buys,
        b.post30_60_sells,
        b.post30_60_unique_wallets,
        b.post30_60_unique_buyers,
        b.post30_60_unique_sellers,
        b.post30_60_buy_sol,
        b.post30_60_sell_sol,
        b.post30_60_net_sol,
        b.post30_60_buy_share,

        b.liquidity_change_30s,
        b.market_cap_change_30s,
        b.volume_change_30s,

        b.liquidity_change_60s,
        b.market_cap_change_60s,
        b.volume_change_60s

    FROM t108_dump_events e

    JOIN t109b_dump_features b
        ON b.t108_event_id=e.id

    ORDER BY e.trigger_timestamp
    """).fetchall()


# ============================================================
# T109C PATH
# ============================================================

def path_row(event_id):

    return db.execute("""
    SELECT *
    FROM t109c_dump_path
    WHERE t108_event_id=?
    """, (
        event_id,
    )).fetchone()


# ============================================================
# RATE SHIFT
# ============================================================

def rate(v, seconds):

    if v is None:
        return None

    return v / seconds


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


# ============================================================
# CREATE / UPDATE STAGE
# ============================================================

def build_stage(e, stage):

    p = path_row(
        e["id"]
    )

    if not p:
        return


    done_col = (
        f"done_{stage}s"
    )

    if not p[done_col]:
        return


    stage_price = p[
        f"end_price_{stage}s"
    ]

    if not valid(stage_price):
        return


    # --------------------------------------------------------
    # PATH DATA KNOWN AT STAGE
    # --------------------------------------------------------

    stage_return = pct(
        e["trigger_price"],
        stage_price
    )

    path_new_low = p[
        f"ever_new_low_{stage}s"
    ]

    path_mfe = p[
        f"mfe_{stage}s"
    ]

    path_mae = p[
        f"mae_{stage}s"
    ]


    reclaim_trigger_time = p[
        f"first_reclaim_trigger_time_{stage}s"
    ]

    reclaimed_trigger = int(
        reclaim_trigger_time is not None
    )

    reclaimed_old_peak = p[
        f"ever_reclaim_peak_{stage}s"
    ]


    # --------------------------------------------------------
    # FLOW AVAILABLE AT STAGE
    # --------------------------------------------------------

    if stage == 30:

        post_duration = 30

        post_swaps = e[
            "post30_swaps"
        ]

        post_buys = e[
            "post30_buys"
        ]

        post_sells = e[
            "post30_sells"
        ]

        post_wallets = e[
            "post30_unique_wallets"
        ]

        post_buyers = e[
            "post30_unique_buyers"
        ]

        post_sellers = e[
            "post30_unique_sellers"
        ]

        post_buy_sol = e[
            "post30_buy_sol"
        ]

        post_sell_sol = e[
            "post30_sell_sol"
        ]

        post_net_sol = e[
            "post30_net_sol"
        ]

        post_buy_share = e[
            "post30_buy_share"
        ]

        liq_change = e[
            "liquidity_change_30s"
        ]

        mc_change = e[
            "market_cap_change_30s"
        ]

        vol_change = e[
            "volume_change_30s"
        ]


    elif stage == 60:

        post_duration = 60

        def add(a, b):

            if a is None and b is None:
                return None

            return (
                (a or 0)
                + (b or 0)
            )


        post_swaps = add(
            e["post30_swaps"],
            e["post30_60_swaps"]
        )

        post_buys = add(
            e["post30_buys"],
            e["post30_60_buys"]
        )

        post_sells = add(
            e["post30_sells"],
            e["post30_60_sells"]
        )


        # Approximation:
        # unique sets cannot be reconstructed by simply adding.
        # Keep NULL instead of inventing duplicate-free counts.

        post_wallets = None
        post_buyers = None
        post_sellers = None


        post_buy_sol = add(
            e["post30_buy_sol"],
            e["post30_60_buy_sol"]
        )

        post_sell_sol = add(
            e["post30_sell_sol"],
            e["post30_60_sell_sol"]
        )

        post_net_sol = add(
            e["post30_net_sol"],
            e["post30_60_net_sol"]
        )


        if (
            post_buys is not None
            and post_sells is not None
            and (
                post_buys
                + post_sells
            ) > 0
        ):

            post_buy_share = (
                post_buys
                / (
                    post_buys
                    + post_sells
                )
            )

        else:

            post_buy_share = None


        liq_change = e[
            "liquidity_change_60s"
        ]

        mc_change = e[
            "market_cap_change_60s"
        ]

        vol_change = e[
            "volume_change_60s"
        ]


    else:
        return


    # --------------------------------------------------------
    # PRE → POST SHIFTS
    # --------------------------------------------------------

    buyer_shift = None

    if (
        post_buyers is not None
        and e["pre60_unique_buyers"] is not None
    ):

        buyer_shift = ratio_shift(
            rate(
                e["pre60_unique_buyers"],
                60
            ),
            rate(
                post_buyers,
                post_duration
            )
        )


    seller_shift = None

    if (
        post_sellers is not None
        and e["pre60_unique_sellers"] is not None
    ):

        seller_shift = ratio_shift(
            rate(
                e["pre60_unique_sellers"],
                60
            ),
            rate(
                post_sellers,
                post_duration
            )
        )


    wallet_shift = None

    if (
        post_wallets is not None
        and e["pre60_unique_wallets"] is not None
    ):

        wallet_shift = ratio_shift(
            rate(
                e["pre60_unique_wallets"],
                60
            ),
            rate(
                post_wallets,
                post_duration
            )
        )


    pre_net_rate = rate(
        e["pre60_net_sol"],
        60
    )

    post_net_rate = rate(
        post_net_sol,
        post_duration
    )


    net_shift = None

    if (
        pre_net_rate is not None
        and post_net_rate is not None
    ):

        net_shift = (
            post_net_rate
            - pre_net_rate
        )


    buy_share_shift = None

    if (
        e["pre60_buy_share"] is not None
        and post_buy_share is not None
    ):

        buy_share_shift = (
            post_buy_share
            - e["pre60_buy_share"]
        )


    # --------------------------------------------------------
    # FUTURE OUTCOMES AFTER CONFIRMATION
    # --------------------------------------------------------

    future_end_300 = None
    future_max_300 = None
    future_min_300 = None

    reclaim300 = None

    up10 = None
    up20 = None
    up30 = None
    up50 = None

    mature300 = 0


    if p["done_300s"]:

        mature300 = 1

        future_end_300 = pct(
            stage_price,
            p["end_price_300s"]
        )

        future_max_300 = pct(
            stage_price,
            p["high_price_300s"]
        )

        future_min_300 = pct(
            stage_price,
            p["low_price_300s"]
        )

        reclaim300 = p[
            "ever_reclaim_peak_300s"
        ]

        if future_max_300 is not None:

            up10 = int(
                future_max_300 >= 10
            )

            up20 = int(
                future_max_300 >= 20
            )

            up30 = int(
                future_max_300 >= 30
            )

            up50 = int(
                future_max_300 >= 50
            )


    future_end_900 = None
    future_max_900 = None
    future_min_900 = None

    reclaim900 = None

    mature900 = 0


    if p["done_900s"]:

        mature900 = 1

        future_end_900 = pct(
            stage_price,
            p["end_price_900s"]
        )

        future_max_900 = pct(
            stage_price,
            p["high_price_900s"]
        )

        future_min_900 = pct(
            stage_price,
            p["low_price_900s"]
        )

        reclaim900 = p[
            "ever_reclaim_peak_900s"
        ]


    # --------------------------------------------------------
    # STORE
    # --------------------------------------------------------

    db.execute(f"""
    INSERT INTO {TABLE} (

        t108_event_id,

        token_mint,

        dump_level,

        stage_seconds,

        trigger_timestamp,
        stage_timestamp,

        trigger_price,
        old_peak_price,

        stage_price,

        return_trigger_to_stage,

        path_new_low,

        path_mfe,
        path_mae,

        path_reclaimed_trigger,
        path_reclaimed_old_peak,

        path_first_reclaim_trigger_s,

        pre60_swaps,
        pre60_buys,
        pre60_sells,

        pre60_unique_wallets,
        pre60_unique_buyers,
        pre60_unique_sellers,

        pre60_buy_sol,
        pre60_sell_sol,
        pre60_net_sol,

        pre60_buy_share,

        post_swaps,
        post_buys,
        post_sells,

        post_unique_wallets,
        post_unique_buyers,
        post_unique_sellers,

        post_buy_sol,
        post_sell_sol,
        post_net_sol,

        post_buy_share,

        buyer_rate_shift,
        seller_rate_shift,
        wallet_rate_shift,

        net_sol_rate_shift,
        buy_share_shift,

        liquidity_at_dump,
        market_cap_at_dump,
        volume_m5_at_dump,

        liquidity_change_stage,
        market_cap_change_stage,
        volume_change_stage,

        future_end_return_300,
        future_max_return_300,
        future_min_return_300,

        future_end_return_900,
        future_max_return_900,
        future_min_return_900,

        future_reclaim_old_peak_300,
        future_reclaim_old_peak_900,

        future_up10_300,
        future_up20_300,
        future_up30_300,
        future_up50_300,

        mature_300,
        mature_900,

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
        ?,
        ?,
        ?,?,
        ?,?,
        ?,
        ?,?,
        ?,
        ?,?,?,
        ?,?,?,
        ?,
        ?,?,?,
        ?,?,?,
        ?,?,?,
        ?,
        ?,?,?,
        ?,?,
        ?,?,?,
        ?,?,?,
        ?,?,?,
        ?,?,?,
        ?,?,
        ?,?,?,?,
        ?,?,
        ?,?
    )

    ON CONFLICT(
        t108_event_id,
        stage_seconds
    )

    DO UPDATE SET

        stage_price=excluded.stage_price,

        return_trigger_to_stage=
            excluded.return_trigger_to_stage,

        path_new_low=
            excluded.path_new_low,

        path_mfe=
            excluded.path_mfe,

        path_mae=
            excluded.path_mae,

        path_reclaimed_trigger=
            excluded.path_reclaimed_trigger,

        path_reclaimed_old_peak=
            excluded.path_reclaimed_old_peak,

        path_first_reclaim_trigger_s=
            excluded.path_first_reclaim_trigger_s,

        future_end_return_300=
            excluded.future_end_return_300,

        future_max_return_300=
            excluded.future_max_return_300,

        future_min_return_300=
            excluded.future_min_return_300,

        future_end_return_900=
            excluded.future_end_return_900,

        future_max_return_900=
            excluded.future_max_return_900,

        future_min_return_900=
            excluded.future_min_return_900,

        future_reclaim_old_peak_300=
            excluded.future_reclaim_old_peak_300,

        future_reclaim_old_peak_900=
            excluded.future_reclaim_old_peak_900,

        future_up10_300=
            excluded.future_up10_300,

        future_up20_300=
            excluded.future_up20_300,

        future_up30_300=
            excluded.future_up30_300,

        future_up50_300=
            excluded.future_up50_300,

        mature_300=
            excluded.mature_300,

        mature_900=
            excluded.mature_900,

        last_update_at=
            excluded.last_update_at
    """, (

        e["id"],

        e["token_mint"],

        e["dump_level"],

        stage,

        e["trigger_timestamp"],
        e["trigger_timestamp"] + stage,

        e["trigger_price"],
        e["peak_price"],

        stage_price,

        stage_return,

        path_new_low,

        path_mfe,
        path_mae,

        reclaimed_trigger,
        reclaimed_old_peak,

        reclaim_trigger_time,

        e["pre60_swaps"],
        e["pre60_buys"],
        e["pre60_sells"],

        e["pre60_unique_wallets"],
        e["pre60_unique_buyers"],
        e["pre60_unique_sellers"],

        e["pre60_buy_sol"],
        e["pre60_sell_sol"],
        e["pre60_net_sol"],

        e["pre60_buy_share"],

        post_swaps,
        post_buys,
        post_sells,

        post_wallets,
        post_buyers,
        post_sellers,

        post_buy_sol,
        post_sell_sol,
        post_net_sol,

        post_buy_share,

        buyer_shift,
        seller_shift,
        wallet_shift,

        net_shift,
        buy_share_shift,

        e["liquidity_usd"],
        e["market_cap"],
        e["volume_m5"],

        liq_change,
        mc_change,
        vol_change,

        future_end_300,
        future_max_300,
        future_min_300,

        future_end_900,
        future_max_900,
        future_min_900,

        reclaim300,
        reclaim900,

        up10,
        up20,
        up30,
        up50,

        mature300,
        mature900,

        time.time(),
        time.time(),
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
        stage_seconds
    """).fetchall()


    print("=" * 190)

    print(
        "MEMECOIN LAB — T110B CONFIRMED REVERSAL DATASET"
    )

    print("=" * 190)


    print(
        f"ROWS             : {len(rows)}"
    )

    print(
        f"UNIQUE DUMPS     : "
        f"{len(set(r['t108_event_id'] for r in rows))}"
    )

    print(
        f"UNIQUE TOKENS    : "
        f"{len(set(r['token_mint'] for r in rows))}"
    )


    print()

    print(
        "STAGE30/60       : FEATURES AVAILABLE ONLY UP TO THAT TIME"
    )

    print(
        "FUTURE300/900    : OUTCOMES ONLY — NEVER PREDICTORS"
    )

    print(
        "MODEL FITTING    : NONE"
    )

    print(
        "ENTRY RULE       : NONE YET"
    )


    print()
    print("=" * 190)
    print("CONFIRMATION STATE")
    print("=" * 190)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| T={r['stage_seconds']:2d}s "
            f"| NOW={fmt(r['return_trigger_to_stage']):>7}% "
            f"| LOW={r['path_new_low']} "
            f"| RECLAIM={r['path_reclaimed_trigger']} "
            f"| PRENET={fmt(r['pre60_net_sol'],3):>8} "
            f"| POSTNET={fmt(r['post_net_sol'],3):>8} "
            f"| NETΔ={fmt(r['net_sol_rate_shift'],3):>8}"
        )


    print()
    print("=" * 190)
    print("REMAINING OPPORTUNITY AFTER CONFIRMATION")
    print("=" * 190)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| D=-{r['dump_level']:2d}% "
            f"| T={r['stage_seconds']:2d}s "
            f"| FUT_END300={fmt(r['future_end_return_300']):>8}% "
            f"| FUT_MAX300={fmt(r['future_max_return_300']):>8}% "
            f"| FUT_MIN300={fmt(r['future_min_return_300']):>8}% "
            f"| +20={str(r['future_up20_300']):>4} "
            f"| +50={str(r['future_up50_300']):>4} "
            f"| PEAK={str(r['future_reclaim_old_peak_300']):>4}"
        )


    print()
    print("=" * 190)
    print("READINESS")
    print("=" * 190)


    mature = [
        r
        for r in rows
        if r["mature_300"]
    ]

    tokens = {
        r["token_mint"]
        for r in mature
    }


    print(
        f"MATURE 300s ROWS : {len(mature)}"
    )

    print(
        f"MATURE TOKENS     : {len(tokens)}"
    )


    if len(tokens) >= 50:

        print(
            "🟢 ENOUGH TOKENS FOR CONTROLLED CONFIRMATION DISCOVERY."
        )

    elif len(tokens) >= 20:

        print(
            "🟡 DESCRIPTIVE CONFIRMATION ANALYSIS BECOMING USEFUL."
        )

    else:

        print(
            "🔵 COLLECTING CONFIRMED-REVERSAL CASES."
        )


    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T110B only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for e in source_events():

            build_stage(
                e,
                30
            )

            build_stage(
                e,
                60
            )


        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()

    print(
        "T110B stopped safely."
    )


finally:

    db.close()
