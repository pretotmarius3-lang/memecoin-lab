#!/usr/bin/env python3

import sqlite3
import time
import math

DB = "validation_v090.db"

T104 = "t104_resurrection_cohort"
HOLDERS = "t101_migrated_holder_snapshots"
TABLE = "t106_resurrection_features"

REFRESH = 15

# T107 uses deterministic sampling before the expensive RPC fetch.
FLOW_SOURCE = "T107_TARGETED_PUMPSWAP"
FLOW_SAMPLE_PERCENT = 10.0
FLOW_SAMPLE_RATE = FLOW_SAMPLE_PERCENT / 100.0

WINDOWS = {
    "pre60": (-60, 0),
    "post30": (0, 30),
    "post30_60": (30, 60),
    "post60_300": (60, 300),
}


# ============================================================
# HELPERS
# ============================================================

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
    if (
        not valid(a)
        or not valid(b)
        or a <= 0
    ):
        return None

    return 100.0 * (
        b / a - 1.0
    )


def fmt(x, n=3):
    return "NA" if x is None else f"{x:.{n}f}"


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
# TABLE
# ============================================================

flow_cols = []

for name in WINDOWS:

    flow_cols.append(f"""
        {name}_done INTEGER NOT NULL DEFAULT 0,

        {name}_swaps INTEGER,
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

    token_mint TEXT PRIMARY KEY,

    t104_id INTEGER NOT NULL,
    crash_timestamp REAL NOT NULL,

    holders_at_crash INTEGER,

    holder_30s INTEGER,
    holder_60s INTEGER,
    holder_300s INTEGER,

    holder_delta_30s INTEGER,
    holder_delta_60s INTEGER,
    holder_delta_300s INTEGER,

    crash_price REAL,
    pre_crash_peak REAL,

    first_run_return_pct REAL,
    crash_drawdown_pct REAL,

    migration_to_run_s REAL,
    run_to_crash_s REAL,
    peak_to_crash_s REAL,

    liquidity_at_crash REAL,
    market_cap_at_crash REAL,
    fdv_at_crash REAL,

    volume_m5_at_crash REAL,
    buys_m5_at_crash INTEGER,
    sells_m5_at_crash INTEGER,

    buy_share_m5_at_crash REAL,
    liquidity_to_mc REAL,
    volume_to_liquidity REAL,

    {",".join(flow_cols)},

    buyer_accel_post30_vs_pre60 REAL,
    seller_accel_post30_vs_pre60 REAL,

    wallet_accel_post30_vs_pre60 REAL,

    buy_sol_accel_post30_vs_pre60 REAL,
    sell_sol_accel_post30_vs_pre60 REAL,
    net_sol_shift_post30_vs_pre60 REAL,

    buy_share_shift_post30_vs_pre60 REAL,

    volume_change_30s REAL,
    liquidity_change_30s REAL,
    market_cap_change_30s REAL,

    volume_change_60s REAL,
    liquidity_change_60s REAL,
    market_cap_change_60s REAL,

    volume_change_300s REAL,
    liquidity_change_300s REAL,
    market_cap_change_300s REAL,

    outcome_recovery50_300 INTEGER,
    outcome_reclaim_peak_300 INTEGER,

    outcome_recovery50_900 INTEGER,
    outcome_reclaim_peak_900 INTEGER,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# T107 SAMPLING METADATA
# ============================================================

def ensure_sampling_columns():

    existing = {
        row["name"]
        for row in db.execute(
            f"PRAGMA table_info({TABLE})"
        ).fetchall()
    }

    wanted = {
        "flow_source":
            "TEXT",

        "flow_sample_percent":
            "REAL",

        "pre60_estimated_total_swaps":
            "REAL",

        "post30_estimated_total_swaps":
            "REAL",

        "post30_60_estimated_total_swaps":
            "REAL",

        "post60_300_estimated_total_swaps":
            "REAL",
    }

    for name, sql_type in wanted.items():

        if name in existing:
            continue

        db.execute(
            f"""
            ALTER TABLE {TABLE}
            ADD COLUMN {name} {sql_type}
            """
        )

        print(
            f"✅ Added T106 column: {name}"
        )

    db.commit()


def refresh_sampling_metadata():

    # Metadata provenance.
    db.execute(
        f"""
        UPDATE {TABLE}

        SET
            flow_source=?,
            flow_sample_percent=?
        """,
        (
            FLOW_SOURCE,
            FLOW_SAMPLE_PERCENT,
        )
    )

    # These are rough inverse-probability estimates only.
    # They are NOT treated as exact transaction counts.
    if FLOW_SAMPLE_RATE > 0:

        db.execute(
            f"""
            UPDATE {TABLE}

            SET
                pre60_estimated_total_swaps =
                    CASE
                        WHEN pre60_swaps IS NULL
                        THEN NULL
                        ELSE pre60_swaps / ?
                    END,

                post30_estimated_total_swaps =
                    CASE
                        WHEN post30_swaps IS NULL
                        THEN NULL
                        ELSE post30_swaps / ?
                    END,

                post30_60_estimated_total_swaps =
                    CASE
                        WHEN post30_60_swaps IS NULL
                        THEN NULL
                        ELSE post30_60_swaps / ?
                    END,

                post60_300_estimated_total_swaps =
                    CASE
                        WHEN post60_300_swaps IS NULL
                        THEN NULL
                        ELSE post60_300_swaps / ?
                    END
            """,
            (
                FLOW_SAMPLE_RATE,
                FLOW_SAMPLE_RATE,
                FLOW_SAMPLE_RATE,
                FLOW_SAMPLE_RATE,
            )
        )

    db.commit()


ensure_sampling_columns()
refresh_sampling_metadata()


# ============================================================
# SOURCE COHORT
# ============================================================

def cohort():

    return db.execute(f"""
    SELECT *
    FROM {T104}
    ORDER BY crash_timestamp
    """).fetchall()


# ============================================================
# SWAP WINDOW
# ============================================================

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


def swap_features(
    mint,
    start_ts,
    end_ts
):

    rows = db.execute("""
    SELECT
        timestamp,
        wallet,
        side,
        sol_delta

    FROM swaps

    WHERE
        token_mint=?
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

        side = (
            str(r["side"]).upper()
            if r["side"] is not None
            else ""
        )

        wallet = r[
            "wallet"
        ]

        sol = r[
            "sol_delta"
        ]

        if wallet:
            wallets.add(wallet)

        if not valid(sol):
            continue

        amount = abs(sol)

        if side == "BUY":

            buys.append(
                amount
            )

            if wallet:
                buyers.add(wallet)


        elif side == "SELL":

            sells.append(
                amount
            )

            if wallet:
                sellers.add(wallet)


    buy_sol = sum(buys)
    sell_sol = sum(sells)

    total_sol = (
        buy_sol
        + sell_sol
    )

    net_sol = (
        buy_sol
        - sell_sol
    )

    duration = (
        end_ts
        - start_ts
    )


    return {

        "swaps":
            len(rows),

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
            net_sol,

        "buy_share":
            div(
                len(buys),
                len(buys) + len(sells)
            ),

        "net_share":
            div(
                net_sol,
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
# HOLDERS
# ============================================================

def first_holder_after(
    mint,
    target
):

    return db.execute(f"""
    SELECT
        checked_at,
        holder_count

    FROM {HOLDERS}

    WHERE
        token_mint=?
        AND status='OK'
        AND holder_count IS NOT NULL
        AND checked_at >= ?

    ORDER BY checked_at ASC

    LIMIT 1
    """, (
        mint,
        target
    )).fetchone()


# ============================================================
# DEX TARGET
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

    ORDER BY timestamp ASC

    LIMIT 1
    """, (
        mint,
        target
    )).fetchone()


# ============================================================
# CREATE BASE ROW
# ============================================================

def ensure_row(r):

    exists = db.execute(f"""
    SELECT token_mint
    FROM {TABLE}
    WHERE token_mint=?
    """, (
        r["token_mint"],
    )).fetchone()

    if exists:
        return


    db.execute(f"""
    INSERT INTO {TABLE} (

        token_mint,
        t104_id,
        crash_timestamp,

        holders_at_crash,

        crash_price,
        pre_crash_peak,

        first_run_return_pct,
        crash_drawdown_pct,

        migration_to_run_s,
        run_to_crash_s,
        peak_to_crash_s,

        liquidity_at_crash,
        market_cap_at_crash,
        fdv_at_crash,

        volume_m5_at_crash,
        buys_m5_at_crash,
        sells_m5_at_crash,

        buy_share_m5_at_crash,

        liquidity_to_mc,
        volume_to_liquidity,

        created_at,
        last_update_at
    )

    VALUES (
        ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?,
        ?, ?,
        ?, ?
    )
    """, (

        r["token_mint"],
        r["id"],
        r["crash_timestamp"],

        r["holders_at_crash"],

        r["crash_price"],
        r["pre_crash_peak"],

        r["first_run_return_pct"],
        r["crash_drawdown_pct"],

        r["migration_to_run_s"],
        r["run_to_crash_s"],
        r["peak_to_crash_s"],

        r["liquidity_at_crash"],
        r["market_cap_at_crash"],
        r["fdv_at_crash"],

        r["volume_m5_at_crash"],
        r["buys_m5_at_crash"],
        r["sells_m5_at_crash"],

        r["buy_share_m5_at_crash"],

        r["liquidity_to_mc"],
        r["volume_to_liquidity"],

        time.time(),
        time.time(),
    ))

    db.commit()


# ============================================================
# FILL SWAP WINDOWS
# ============================================================

def fill_windows(r):

    now = time.time()

    mint = r[
        "token_mint"
    ]

    crash = r[
        "crash_timestamp"
    ]


    current = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE token_mint=?
    """, (
        mint,
    )).fetchone()


    for name, (
        start_offset,
        end_offset
    ) in WINDOWS.items():

        if current[
            f"{name}_done"
        ] == 1:
            continue


        start_ts = (
            crash
            + start_offset
        )

        end_ts = (
            crash
            + end_offset
        )


        # Do not calculate a future window before it is mature.
        if now < end_ts:
            continue


        f = swap_features(
            mint,
            start_ts,
            end_ts
        )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            {name}_done=1,

            {name}_swaps=?,
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

        WHERE token_mint=?
        """, (

            f["swaps"],
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

            mint,
        ))

        db.commit()


# ============================================================
# FILL HOLDERS / MARKET TARGETS
# ============================================================

def fill_future_market(r):

    mint = r[
        "token_mint"
    ]

    crash = r[
        "crash_timestamp"
    ]

    current = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE token_mint=?
    """, (
        mint,
    )).fetchone()


    # --------------------------------------------------------
    # HOLDERS
    # --------------------------------------------------------

    for h in [
        30,
        60,
        300,
    ]:

        col = f"holder_{h}s"

        if current[col] is not None:
            continue


        target = (
            crash + h
        )


        if time.time() < target:
            continue


        row = first_holder_after(
            mint,
            target
        )


        if not row:
            continue


        hc = row[
            "holder_count"
        ]


        delta = None

        if (
            current[
                "holders_at_crash"
            ] is not None
            and hc is not None
        ):

            delta = (
                hc
                - current[
                    "holders_at_crash"
                ]
            )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            holder_{h}s=?,
            holder_delta_{h}s=?,
            last_update_at=?

        WHERE token_mint=?
        """, (
            hc,
            delta,
            time.time(),
            mint,
        ))

        db.commit()


    # --------------------------------------------------------
    # DEX CHANGES
    # --------------------------------------------------------

    for h in [
        30,
        60,
        300,
    ]:

        target = (
            crash + h
        )


        if time.time() < target:
            continue


        d = first_dex_after(
            mint,
            target
        )


        if not d:
            continue


        vol_change = pct(
            current[
                "volume_m5_at_crash"
            ],
            d[
                "volume_m5"
            ]
        )

        liq_change = pct(
            current[
                "liquidity_at_crash"
            ],
            d[
                "liquidity_usd"
            ]
        )

        mc_change = pct(
            current[
                "market_cap_at_crash"
            ],
            d[
                "market_cap"
            ]
        )


        db.execute(f"""
        UPDATE {TABLE}

        SET
            volume_change_{h}s=?,
            liquidity_change_{h}s=?,
            market_cap_change_{h}s=?,
            last_update_at=?

        WHERE token_mint=?
        """, (
            vol_change,
            liq_change,
            mc_change,
            time.time(),
            mint,
        ))

        db.commit()


# ============================================================
# DERIVED ACCELERATION
# ============================================================

def fill_derived(mint):

    r = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE token_mint=?
    """, (
        mint,
    )).fetchone()


    if not (
        r["pre60_done"]
        and r["post30_done"]
    ):
        return


    # Convert counts to per-second rates because windows differ:
    # pre60=60 sec and post30=30 sec.

    pre_buy_rate = div(
        r["pre60_unique_buyers"],
        60
    )

    post_buy_rate = div(
        r["post30_unique_buyers"],
        30
    )

    pre_sell_rate = div(
        r["pre60_unique_sellers"],
        60
    )

    post_sell_rate = div(
        r["post30_unique_sellers"],
        30
    )

    pre_wallet_rate = div(
        r["pre60_unique_wallets"],
        60
    )

    post_wallet_rate = div(
        r["post30_unique_wallets"],
        30
    )

    pre_buy_sol_rate = div(
        r["pre60_buy_sol"],
        60
    )

    post_buy_sol_rate = div(
        r["post30_buy_sol"],
        30
    )

    pre_sell_sol_rate = div(
        r["pre60_sell_sol"],
        60
    )

    post_sell_sol_rate = div(
        r["post30_sell_sol"],
        30
    )

    pre_net_rate = div(
        r["pre60_net_sol"],
        60
    )

    post_net_rate = div(
        r["post30_net_sol"],
        30
    )


    def ratio_shift(before, after):

        if (
            before is None
            or after is None
        ):
            return None

        if abs(before) < 1e-12:
            return None

        return (
            after / before
            - 1.0
        )


    buyer_accel = ratio_shift(
        pre_buy_rate,
        post_buy_rate
    )

    seller_accel = ratio_shift(
        pre_sell_rate,
        post_sell_rate
    )

    wallet_accel = ratio_shift(
        pre_wallet_rate,
        post_wallet_rate
    )

    buy_sol_accel = ratio_shift(
        pre_buy_sol_rate,
        post_buy_sol_rate
    )

    sell_sol_accel = ratio_shift(
        pre_sell_sol_rate,
        post_sell_sol_rate
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
        r["pre60_buy_share"] is not None
        and r["post30_buy_share"] is not None
    ):
        buy_share_shift = (
            r["post30_buy_share"]
            - r["pre60_buy_share"]
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

    WHERE token_mint=?
    """, (

        buyer_accel,
        seller_accel,
        wallet_accel,

        buy_sol_accel,
        sell_sol_accel,
        net_shift,

        buy_share_shift,

        time.time(),

        mint,
    ))

    db.commit()


# ============================================================
# COPY OUTCOMES FROM T104
# ============================================================

def fill_outcomes(r):

    db.execute(f"""
    UPDATE {TABLE}

    SET
        outcome_recovery50_300=?,
        outcome_reclaim_peak_300=?,

        outcome_recovery50_900=?,
        outcome_reclaim_peak_900=?,

        last_update_at=?

    WHERE token_mint=?
    """, (

        r[
            "recovery50_300s"
        ],

        r[
            "reclaim_peak_300s"
        ],

        r[
            "recovery50_900s"
        ],

        r[
            "reclaim_peak_900s"
        ],

        time.time(),

        r[
            "token_mint"
        ],
    ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    ORDER BY
        crash_timestamp DESC
    """).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 195)

    print(
        "MEMECOIN LAB — T106 PROSPECTIVE RESURRECTION FEATURE RECORDER"
    )

    print("=" * 195)


    print(
        f"CRASH TOKENS       : {len(rows)}"
    )

    print(
        f"PRE60 COMPLETE     : "
        f"{sum(r['pre60_done'] for r in rows)}"
    )

    print(
        f"POST30 COMPLETE    : "
        f"{sum(r['post30_done'] for r in rows)}"
    )

    print(
        f"POST60 COMPLETE    : "
        f"{sum(r['post30_60_done'] for r in rows)}"
    )

    print(
        f"POST300 COMPLETE   : "
        f"{sum(r['post60_300_done'] for r in rows)}"
    )


    print()

    print(
        "MODE               : FEATURE COLLECTION ONLY"
    )

    print(
        "MODEL FITTING      : NONE"
    )

    print(
        "THRESHOLD SEARCH   : NONE"
    )

    print(
        "PRE60              : AVAILABLE AT CRASH"
    )

    print(
        "POST WINDOWS       : FUTURE / STAGED SIGNALS ONLY"
    )


    print()

    print("=" * 195)
    print("RESURRECTION FLOW")
    print("=" * 195)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| H={str(r['holders_at_crash']):>5} "
            f"| PRE B/S="
            f"{str(r['pre60_buys']):>4}/"
            f"{str(r['pre60_sells']):<4} "
            f"| P30 B/S="
            f"{str(r['post30_buys']):>4}/"
            f"{str(r['post30_sells']):<4} "
            f"| PRE NET={fmt(r['pre60_net_sol'],2):>7} "
            f"| P30 NET={fmt(r['post30_net_sol'],2):>7} "
            f"| BACC={fmt(r['buyer_accel_post30_vs_pre60'],2):>7} "
            f"| NETΔ={fmt(r['net_sol_shift_post30_vs_pre60'],3):>8} "
            f"| REC300={str(r['outcome_recovery50_300']):>4} "
            f"| PEAK300={str(r['outcome_reclaim_peak_300']):>4}"
        )


    print()

    print("=" * 195)
    print("MARKET / HOLDER CHANGES")
    print("=" * 195)


    for r in rows[:20]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| HΔ30={str(r['holder_delta_30s']):>5} "
            f"| HΔ60={str(r['holder_delta_60s']):>5} "
            f"| HΔ300={str(r['holder_delta_300s']):>5} "
            f"| LIQ30={fmt(r['liquidity_change_30s'],1):>7}% "
            f"| MC30={fmt(r['market_cap_change_30s'],1):>7}% "
            f"| VOL30={fmt(r['volume_change_30s'],1):>7}% "
            f"| LIQ300={fmt(r['liquidity_change_300s'],1):>7}% "
            f"| MC300={fmt(r['market_cap_change_300s'],1):>7}%"
        )


    print()

    print("=" * 195)
    print("READINESS")
    print("=" * 195)


    n = len(rows)


    print(
        f"CRASH TOKENS       : {n}/30 integrity"
    )

    print(
        f"CRASH TOKENS       : {n}/50 descriptive"
    )

    print(
        f"CRASH TOKENS       : {n}/100 discovery"
    )


    if n >= 100:

        print(
            "🟢 T106 READY FOR RESURRECTION FEATURE DISCOVERY."
        )

    elif n >= 50:

        print(
            "🟡 T106 DESCRIPTIVE CHECKPOINT."
        )

    elif n >= 30:

        print(
            "🔵 T106 INTEGRITY CHECKPOINT."
        )

    else:

        print(
            "🔵 T106 COLLECTING."
        )


    print()

    print(
        f"Refresh every {REFRESH}s."
    )

    print(
        "CTRL+C stops T106 only."
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for r in cohort():

            ensure_row(
                r
            )

            fill_windows(
                r
            )

            fill_future_market(
                r
            )

            fill_derived(
                r[
                    "token_mint"
                ]
            )

            fill_outcomes(
                r
            )


        refresh_sampling_metadata()

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T106 stopped safely."
    )


finally:

    db.close()
