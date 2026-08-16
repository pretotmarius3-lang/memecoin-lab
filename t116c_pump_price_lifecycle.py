#!/usr/bin/env python3

import sqlite3
import time
import os
import math
import statistics

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

RAW_TABLE = "t116_pump_swaps"

CLEAN_TABLE = "t116_clean_swaps"
STATE_TABLE = "t116_token_state"
META_TABLE = "t116c_meta"

REFRESH = 5

MIN_HISTORY = 3
HISTORY_SIZE = 15

MAX_RATIO = 5.0
MIN_RATIO = 0.20

FLOW_WINDOWS = [
    30,
    60,
    120,
    300,
]


# ============================================================
# HELPERS
# ============================================================

def valid(x):

    return (
        x is not None
        and isinstance(
            x,
            (int, float)
        )
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


def median(xs):

    xs = [
        x
        for x in xs
        if valid(x)
        and x > 0
    ]

    if not xs:
        return None

    return statistics.median(xs)


def fmt(x, n=2):

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

db.execute(
    "PRAGMA busy_timeout=5000"
)


# ============================================================
# TABLES
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (

    key TEXT PRIMARY KEY,

    value TEXT NOT NULL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {CLEAN_TABLE} (

    signature TEXT PRIMARY KEY,

    timestamp REAL NOT NULL,

    slot INTEGER,

    token_mint TEXT NOT NULL,

    wallet TEXT,

    side TEXT,

    token_delta REAL,

    sol_delta REAL,

    raw_price_sol REAL,

    clean_price_sol REAL,

    price_valid INTEGER NOT NULL,

    reject_reason TEXT,

    reference_price REAL,

    price_ratio REAL,

    sampled_percent REAL,

    processed_at REAL NOT NULL
)
""")


db.execute(f"""
CREATE INDEX IF NOT EXISTS
idx_t116_clean_mint_time

ON {CLEAN_TABLE} (
    token_mint,
    timestamp
)
""")


flow_cols = []

for w in FLOW_WINDOWS:

    flow_cols.append(f"""
        swaps_{w}s INTEGER,
        buys_{w}s INTEGER,
        sells_{w}s INTEGER,

        unique_wallets_{w}s INTEGER,
        unique_buyers_{w}s INTEGER,
        unique_sellers_{w}s INTEGER,

        buy_sol_{w}s REAL,
        sell_sol_{w}s REAL,
        net_sol_{w}s REAL,

        buy_share_{w}s REAL
    """)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (

    token_mint TEXT PRIMARY KEY,

    first_seen REAL NOT NULL,

    last_seen REAL NOT NULL,

    raw_swaps INTEGER NOT NULL DEFAULT 0,

    clean_swaps INTEGER NOT NULL DEFAULT 0,

    rejected_swaps INTEGER NOT NULL DEFAULT 0,

    buys_total INTEGER NOT NULL DEFAULT 0,
    sells_total INTEGER NOT NULL DEFAULT 0,

    buy_sol_total REAL NOT NULL DEFAULT 0,
    sell_sol_total REAL NOT NULL DEFAULT 0,
    net_sol_total REAL NOT NULL DEFAULT 0,

    current_price REAL,

    current_price_at REAL,

    peak_price REAL,

    peak_at REAL,

    drawdown_from_peak_pct REAL,

    trough_price REAL,

    trough_at REAL,

    run_from_first_pct REAL,

    {",".join(flow_cols)},

    migrated INTEGER NOT NULL DEFAULT 0,

    migration_timestamp REAL,

    migration_status TEXT,

    migration_signature TEXT,

    age_s REAL,

    seconds_since_last_clean_swap REAL,

    last_update_at REAL NOT NULL
)
""")


db.commit()


# ============================================================
# META
# ============================================================

row = db.execute(f"""
SELECT value
FROM {META_TABLE}
WHERE key='last_raw_timestamp'
""").fetchone()


LAST_RAW_TIMESTAMP = (
    float(
        row["value"]
    )
    if row
    else 0.0
)


# ============================================================
# CLEAN HISTORY
# ============================================================

def clean_history(
    mint,
    before_ts
):

    rows = db.execute(f"""
    SELECT clean_price_sol

    FROM {CLEAN_TABLE}

    WHERE
        token_mint=?
        AND timestamp < ?
        AND price_valid=1
        AND clean_price_sol IS NOT NULL
        AND clean_price_sol > 0

    ORDER BY timestamp DESC

    LIMIT ?
    """, (
        mint,
        before_ts,
        HISTORY_SIZE
    )).fetchall()


    return [
        r["clean_price_sol"]
        for r in rows
    ]


# ============================================================
# PRICE CLEANING
# ============================================================

def clean_one(r):

    raw = r[
        "raw_price_sol"
    ]


    if (
        not valid(raw)
        or raw <= 0
    ):

        return (
            False,
            None,
            "INVALID_PRICE",
            None,
            None
        )


    hist = clean_history(
        r["token_mint"],
        r["timestamp"]
    )


    if len(hist) < MIN_HISTORY:

        return (
            True,
            raw,
            "WARMUP",
            median(hist),
            None
        )


    ref = median(
        hist
    )


    if (
        not valid(ref)
        or ref <= 0
    ):

        return (
            True,
            raw,
            "NO_REFERENCE",
            ref,
            None
        )


    ratio = (
        raw / ref
    )


    if ratio > MAX_RATIO:

        return (
            False,
            None,
            "JUMP_HIGH",
            ref,
            ratio
        )


    if ratio < MIN_RATIO:

        return (
            False,
            None,
            "JUMP_LOW",
            ref,
            ratio
        )


    return (
        True,
        raw,
        "OK",
        ref,
        ratio
    )


# ============================================================
# PROCESS RAW SWAPS
# ============================================================

def process_new_raw():

    global LAST_RAW_TIMESTAMP


    rows = db.execute(f"""
    SELECT *

    FROM {RAW_TABLE}

    WHERE timestamp >= ?

    ORDER BY
        timestamp ASC,
        signature ASC
    """, (
        LAST_RAW_TIMESTAMP,
    )).fetchall()


    if not rows:
        return 0


    processed = 0


    for r in rows:

        exists = db.execute(f"""
        SELECT 1

        FROM {CLEAN_TABLE}

        WHERE signature=?
        """, (
            r["signature"],
        )).fetchone()


        if exists:

            LAST_RAW_TIMESTAMP = max(
                LAST_RAW_TIMESTAMP,
                r["timestamp"]
            )

            continue


        (
            ok,
            clean_price,
            reason,
            reference,
            ratio
        ) = clean_one(
            r
        )


        db.execute(f"""
        INSERT INTO {CLEAN_TABLE} (

            signature,

            timestamp,

            slot,

            token_mint,

            wallet,

            side,

            token_delta,

            sol_delta,

            raw_price_sol,

            clean_price_sol,

            price_valid,

            reject_reason,

            reference_price,

            price_ratio,

            sampled_percent,

            processed_at
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
        """, (

            r["signature"],

            r["timestamp"],

            r["slot"],

            r["token_mint"],

            r["wallet"],

            r["side"],

            r["token_delta"],

            r["sol_delta"],

            r["raw_price_sol"],

            clean_price,

            int(ok),

            reason,

            reference,

            ratio,

            r["sampled_percent"],

            time.time(),
        ))


        LAST_RAW_TIMESTAMP = max(
            LAST_RAW_TIMESTAMP,
            r["timestamp"]
        )


        processed += 1


    db.execute(f"""
    INSERT OR REPLACE INTO {META_TABLE}
    (key,value)

    VALUES (
        'last_raw_timestamp',
        ?
    )
    """, (
        str(
            LAST_RAW_TIMESTAMP
        ),
    ))


    db.commit()

    return processed


# ============================================================
# MIGRATION MATCH
# ============================================================

def migration_info(
    mint
):

    return db.execute("""
    SELECT
        signature,

        COALESCE(
            block_time,
            detected_at
        ) AS migration_ts,

        status

    FROM t101_migrations

    WHERE
        token_mint=?
        AND status='OK'

    ORDER BY
        COALESCE(
            block_time,
            detected_at
        ) ASC

    LIMIT 1
    """, (
        mint,
    )).fetchone()


# ============================================================
# FLOW WINDOW
# ============================================================

def flow_window(
    mint,
    start_ts,
    end_ts
):

    rows = db.execute(f"""
    SELECT
        wallet,
        side,
        sol_delta

    FROM {CLEAN_TABLE}

    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp <= ?
        AND price_valid=1
    """, (
        mint,
        start_ts,
        end_ts
    )).fetchall()


    buys = 0
    sells = 0

    buy_sol = 0.0
    sell_sol = 0.0

    wallets = set()
    buyers = set()
    sellers = set()


    for r in rows:

        wallet = r[
            "wallet"
        ]

        side = r[
            "side"
        ]

        amount = abs(
            r["sol_delta"]
            or 0.0
        )


        if wallet:

            wallets.add(
                wallet
            )


        if side == "BUY":

            buys += 1

            buy_sol += amount

            if wallet:

                buyers.add(
                    wallet
                )


        elif side == "SELL":

            sells += 1

            sell_sol += amount

            if wallet:

                sellers.add(
                    wallet
                )


    total = (
        buys
        + sells
    )


    return {

        "swaps":
            len(rows),

        "buys":
            buys,

        "sells":
            sells,

        "wallets":
            len(wallets),

        "buyers":
            len(buyers),

        "sellers":
            len(sellers),

        "buy_sol":
            buy_sol,

        "sell_sol":
            sell_sol,

        "net_sol":
            buy_sol
            - sell_sol,

        "buy_share":
            (
                buys / total
                if total > 0
                else None
            ),
    }


# ============================================================
# TOKEN STATE
# ============================================================

def rebuild_state(
    mint
):

    raw_stats = db.execute(f"""
    SELECT
        MIN(timestamp) AS first_seen,

        MAX(timestamp) AS last_seen,

        COUNT(*) AS raw_swaps

    FROM {CLEAN_TABLE}

    WHERE token_mint=?
    """, (
        mint,
    )).fetchone()


    clean = db.execute(f"""
    SELECT *

    FROM {CLEAN_TABLE}

    WHERE
        token_mint=?
        AND price_valid=1

    ORDER BY timestamp ASC
    """, (
        mint,
    )).fetchall()


    rejected = db.execute(f"""
    SELECT COUNT(*)

    FROM {CLEAN_TABLE}

    WHERE
        token_mint=?
        AND price_valid=0
    """, (
        mint,
    )).fetchone()[0]


    if not raw_stats[
        "first_seen"
    ]:

        return


    first_seen = raw_stats[
        "first_seen"
    ]

    last_seen = raw_stats[
        "last_seen"
    ]


    now = time.time()


    current_price = None
    current_at = None

    peak_price = None
    peak_at = None

    trough_price = None
    trough_at = None

    first_price = None


    buys_total = 0
    sells_total = 0

    buy_sol_total = 0.0
    sell_sol_total = 0.0


    for r in clean:

        price = r[
            "clean_price_sol"
        ]


        if first_price is None:

            first_price = price


        current_price = price

        current_at = r[
            "timestamp"
        ]


        if (
            peak_price is None
            or price > peak_price
        ):

            peak_price = price

            peak_at = r[
                "timestamp"
            ]


        if (
            trough_price is None
            or price < trough_price
        ):

            trough_price = price

            trough_at = r[
                "timestamp"
            ]


        amount = abs(
            r["sol_delta"]
            or 0
        )


        if r[
            "side"
        ] == "BUY":

            buys_total += 1

            buy_sol_total += amount


        elif r[
            "side"
        ] == "SELL":

            sells_total += 1

            sell_sol_total += amount


    drawdown = pct(
        peak_price,
        current_price
    )


    run_from_first = pct(
        first_price,
        current_price
    )


    migration = migration_info(
        mint
    )


    migrated = int(
        migration
        is not None
    )


    migration_ts = (
        migration[
            "migration_ts"
        ]
        if migration
        else None
    )


    migration_status = (
        migration[
            "status"
        ]
        if migration
        else None
    )


    migration_signature = (
        migration[
            "signature"
        ]
        if migration
        else None
    )


    flow_data = {}


    for window in FLOW_WINDOWS:

        flow_data[
            window
        ] = flow_window(

            mint,

            now
            - window,

            now
        )


    db.execute(f"""
    INSERT INTO {STATE_TABLE} (

        token_mint,

        first_seen,
        last_seen,

        raw_swaps,
        clean_swaps,
        rejected_swaps,

        buys_total,
        sells_total,

        buy_sol_total,
        sell_sol_total,
        net_sol_total,

        current_price,
        current_price_at,

        peak_price,
        peak_at,

        drawdown_from_peak_pct,

        trough_price,
        trough_at,

        run_from_first_pct,

        swaps_30s,
        buys_30s,
        sells_30s,
        unique_wallets_30s,
        unique_buyers_30s,
        unique_sellers_30s,
        buy_sol_30s,
        sell_sol_30s,
        net_sol_30s,
        buy_share_30s,

        swaps_60s,
        buys_60s,
        sells_60s,
        unique_wallets_60s,
        unique_buyers_60s,
        unique_sellers_60s,
        buy_sol_60s,
        sell_sol_60s,
        net_sol_60s,
        buy_share_60s,

        swaps_120s,
        buys_120s,
        sells_120s,
        unique_wallets_120s,
        unique_buyers_120s,
        unique_sellers_120s,
        buy_sol_120s,
        sell_sol_120s,
        net_sol_120s,
        buy_share_120s,

        swaps_300s,
        buys_300s,
        sells_300s,
        unique_wallets_300s,
        unique_buyers_300s,
        unique_sellers_300s,
        buy_sol_300s,
        sell_sol_300s,
        net_sol_300s,
        buy_share_300s,

        migrated,
        migration_timestamp,
        migration_status,
        migration_signature,

        age_s,

        seconds_since_last_clean_swap,

        last_update_at
    )

    VALUES ({",".join(["?"] * 66)})

    ON CONFLICT(token_mint)

    DO UPDATE SET

        first_seen=excluded.first_seen,
        last_seen=excluded.last_seen,

        raw_swaps=excluded.raw_swaps,
        clean_swaps=excluded.clean_swaps,
        rejected_swaps=excluded.rejected_swaps,

        buys_total=excluded.buys_total,
        sells_total=excluded.sells_total,

        buy_sol_total=excluded.buy_sol_total,
        sell_sol_total=excluded.sell_sol_total,
        net_sol_total=excluded.net_sol_total,

        current_price=excluded.current_price,
        current_price_at=excluded.current_price_at,

        peak_price=excluded.peak_price,
        peak_at=excluded.peak_at,

        drawdown_from_peak_pct=
            excluded.drawdown_from_peak_pct,

        trough_price=excluded.trough_price,
        trough_at=excluded.trough_at,

        run_from_first_pct=
            excluded.run_from_first_pct,

        swaps_30s=excluded.swaps_30s,
        buys_30s=excluded.buys_30s,
        sells_30s=excluded.sells_30s,
        unique_wallets_30s=
            excluded.unique_wallets_30s,
        unique_buyers_30s=
            excluded.unique_buyers_30s,
        unique_sellers_30s=
            excluded.unique_sellers_30s,
        buy_sol_30s=excluded.buy_sol_30s,
        sell_sol_30s=excluded.sell_sol_30s,
        net_sol_30s=excluded.net_sol_30s,
        buy_share_30s=excluded.buy_share_30s,

        swaps_60s=excluded.swaps_60s,
        buys_60s=excluded.buys_60s,
        sells_60s=excluded.sells_60s,
        unique_wallets_60s=
            excluded.unique_wallets_60s,
        unique_buyers_60s=
            excluded.unique_buyers_60s,
        unique_sellers_60s=
            excluded.unique_sellers_60s,
        buy_sol_60s=excluded.buy_sol_60s,
        sell_sol_60s=excluded.sell_sol_60s,
        net_sol_60s=excluded.net_sol_60s,
        buy_share_60s=excluded.buy_share_60s,

        swaps_120s=excluded.swaps_120s,
        buys_120s=excluded.buys_120s,
        sells_120s=excluded.sells_120s,
        unique_wallets_120s=
            excluded.unique_wallets_120s,
        unique_buyers_120s=
            excluded.unique_buyers_120s,
        unique_sellers_120s=
            excluded.unique_sellers_120s,
        buy_sol_120s=excluded.buy_sol_120s,
        sell_sol_120s=excluded.sell_sol_120s,
        net_sol_120s=excluded.net_sol_120s,
        buy_share_120s=excluded.buy_share_120s,

        swaps_300s=excluded.swaps_300s,
        buys_300s=excluded.buys_300s,
        sells_300s=excluded.sells_300s,
        unique_wallets_300s=
            excluded.unique_wallets_300s,
        unique_buyers_300s=
            excluded.unique_buyers_300s,
        unique_sellers_300s=
            excluded.unique_sellers_300s,
        buy_sol_300s=excluded.buy_sol_300s,
        sell_sol_300s=excluded.sell_sol_300s,
        net_sol_300s=excluded.net_sol_300s,
        buy_share_300s=excluded.buy_share_300s,

        migrated=excluded.migrated,
        migration_timestamp=
            excluded.migration_timestamp,
        migration_status=
            excluded.migration_status,
        migration_signature=
            excluded.migration_signature,

        age_s=excluded.age_s,

        seconds_since_last_clean_swap=
            excluded.seconds_since_last_clean_swap,

        last_update_at=
            excluded.last_update_at
    """, (

        mint,

        first_seen,
        last_seen,

        raw_stats[
            "raw_swaps"
        ],

        len(clean),

        rejected,

        buys_total,
        sells_total,

        buy_sol_total,
        sell_sol_total,

        buy_sol_total
        - sell_sol_total,

        current_price,
        current_at,

        peak_price,
        peak_at,

        drawdown,

        trough_price,
        trough_at,

        run_from_first,

        flow_data[30]["swaps"],
        flow_data[30]["buys"],
        flow_data[30]["sells"],
        flow_data[30]["wallets"],
        flow_data[30]["buyers"],
        flow_data[30]["sellers"],
        flow_data[30]["buy_sol"],
        flow_data[30]["sell_sol"],
        flow_data[30]["net_sol"],
        flow_data[30]["buy_share"],

        flow_data[60]["swaps"],
        flow_data[60]["buys"],
        flow_data[60]["sells"],
        flow_data[60]["wallets"],
        flow_data[60]["buyers"],
        flow_data[60]["sellers"],
        flow_data[60]["buy_sol"],
        flow_data[60]["sell_sol"],
        flow_data[60]["net_sol"],
        flow_data[60]["buy_share"],

        flow_data[120]["swaps"],
        flow_data[120]["buys"],
        flow_data[120]["sells"],
        flow_data[120]["wallets"],
        flow_data[120]["buyers"],
        flow_data[120]["sellers"],
        flow_data[120]["buy_sol"],
        flow_data[120]["sell_sol"],
        flow_data[120]["net_sol"],
        flow_data[120]["buy_share"],

        flow_data[300]["swaps"],
        flow_data[300]["buys"],
        flow_data[300]["sells"],
        flow_data[300]["wallets"],
        flow_data[300]["buyers"],
        flow_data[300]["sellers"],
        flow_data[300]["buy_sol"],
        flow_data[300]["sell_sol"],
        flow_data[300]["net_sol"],
        flow_data[300]["buy_share"],

        migrated,

        migration_ts,

        migration_status,

        migration_signature,

        now - first_seen,

        (
            now - current_at
            if current_at
            is not None
            else None
        ),

        time.time(),
    ))


    db.commit()


# ============================================================
# REBUILD ACTIVE STATES
# ============================================================

def rebuild_states():

    mints = db.execute(f"""
    SELECT DISTINCT token_mint

    FROM {CLEAN_TABLE}

    WHERE token_mint IS NOT NULL
    """).fetchall()


    for r in mints:

        rebuild_state(
            r["token_mint"]
        )


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system(
        "clear"
    )


    quality = db.execute(f"""
    SELECT
        COUNT(*) AS total,

        SUM(price_valid=1) AS clean,

        SUM(price_valid=0) AS rejected,

        SUM(reject_reason='JUMP_HIGH') AS jump_high,

        SUM(reject_reason='JUMP_LOW') AS jump_low

    FROM {CLEAN_TABLE}
    """).fetchone()


    states = db.execute(f"""
    SELECT *

    FROM {STATE_TABLE}

    ORDER BY last_seen DESC
    """).fetchall()


    print("=" * 185)

    print(
        "MEMECOIN LAB — T116C "
        "PUMP PRICE CLEANER / TOKEN LIFECYCLE"
    )

    print("=" * 185)


    print(
        f"RAW PROCESSED     : {quality['total'] or 0}"
    )

    print(
        f"CLEAN             : {quality['clean'] or 0}"
    )

    print(
        f"REJECTED          : {quality['rejected'] or 0}"
    )

    print(
        f"JUMP HIGH         : {quality['jump_high'] or 0}"
    )

    print(
        f"JUMP LOW          : {quality['jump_low'] or 0}"
    )

    print(
        f"TOKENS TRACKED    : {len(states)}"
    )

    print(
        f"MIGRATED MATCHED  : "
        f"{sum(r['migrated'] for r in states)}"
    )


    print()

    print(
        "PRICE FILTER      : "
        "MEDIAN LAST 15 CLEAN"
    )

    print(
        "VALID RANGE       : "
        "0.20x → 5.00x REFERENCE"
    )

    print(
        "MODEL FITTING     : NONE"
    )

    print(
        "DUMP THRESHOLD    : NONE YET"
    )


    print()

    print("=" * 185)

    print(
        "ACTIVE PRE-MIGRATION TOKENS"
    )

    print("=" * 185)


    active = [
        r
        for r in states

        if (
            not r["migrated"]

            and r[
                "seconds_since_last_clean_swap"
            ] is not None

            and r[
                "seconds_since_last_clean_swap"
            ] <= 300
        )
    ]


    active.sort(
        key=lambda r:
            (
                r[
                    "clean_swaps"
                ],
                r[
                    "buy_sol_total"
                ]
                + r[
                    "sell_sol_total"
                ]
            ),
        reverse=True
    )


    for r in active[:30]:

        print(
            f"{r['token_mint'][:18]:18} "

            f"| N={r['clean_swaps']:3d} "

            f"| DD={fmt(r['drawdown_from_peak_pct'],1):>7}% "

            f"| RUN={fmt(r['run_from_first_pct'],1):>7}% "

            f"| B/S="
            f"{r['buys_total']:3d}/"
            f"{r['sells_total']:<3d} "

            f"| NET="
            f"{r['net_sol_total']:+8.3f} "

            f"| 60s="
            f"{r['swaps_60s']:2d}sw "
            f"{r['net_sol_60s']:+7.3f}SOL "

            f"| MIG={r['migrated']}"
        )


    print()

    print("=" * 185)

    print(
        "DEEPEST CURRENT DRAWDOWNS"
    )

    print("=" * 185)


    dd = [
        r
        for r in active

        if r[
            "drawdown_from_peak_pct"
        ] is not None
    ]


    dd.sort(
        key=lambda r:
            r[
                "drawdown_from_peak_pct"
            ]
    )


    for r in dd[:20]:

        print(
            f"{r['token_mint'][:18]:18} "

            f"| DD="
            f"{fmt(r['drawdown_from_peak_pct'],1):>8}% "

            f"| PEAK="
            f"{r['peak_price']:.3e} "

            f"| NOW="
            f"{r['current_price']:.3e} "

            f"| N="
            f"{r['clean_swaps']:3d} "

            f"| NET="
            f"{r['net_sol_total']:+8.3f}"
        )


    print()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops T116C only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        new_n = process_new_raw()


        if new_n:

            rebuild_states()


        else:

            # Still refresh migration state / age / flow
            rebuild_states()


        show()


        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()

    print(
        "T116C stopped safely."
    )


finally:

    db.close()
