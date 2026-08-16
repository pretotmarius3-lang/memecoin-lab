#!/usr/bin/env python3

import sqlite3
import time
import os

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

REFRESH = 10

DUMP_LEVELS = [
    20,
    30,
    40,
    50,
]

HORIZONS = [
    10,
    30,
    60,
    300,
    900,
]

STATE = "t108_dump_state"
EVENTS = "t108_dump_events"
META = "t108_meta"


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


def pct(a, b):

    if (
        a is None
        or b is None
        or a <= 0
    ):
        return None

    return 100.0 * (
        b / a - 1.0
    )


# ============================================================
# META
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {META} (
    key TEXT PRIMARY KEY,
    value REAL
)
""")

row = db.execute(
    f"""
    SELECT value
    FROM {META}
    WHERE key='started_at'
    """
).fetchone()

if row:

    STARTED_AT = float(
        row["value"]
    )

else:

    STARTED_AT = time.time()

    db.execute(
        f"""
        INSERT INTO {META}(key,value)
        VALUES ('started_at',?)
        """,
        (
            STARTED_AT,
        )
    )

    db.commit()


# ============================================================
# STATE
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {STATE} (

    token_mint TEXT PRIMARY KEY,

    migrated_at REAL,

    initialized_at REAL,

    first_price REAL,
    first_price_at REAL,

    current_price REAL,
    current_price_at REAL,

    peak_price REAL,
    peak_at REAL,

    current_drawdown REAL,

    last_processed_ts REAL,

    hit_20 INTEGER NOT NULL DEFAULT 0,
    hit_30 INTEGER NOT NULL DEFAULT 0,
    hit_40 INTEGER NOT NULL DEFAULT 0,
    hit_50 INTEGER NOT NULL DEFAULT 0,

    last_update_at REAL
)
""")


# ============================================================
# EVENTS
# ============================================================

future_cols = []

for h in HORIZONS:

    future_cols.append(f"""
        price_{h}s REAL,
        return_{h}s REAL,
        peak_recovery_{h}s REAL,
        done_{h}s INTEGER NOT NULL DEFAULT 0
    """)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {EVENTS} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    token_mint TEXT NOT NULL,

    dump_level INTEGER NOT NULL,

    trigger_timestamp REAL NOT NULL,

    peak_price REAL NOT NULL,
    peak_at REAL NOT NULL,

    trigger_price REAL NOT NULL,
    drawdown_pct REAL NOT NULL,

    holders_at_trigger INTEGER,

    liquidity_usd REAL,
    market_cap REAL,
    fdv REAL,
    volume_m5 REAL,

    buys_m5 INTEGER,
    sells_m5 INTEGER,

    pair_address TEXT,
    dex_id TEXT,

    pre60_swaps INTEGER,
    pre60_buys INTEGER,
    pre60_sells INTEGER,
    pre60_buy_sol REAL,
    pre60_sell_sol REAL,
    pre60_net_sol REAL,

    post30_swaps INTEGER,
    post30_buys INTEGER,
    post30_sells INTEGER,
    post30_buy_sol REAL,
    post30_sell_sol REAL,
    post30_net_sol REAL,

    {",".join(future_cols)},

    created_at REAL NOT NULL,

    UNIQUE(
        token_mint,
        dump_level
    )
)
""")

db.commit()


# ============================================================
# SOURCES
# ============================================================

def migrated_tokens():

    return db.execute("""
    SELECT DISTINCT
        token_mint,
        COALESCE(
            block_time,
            detected_at
        ) AS migrated_at

    FROM t101_migrations

    WHERE
        status='OK'
        AND confirmed=1
        AND migrate_v2=1
        AND create_pool=1
        AND token_mint IS NOT NULL
    """).fetchall()


def latest_holder_before(
    mint,
    ts
):

    row = db.execute("""
    SELECT holder_count

    FROM t101_migrated_holder_snapshots

    WHERE
        token_mint=?
        AND status='OK'
        AND checked_at <= ?

    ORDER BY checked_at DESC

    LIMIT 1
    """, (
        mint,
        ts
    )).fetchone()

    return (
        row["holder_count"]
        if row
        else None
    )


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

    return {
        "swaps":
            len(rows),

        "buys":
            buys,

        "sells":
            sells,

        "buy_sol":
            buy_sol,

        "sell_sol":
            sell_sol,

        "net_sol":
            buy_sol - sell_sol,
    }


# ============================================================
# INITIALIZE TOKEN
# ============================================================

def ensure_token(r):

    mint = r[
        "token_mint"
    ]

    exists = db.execute(
        f"""
        SELECT 1
        FROM {STATE}
        WHERE token_mint=?
        """,
        (
            mint,
        )
    ).fetchone()

    if exists:
        return


    # Strictly prospective baseline.
    # First usable price at/after T108 starts,
    # or migration time if token migrates later.

    start = max(
        STARTED_AT,
        r["migrated_at"] or 0
    )


    first = db.execute("""
    SELECT
        timestamp,
        price_usd

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
        start
    )).fetchone()


    if not first:
        return


    db.execute(
        f"""
        INSERT INTO {STATE} (

            token_mint,
            migrated_at,

            initialized_at,

            first_price,
            first_price_at,

            current_price,
            current_price_at,

            peak_price,
            peak_at,

            current_drawdown,

            last_processed_ts,

            last_update_at
        )

        VALUES (
            ?,?,
            ?,
            ?,?,
            ?,?,
            ?,?,
            0,
            ?,
            ?
        )
        """,
        (

            mint,
            r["migrated_at"],

            time.time(),

            first["price_usd"],
            first["timestamp"],

            first["price_usd"],
            first["timestamp"],

            first["price_usd"],
            first["timestamp"],

            first["timestamp"],

            time.time(),
        )
    )

    db.commit()


# ============================================================
# TRIGGER
# ============================================================

def create_dump_event(
    mint,
    level,
    ts,
    price,
    peak,
    peak_at,
    dex
):

    existing = db.execute(
        f"""
        SELECT 1

        FROM {EVENTS}

        WHERE
            token_mint=?
            AND dump_level=?
        """,
        (
            mint,
            level
        )
    ).fetchone()

    if existing:
        return


    holders = latest_holder_before(
        mint,
        ts
    )

    pre = flow_window(
        mint,
        ts - 60,
        ts
    )

    dd = pct(
        peak,
        price
    )


    db.execute(
        f"""
        INSERT INTO {EVENTS} (

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

            pre60_swaps,
            pre60_buys,
            pre60_sells,
            pre60_buy_sol,
            pre60_sell_sol,
            pre60_net_sol,

            created_at
        )

        VALUES (
            ?,?,
            ?,
            ?,?,
            ?,?,
            ?,
            ?,?,?,?,
            ?,?,
            ?,?,
            ?,?,?,?,?,?,
            ?
        )
        """,
        (

            mint,
            level,

            ts,

            peak,
            peak_at,

            price,
            dd,

            holders,

            dex["liquidity_usd"],
            dex["market_cap"],
            dex["fdv"],
            dex["volume_m5"],

            dex["buys_m5"],
            dex["sells_m5"],

            dex["pair_address"],
            dex["dex_id"],

            pre["swaps"],
            pre["buys"],
            pre["sells"],
            pre["buy_sol"],
            pre["sell_sol"],
            pre["net_sol"],

            time.time(),
        )
    )

    db.commit()

    print(
        f"🔥 T108 DUMP "
        f"| {mint[:16]}... "
        f"| LEVEL=-{level}% "
        f"| DD={dd:.1f}% "
        f"| H={holders}"
    )


# ============================================================
# PROCESS PRICES
# ============================================================

def process_token(mint):

    s = db.execute(
        f"""
        SELECT *
        FROM {STATE}
        WHERE token_mint=?
        """,
        (
            mint,
        )
    ).fetchone()

    if not s:
        return


    rows = db.execute("""
    SELECT *

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp > ?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp ASC
    """, (
        mint,
        s["last_processed_ts"]
    )).fetchall()


    if not rows:
        return


    peak = s[
        "peak_price"
    ]

    peak_at = s[
        "peak_at"
    ]


    flags = {
        20: s["hit_20"],
        30: s["hit_30"],
        40: s["hit_40"],
        50: s["hit_50"],
    }


    last_ts = s[
        "last_processed_ts"
    ]

    current_price = s[
        "current_price"
    ]

    current_dd = s[
        "current_drawdown"
    ]


    for r in rows:

        ts = r[
            "timestamp"
        ]

        price = r[
            "price_usd"
        ]


        if price > peak:

            peak = price
            peak_at = ts


        dd = pct(
            peak,
            price
        )


        for level in DUMP_LEVELS:

            if flags[level]:
                continue

            if (
                dd is not None
                and dd <= -level
            ):

                create_dump_event(
                    mint,
                    level,
                    ts,
                    price,
                    peak,
                    peak_at,
                    r
                )

                flags[level] = 1


        current_price = price
        current_dd = dd
        last_ts = ts


    db.execute(
        f"""
        UPDATE {STATE}

        SET
            current_price=?,
            current_price_at=?,

            peak_price=?,
            peak_at=?,

            current_drawdown=?,

            last_processed_ts=?,

            hit_20=?,
            hit_30=?,
            hit_40=?,
            hit_50=?,

            last_update_at=?

        WHERE token_mint=?
        """,
        (

            current_price,
            last_ts,

            peak,
            peak_at,

            current_dd,

            last_ts,

            flags[20],
            flags[30],
            flags[40],
            flags[50],

            time.time(),

            mint,
        )
    )

    db.commit()


# ============================================================
# FUTURE OUTCOMES
# ============================================================

def fill_future():

    events = db.execute(
        f"""
        SELECT *
        FROM {EVENTS}
        ORDER BY trigger_timestamp
        """
    ).fetchall()


    now = time.time()


    for e in events:

        updates = {}


        # POST30 sampled flow
        if (
            now >= e["trigger_timestamp"] + 30
            and e["post30_swaps"] is None
        ):

            post = flow_window(
                e["token_mint"],
                e["trigger_timestamp"],
                e["trigger_timestamp"] + 30
            )

            updates[
                "post30_swaps"
            ] = post["swaps"]

            updates[
                "post30_buys"
            ] = post["buys"]

            updates[
                "post30_sells"
            ] = post["sells"]

            updates[
                "post30_buy_sol"
            ] = post["buy_sol"]

            updates[
                "post30_sell_sol"
            ] = post["sell_sol"]

            updates[
                "post30_net_sol"
            ] = post["net_sol"]


        for h in HORIZONS:

            if e[
                f"done_{h}s"
            ]:
                continue


            target = (
                e["trigger_timestamp"]
                + h
            )

            if now < target:
                continue


            r = db.execute("""
            SELECT
                timestamp,
                price_usd

            FROM dex_prices

            WHERE
                token_mint=?
                AND timestamp >= ?
                AND price_usd IS NOT NULL
                AND price_usd > 0

            ORDER BY timestamp ASC

            LIMIT 1
            """, (
                e["token_mint"],
                target
            )).fetchone()


            if not r:
                continue


            updates[
                f"price_{h}s"
            ] = r["price_usd"]

            updates[
                f"return_{h}s"
            ] = pct(
                e["trigger_price"],
                r["price_usd"]
            )

            updates[
                f"peak_recovery_{h}s"
            ] = pct(
                e["peak_price"],
                r["price_usd"]
            )

            updates[
                f"done_{h}s"
            ] = 1


        if not updates:
            continue


        sql = ", ".join(
            f"{k}=?"
            for k in updates
        )

        db.execute(
            f"""
            UPDATE {EVENTS}

            SET {sql}

            WHERE id=?
            """,
            (
                *updates.values(),
                e["id"]
            )
        )

        db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    os.system("clear")


    states = db.execute(
        f"""
        SELECT *
        FROM {STATE}
        ORDER BY current_drawdown ASC
        """
    ).fetchall()


    events = db.execute(
        f"""
        SELECT *
        FROM {EVENTS}
        ORDER BY trigger_timestamp DESC
        """
    ).fetchall()


    print("=" * 150)

    print(
        "MEMECOIN LAB — T108 DUMP → RESURRECTION"
    )

    print("=" * 150)


    print(
        f"MODE             : PROSPECTIVE / OBSERVATION ONLY"
    )

    print(
        f"T108 START       : "
        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(STARTED_AT))}"
    )

    print(
        f"TOKENS TRACKED   : {len(states)}"
    )

    print(
        f"DUMP EVENTS      : {len(events)}"
    )

    for level in DUMP_LEVELS:

        n = sum(
            e["dump_level"] == level
            for e in events
        )

        print(
            f"TRIGGER -{level:2d}%     : {n}"
        )


    print()

    print("=" * 150)
    print("DEEPEST CURRENT DRAWDOWNS")
    print("=" * 150)


    for r in states[:15]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| DD={r['current_drawdown'] or 0:8.1f}% "
            f"| PEAK={r['peak_price'] or 0:.8g} "
            f"| NOW={r['current_price'] or 0:.8g}"
        )


    print()

    print("=" * 150)
    print("LATEST DUMP EVENTS")
    print("=" * 150)


    for e in events[:20]:

        ret30 = e[
            "return_30s"
        ]

        ret300 = e[
            "return_300s"
        ]

        print(
            f"{e['token_mint'][:18]:18} "
            f"| DUMP=-{e['dump_level']:2d}% "
            f"| H={str(e['holders_at_trigger']):>5} "
            f"| PRE B/S={e['pre60_buys']}/{e['pre60_sells']} "
            f"| PRE NET={e['pre60_net_sol'] or 0:+.3f} "
            f"| P30 NET={e['post30_net_sol'] if e['post30_net_sol'] is not None else 0:+.3f} "
            f"| R30={ret30 if ret30 is not None else 0:+7.1f}% "
            f"| R300={ret300 if ret300 is not None else 0:+7.1f}%"
        )


    print()

    print(
        f"Refresh every {REFRESH}s | CTRL+C stops T108 only"
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for r in migrated_tokens():

            ensure_token(
                r
            )

            process_token(
                r["token_mint"]
            )


        fill_future()

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T108 stopped safely."
    )


finally:

    db.close()
