import asyncio
import aiohttp
import sqlite3
import time
import statistics
import random

DB = "validation_v090.db"

DEX_URL = (
    "https://api.dexscreener.com/"
    "token-pairs/v1/solana/{}"
)

# ============================================================
# API PROTECTION V1.0.2
# ============================================================

# Base polling interval. Actual interval becomes slower
# automatically when many events are tracked.
POLL_SECONDS = 3.0

# Conservative global rate limit.
# DexScreener documents generous limits, but bursts from
# many simultaneous trackers can still trigger HTTP 429.
MAX_REQUESTS_PER_MINUTE = 180

MIN_REQUEST_INTERVAL = 60.0 / MAX_REQUESTS_PER_MINUTE

# Global lock / timestamp shared by every event tracker
api_lock = asyncio.Lock()
last_api_request = 0.0

# Backoff state
BACKOFF_MIN = 2.0
BACKOFF_MAX = 60.0


HORIZONS = [
    5,
    10,
    20,
    30,
    60,
    300
]

# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute("""
CREATE TABLE IF NOT EXISTS dex_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    event_id INTEGER,
    token_mint TEXT,

    timestamp REAL,

    price_usd REAL,
    price_native REAL,

    liquidity_usd REAL,
    market_cap REAL,
    fdv REAL,

    volume_m5 REAL,

    buys_m5 INTEGER,
    sells_m5 INTEGER,

    pair_address TEXT,
    dex_id TEXT
)
""")

db.execute("""
CREATE INDEX IF NOT EXISTS idx_dex_event_time
ON dex_prices(event_id,timestamp)
""")

# Add V1 outcome columns if absent

existing = {
    row[1]
    for row in db.execute(
        "PRAGMA table_info(events)"
    )
}

columns = []

for h in HORIZONS:
    columns += [
        (
            f"dex_return_{h}s",
            "REAL"
        ),
        (
            f"dex_delay_{h}s",
            "REAL"
        ),
        (
            f"dex_done_{h}s",
            "INTEGER DEFAULT 0"
        ),
    ]

for name, datatype in columns:

    if name not in existing:

        db.execute(
            f"""
            ALTER TABLE events
            ADD COLUMN {name} {datatype}
            """
        )

db.commit()

# ============================================================
# HELPERS
# ============================================================

def safe_float(x):

    try:
        return float(x)

    except Exception:
        return None


def safe_int(x):

    try:
        return int(x)

    except Exception:
        return 0


def select_pair(
    pairs,
    mint
):

    candidates = []

    for p in pairs:

        if p.get("chainId") != "solana":
            continue

        base = (
            p.get("baseToken")
            or {}
        )

        quote = (
            p.get("quoteToken")
            or {}
        )

        # We strongly prefer mint as BASE token.
        if base.get("address") == mint:

            liq = safe_float(
                (
                    p.get("liquidity")
                    or {}
                ).get("usd")
            ) or 0.0

            candidates.append(
                (
                    2,
                    liq,
                    p
                )
            )

        elif quote.get("address") == mint:

            liq = safe_float(
                (
                    p.get("liquidity")
                    or {}
                ).get("usd")
            ) or 0.0

            candidates.append(
                (
                    1,
                    liq,
                    p
                )
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (
            x[0],
            x[1]
        ),
        reverse=True
    )

    return candidates[0][2]


async def rate_limit_wait():
    global last_api_request

    async with api_lock:

        now = time.monotonic()

        elapsed = (
            now
            - last_api_request
        )

        if elapsed < MIN_REQUEST_INTERVAL:

            await asyncio.sleep(
                MIN_REQUEST_INTERVAL
                - elapsed
            )

        # Tiny jitter prevents synchronized bursts
        await asyncio.sleep(
            random.uniform(
                0.02,
                0.12
            )
        )

        last_api_request = (
            time.monotonic()
        )


async def dex_snapshot(
    session,
    mint
):

    backoff = BACKOFF_MIN

    for attempt in range(6):

        await rate_limit_wait()

        try:

            async with session.get(
                DEX_URL.format(mint),
                timeout=10
            ) as response:

                if response.status == 429:

                    retry_after = (
                        response.headers.get(
                            "Retry-After"
                        )
                    )

                    if retry_after:

                        try:
                            wait = float(
                                retry_after
                            )

                        except Exception:
                            wait = backoff

                    else:
                        wait = backoff

                    wait += random.uniform(
                        0.25,
                        1.0
                    )

                    print(
                        f"DEX 429 | "
                        f"wait={wait:.1f}s "
                        f"| {mint[:10]}..."
                    )

                    await asyncio.sleep(
                        wait
                    )

                    backoff = min(
                        backoff * 2,
                        BACKOFF_MAX
                    )

                    continue


                if response.status != 200:

                    print(
                        f"DEX HTTP "
                        f"{response.status}"
                    )

                    return None


                pairs = (
                    await response.json()
                )


        except asyncio.TimeoutError:

            await asyncio.sleep(
                backoff
            )

            backoff = min(
                backoff * 2,
                BACKOFF_MAX
            )

            continue


        except Exception as e:

            print(
                "DEX ERROR:",
                type(e).__name__
            )

            await asyncio.sleep(
                min(
                    backoff,
                    10
                )
            )

            backoff = min(
                backoff * 2,
                BACKOFF_MAX
            )

            continue


        if not isinstance(
            pairs,
            list
        ):

            return None


        pair = select_pair(
            pairs,
            mint
        )

        if pair is None:

            return None


        price_usd = safe_float(
            pair.get(
                "priceUsd"
            )
        )


        price_native = safe_float(
            pair.get(
                "priceNative"
            )
        )


        liquidity = safe_float(
            (
                pair.get("liquidity")
                or {}
            ).get("usd")
        )


        market_cap = safe_float(
            pair.get(
                "marketCap"
            )
        )


        fdv = safe_float(
            pair.get(
                "fdv"
            )
        )


        volume = (
            pair.get("volume")
            or {}
        )


        volume_m5 = safe_float(
            volume.get("m5")
        )


        txns = (
            pair.get("txns")
            or {}
        )


        m5 = (
            txns.get("m5")
            or {}
        )


        return {
            "price_usd":
                price_usd,

            "price_native":
                price_native,

            "liquidity_usd":
                liquidity,

            "market_cap":
                market_cap,

            "fdv":
                fdv,

            "volume_m5":
                volume_m5,

            "buys_m5":
                safe_int(
                    m5.get("buys")
                ),

            "sells_m5":
                safe_int(
                    m5.get("sells")
                ),

            "pair_address":
                pair.get(
                    "pairAddress"
                ),

            "dex_id":
                pair.get(
                    "dexId"
                ),
        }


    print(
        f"DEX GIVEUP | "
        f"{mint[:10]}..."
    )

    return None


def save_snapshot(
    event_id,
    mint,
    snap
):

    if (
        snap is None
        or snap["price_usd"]
        is None
        or snap["price_usd"] <= 0
    ):
        return False

    db.execute("""
        INSERT INTO dex_prices (
            event_id,
            token_mint,
            timestamp,

            price_usd,
            price_native,

            liquidity_usd,
            market_cap,
            fdv,

            volume_m5,

            buys_m5,
            sells_m5,

            pair_address,
            dex_id
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        event_id,
        mint,
        time.time(),

        snap[
            "price_usd"
        ],

        snap[
            "price_native"
        ],

        snap[
            "liquidity_usd"
        ],

        snap[
            "market_cap"
        ],

        snap[
            "fdv"
        ],

        snap[
            "volume_m5"
        ],

        snap[
            "buys_m5"
        ],

        snap[
            "sells_m5"
        ],

        snap[
            "pair_address"
        ],

        snap[
            "dex_id"
        ],
    ))

    db.commit()

    return True

# ============================================================
# ENTRY PRICE
# ============================================================

def dex_entry_price(
    event_id
):

    row = db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE event_id=?

        ORDER BY timestamp ASC

        LIMIT 1
    """, (
        event_id,
    )).fetchone()

    if not row:
        return None, None

    return (
        row["price_usd"],
        row["timestamp"]
    )

# ============================================================
# OUTCOME CALCULATION
# ============================================================

def update_event_outcomes(
    event
):

    event_id = event["id"]

    entry_price, entry_time = (
        dex_entry_price(
            event_id
        )
    )

    if (
        entry_price is None
        or entry_price <= 0
    ):
        return

    now = time.time()

    for h in HORIZONS:

        done_col = (
            f"dex_done_{h}s"
        )

        ret_col = (
            f"dex_return_{h}s"
        )

        delay_col = (
            f"dex_delay_{h}s"
        )

        current = db.execute(
            f"""
            SELECT {done_col}
            FROM events
            WHERE id=?
            """,
            (
                event_id,
            )
        ).fetchone()

        if current[0] == 1:
            continue

        target = (
            entry_time + h
        )

        if now < target:
            continue

        row = db.execute("""
            SELECT
                timestamp,
                price_usd

            FROM dex_prices

            WHERE
                event_id=?
                AND timestamp >= ?

            ORDER BY timestamp ASC

            LIMIT 1
        """, (
            event_id,
            target
        )).fetchone()

        if not row:
            continue

        price = row[
            "price_usd"
        ]

        if (
            price is None
            or price <= 0
        ):
            continue

        delay = (
            row["timestamp"]
            - target
        )

        ret = (
            price
            / entry_price
            - 1
        ) * 100

        db.execute(
            f"""
            UPDATE events

            SET
                {ret_col}=?,
                {delay_col}=?,
                {done_col}=1

            WHERE id=?
            """,
            (
                ret,
                delay,
                event_id
            )
        )

    db.commit()

# ============================================================
# EVENT TRACKER
# ============================================================

async def track_event(
    session,
    event
):

    event_id = event["id"]
    mint = event["token_mint"]

    print()
    print(
        "🎯 TRACKING"
        f" event={event_id}"
        f" | {mint[:14]}..."
    )

    start = time.time()

    first_price = None

    while (
        time.time() - start
        < 330
    ):

        snap = await dex_snapshot(
            session,
            mint
        )

        if save_snapshot(
            event_id,
            mint,
            snap
        ):

            if first_price is None:

                first_price = (
                    snap["price_usd"]
                )

                print(
                    f"   ENTRY "
                    f"${first_price:.12g}"
                    f" | LIQ "
                    f"${(snap['liquidity_usd'] or 0):,.0f}"
                    f" | M5 "
                    f"B{snap['buys_m5']}"
                    f"/S{snap['sells_m5']}"
                )

            update_event_outcomes(
                event
            )

        # Dynamic polling:
        # slower when many trackers run simultaneously.
        active_now = len(
            asyncio.all_tasks()
        )

        dynamic_poll = max(
            POLL_SECONDS,
            active_now
            * MIN_REQUEST_INTERVAL
            * 0.35
        )

        await asyncio.sleep(
            dynamic_poll
            + random.uniform(
                0.05,
                0.35
            )
        )

    update_event_outcomes(
        event
    )

    print(
        f"✅ FINISHED "
        f"event={event_id}"
    )

# ============================================================
# MONITOR
# ============================================================

async def monitor():

    while True:

        await asyncio.sleep(
            10
        )

        events = db.execute("""
            SELECT COUNT(*)
            FROM events
        """).fetchone()[0]

        tracked = db.execute("""
            SELECT COUNT(
                DISTINCT event_id
            )
            FROM dex_prices
        """).fetchone()[0]

        print()
        print("─" * 90)

        print(
            f"EVENTS {events}"
            f" | DEX TRACKED {tracked}"
        )

        values = []

        for h in HORIZONS:

            n = db.execute(
                f"""
                SELECT COUNT(*)
                FROM events
                WHERE dex_return_{h}s
                IS NOT NULL
                """
            ).fetchone()[0]

            values.append(
                f"{h}s={n}"
            )

        print(
            "DEX OUTCOMES "
            + " | ".join(
                values
            )
        )

# ============================================================
# MAIN
# ============================================================


async def main():

    print()
    print("=" * 90)
    print(
        "MEMECOIN LAB — "
        "PRICE TRACKER V1.0.1"
    )
    print("=" * 90)

    print("SOURCE : DEX Screener")
    print("POLL   : 2 seconds")
    print("MODE   : attente automatique des nouveaux events")
    print("=" * 90)

    connector = aiohttp.TCPConnector(
        limit=20
    )

    tracked_events = set()

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        asyncio.create_task(
            monitor()
        )

        while True:

            rows = db.execute("""
                SELECT *
                FROM events
                ORDER BY id ASC
            """).fetchall()

            for event in rows:

                event_id = event["id"]

                if event_id in tracked_events:
                    continue

                tracked_events.add(
                    event_id
                )

                print()
                print(
                    "🔥 NOUVEL EVENT DETECTE "
                    f"id={event_id}"
                )

                asyncio.create_task(
                    track_event(
                        session,
                        event
                    )
                )

            if not tracked_events:

                print(
                    "\r⏳ En attente d'un EVENT V0.9...",
                    end="",
                    flush=True
                )

            await asyncio.sleep(2)


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "\nV1 stopped."
        )

    finally:

        db.commit()
        db.close()
