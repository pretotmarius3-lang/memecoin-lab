#!/usr/bin/env python3

import sqlite3
import time
import math

DB = "validation_v090.db"

MIGRATIONS = "t101_migrations"
HOLDERS = "t101_migrated_holder_snapshots"
TABLE = "t102_migrated_token_watchlist"

REFRESH = 30

# Research labels only — not trading thresholds
RUN_RETURN = 100.0
CRASH_DRAWDOWN = -50.0
RECOVERY_FROM_TROUGH = 50.0

MAX_DEX_STALENESS = 120.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def fmt(x, n=2):
    return "NA" if x is None else f"{x:.{n}f}"


def pct_change(a, b):
    if not valid(a) or not valid(b) or a <= 0:
        return None
    return 100.0 * (b / a - 1.0)


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    token_mint TEXT PRIMARY KEY,

    migration_signature TEXT NOT NULL,
    migrated_at REAL,

    first_seen_at REAL,
    last_update_at REAL,

    holder_count INTEGER,

    first_price REAL,
    current_price REAL,

    peak_price REAL,
    peak_at REAL,

    trough_after_peak REAL,
    trough_at REAL,

    return_from_first REAL,
    drawdown_from_peak REAL,
    recovery_from_trough REAL,

    liquidity_usd REAL,
    market_cap REAL,
    fdv REAL,

    volume_m5 REAL,
    buys_m5 INTEGER,
    sells_m5 INTEGER,

    pair_address TEXT,
    dex_id TEXT,

    dex_timestamp REAL,
    dex_age REAL,

    ever_run INTEGER NOT NULL DEFAULT 0,
    ever_crashed INTEGER NOT NULL DEFAULT 0,
    ever_recovered INTEGER NOT NULL DEFAULT 0,
    second_new_high INTEGER NOT NULL DEFAULT 0,

    state TEXT NOT NULL DEFAULT 'MIGRATED'
)
""")

db.commit()


def migrations():
    return db.execute(f"""
    SELECT
        signature,
        token_mint,
        COALESCE(block_time, detected_at) AS migrated_at
    FROM {MIGRATIONS}
    WHERE
        status='OK'
        AND confirmed=1
        AND migrate_v2=1
        AND create_pool=1
        AND token_mint IS NOT NULL
    ORDER BY migrated_at
    """).fetchall()


def latest_holders(mint):
    row = db.execute(f"""
    SELECT holder_count
    FROM {HOLDERS}
    WHERE
        token_mint=?
        AND status='OK'
        AND holder_count IS NOT NULL
    ORDER BY checked_at DESC
    LIMIT 1
    """, (mint,)).fetchone()

    return row["holder_count"] if row else None


def dex_history(mint, migrated_at):
    return db.execute("""
    SELECT
        timestamp,
        price_usd,
        liquidity_usd,
        market_cap,
        fdv,
        volume_m5,
        buys_m5,
        sells_m5,
        pair_address,
        dex_id
    FROM dex_prices
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND price_usd IS NOT NULL
        AND price_usd > 0
    ORDER BY timestamp
    """, (mint, migrated_at)).fetchall()


def update_token(migration):
    mint = migration["token_mint"]
    migrated_at = migration["migrated_at"]

    holder_count = latest_holders(mint)
    prices = dex_history(mint, migrated_at)

    if not prices:
        db.execute(f"""
        INSERT INTO {TABLE} (
            token_mint,
            migration_signature,
            migrated_at,
            first_seen_at,
            last_update_at,
            holder_count,
            state
        )
        VALUES (?, ?, ?, ?, ?, ?, 'WAIT_PRICE')
        ON CONFLICT(token_mint)
        DO UPDATE SET
            last_update_at=excluded.last_update_at,
            holder_count=excluded.holder_count,
            state='WAIT_PRICE'
        """, (
            mint,
            migration["signature"],
            migrated_at,
            time.time(),
            time.time(),
            holder_count,
        ))

        db.commit()
        return

    first = prices[0]
    latest = prices[-1]

    first_price = first["price_usd"]
    current_price = latest["price_usd"]

    peak = max(
        prices,
        key=lambda r: r["price_usd"]
    )

    peak_price = peak["price_usd"]
    peak_at = peak["timestamp"]

    after_peak = [
        r for r in prices
        if r["timestamp"] >= peak_at
    ]

    trough = min(
        after_peak,
        key=lambda r: r["price_usd"]
    )

    trough_price = trough["price_usd"]
    trough_at = trough["timestamp"]

    current_return = pct_change(
        first_price,
        current_price
    )

    peak_return = pct_change(
        first_price,
        peak_price
    )

    current_drawdown = pct_change(
        peak_price,
        current_price
    )

    recovery = pct_change(
        trough_price,
        current_price
    )

    old = db.execute(f"""
    SELECT *
    FROM {TABLE}
    WHERE token_mint=?
    """, (mint,)).fetchone()

    ever_run = int(old["ever_run"]) if old else 0
    ever_crashed = int(old["ever_crashed"]) if old else 0
    ever_recovered = int(old["ever_recovered"]) if old else 0
    second_new_high = int(old["second_new_high"]) if old else 0

    if peak_return is not None and peak_return >= RUN_RETURN:
        ever_run = 1

    if (
        ever_run
        and current_drawdown is not None
        and current_drawdown <= CRASH_DRAWDOWN
    ):
        ever_crashed = 1

    if (
        ever_crashed
        and recovery is not None
        and recovery >= RECOVERY_FROM_TROUGH
    ):
        ever_recovered = 1

    # Important: this uses the previously stored peak as memory
    # when determining a genuine post-crash new high.
    old_peak = old["peak_price"] if old and valid(old["peak_price"]) else None

    if (
        ever_crashed
        and old_peak is not None
        and current_price > old_peak
    ):
        second_new_high = 1

    dex_timestamp = latest["timestamp"]
    dex_age = time.time() - dex_timestamp

    if dex_age > MAX_DEX_STALENESS:
        state = "STALE"
    elif second_new_high:
        state = "SECOND_RUN"
    elif ever_recovered:
        state = "RECOVERY"
    elif ever_crashed:
        state = "CRASH_WATCH"
    elif ever_run:
        state = "RUNNING"
    else:
        state = "MIGRATED"

    db.execute(f"""
    INSERT INTO {TABLE} (
        token_mint,
        migration_signature,
        migrated_at,
        first_seen_at,
        last_update_at,
        holder_count,

        first_price,
        current_price,
        peak_price,
        peak_at,
        trough_after_peak,
        trough_at,

        return_from_first,
        drawdown_from_peak,
        recovery_from_trough,

        liquidity_usd,
        market_cap,
        fdv,
        volume_m5,
        buys_m5,
        sells_m5,

        pair_address,
        dex_id,
        dex_timestamp,
        dex_age,

        ever_run,
        ever_crashed,
        ever_recovered,
        second_new_high,
        state
    )
    VALUES (
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?
    )
    ON CONFLICT(token_mint)
    DO UPDATE SET
        last_update_at=excluded.last_update_at,
        holder_count=excluded.holder_count,

        current_price=excluded.current_price,
        peak_price=excluded.peak_price,
        peak_at=excluded.peak_at,
        trough_after_peak=excluded.trough_after_peak,
        trough_at=excluded.trough_at,

        return_from_first=excluded.return_from_first,
        drawdown_from_peak=excluded.drawdown_from_peak,
        recovery_from_trough=excluded.recovery_from_trough,

        liquidity_usd=excluded.liquidity_usd,
        market_cap=excluded.market_cap,
        fdv=excluded.fdv,
        volume_m5=excluded.volume_m5,
        buys_m5=excluded.buys_m5,
        sells_m5=excluded.sells_m5,

        pair_address=excluded.pair_address,
        dex_id=excluded.dex_id,
        dex_timestamp=excluded.dex_timestamp,
        dex_age=excluded.dex_age,

        ever_run=excluded.ever_run,
        ever_crashed=excluded.ever_crashed,
        ever_recovered=excluded.ever_recovered,
        second_new_high=excluded.second_new_high,
        state=excluded.state
    """, (
        mint,
        migration["signature"],
        migrated_at,
        time.time(),
        time.time(),
        holder_count,

        first_price,
        current_price,
        peak_price,
        peak_at,
        trough_price,
        trough_at,

        current_return,
        current_drawdown,
        recovery,

        latest["liquidity_usd"],
        latest["market_cap"],
        latest["fdv"],
        latest["volume_m5"],
        latest["buys_m5"],
        latest["sells_m5"],

        latest["pair_address"],
        latest["dex_id"],
        dex_timestamp,
        dex_age,

        ever_run,
        ever_crashed,
        ever_recovered,
        second_new_high,
        state,
    ))

    db.commit()


def show():
    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}
    ORDER BY
        CASE state
            WHEN 'SECOND_RUN' THEN 1
            WHEN 'RECOVERY' THEN 2
            WHEN 'CRASH_WATCH' THEN 3
            WHEN 'RUNNING' THEN 4
            WHEN 'MIGRATED' THEN 5
            WHEN 'WAIT_PRICE' THEN 6
            WHEN 'STALE' THEN 7
            ELSE 8
        END,
        holder_count DESC
    """).fetchall()

    print("\033[2J\033[H", end="")

    print("=" * 175)
    print("MEMECOIN LAB — T102 MIGRATED TOKEN RUN / CRASH / RESURRECTION WATCHLIST")
    print("=" * 175)

    print(f"TOKENS              : {len(rows)}")
    print(
        f">=50 HOLDERS        : "
        f"{sum((r['holder_count'] or 0) >= 50 for r in rows)}"
    )
    print(
        f"EVER RUN            : "
        f"{sum(r['ever_run'] for r in rows)}"
    )
    print(
        f"EVER CRASHED        : "
        f"{sum(r['ever_crashed'] for r in rows)}"
    )
    print(
        f"EVER RECOVERED      : "
        f"{sum(r['ever_recovered'] for r in rows)}"
    )
    print(
        f"SECOND RUN          : "
        f"{sum(r['second_new_high'] for r in rows)}"
    )

    print()
    print(
        "RESEARCH LABELS     : "
        "RUN +100% | CRASH -50% FROM PEAK | RECOVERY +50% FROM TROUGH"
    )
    print(
        ">=50 FILTER         : NOT ACTIVE"
    )

    print()
    print("=" * 175)
    print("WATCHLIST")
    print("=" * 175)

    for r in rows[:30]:
        print(
            f"{r['token_mint'][:18]:18} "
            f"| H={str(r['holder_count']):>5} "
            f"| STATE={r['state']:12} "
            f"| FROM0={fmt(r['return_from_first']):>8}% "
            f"| DD={fmt(r['drawdown_from_peak']):>8}% "
            f"| REC={fmt(r['recovery_from_trough']):>8}% "
            f"| MC={fmt(r['market_cap'],0):>10} "
            f"| LIQ={fmt(r['liquidity_usd'],0):>9} "
            f"| VOL5={fmt(r['volume_m5'],0):>9}"
        )

    print()
    print(f"Refresh every {REFRESH}s.")
    print("CTRL+C stops T102 only.")


try:
    while True:
        for migration in migrations():
            update_token(migration)

        show()
        time.sleep(REFRESH)

except KeyboardInterrupt:
    print()
    print("T102 stopped safely.")

finally:
    db.close()
