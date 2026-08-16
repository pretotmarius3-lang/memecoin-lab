#!/usr/bin/env python3

import asyncio
import sqlite3
import time
import random
import aiohttp

# On réutilise ton moteur DEX Screener existant.
import price_tracker_v100 as pt

DB = "validation_v090.db"
MIGRATIONS = "t101_migrations"

# Surveillance continue.
# On ralentit naturellement avec l'âge du token.
POLL_YOUNG = 5.0       # < 15 min
POLL_MEDIUM = 15.0     # 15-60 min
POLL_OLD = 30.0        # > 60 min

MAX_CONCURRENT = 20


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


def migrated_tokens():
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

    ORDER BY
        COALESCE(block_time, detected_at)
    """).fetchall()


def save_migrated_snapshot(
    mint,
    snap
):
    """
    event_id=NULL volontairement.

    Ces lignes appartiennent à la branche migrated/watchlist,
    pas aux anciens events.
    """

    if (
        snap is None
        or snap.get("price_usd") is None
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
            NULL, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?,
            ?, ?,
            ?, ?
        )
    """, (
        mint,
        time.time(),

        snap.get("price_usd"),
        snap.get("price_native"),

        snap.get("liquidity_usd"),
        snap.get("market_cap"),
        snap.get("fdv"),

        snap.get("volume_m5"),

        snap.get("buys_m5"),
        snap.get("sells_m5"),

        snap.get("pair_address"),
        snap.get("dex_id"),
    ))

    db.commit()

    return True


def polling_interval(
    migrated_at
):
    age = (
        time.time()
        - migrated_at
    )

    if age < 15 * 60:
        return POLL_YOUNG

    if age < 60 * 60:
        return POLL_MEDIUM

    return POLL_OLD


async def track_token(
    session,
    mint,
    migrated_at,
    semaphore
):

    print(
        f"🎯 MIGRATED TRACKING START "
        f"| {mint[:16]}..."
    )

    first_price = None
    snapshots = 0

    while True:

        try:

            async with semaphore:

                snap = await pt.dex_snapshot(
                    session,
                    mint
                )

            if save_migrated_snapshot(
                mint,
                snap
            ):

                snapshots += 1

                price = snap[
                    "price_usd"
                ]

                if first_price is None:

                    first_price = price

                    print(
                        f"✅ FIRST PRICE "
                        f"| {mint[:14]}... "
                        f"| ${price:.12g} "
                        f"| LIQ=${(snap.get('liquidity_usd') or 0):,.0f} "
                        f"| MC=${(snap.get('market_cap') or 0):,.0f}"
                    )

                elif snapshots % 20 == 0:

                    move = (
                        100.0
                        * (
                            price
                            / first_price
                            - 1.0
                        )
                        if first_price > 0
                        else 0
                    )

                    print(
                        f"📡 {mint[:14]}... "
                        f"| SNAP={snapshots} "
                        f"| PRICE=${price:.12g} "
                        f"| FROM_FIRST={move:+.1f}%"
                    )

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            print(
                f"⚠️ TRACK ERROR "
                f"| {mint[:14]}... "
                f"| {exc}"
            )

        delay = polling_interval(
            migrated_at
        )

        await asyncio.sleep(
            delay
            + random.uniform(
                0.1,
                0.8
            )
        )


async def monitor(
    tracked
):

    while True:

        await asyncio.sleep(15)

        migrations = len(
            migrated_tokens()
        )

        rows = db.execute("""
        SELECT COUNT(*)
        FROM dex_prices
        WHERE event_id IS NULL
        """).fetchone()[0]

        tokens_with_prices = db.execute("""
        SELECT COUNT(
            DISTINCT token_mint
        )
        FROM dex_prices
        WHERE event_id IS NULL
        """).fetchone()[0]

        print()
        print("─" * 100)

        print(
            f"MIGRATED={migrations}"
            f" | ACTIVE TRACKERS={len(tracked)}"
            f" | TOKENS WITH PRICE={tokens_with_prices}"
            f" | MIGRATED SNAPSHOTS={rows}"
        )


async def main():

    print()
    print("=" * 100)
    print(
        "MEMECOIN LAB — T102A MIGRATED TOKEN CONTINUOUS PRICE TRACKER"
    )
    print("=" * 100)

    print(
        "SOURCE       : existing price_tracker_v100.dex_snapshot"
    )

    print(
        "WRITE        : dex_prices with event_id=NULL"
    )

    print(
        "TRACKING     : continuous"
    )

    print(
        "POLL         : 5s young / 15s medium / 30s old"
    )

    print(
        "OLD EVENTS   : untouched"
    )

    print("=" * 100)

    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT
    )

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT
    )

    tracked = {}

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        asyncio.create_task(
            monitor(
                tracked
            )
        )

        while True:

            rows = migrated_tokens()

            for row in rows:

                mint = row[
                    "token_mint"
                ]

                if mint in tracked:
                    continue

                task = asyncio.create_task(
                    track_token(
                        session,
                        mint,
                        row["migrated_at"],
                        semaphore
                    )
                )

                tracked[mint] = task

                print(
                    f"🔥 NEW MIGRATED TOKEN "
                    f"| {mint[:18]}..."
                )

            # Remove trackers only if they unexpectedly terminate.
            dead = [
                mint
                for mint, task in tracked.items()
                if task.done()
            ]

            for mint in dead:

                task = tracked.pop(
                    mint
                )

                try:
                    task.result()

                except Exception as exc:

                    print(
                        f"⚠️ TRACKER DIED "
                        f"| {mint[:14]}... "
                        f"| {exc}"
                    )

            await asyncio.sleep(2)


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "T102A stopped safely."
        )

    finally:

        db.close()
