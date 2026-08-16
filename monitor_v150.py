import sqlite3
import statistics
import time
import os

DB = "validation_v090.db"

# ------------------------------------------------------------
# FROZEN V2 THRESHOLDS
# ------------------------------------------------------------

NEW30_MIN = 2
VOLUME_M5_MIN = 8837.925

# Only validate FUTURE events from now on.
# This timestamp is written once at first launch.
STATE_FILE = "v150_start.txt"

if not os.path.exists(STATE_FILE):
    with open(STATE_FILE, "w") as f:
        f.write(str(time.time()))

with open(STATE_FILE) as f:
    START_TIME = float(f.read().strip())


def pct_positive(vals):
    return (
        100 * sum(x > 0 for x in vals) / len(vals)
        if vals else 0
    )


def report(name, rows):

    print()
    print(name)
    print("-" * 90)

    tokens = len(set(
        r["token_mint"]
        for r in rows
    ))

    print(
        f"EVENTS={len(rows)}"
        f" | TOKENS={tokens}"
    )

    for h in [10, 20, 30, 60, 300]:

        col = f"dex_return_{h}s"

        vals = [
            r[col]
            for r in rows
            if r[col] is not None
        ]

        if not vals:
            print(
                f"{h:>3}s | N=0"
            )
            continue

        print(
            f"{h:>3}s | "
            f"N={len(vals):>3} | "
            f"AVG={statistics.mean(vals):+7.2f}% | "
            f"MED={statistics.median(vals):+7.2f}% | "
            f"WIN={pct_positive(vals):5.1f}% | "
            f"WORST={min(vals):+7.2f}% | "
            f"BEST={max(vals):+7.2f}%"
        )


while True:

    try:

        db = sqlite3.connect(
            DB,
            timeout=30
        )

        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")

        # --------------------------------------------------------
        # Load only FUTURE FA95 events
        # --------------------------------------------------------

        rows = db.execute("""
            WITH first_dex AS (
                SELECT d.*
                FROM dex_prices d

                JOIN (
                    SELECT
                        event_id,
                        MIN(timestamp) AS first_time

                    FROM dex_prices

                    GROUP BY event_id
                ) x

                ON d.event_id = x.event_id
                AND d.timestamp = x.first_time
            )

            SELECT
                e.*,

                d.volume_m5,
                d.liquidity_usd,
                d.market_cap,

                d.buys_m5,
                d.sells_m5

            FROM events e

            LEFT JOIN first_dex d
            ON d.event_id = e.id

            WHERE
                e.fa95 = 1
                AND e.timestamp >= ?

            ORDER BY e.timestamp ASC
        """, (
            START_TIME,
        )).fetchall()

        base = list(rows)

        new30 = [
            r for r in rows
            if (
                r["new_wallets30"] is not None
                and r["new_wallets30"] >= NEW30_MIN
            )
        ]

        volume = [
            r for r in rows
            if (
                r["volume_m5"] is not None
                and r["volume_m5"] >= VOLUME_M5_MIN
            )
        ]

        combo = [
            r for r in rows
            if (
                r["new_wallets30"] is not None
                and r["new_wallets30"] >= NEW30_MIN

                and r["volume_m5"] is not None
                and r["volume_m5"] >= VOLUME_M5_MIN
            )
        ]

        os.system("clear")

        print("=" * 90)
        print("MEMECOIN LAB — V1.5 FUTURE V2 VALIDATION")
        print("=" * 90)

        print(
            f"START TIME      : {START_TIME:.0f}"
        )

        print(
            f"NEW30 THRESHOLD : >= {NEW30_MIN}"
        )

        print(
            f"VOLUME_M5       : >= {VOLUME_M5_MIN:.3f}"
        )

        print()
        print(
            f"FA95 FUTURE EVENTS : {len(base)}"
        )

        print("=" * 90)

        report(
            "FA95_BASE",
            base
        )

        report(
            "FA95_NEW30",
            new30
        )

        report(
            "FA95_VOLUME",
            volume
        )

        report(
            "FA95_NEW30_VOLUME",
            combo
        )

        print()
        print("=" * 90)

        if len(base) < 30:
            status = (
                "COLLECT — "
                "V2 encore trop petit"
            )

        elif len(base) < 100:
            status = (
                "COLLECT — "
                "validation V2 en cours"
            )

        else:
            status = (
                "CHECKPOINT V2 100 ATTEINT"
            )

        print(status)

        print()
        print(
            "Ne change pas NEW30 ni VOLUME_M5 pendant cette validation."
        )

        db.close()

        time.sleep(10)

    except KeyboardInterrupt:

        print(
            "\nV1.5 stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            e
        )

        time.sleep(5)
