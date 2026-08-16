import sqlite3
import statistics
import time
import os

DB = "validation_v090.db"

NEW30_MIN = 2
VOLUME_M5_MIN = 8837.925

STATE_FILE = "v151_start_id.txt"


def connect():
    db = sqlite3.connect(
        DB,
        timeout=30
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")

    return db


# ============================================================
# FREEZE START EVENT ID
# ============================================================

if not os.path.exists(STATE_FILE):

    db = connect()

    row = db.execute("""
        SELECT COALESCE(MAX(id), 0) AS max_id
        FROM events
    """).fetchone()

    start_id = row["max_id"]

    db.close()

    with open(STATE_FILE, "w") as f:
        f.write(str(start_id))

else:

    with open(STATE_FILE) as f:
        start_id = int(
            f.read().strip()
        )


print(
    f"Validation future commencera "
    f"après EVENT ID {start_id}"
)

time.sleep(2)


def pct_positive(vals):

    if not vals:
        return 0

    return (
        100
        * sum(x > 0 for x in vals)
        / len(vals)
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

    for h in [
        10,
        20,
        30,
        60,
        300
    ]:

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

        db = connect()

        # ====================================================
        # ONLY EVENTS CREATED AFTER V1.5.1 START
        # ====================================================

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
                AND e.id > ?

            ORDER BY e.id ASC
        """, (
            start_id,
        )).fetchall()


        base = list(rows)


        new30 = [
            r for r in rows

            if (
                r["new_wallets30"]
                is not None

                and r["new_wallets30"]
                >= NEW30_MIN
            )
        ]


        volume = [
            r for r in rows

            if (
                r["volume_m5"]
                is not None

                and r["volume_m5"]
                >= VOLUME_M5_MIN
            )
        ]


        combo = [
            r for r in rows

            if (
                r["new_wallets30"]
                is not None

                and r["new_wallets30"]
                >= NEW30_MIN

                and r["volume_m5"]
                is not None

                and r["volume_m5"]
                >= VOLUME_M5_MIN
            )
        ]


        # ====================================================
        # LIVE DB CHECK
        # ====================================================

        total_events = db.execute("""
            SELECT COUNT(*)
            FROM events
        """).fetchone()[0]


        total_fa95 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE fa95=1
        """).fetchone()[0]


        events_since = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE id > ?
        """, (
            start_id,
        )).fetchone()[0]


        # ====================================================
        # DISPLAY
        # ====================================================

        os.system("clear")

        print("=" * 90)
        print(
            "MEMECOIN LAB — "
            "V1.5.1 FUTURE V2 VALIDATION"
        )
        print("=" * 90)

        print(
            f"START EVENT ID   : {start_id}"
        )

        print(
            f"CURRENT EVENT ID : {total_events}"
        )

        print(
            f"NEW EVENTS       : {events_since}"
        )

        print(
            f"TOTAL FA95       : {total_fa95}"
        )

        print()

        print(
            f"NEW30 THRESHOLD  : >= {NEW30_MIN}"
        )

        print(
            f"VOLUME_M5        : >= {VOLUME_M5_MIN:.3f}"
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

        if len(base) < 20:

            print(
                "COLLECT — "
                "attente de nouveaux FA95 OOS"
            )

        elif len(base) < 100:

            print(
                "COLLECT — "
                "validation V2 en cours"
            )

        else:

            print(
                "CHECKPOINT V2 100 ATTEINT"
            )

        print()
        print(
            "Ne change pas les seuils."
        )

        db.close()

        time.sleep(10)


    except KeyboardInterrupt:

        print(
            "\nV1.5.1 stopped."
        )

        break


    except Exception as e:

        print(
            "ERROR:",
            e
        )

        time.sleep(5)
