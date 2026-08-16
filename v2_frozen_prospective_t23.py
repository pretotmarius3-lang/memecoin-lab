import sqlite3
import time
import os
import math
import statistics

DB = "validation_v090.db"

BOUNDARY_ID = 545
VOLUME_CUT = 8837.925

RUNNER = 10.0
DUMP = -10.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def ensure_table(db):

    db.execute("""
        CREATE TABLE IF NOT EXISTS v2_frozen_firstsignal_t23 (
            token_mint TEXT PRIMARY KEY,
            event_id INTEGER UNIQUE,
            event_timestamp REAL,

            captured_at REAL,
            boundary_id INTEGER,

            fa REAL,
            new_wallets30 INTEGER,
            volume_m5 REAL,

            dex_return_30s REAL,
            dex_return_60s REAL,
            dex_return_300s REAL,

            status TEXT DEFAULT 'WAIT',
            binary_label INTEGER,
            labeled_60 INTEGER DEFAULT 0
        )
    """)

    db.commit()


def get_candidates(db):

    return db.execute("""
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
              ON d.event_id=x.event_id
             AND d.timestamp=x.first_time
        )

        SELECT
            e.id,
            e.timestamp,
            e.token_mint,

            e.fa,
            e.new_wallets30,

            e.dex_return_30s,
            e.dex_return_60s,
            e.dex_return_300s,

            d.volume_m5

        FROM events e

        JOIN first_dex d
        ON d.event_id=e.id

        WHERE
            e.id > ?
            AND e.fa95=1
            AND e.new_wallets30 >= 2
            AND d.volume_m5 >= ?

        ORDER BY e.id ASC
    """, (
        BOUNDARY_ID,
        VOLUME_CUT
    )).fetchall()


def classify(ret):

    if not valid(ret):
        return "WAIT", None

    if ret >= RUNNER:
        return "RUN", 1

    if ret <= DUMP:
        return "DUMP", 0

    return "NEUTRAL", None


def refresh_outcomes(db):

    rows = db.execute("""
        SELECT event_id
        FROM v2_frozen_firstsignal_t23
    """).fetchall()

    for row in rows:

        event = db.execute("""
            SELECT
                dex_return_30s,
                dex_return_60s,
                dex_return_300s
            FROM events
            WHERE id=?
        """, (
            row["event_id"],
        )).fetchone()

        if not event:
            continue

        r30 = event["dex_return_30s"]
        r60 = event["dex_return_60s"]
        r300 = event["dex_return_300s"]

        status, binary = classify(r60)

        labeled_60 = 1 if valid(r60) else 0

        db.execute("""
            UPDATE v2_frozen_firstsignal_t23

            SET
                dex_return_30s=?,
                dex_return_60s=?,
                dex_return_300s=?,
                status=?,
                binary_label=?,
                labeled_60=?

            WHERE event_id=?
        """, (
            r30,
            r60,
            r300,
            status,
            binary,
            labeled_60,
            row["event_id"]
        ))

    db.commit()


def stats(rows, field):

    vals = [
        r[field]
        for r in rows
        if valid(r[field])
    ]

    if not vals:
        return None

    return {
        "n": len(vals),
        "avg": avg(vals),
        "med": med(vals),
        "win": 100 * sum(x > 0 for x in vals) / len(vals),
        "run": 100 * sum(x >= RUNNER for x in vals) / len(vals),
        "dump": 100 * sum(x <= DUMP for x in vals) / len(vals),
        "edge": 100 * (
            sum(x >= RUNNER for x in vals)
            -
            sum(x <= DUMP for x in vals)
        ) / len(vals),
        "worst": min(vals),
        "best": max(vals),
    }


db = connect()
ensure_table(db)

while True:

    try:

        # -----------------------------------------------------
        # Capture first FUTURE V2 signal/token
        # -----------------------------------------------------

        candidates = get_candidates(db)

        existing_tokens = {
            r["token_mint"]
            for r in db.execute("""
                SELECT token_mint
                FROM v2_frozen_firstsignal_t23
            """).fetchall()
        }

        for r in candidates:

            token = r["token_mint"]

            if token in existing_tokens:
                continue

            db.execute("""
                INSERT OR IGNORE INTO
                v2_frozen_firstsignal_t23 (
                    token_mint,
                    event_id,
                    event_timestamp,
                    captured_at,
                    boundary_id,
                    fa,
                    new_wallets30,
                    volume_m5,
                    dex_return_30s,
                    dex_return_60s,
                    dex_return_300s
                )

                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                token,
                r["id"],
                r["timestamp"],
                time.time(),
                BOUNDARY_ID,
                r["fa"],
                r["new_wallets30"],
                r["volume_m5"],
                r["dex_return_30s"],
                r["dex_return_60s"],
                r["dex_return_300s"]
            ))

            existing_tokens.add(token)

        db.commit()

        refresh_outcomes(db)

        rows = db.execute("""
            SELECT *
            FROM v2_frozen_firstsignal_t23
            ORDER BY event_id
        """).fetchall()

        os.system("clear")

        print("=" * 125)
        print("MEMECOIN LAB — T23 V2 FROZEN PROSPECTIVE FIRST-SIGNAL VALIDATOR")
        print("=" * 125)

        print(
            f"BOUNDARY: ID > {BOUNDARY_ID} ONLY"
        )

        print(
            f"FROZEN FILTER: "
            f"FA95 + NEW30>=2 + VOLUME_M5>={VOLUME_CUT}"
        )

        print(
            f"PROSPECTIVE TOKENS CAPTURED: {len(rows)}"
        )

        # -----------------------------------------------------
        # Outcomes
        # -----------------------------------------------------

        print()
        print("=" * 125)
        print("PROSPECTIVE OUTCOMES")
        print("=" * 125)

        for field, label in [
            ("dex_return_30s", "30s"),
            ("dex_return_60s", "60s"),
            ("dex_return_300s", "300s"),
        ]:

            s = stats(rows, field)

            if not s:
                print(
                    f"{label:>5} | N=0"
                )
                continue

            print(
                f"{label:>5} | "
                f"N={s['n']:>3} | "
                f"AVG={s['avg']:+7.2f}% | "
                f"MED={s['med']:+7.2f}% | "
                f"WIN={s['win']:5.1f}% | "
                f"RUN10={s['run']:5.1f}% | "
                f"DUMP10={s['dump']:5.1f}% | "
                f"EDGE={s['edge']:+6.1f}% | "
                f"WORST={s['worst']:+7.2f}% | "
                f"BEST={s['best']:+7.2f}%"
            )

        # -----------------------------------------------------
        # Binary 60s
        # -----------------------------------------------------

        binary = [
            r for r in rows
            if r["binary_label"] is not None
        ]

        runners = sum(
            r["binary_label"] == 1
            for r in binary
        )

        dumps = sum(
            r["binary_label"] == 0
            for r in binary
        )

        print()
        print("=" * 125)
        print("60s BINARY — RUNNER >= +10% / DUMP <= -10%")
        print("=" * 125)

        print(
            f"BINARY={len(binary)} | "
            f"RUN={runners} | "
            f"DUMP={dumps}"
        )

        if binary:

            print(
                f"RUN SHARE={100*runners/len(binary):.1f}% | "
                f"DUMP SHARE={100*dumps/len(binary):.1f}%"
            )

        # -----------------------------------------------------
        # Latest
        # -----------------------------------------------------

        print()
        print("=" * 125)
        print("LATEST PROSPECTIVE TOKENS")
        print("=" * 125)

        print(
            f"{'ID':>5} "
            f"{'FA':>8} "
            f"{'NEW30':>6} "
            f"{'VOLM5':>11} "
            f"{'R30':>9} "
            f"{'R60':>9} "
            f"{'STATUS':>9} "
            f"{'TOKEN':20}"
        )

        print("-" * 100)

        for r in reversed(rows[-20:]):

            r30 = (
                f"{r['dex_return_30s']:+8.2f}%"
                if valid(r["dex_return_30s"])
                else "      NA"
            )

            r60 = (
                f"{r['dex_return_60s']:+8.2f}%"
                if valid(r["dex_return_60s"])
                else "      NA"
            )

            print(
                f"{r['event_id']:>5} "
                f"{r['fa']:>8.3f} "
                f"{r['new_wallets30']:>6} "
                f"{r['volume_m5']:>11.2f} "
                f"{r30} "
                f"{r60} "
                f"{r['status']:>9} "
                f"{r['token_mint'][:20]}"
            )

        # -----------------------------------------------------
        # Checkpoints
        # -----------------------------------------------------

        print()
        print("=" * 125)
        print("FROZEN CHECKPOINTS")
        print("=" * 125)

        n = len(rows)

        if n >= 100:
            print("✅ 100 prospective tokens — STRONG CHECKPOINT")
        elif n >= 50:
            print("✅ 50 prospective tokens — MEANINGFUL CHECKPOINT")
        elif n >= 30:
            print("🟡 30 prospective tokens — FIRST SERIOUS CHECKPOINT")
        elif n >= 15:
            print("🟡 15 prospective tokens — EARLY READ")
        else:
            print(f"⏳ {n}/15 prospective tokens")

        print()
        print("DO NOT CHANGE THE FILTER.")
        print("DO NOT RESET THE TABLE.")
        print("Every token contributes at most ONE event.")
        print("Primary frozen horizon = 60 seconds.")
        print()
        print("Refresh every 10 seconds.")

        time.sleep(10)

    except KeyboardInterrupt:
        print("\nT23 stopped.")
        break

    except Exception as e:
        print("ERROR:", repr(e))
        time.sleep(5)

db.close()
