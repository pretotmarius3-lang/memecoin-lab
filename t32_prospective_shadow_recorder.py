import sqlite3
import time
import math
import os

DB = "validation_v090.db"

T23_TABLE = "v2_frozen_firstsignal_t23"
T31_TABLE = "v2_frozen_execution_t31"

CHECKPOINTS = [0, 10, 20, 30, 45, 60, 75, 90, 120]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def table_exists(db, name):
    return db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
    """, (name,)).fetchone() is not None


def ensure_tables(db):

    db.execute("""
        CREATE TABLE IF NOT EXISTS prospective_shadow_t32 (
            token_mint TEXT,
            event_id INTEGER,
            checkpoint_sec INTEGER,

            target_timestamp REAL,
            snapshot_timestamp REAL,
            snapshot_delay REAL,
            target_error REAL,

            entry_price REAL,
            price_usd REAL,

            current_return REAL,
            max_return REAL,
            min_return REAL,
            drawdown_from_peak REAL,
            recovery_from_low REAL,

            snapshot_count INTEGER,

            liquidity_usd REAL,
            market_cap REAL,
            volume_m5 REAL,

            regime INTEGER,

            t31_status TEXT,
            t31_exit_reason TEXT,
            t31_net_return REAL,

            captured INTEGER DEFAULT 0,
            updated_at REAL,

            PRIMARY KEY (
                token_mint,
                checkpoint_sec
            )
        )
    """)

    db.commit()


def ret(entry, price):
    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    return (price / entry - 1) * 100


def first_dex(db, event_id):

    return db.execute("""
        SELECT
            timestamp,
            price_usd,
            liquidity_usd,
            market_cap,
            volume_m5

        FROM dex_prices

        WHERE
            event_id=?
            AND price_usd IS NOT NULL
            AND price_usd > 0

        ORDER BY timestamp ASC
        LIMIT 1
    """, (event_id,)).fetchone()


def nearest_snapshot(db, event_id, target_ts):

    before = db.execute("""
        SELECT *
        FROM dex_prices
        WHERE
            event_id=?
            AND price_usd IS NOT NULL
            AND price_usd > 0
            AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (
        event_id,
        target_ts
    )).fetchone()

    after = db.execute("""
        SELECT *
        FROM dex_prices
        WHERE
            event_id=?
            AND price_usd IS NOT NULL
            AND price_usd > 0
            AND timestamp >= ?
        ORDER BY timestamp ASC
        LIMIT 1
    """, (
        event_id,
        target_ts
    )).fetchone()

    candidates = []

    if before:
        candidates.append(before)

    if after:
        candidates.append(after)

    if not candidates:
        return None

    return min(
        candidates,
        key=lambda x:
            abs(x["timestamp"] - target_ts)
    )


def path_until(db, event_id, start_ts, end_ts):

    return db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND timestamp >= ?
            AND timestamp <= ?
            AND price_usd IS NOT NULL
            AND price_usd > 0

        ORDER BY timestamp
    """, (
        event_id,
        start_ts,
        end_ts
    )).fetchall()


def get_regime(db, event_id):

    if not table_exists(
        db,
        "frozen_regime_v620"
    ):
        return None

    r = db.execute("""
        SELECT regime
        FROM frozen_regime_v620
        WHERE event_id=?
    """, (
        event_id,
    )).fetchone()

    return (
        r["regime"]
        if r
        else None
    )


def get_t31(db, token):

    if not table_exists(
        db,
        T31_TABLE
    ):
        return None

    return db.execute(f"""
        SELECT
            status,
            exit_reason,
            net_return

        FROM {T31_TABLE}

        WHERE token_mint=?
    """, (
        token,
    )).fetchone()


db = connect()

if not table_exists(
    db,
    T23_TABLE
):
    raise RuntimeError(
        f"Missing table {T23_TABLE}"
    )

ensure_tables(db)


while True:

    try:

        # ====================================================
        # LOAD PROSPECTIVE TOKENS
        # ====================================================

        tokens = db.execute(f"""
            SELECT
                token_mint,
                event_id

            FROM {T23_TABLE}

            ORDER BY event_id
        """).fetchall()

        # ====================================================
        # ENSURE PLACEHOLDER ROWS
        # ====================================================

        for t in tokens:

            entry = first_dex(
                db,
                t["event_id"]
            )

            if not entry:
                continue

            for cp in CHECKPOINTS:

                db.execute("""
                    INSERT OR IGNORE INTO
                    prospective_shadow_t32 (
                        token_mint,
                        event_id,
                        checkpoint_sec,
                        target_timestamp,
                        entry_price,
                        updated_at
                    )
                    VALUES (?,?,?,?,?,?)
                """, (
                    t["token_mint"],
                    t["event_id"],
                    cp,
                    entry["timestamp"] + cp,
                    entry["price_usd"],
                    time.time(),
                ))

        db.commit()

        # ====================================================
        # FILL AVAILABLE CHECKPOINTS
        # ====================================================

        pending = db.execute("""
            SELECT *
            FROM prospective_shadow_t32
            WHERE captured=0
            ORDER BY event_id, checkpoint_sec
        """).fetchall()

        now = time.time()

        for row in pending:

            target_ts = row["target_timestamp"]

            # not due yet
            if now < target_ts:
                continue

            snap = nearest_snapshot(
                db,
                row["event_id"],
                target_ts
            )

            if not snap:
                continue

            entry = row["entry_price"]

            path = path_until(
                db,
                row["event_id"],
                target_ts - row["checkpoint_sec"],
                snap["timestamp"]
            )

            vals = []

            for p in path:
                rr = ret(
                    entry,
                    p["price_usd"]
                )

                if valid(rr):
                    vals.append(rr)

            current = ret(
                entry,
                snap["price_usd"]
            )

            maximum = (
                max(vals)
                if vals
                else current
            )

            minimum = (
                min(vals)
                if vals
                else current
            )

            drawdown = (
                current - maximum
                if (
                    valid(current)
                    and valid(maximum)
                )
                else None
            )

            recovery = (
                current - minimum
                if (
                    valid(current)
                    and valid(minimum)
                )
                else None
            )

            regime = get_regime(
                db,
                row["event_id"]
            )

            t31 = get_t31(
                db,
                row["token_mint"]
            )

            db.execute("""
                UPDATE prospective_shadow_t32

                SET
                    snapshot_timestamp=?,
                    snapshot_delay=?,
                    target_error=?,

                    price_usd=?,

                    current_return=?,
                    max_return=?,
                    min_return=?,
                    drawdown_from_peak=?,
                    recovery_from_low=?,

                    snapshot_count=?,

                    liquidity_usd=?,
                    market_cap=?,
                    volume_m5=?,

                    regime=?,

                    t31_status=?,
                    t31_exit_reason=?,
                    t31_net_return=?,

                    captured=1,
                    updated_at=?

                WHERE
                    token_mint=?
                    AND checkpoint_sec=?
            """, (
                snap["timestamp"],
                snap["timestamp"] - (
                    target_ts
                    - row["checkpoint_sec"]
                ),
                snap["timestamp"] - target_ts,

                snap["price_usd"],

                current,
                maximum,
                minimum,
                drawdown,
                recovery,

                len(path),

                snap["liquidity_usd"],
                snap["market_cap"],
                snap["volume_m5"],

                regime,

                (
                    t31["status"]
                    if t31
                    else None
                ),

                (
                    t31["exit_reason"]
                    if t31
                    else None
                ),

                (
                    t31["net_return"]
                    if t31
                    else None
                ),

                time.time(),

                row["token_mint"],
                row["checkpoint_sec"],
            ))

        db.commit()

        # ====================================================
        # REFRESH T31 FINAL FIELDS EVEN AFTER CHECKPOINT CAPTURE
        # ====================================================

        for t in tokens:

            t31 = get_t31(
                db,
                t["token_mint"]
            )

            if not t31:
                continue

            db.execute("""
                UPDATE prospective_shadow_t32

                SET
                    t31_status=?,
                    t31_exit_reason=?,
                    t31_net_return=?,
                    updated_at=?

                WHERE token_mint=?
            """, (
                t31["status"],
                t31["exit_reason"],
                t31["net_return"],
                time.time(),
                t["token_mint"],
            ))

        db.commit()

        # ====================================================
        # DISPLAY
        # ====================================================

        os.system("clear")

        total_tokens = len(tokens)

        completed_cp = db.execute("""
            SELECT
                checkpoint_sec,
                COUNT(*) AS n

            FROM prospective_shadow_t32

            WHERE captured=1

            GROUP BY checkpoint_sec

            ORDER BY checkpoint_sec
        """).fetchall()

        print("="*135)
        print(
            "MEMECOIN LAB — "
            "T32 PROSPECTIVE SHADOW RECORDER"
        )
        print("="*135)

        print(
            "READ-ONLY RESEARCH RECORDER"
        )

        print(
            "SOURCE = T23 prospective tokens only"
        )

        print()

        print(
            f"T23 TOKENS TRACKED : "
            f"{total_tokens}"
        )

        print()
        print("="*135)
        print(
            "CHECKPOINT COVERAGE"
        )
        print("="*135)

        counts = {
            r["checkpoint_sec"]:
            r["n"]
            for r in completed_cp
        }

        for cp in CHECKPOINTS:

            n = counts.get(cp, 0)

            print(
                f"{cp:>3}s | "
                f"{n:>3}/{total_tokens:<3} captured"
            )

        # ====================================================
        # LATEST TOKEN MATRIX
        # ====================================================

        print()
        print("="*135)
        print(
            "LATEST TOKENS"
        )
        print("="*135)

        latest_tokens = tokens[-10:]

        for t in reversed(latest_tokens):

            print()
            print(
                f"ID={t['event_id']} "
                f"| {t['token_mint'][:20]}"
            )

            checkpoints = db.execute("""
                SELECT *
                FROM prospective_shadow_t32

                WHERE token_mint=?

                ORDER BY checkpoint_sec
            """, (
                t["token_mint"],
            )).fetchall()

            for r in checkpoints:

                if not r["captured"]:

                    print(
                        f"  {r['checkpoint_sec']:>3}s "
                        f"| WAIT"
                    )

                    continue

                cr = (
                    f"{r['current_return']:+7.2f}%"
                    if valid(
                        r["current_return"]
                    )
                    else "     NA"
                )

                mx = (
                    f"{r['max_return']:+7.2f}%"
                    if valid(
                        r["max_return"]
                    )
                    else "     NA"
                )

                mn = (
                    f"{r['min_return']:+7.2f}%"
                    if valid(
                        r["min_return"]
                    )
                    else "     NA"
                )

                print(
                    f"  {r['checkpoint_sec']:>3}s "
                    f"| RET={cr} "
                    f"| MAX={mx} "
                    f"| MIN={mn} "
                    f"| PTS={r['snapshot_count']:>3} "
                    f"| REG="
                    f"{('R'+str(r['regime'])) if r['regime'] is not None else 'NA'}"
                )

        # ====================================================
        # T31 FINAL COVERAGE
        # ====================================================

        final_rows = db.execute("""
            SELECT DISTINCT
                token_mint,
                t31_status,
                t31_exit_reason,
                t31_net_return

            FROM prospective_shadow_t32
        """).fetchall()

        t31_done = [
            r for r in final_rows
            if valid(
                r["t31_net_return"]
            )
        ]

        print()
        print("="*135)
        print(
            "T31 EXECUTION LINK"
        )
        print("="*135)

        print(
            f"T31 COMPLETED LINKED : "
            f"{len(t31_done)}/{total_tokens}"
        )

        print()
        print(
            "NO SIGNAL RULES ARE MODIFIED."
        )

        print(
            "NO FEATURE SELECTION IS PERFORMED."
        )

        print(
            "NO MODEL IS TRAINED."
        )

        print(
            "DO NOT USE THIS TABLE TO RETUNE "
            "BEFORE THE FROZEN CHECKPOINTS."
        )

        print()
        print(
            "Refresh every 10 seconds."
        )

        time.sleep(10)

    except KeyboardInterrupt:
        print(
            "\nT32 stopped."
        )
        break

    except Exception as e:
        print(
            "ERROR:",
            repr(e)
        )
        time.sleep(5)

db.close()
