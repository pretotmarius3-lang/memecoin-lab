import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"

SOURCE_TABLE = "v2_frozen_firstsignal_t23"

ACTIVATION = 5.0
TRAIL = 3.0
HORIZON = 120
TOTAL_EXECUTION_COST = 3.0


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


def percentile(vals, p):
    vals = sorted(x for x in vals if valid(x))

    if not vals:
        return None

    k = (len(vals)-1)*p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return vals[lo]*(hi-k) + vals[hi]*(k-lo)


def ret(entry, price):
    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    return (price/entry - 1)*100


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


def ensure_table(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS v2_frozen_execution_t31 (
            token_mint TEXT PRIMARY KEY,
            event_id INTEGER UNIQUE,

            entry_timestamp REAL,
            entry_price REAL,

            status TEXT DEFAULT 'WAIT',

            trail_activated INTEGER DEFAULT 0,
            trail_activation_time REAL,

            exit_reason TEXT,
            exit_timestamp REAL,
            exit_delay REAL,
            exit_price REAL,

            gross_return REAL,
            net_return REAL,

            max_return REAL,
            min_return REAL,

            path_points INTEGER DEFAULT 0,
            last_path_delay REAL,

            completed INTEGER DEFAULT 0,

            created_at REAL,
            updated_at REAL
        )
    """)
    db.commit()


def get_t23_tokens(db):
    return db.execute(f"""
        SELECT
            token_mint,
            event_id

        FROM {SOURCE_TABLE}

        ORDER BY event_id
    """).fetchall()


def first_dex_snapshot(db, event_id):
    return db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND price_usd IS NOT NULL
            AND price_usd > 0

        ORDER BY timestamp ASC
        LIMIT 1
    """, (event_id,)).fetchone()


def load_path(db, event_id, entry_ts):
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

        ORDER BY timestamp ASC
    """, (
        event_id,
        entry_ts,
        entry_ts + HORIZON
    )).fetchall()


def simulate_current(db, row):
    event_id = row["event_id"]
    entry_ts = row["entry_timestamp"]
    entry_price = row["entry_price"]

    path = load_path(
        db,
        event_id,
        entry_ts
    )

    if not path:
        return {
            "status": "WAIT",
            "completed": 0,
            "points": 0,
            "last_delay": None,
        }

    peak = entry_price

    trail_active = False
    trail_activation_time = None

    max_ret = 0.0
    min_ret = 0.0

    exit_reason = None
    exit_timestamp = None
    exit_delay = None
    exit_price = None

    for p in path:
        price = p["price_usd"]
        ts = p["timestamp"]

        if not valid(price) or price <= 0:
            continue

        current = ret(
            entry_price,
            price
        )

        if not valid(current):
            continue

        max_ret = max(
            max_ret,
            current
        )

        min_ret = min(
            min_ret,
            current
        )

        if price > peak:
            peak = price

        peak_ret = ret(
            entry_price,
            peak
        )

        if (
            not trail_active
            and valid(peak_ret)
            and peak_ret >= ACTIVATION
        ):
            trail_active = True
            trail_activation_time = (
                ts - entry_ts
            )

        if trail_active:
            trail_price = (
                peak
                * (1 - TRAIL/100)
            )

            if price <= trail_price:
                exit_reason = "TRAIL"
                exit_timestamp = ts
                exit_delay = ts - entry_ts
                exit_price = price
                break

    last_delay = (
        path[-1]["timestamp"]
        - entry_ts
    )

    # If no trail exit yet, complete only when path has
    # actually reached the 120s horizon closely enough.
    if exit_price is None:

        if last_delay >= HORIZON - 5:
            exit_reason = "TIME"
            exit_timestamp = path[-1]["timestamp"]
            exit_delay = last_delay
            exit_price = path[-1]["price_usd"]

        else:
            return {
                "status": "LIVE",
                "completed": 0,
                "trail_active": trail_active,
                "trail_activation_time": trail_activation_time,
                "max_ret": max_ret,
                "min_ret": min_ret,
                "points": len(path),
                "last_delay": last_delay,
            }

    gross = ret(
        entry_price,
        exit_price
    )

    net = (
        gross - TOTAL_EXECUTION_COST
        if valid(gross)
        else None
    )

    return {
        "status": "DONE",
        "completed": 1,

        "trail_active": trail_active,
        "trail_activation_time":
            trail_activation_time,

        "exit_reason":
            exit_reason,

        "exit_timestamp":
            exit_timestamp,

        "exit_delay":
            exit_delay,

        "exit_price":
            exit_price,

        "gross":
            gross,

        "net":
            net,

        "max_ret":
            max_ret,

        "min_ret":
            min_ret,

        "points":
            len(path),

        "last_delay":
            last_delay,
    }


def stats(rows):
    vals = [
        r["net_return"]
        for r in rows
        if (
            r["completed"] == 1
            and valid(r["net_return"])
        )
    ]

    if not vals:
        return None

    return {
        "n": len(vals),
        "avg": avg(vals),
        "med": med(vals),
        "win":
            100*sum(x > 0 for x in vals)/len(vals),

        "p10":
            percentile(vals,.10),

        "p25":
            percentile(vals,.25),

        "worst":
            min(vals),

        "best":
            max(vals),
    }


db = connect()

if not table_exists(
    db,
    SOURCE_TABLE
):
    raise RuntimeError(
        f"Missing source table: {SOURCE_TABLE}"
    )

ensure_table(db)


while True:

    try:

        # ====================================================
        # CAPTURE NEW T23 TOKENS
        # ====================================================

        tokens = get_t23_tokens(db)

        existing = {
            r["token_mint"]
            for r in db.execute("""
                SELECT token_mint
                FROM v2_frozen_execution_t31
            """).fetchall()
        }

        for t in tokens:

            token = t["token_mint"]

            if token in existing:
                continue

            snap = first_dex_snapshot(
                db,
                t["event_id"]
            )

            if not snap:
                continue

            now = time.time()

            db.execute("""
                INSERT OR IGNORE INTO
                v2_frozen_execution_t31 (
                    token_mint,
                    event_id,

                    entry_timestamp,
                    entry_price,

                    status,
                    completed,

                    created_at,
                    updated_at
                )
                VALUES (
                    ?,?,?,?,?,?,?,?
                )
            """, (
                token,
                t["event_id"],

                snap["timestamp"],
                snap["price_usd"],

                "WAIT",
                0,

                now,
                now,
            ))

            existing.add(token)

        db.commit()

        # ====================================================
        # REFRESH EACH TOKEN
        # ====================================================

        current_rows = db.execute("""
            SELECT *
            FROM v2_frozen_execution_t31
            ORDER BY event_id
        """).fetchall()

        for row in current_rows:

            # completed is frozen forever
            if row["completed"] == 1:
                continue

            sim = simulate_current(
                db,
                row
            )

            db.execute("""
                UPDATE v2_frozen_execution_t31

                SET
                    status=?,

                    trail_activated=?,
                    trail_activation_time=?,

                    exit_reason=?,
                    exit_timestamp=?,
                    exit_delay=?,
                    exit_price=?,

                    gross_return=?,
                    net_return=?,

                    max_return=?,
                    min_return=?,

                    path_points=?,
                    last_path_delay=?,

                    completed=?,
                    updated_at=?

                WHERE token_mint=?
            """, (
                sim["status"],

                1 if sim.get(
                    "trail_active",
                    False
                ) else 0,

                sim.get(
                    "trail_activation_time"
                ),

                sim.get(
                    "exit_reason"
                ),

                sim.get(
                    "exit_timestamp"
                ),

                sim.get(
                    "exit_delay"
                ),

                sim.get(
                    "exit_price"
                ),

                sim.get(
                    "gross"
                ),

                sim.get(
                    "net"
                ),

                sim.get(
                    "max_ret"
                ),

                sim.get(
                    "min_ret"
                ),

                sim.get(
                    "points",
                    0
                ),

                sim.get(
                    "last_delay"
                ),

                sim["completed"],

                time.time(),

                row["token_mint"],
            ))

        db.commit()

        rows = db.execute("""
            SELECT *
            FROM v2_frozen_execution_t31
            ORDER BY event_id
        """).fetchall()

        completed = [
            r for r in rows
            if r["completed"] == 1
        ]

        live = [
            r for r in rows
            if r["completed"] == 0
        ]

        st = stats(rows)

        os.system("clear")

        print("="*135)
        print(
            "MEMECOIN LAB — "
            "T31 FROZEN BASE PROSPECTIVE EXECUTION"
        )
        print("="*135)

        print(
            "SOURCE: T23 prospective first-signal tokens only"
        )

        print()

        print(
            "FROZEN EXECUTION:"
        )

        print(
            f"ACT +{ACTIVATION:.0f}% "
            f"| TRAIL {TRAIL:.0f}% "
            f"| TIME {HORIZON}s "
            f"| COST {TOTAL_EXECUTION_COST:.0f}%"
        )

        print()

        print(
            f"T23 TOKENS SEEN : "
            f"{len(rows)}"
        )

        print(
            f"COMPLETED       : "
            f"{len(completed)}"
        )

        print(
            f"LIVE / WAIT     : "
            f"{len(live)}"
        )

        # ====================================================
        # OUTCOMES
        # ====================================================

        print()
        print("="*135)
        print(
            "PROSPECTIVE EXECUTION OUTCOMES"
        )
        print("="*135)

        if not st:
            print(
                "No completed trades yet."
            )

        else:
            print(
                f"N={st['n']} | "
                f"AVG={st['avg']:+.2f}% | "
                f"MED={st['med']:+.2f}% | "
                f"WIN={st['win']:.1f}% | "
                f"P10={st['p10']:+.2f}% | "
                f"P25={st['p25']:+.2f}% | "
                f"WORST={st['worst']:+.2f}% | "
                f"BEST={st['best']:+.2f}%"
            )

        # ====================================================
        # EXIT REASONS
        # ====================================================

        print()
        print("="*135)
        print(
            "EXIT REASONS"
        )
        print("="*135)

        reasons = {}

        for r in completed:
            reason = r[
                "exit_reason"
            ]

            reasons[reason] = (
                reasons.get(reason,0)
                + 1
            )

        print(
            reasons
            if reasons
            else "No exits yet."
        )

        # ====================================================
        # DETAIL
        # ====================================================

        print()
        print("="*135)
        print(
            "LATEST PROSPECTIVE EXECUTIONS"
        )
        print("="*135)

        print(
            f"{'ID':>5} "
            f"{'STATUS':>7} "
            f"{'TRAIL':>6} "
            f"{'MAX':>9} "
            f"{'MIN':>9} "
            f"{'NET':>9} "
            f"{'EXIT':>8} "
            f"{'TIME':>8} "
            f"{'PTS':>5} "
            f"{'TOKEN':20}"
        )

        print("-"*110)

        for r in reversed(
            rows[-25:]
        ):

            net = (
                f"{r['net_return']:+8.2f}%"
                if valid(
                    r["net_return"]
                )
                else "      NA"
            )

            mx = (
                f"{r['max_return']:+8.2f}%"
                if valid(
                    r["max_return"]
                )
                else "      NA"
            )

            mn = (
                f"{r['min_return']:+8.2f}%"
                if valid(
                    r["min_return"]
                )
                else "      NA"
            )

            exit_reason = (
                r["exit_reason"]
                if r["exit_reason"]
                else "-"
            )

            exit_time = (
                f"{r['exit_delay']:7.1f}s"
                if valid(
                    r["exit_delay"]
                )
                else "     NA"
            )

            print(
                f"{r['event_id']:5d} "
                f"{r['status']:>7} "
                f"{('YES' if r['trail_activated'] else 'NO'):>6} "
                f"{mx} "
                f"{mn} "
                f"{net} "
                f"{exit_reason:>8} "
                f"{exit_time} "
                f"{r['path_points']:5d} "
                f"{r['token_mint'][:20]}"
            )

        # ====================================================
        # CHECKPOINTS
        # ====================================================

        print()
        print("="*135)
        print(
            "PROSPECTIVE CHECKPOINTS"
        )
        print("="*135)

        n = len(completed)

        if n >= 50:
            print(
                "✅ 50 completed unique tokens — "
                "MEANINGFUL EXECUTION TEST"
            )

        elif n >= 30:
            print(
                "🟡 30 completed unique tokens — "
                "FIRST SERIOUS CHECKPOINT"
            )

        elif n >= 15:
            print(
                "🟡 15 completed unique tokens — "
                "EARLY READ"
            )

        else:
            print(
                f"⏳ {n}/15 completed unique tokens"
            )

        print()
        print(
            "Primary things to watch:"
        )

        print(
            "• AVG and MED both preferably > 0"
        )

        print(
            "• P10 / worst relative to historical"
        )

        print(
            "• enough TRAIL and TIME exits"
        )

        print(
            "• no parameter changes"
        )

        print()
        print(
            "DO NOT RESET THIS TABLE."
        )

        print(
            "Refresh every 10 seconds."
        )

        time.sleep(10)

    except KeyboardInterrupt:
        print(
            "\nT31 stopped."
        )
        break

    except Exception as e:
        print(
            "ERROR:",
            repr(e)
        )
        time.sleep(5)

db.close()
