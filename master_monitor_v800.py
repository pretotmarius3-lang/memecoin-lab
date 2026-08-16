import sqlite3
import time
import math

DB = "validation_v090.db"
REFRESH = 10


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def pct(a, b):
    if not b:
        return "NA"
    return f"{100*a/b:.1f}%"


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


def table_exists(name):
    r = db.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name=?
    """, (name,)).fetchone()
    return r is not None


def scalar(sql, args=()):
    try:
        r = db.execute(sql, args).fetchone()
        return r[0] if r else None
    except Exception:
        return None


def show():

    print("\033[2J\033[H", end="")

    max_id = scalar(
        "SELECT COALESCE(MAX(id),0) FROM events"
    )

    event_n = scalar(
        "SELECT COUNT(*) FROM events"
    )

    token_n = scalar(
        "SELECT COUNT(DISTINCT token_mint) FROM events"
    )

    print("="*135)
    print("MEMECOIN LAB — MASTER MONITOR V8.0")
    print("="*135)

    print()
    print("GLOBAL")
    print("-"*135)
    print(
        f"EVENTS={event_n} | TOKENS={token_n} | MAX_ID={max_id}"
    )

    # ========================================================
    # T23
    # ========================================================

    print()
    print("="*135)
    print("T23 — V2 FROZEN PROSPECTIVE")
    print("="*135)

    if table_exists("v2_frozen_firstsignal_t23"):

        total = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
        """)

        labeled = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE labeled_60=1
        """)

        binary = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE binary_label IS NOT NULL
        """)

        run = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE binary_label=1
        """)

        dump = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE binary_label=0
        """)

        print(
            f"TOKENS={total} | LABELED60={labeled} | "
            f"BINARY={binary} | RUN={run} | DUMP={dump}"
        )

        if total is not None:
            if total < 15:
                print(f"⏳ {total}/15 prospective tokens")
            elif total < 30:
                print(f"🟡 checkpoint 15 reached | {total}/30")
            else:
                print(f"🟢 checkpoint 30 reached | N={total}")

    else:
        print("Table T23 not found")

    # ========================================================
    # T31
    # ========================================================

    print()
    print("="*135)
    print("T31 — FROZEN BASE PROSPECTIVE EXECUTION")
    print("="*135)

    # Search likely table names automatically.
    t31_candidates = [
        "v2_frozen_execution_t31",
        "t31_frozen_base_prospective_execution",
        "t31_prospective_execution",
        "frozen_base_prospective_execution"
    ]

    t31 = next(
        (x for x in t31_candidates if table_exists(x)),
        None
    )

    if t31:
        total = scalar(f"SELECT COUNT(*) FROM {t31}")

        done = scalar(f"""
            SELECT COUNT(*)
            FROM {t31}
            WHERE status='DONE'
        """)

        live = scalar(f"""
            SELECT COUNT(*)
            FROM {t31}
            WHERE status!='DONE'
        """)

        print(
            f"TABLE={t31} | TOTAL={total} | DONE={done} | LIVE/WAIT={live}"
        )

        # Try common net column names.
        cols = [
            r["name"]
            for r in db.execute(
                f"PRAGMA table_info({t31})"
            ).fetchall()
        ]

        net_col = next(
            (
                c for c in [
                    "net",
                    "net_return",
                    "return_net",
                    "net_pct"
                ]
                if c in cols
            ),
            None
        )

        if net_col:
            vals = [
                r[0]
                for r in db.execute(
                    f"""
                    SELECT {net_col}
                    FROM {t31}
                    WHERE {net_col} IS NOT NULL
                    """
                ).fetchall()
                if valid(r[0])
            ]

            if vals:
                avg = sum(vals)/len(vals)
                med = sorted(vals)[len(vals)//2]
                win = sum(x > 0 for x in vals)

                print(
                    f"AVG={avg:+.2f}% | MED={med:+.2f}% | "
                    f"WIN={pct(win,len(vals))}"
                )

    else:
        print("T31 table not found automatically")

    # ========================================================
    # T47
    # ========================================================

    print()
    print("="*135)
    print("T47 — FAST-FLIP PROSPECTIVE SHADOW")
    print("="*135)

    if table_exists("t47_fastflip_prospective"):

        total = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
        """)

        labeled = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE labeled_60=1
        """)

        binary = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE binary_label IS NOT NULL
        """)

        run = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE binary_label=1
        """)

        dump = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE binary_label=0
        """)

        wait = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE labeled_60=0
        """)

        print(
            f"TOKENS={total} | LABELED={labeled} | BINARY={binary} | "
            f"RUN={run} | DUMP={dump} | WAIT={wait}"
        )

        rows = db.execute("""
            SELECT buyer_fast_mean, binary_label
            FROM t47_fastflip_prospective
            WHERE binary_label IS NOT NULL
              AND buyer_fast_mean IS NOT NULL
        """).fetchall()

        if rows:
            runs = [
                r["buyer_fast_mean"]
                for r in rows
                if r["binary_label"] == 1
            ]

            dumps = [
                r["buyer_fast_mean"]
                for r in rows
                if r["binary_label"] == 0
            ]

            if runs and dumps:
                import statistics

                print(
                    f"RUN FAST MED={statistics.median(runs):.3f} | "
                    f"DUMP FAST MED={statistics.median(dumps):.3f}"
                )

        if total is not None:
            if total < 15:
                print(f"⏳ {total}/15 prospective tokens")
            elif total < 30:
                print(f"🟡 checkpoint 15 reached | {total}/30")
            else:
                print(f"🟢 checkpoint 30 reached | N={total}")

    else:
        print("T47 table not created yet / recorder not started")

    # ========================================================
    # RECENT EVENTS
    # ========================================================

    print()
    print("="*135)
    print("LATEST EVENTS")
    print("="*135)

    recent = db.execute("""
        SELECT
            id,
            token_mint,
            fa,
            new_wallets30,
            dex_return_60s
        FROM events
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()

    for r in recent:
        r60 = (
            f"{r['dex_return_60s']:+.2f}%"
            if valid(r["dex_return_60s"])
            else "NA"
        )

        print(
            f"ID={r['id']:4d} | "
            f"FA={r['fa'] if r['fa'] is not None else 'NA'} | "
            f"NEW30={r['new_wallets30'] if r['new_wallets30'] is not None else 'NA'} | "
            f"R60={r60:>9} | "
            f"{r['token_mint'][:24]}"
        )

    print()
    print("="*135)
    print("STATUS")
    print("="*135)
    print("READ-ONLY MONITOR")
    print("T23 / T31 / T47 stay untouched.")
    print(f"Refresh every {REFRESH}s.")
    print("CTRL+C to stop.")


try:
    while True:
        show()
        time.sleep(REFRESH)

except KeyboardInterrupt:
    print("\nMonitor stopped safely.")

finally:
    db.close()
