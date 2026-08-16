#!/usr/bin/env python3

import sqlite3
import time
import os

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")
REFRESH = 5

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


def exists(name):
    return db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
    """, (name,)).fetchone() is not None


def val(x, default=0):
    return default if x is None else x


def line():
    print("─" * 145)


def header(title):
    print()
    print("=" * 145)
    print(title)
    print("=" * 145)


def monitor():

    os.system("clear")

    now = time.time()

    header("MEMECOIN LAB — GLOBAL LIVE RESEARCH MONITOR")

    print(
        time.strftime(
            "LOCAL TIME : %Y-%m-%d %H:%M:%S",
            time.localtime(now)
        )
    )

    # ========================================================
    # T101 — MIGRATIONS
    # ========================================================

    header("T101B — MIGRATION RECORDER")

    if exists("t101_migrations"):

        r = db.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(status='OK') AS ok,

                SUM(
                    CASE
                        WHEN status='OK'
                         AND COALESCE(block_time,detected_at)
                             >= strftime('%s','now')-300
                        THEN 1 ELSE 0
                    END
                ) AS ok5,

                SUM(
                    CASE
                        WHEN status='OK'
                         AND COALESCE(block_time,detected_at)
                             >= strftime('%s','now')-3600
                        THEN 1 ELSE 0
                    END
                ) AS ok1h,

                MAX(
                    CASE
                        WHEN status='OK'
                        THEN COALESCE(block_time,detected_at)
                    END
                ) AS newest
            FROM t101_migrations
        """).fetchone()

        newest_age = (
            now - r["newest"]
            if r["newest"] is not None
            else None
        )

        print(
            f"TOTAL={val(r['total'])} "
            f"| OK={val(r['ok'])} "
            f"| LAST5M={val(r['ok5'])} "
            f"| LAST1H={val(r['ok1h'])}"
        )

        if newest_age is not None:
            print(
                f"NEWEST MIGRATION AGE = {newest_age:.0f}s"
            )

        if val(r["ok1h"]) > 0:
            print("🟢 MIGRATION STREAM ACTIVE")
        else:
            print("🟡 NO OK MIGRATION IN LAST HOUR")

    else:
        print("🔴 t101_migrations missing")

    # ========================================================
    # T116E — RAW COLLECTOR
    # ========================================================

    header("T116E — ADAPTIVE PUMP COLLECTOR")

    if (
        exists("t116_pump_signatures")
        and exists("t116_pump_swaps")
    ):

        q = db.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(status='DONE') AS done,
                SUM(status='WAITING') AS waiting,
                SUM(status='RETRY') AS retry,
                SUM(status='NOT_SWAP') AS not_swap,
                SUM(status='FAILED') AS failed,

                MIN(
                    CASE
                        WHEN status IN ('WAITING','RETRY')
                        THEN received_at
                    END
                ) AS oldest_wait
            FROM t116_pump_signatures
        """).fetchone()

        s = db.execute("""
            SELECT
                COUNT(*) AS swaps,
                COUNT(DISTINCT token_mint) AS tokens,

                SUM(side='BUY') AS buys,
                SUM(side='SELL') AS sells,

                SUM(
                    timestamp >= strftime('%s','now')-120
                ) AS swaps120,

                COUNT(
                    DISTINCT CASE
                        WHEN timestamp >= strftime('%s','now')-120
                        THEN token_mint
                    END
                ) AS tokens120,

                MAX(timestamp) AS newest_swap
            FROM t116_pump_swaps
        """).fetchone()

        oldest_age = (
            now - q["oldest_wait"]
            if q["oldest_wait"] is not None
            else 0
        )

        newest_age = (
            now - s["newest_swap"]
            if s["newest_swap"] is not None
            else None
        )

        print(
            f"SIG TOTAL={val(q['total']):,} "
            f"| DONE={val(q['done']):,} "
            f"| WAIT={val(q['waiting']):,} "
            f"| RETRY={val(q['retry']):,} "
            f"| FAILED={val(q['failed']):,}"
        )

        print(
            f"SWAPS={val(s['swaps']):,} "
            f"| TOKENS={val(s['tokens']):,} "
            f"| BUY={val(s['buys']):,} "
            f"| SELL={val(s['sells']):,}"
        )

        print(
            f"LAST120s: "
            f"SWAPS={val(s['swaps120'])} "
            f"| TOKENS={val(s['tokens120'])}"
        )

        print(
            f"OLDEST WAIT={oldest_age:.1f}s"
        )

        if newest_age is not None:
            print(
                f"NEWEST SWAP AGE={newest_age:.1f}s"
            )

        if (
            oldest_age < 60
            and val(q["failed"]) == 0
            and val(s["swaps120"]) > 0
        ):
            print("🟢 COLLECTOR LIVE / HEALTHY")

        elif oldest_age < 180:
            print("🟡 COLLECTOR BUSY BUT USABLE")

        else:
            print("🔴 COLLECTOR BACKLOG")

    else:
        print("🔴 T116 raw tables missing")

    # ========================================================
    # T116C — CLEANER / LIFECYCLE
    # ========================================================

    header("T116C — PRICE CLEANER / LIFECYCLE")

    if (
        exists("t116_clean_swaps")
        and exists("t116_token_state")
    ):

        c = db.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(price_valid=1) AS clean,
                SUM(price_valid=0) AS rejected,

                SUM(reject_reason='JUMP_HIGH') AS high,
                SUM(reject_reason='JUMP_LOW') AS low
            FROM t116_clean_swaps
        """).fetchone()

        st = db.execute("""
            SELECT
                COUNT(*) AS tokens,
                SUM(migrated=1) AS migrated,

                SUM(
                    CASE
                        WHEN seconds_since_last_clean_swap <= 300
                         AND migrated=0
                        THEN 1 ELSE 0
                    END
                ) AS active,

                SUM(
                    CASE
                        WHEN drawdown_from_peak_pct <= -20
                         AND migrated=0
                        THEN 1 ELSE 0
                    END
                ) AS dd20,

                SUM(
                    CASE
                        WHEN run_from_first_pct >= 50
                         AND migrated=0
                        THEN 1 ELSE 0
                    END
                ) AS pump50
            FROM t116_token_state
        """).fetchone()

        print(
            f"RAW PROCESSED={val(c['total']):,} "
            f"| CLEAN={val(c['clean']):,} "
            f"| REJECTED={val(c['rejected']):,}"
        )

        print(
            f"OUTLIERS HIGH={val(c['high'])} "
            f"| LOW={val(c['low'])}"
        )

        print(
            f"TOKENS={val(st['tokens'])} "
            f"| ACTIVE={val(st['active'])} "
            f"| PUMP>=50%={val(st['pump50'])} "
            f"| DD<=-20%={val(st['dd20'])} "
            f"| MIGRATED MATCHED={val(st['migrated'])}"
        )

        if val(c["clean"]) > 0:
            reject_pct = (
                100.0
                * val(c["rejected"])
                / max(1, val(c["total"]))
            )

            print(
                f"REJECT RATE={reject_pct:.2f}%"
            )

        print("🟢 CLEAN LIFECYCLE ACTIVE")

    else:
        print("🔴 T116C tables missing")

    # ========================================================
    # T116D — EVENTS
    # ========================================================

    header("T116D — PRE-MIGRATION PUMPS / DUMPS")

    if (
        exists("t116_pump_events")
        and exists("t116_premigration_dump_events")
    ):

        p = db.execute("""
            SELECT
                COUNT(*) AS events,
                COUNT(DISTINCT token_mint) AS tokens,

                SUM(pump_level=20) AS p20,
                SUM(pump_level=50) AS p50,
                SUM(pump_level=100) AS p100,
                SUM(pump_level=200) AS p200
            FROM t116_pump_events
        """).fetchone()

        d = db.execute("""
            SELECT
                COUNT(*) AS events,
                COUNT(DISTINCT token_mint) AS tokens,

                SUM(dump_level=10) AS d10,
                SUM(dump_level=20) AS d20,
                SUM(dump_level=30) AS d30,
                SUM(dump_level=40) AS d40,
                SUM(dump_level=50) AS d50
            FROM t116_premigration_dump_events
        """).fetchone()

        print(
            f"🔥 PUMPS: EVENTS={val(p['events'])} "
            f"| TOKENS={val(p['tokens'])}"
        )

        print(
            f"   +20={val(p['p20'])} "
            f"+50={val(p['p50'])} "
            f"+100={val(p['p100'])} "
            f"+200={val(p['p200'])}"
        )

        print(
            f"🔻 DUMPS: EVENTS={val(d['events'])} "
            f"| TOKENS={val(d['tokens'])}"
        )

        print(
            f"   -10={val(d['d10'])} "
            f"-20={val(d['d20'])} "
            f"-30={val(d['d30'])} "
            f"-40={val(d['d40'])} "
            f"-50={val(d['d50'])}"
        )

        print("🟢 EVENT RECORDER ACTIVE")

    else:
        print("🔴 T116D tables missing")

    # ========================================================
    # T117 — OUTCOMES
    # ========================================================

    header("T117 — FOLLOW-THROUGH / RESURRECTION")

    if (
        exists("t117_pump_outcomes")
        and exists("t117_dump_outcomes")
    ):

        p = db.execute("""
            SELECT
                COUNT(*) AS events,
                COUNT(DISTINCT token_mint) AS tokens,

                SUM(path_done_300s=1) AS mature,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s>=1
                ) AS observed,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s=0
                ) AS no_data,

                SUM(migrated_after_event=1) AS migrated
            FROM t117_pump_outcomes
        """).fetchone()

        d = db.execute("""
            SELECT
                COUNT(*) AS events,
                COUNT(DISTINCT token_mint) AS tokens,

                SUM(path_done_300s=1) AS mature,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s>=1
                ) AS observed,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s=0
                ) AS no_data,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s>=1
                    AND rebound20_300=1
                ) AS r20,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s>=1
                    AND rebound50_300=1
                ) AS r50,

                SUM(
                    path_done_300s=1
                    AND path_snapshots_300s>=1
                    AND reclaim_old_peak_300=1
                ) AS peak,

                SUM(migrated_after_event=1) AS migrated
            FROM t117_dump_outcomes
        """).fetchone()

        print(
            f"🔥 PUMP OUTCOMES "
            f"| EVENTS={val(p['events'])} "
            f"| TOKENS={val(p['tokens'])} "
            f"| MATURE={val(p['mature'])} "
            f"| OBSERVED={val(p['observed'])} "
            f"| NO_DATA={val(p['no_data'])} "
            f"| MIG={val(p['migrated'])}"
        )

        print(
            f"🔻 DUMP OUTCOMES "
            f"| EVENTS={val(d['events'])} "
            f"| TOKENS={val(d['tokens'])} "
            f"| MATURE={val(d['mature'])} "
            f"| OBSERVED={val(d['observed'])} "
            f"| NO_DATA={val(d['no_data'])}"
        )

        observed = max(
            1,
            val(d["observed"])
        )

        print(
            f"   +20={val(d['r20'])}/{val(d['observed'])} "
            f"({100*val(d['r20'])/observed:.1f}%)"
        )

        print(
            f"   +50={val(d['r50'])}/{val(d['observed'])} "
            f"({100*val(d['r50'])/observed:.1f}%)"
        )

        print(
            f"   RECLAIM={val(d['peak'])}/{val(d['observed'])} "
            f"({100*val(d['peak'])/observed:.1f}%)"
        )

        print(
            f"   MIGRATED AFTER EVENT={val(d['migrated'])}"
        )

    else:
        print("🔴 T117 tables missing")

    # ========================================================
    # CROSS-LINK
    # ========================================================

    header("PRE-MIGRATION → MIGRATION LINK")

    if (
        exists("t116_token_state")
        and exists("t101_migrations")
    ):

        x = db.execute("""
            SELECT
                COUNT(*) AS t116_tokens,

                COUNT(
                    DISTINCT CASE
                        WHEN m.token_mint IS NOT NULL
                        THEN s.token_mint
                    END
                ) AS exact_matches,

                COUNT(
                    DISTINCT CASE
                        WHEN m.token_mint IS NOT NULL
                         AND COALESCE(
                             m.block_time,
                             m.detected_at
                         ) >= s.first_seen
                        THEN s.token_mint
                    END
                ) AS true_pre_to_migration

            FROM t116_token_state s

            LEFT JOIN t101_migrations m
                ON m.token_mint=s.token_mint
               AND m.status='OK'
        """).fetchone()

        print(
            f"T116 TOKENS={val(x['t116_tokens'])}"
        )

        print(
            f"EXACT T101 MATCHES={val(x['exact_matches'])}"
        )

        print(
            f"OBSERVED PRE → MIGRATION="
            f"{val(x['true_pre_to_migration'])}"
        )

        if val(x["true_pre_to_migration"]) > 0:
            print(
                "🟢 FIRST COMPLETE PRE→MIGRATION CASES AVAILABLE"
            )
        else:
            print(
                "🔵 WAITING FOR FIRST COMPLETE PRE→MIGRATION CASE"
            )

    print()
    line()

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops monitor only"
    )


try:

    while True:

        monitor()

        time.sleep(
            REFRESH
        )

except KeyboardInterrupt:

    print()
    print(
        "Global monitor stopped safely."
    )

finally:

    db.close()
