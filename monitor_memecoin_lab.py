#!/usr/bin/env python3

import sqlite3
import time
import os
from datetime import datetime

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

REFRESH = 10


def scalar(db, sql, params=()):
    try:
        row = db.execute(sql, params).fetchone()
        if not row:
            return 0
        return row[0] if row[0] is not None else 0
    except Exception:
        return 0


def table_exists(db, name):
    return bool(
        db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        """, (name,)).fetchone()
    )


def section(title):
    print()
    print("─" * 120)
    print(title)
    print("─" * 120)


def health(waiting, done):
    total = waiting + done

    if total == 0:
        return "⚪ NO DATA"

    ratio = waiting / total

    if ratio <= 0.10:
        return "🟢 HEALTHY"

    if ratio <= 0.30:
        return "🟡 BUSY"

    return "🔴 BACKLOG"


while True:

    os.system("clear")

    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row

    print("╔" + "═" * 118 + "╗")
    print("║ MEMECOIN LAB — LIVE RESEARCH CONTROL ROOM".ljust(119) + "║")
    print("╚" + "═" * 118 + "╝")

    print(
        "TIME :",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    # ========================================================
    # MIGRATIONS
    # ========================================================

    section("1) MIGRATIONS — T101B")

    if table_exists(db, "t101_migrations"):

        total = scalar(
            db,
            "SELECT COUNT(*) FROM t101_migrations"
        )

        ok = scalar(
            db,
            """
            SELECT COUNT(*)
            FROM t101_migrations
            WHERE status='OK'
            """
        )

        recent = scalar(
            db,
            """
            SELECT COUNT(*)
            FROM t101_migrations
            WHERE COALESCE(block_time,detected_at)
                  >= strftime('%s','now') - 600
            """
        )

        print(
            f"TOTAL={total:<5} "
            f"| OK={ok:<5} "
            f"| NEW 10m={recent:<5}"
        )

    else:
        print("⚪ table t101_migrations absent")

    # ========================================================
    # HOLDERS
    # ========================================================

    section("2) HOLDERS — T101C")

    if table_exists(db, "t101_migrated_holder_snapshots"):

        latest = db.execute("""
        WITH x AS (
            SELECT
                token_mint,
                holder_count,
                ROW_NUMBER() OVER (
                    PARTITION BY token_mint
                    ORDER BY checked_at DESC
                ) AS rn
            FROM t101_migrated_holder_snapshots
            WHERE status='OK'
        )
        SELECT
            COUNT(*) AS tokens,
            SUM(holder_count >= 50) AS h50,
            SUM(holder_count >= 200) AS h200,
            MAX(holder_count) AS max_h
        FROM x
        WHERE rn=1
        """).fetchone()

        print(
            f"TOKENS={latest['tokens'] or 0:<5} "
            f"| >=50={latest['h50'] or 0:<5} "
            f"| >=200={latest['h200'] or 0:<5} "
            f"| MAX={latest['max_h'] or 0}"
        )

    else:
        print("⚪ holder table absent")

    # ========================================================
    # PRICE
    # ========================================================

    section("3) PRICE / MARKET — T102 / T102A")

    if table_exists(db, "t102_migrated_token_watchlist"):

        total = scalar(
            db,
            "SELECT COUNT(*) FROM t102_migrated_token_watchlist"
        )

        wait = scalar(
            db,
            """
            SELECT COUNT(*)
            FROM t102_migrated_token_watchlist
            WHERE state='WAIT_PRICE'
            """
        )

        stale = scalar(
            db,
            """
            SELECT COUNT(*)
            FROM t102_migrated_token_watchlist
            WHERE state='STALE'
            """
        )

        priced = total - wait

        print(
            f"TOKENS={total:<5} "
            f"| PRICE OK={priced:<5} "
            f"| WAIT_PRICE={wait:<5} "
            f"| STALE={stale:<5}"
        )

    else:
        print("⚪ T102 table absent")

    # ========================================================
    # LIFECYCLE
    # ========================================================

    section("4) LIFECYCLE — T103")

    if table_exists(db, "t103_token_lifecycle_state"):

        lifecycle = db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(run_confirmed) AS run,
            SUM(crash_confirmed) AS crash,
            SUM(recovery_confirmed) AS recovery,
            SUM(second_run_confirmed) AS second_run
        FROM t103_token_lifecycle_state
        """).fetchone()

        print(
            f"TOKENS={lifecycle['total'] or 0:<5} "
            f"| RUN={lifecycle['run'] or 0:<5} "
            f"| CRASH={lifecycle['crash'] or 0:<5} "
            f"| RECOVERY={lifecycle['recovery'] or 0:<5} "
            f"| SECOND_RUN={lifecycle['second_run'] or 0:<5}"
        )

    else:
        print("⚪ T103 table absent")

    # ========================================================
    # T104
    # ========================================================

    section("5) RESURRECTION COHORT — T104")

    if table_exists(db, "t104_resurrection_cohort"):

        r = db.execute("""
        SELECT
            COUNT(*) AS crashes,
            SUM(ever_recovery50) AS recovery,
            SUM(ever_reclaim_peak) AS reclaim,
            SUM(CASE
                WHEN done_300s=0
                THEN 1 ELSE 0
            END) AS pending300
        FROM t104_resurrection_cohort
        """).fetchone()

        print(
            f"CRASHES={r['crashes'] or 0:<5} "
            f"| RECOVERY50={r['recovery'] or 0:<5} "
            f"| RECLAIM_PEAK={r['reclaim'] or 0:<5} "
            f"| PENDING300={r['pending300'] or 0:<5}"
        )

    else:
        print("⚪ T104 table absent")

    # ========================================================
    # T107 HEALTH
    # ========================================================

    section("6) TARGETED PUMPSWAP FLOW — T107")

    if table_exists(
        db,
        "t107_targeted_pumpswap_signatures"
    ):

        r = db.execute("""
        SELECT
            COUNT(*) AS received,
            SUM(status='DONE') AS done,
            SUM(status='WAITING') AS waiting,
            SUM(status='NOT_SWAP') AS not_swap,
            SUM(status='RETRY') AS retry,
            COUNT(DISTINCT token_mint) AS tokens
        FROM t107_targeted_pumpswap_signatures
        WHERE received_at >= strftime('%s','now') - 120
        """).fetchone()

        done = r["done"] or 0
        waiting = r["waiting"] or 0

        print(
            f"SAMPLE=10% "
            f"| RX120={r['received'] or 0:<5} "
            f"| DONE={done:<5} "
            f"| WAIT={waiting:<5} "
            f"| NOT_SWAP={r['not_swap'] or 0:<5} "
            f"| RETRY={r['retry'] or 0:<5} "
            f"| TOKENS={r['tokens'] or 0:<5}"
        )

        print(
            "FLOW HEALTH :",
            health(waiting, done)
        )

    else:
        print("⚪ T107 table absent")

    # ========================================================
    # POST-T107 COMPLETE CASES
    # ========================================================

    section("7) COMPLETE PROSPECTIVE CASES — T106 + T107")

    full_rows = []

    if (
        table_exists(db, "t106_resurrection_features")
        and table_exists(
            db,
            "t107_targeted_pumpswap_signatures"
        )
    ):

        start = scalar(
            db,
            """
            SELECT MIN(received_at)
            FROM t107_targeted_pumpswap_signatures
            """
        )

        if start:

            full_rows = db.execute("""
            SELECT
                token_mint,
                crash_timestamp,
                holders_at_crash,
                pre60_swaps,
                post30_swaps,
                post60_300_swaps,
                outcome_recovery50_300,
                outcome_reclaim_peak_300
            FROM t106_resurrection_features
            WHERE crash_timestamp > ?
            ORDER BY crash_timestamp DESC
            LIMIT 10
            """, (start,)).fetchall()

    if not full_rows:

        print(
            "🟡 WAITING FOR FIRST FULL "
            "POST-T107 RUN → CRASH CASE"
        )

    else:

        print(
            f"🔥 COMPLETE CASES : {len(full_rows)}"
        )

        for r in full_rows:

            if r["outcome_recovery50_300"] == 1:
                state = "🟢 RECOVERED"

            elif r["outcome_recovery50_300"] == 0:
                state = "🔴 FAILED"

            else:
                state = "🟠 PENDING"

            print(
                f"{r['token_mint'][:18]:18} "
                f"| H={str(r['holders_at_crash']):>5} "
                f"| PRE60={str(r['pre60_swaps']):>4} "
                f"| P30={str(r['post30_swaps']):>4} "
                f"| P60-300={str(r['post60_300_swaps']):>4} "
                f"| {state}"
            )

    # ========================================================
    # READINESS
    # ========================================================

    section("8) RESEARCH READINESS")

    crashes = scalar(
        db,
        """
        SELECT COUNT(*)
        FROM t104_resurrection_cohort
        """
    ) if table_exists(
        db,
        "t104_resurrection_cohort"
    ) else 0

    print(
        f"INTEGRITY    : {crashes:>3}/30"
    )

    print(
        f"DESCRIPTIVE  : {crashes:>3}/50"
    )

    print(
        f"DISCOVERY    : {crashes:>3}/100"
    )

    if crashes >= 100:
        print(
            "🟢 READY FOR DISCOVERY"
        )

    elif crashes >= 50:
        print(
            "🟡 DESCRIPTIVE PHASE"
        )

    elif crashes >= 30:
        print(
            "🔵 INTEGRITY PHASE"
        )

    else:
        print(
            "⚪ COLLECTING"
        )

    # ========================================================
    # LATEST EVENTS
    # ========================================================

    section("9) LATEST LIFECYCLE EVENTS")

    if table_exists(
        db,
        "t103_token_lifecycle_events"
    ):

        rows = db.execute("""
        SELECT
            token_mint,
            lifecycle_event,
            move_pct
        FROM t103_token_lifecycle_events
        ORDER BY event_timestamp DESC
        LIMIT 8
        """).fetchall()

        for r in rows:

            print(
                f"{r['lifecycle_event']:<24} "
                f"| {r['token_mint'][:18]:18} "
                f"| MOVE={r['move_pct'] if r['move_pct'] is not None else 'NA'}"
            )

    db.close()

    print()
    print("=" * 120)
    print(
        f"AUTO REFRESH {REFRESH}s "
        "| CTRL+C stops dashboard only"
    )
    print("=" * 120)

    time.sleep(REFRESH)
