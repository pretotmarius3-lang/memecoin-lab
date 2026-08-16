#!/usr/bin/env python3

import os
import time
import signal
import sqlite3
from datetime import datetime

DB = "validation_v090.db"

T59 = "t59_capv2_prospective"
T78 = "t78_capv2_buyervel10_prospective"

REFRESH_SEC = 10

T78_BOUNDARY = 1049

FIRST_TOKENS = 30
FIRST_RUN = 5
FIRST_DUMP = 5

SERIOUS_TOKENS = 50
SERIOUS_RUN = 10
SERIOUS_DUMP = 10


def clear():
    os.system("clear")


def yesno(x):
    return "✅ YES" if x else "❌ NO"


def nz(x):
    return 0 if x is None else int(x)


def bar(value, target, width=24):
    if target <= 0:
        return "[" + "?"*width + "]"

    ratio = min(max(value/target, 0), 1)
    filled = int(ratio*width)

    return (
        "["
        + "█"*filled
        + "·"*(width-filled)
        + "]"
    )


def progress(label, value, target):
    pct = min(100.0, 100*value/target)

    print(
        f"{label:28} "
        f"{value:3d}/{target:<3d} "
        f"{bar(value,target)} "
        f"{pct:5.1f}%"
    )


def table_exists(db, name):
    return bool(
        db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        """, (name,)).fetchone()
    )


def snapshot():

    db = sqlite3.connect(
        f"file:{DB}?mode=ro",
        uri=True,
        timeout=10
    )

    db.row_factory = sqlite3.Row

    if not table_exists(db, T59):
        raise RuntimeError(f"Missing {T59}")

    if not table_exists(db, T78):
        raise RuntimeError(f"Missing {T78}")


    # ========================================================
    # T59
    # ========================================================

    t59 = db.execute(f"""
    SELECT
        COUNT(*) AS rows,

        COUNT(DISTINCT token_mint) AS tokens,

        SUM(CASE WHEN status='RUN' THEN 1 ELSE 0 END) AS run,
        SUM(CASE WHEN status='DUMP' THEN 1 ELSE 0 END) AS dump,
        SUM(CASE WHEN status='NEUTRAL' THEN 1 ELSE 0 END) AS neutral,
        SUM(CASE WHEN status='WAIT' THEN 1 ELSE 0 END) AS wait,

        SUM(
            CASE
            WHEN capv2_score IS NOT NULL
            THEN 1 ELSE 0
            END
        ) AS scored

    FROM {T59}
    """).fetchone()


    # ========================================================
    # T78
    # ========================================================

    t78 = db.execute(f"""
    SELECT
        COUNT(*) AS rows,

        COUNT(DISTINCT token_mint) AS tokens,

        COUNT(
            DISTINCT CASE
            WHEN t78_score IS NOT NULL
            THEN token_mint
            END
        ) AS scored_tokens,

        SUM(
            CASE
            WHEN t78_score IS NOT NULL
            THEN 1 ELSE 0
            END
        ) AS scored_rows,

        SUM(
            CASE
            WHEN t78_score IS NULL
            THEN 1 ELSE 0
            END
        ) AS unscored_rows,

        SUM(CASE WHEN status='RUN' THEN 1 ELSE 0 END) AS run,
        SUM(CASE WHEN status='DUMP' THEN 1 ELSE 0 END) AS dump,
        SUM(CASE WHEN status='NEUTRAL' THEN 1 ELSE 0 END) AS neutral,
        SUM(CASE WHEN status='WAIT' THEN 1 ELSE 0 END) AS wait

    FROM {T78}
    """).fetchone()


    # ========================================================
    # COMMON POST-T78-BOUNDARY COHORT
    #
    # Must exist in BOTH tables and both scores available.
    # ========================================================

    common = db.execute(f"""
    SELECT
        COUNT(*) AS events,

        COUNT(DISTINCT a.token_mint) AS tokens,

        SUM(
            CASE
            WHEN a.binary_label=1
            THEN 1 ELSE 0
            END
        ) AS run,

        SUM(
            CASE
            WHEN a.binary_label=0
            THEN 1 ELSE 0
            END
        ) AS dump,

        SUM(
            CASE
            WHEN a.status='NEUTRAL'
            THEN 1 ELSE 0
            END
        ) AS neutral,

        SUM(
            CASE
            WHEN a.status='WAIT'
            THEN 1 ELSE 0
            END
        ) AS wait

    FROM {T59} a

    JOIN {T78} b
        ON b.event_id=a.event_id

    WHERE
        a.event_id > ?
        AND b.event_id > ?
        AND a.capv2_score IS NOT NULL
        AND b.t78_score IS NOT NULL
    """, (
        T78_BOUNDARY,
        T78_BOUNDARY,
    )).fetchone()


    # ========================================================
    # COMMON FIRST-EVENT/TOKEN
    # ========================================================

    first_common = db.execute(f"""
    WITH common AS (

        SELECT
            a.event_id,
            a.token_mint,
            a.event_timestamp,
            a.binary_label,
            a.status,

            ROW_NUMBER() OVER (
                PARTITION BY a.token_mint
                ORDER BY
                    a.event_timestamp,
                    a.event_id
            ) AS rn

        FROM {T59} a

        JOIN {T78} b
            ON b.event_id=a.event_id

        WHERE
            a.event_id > ?
            AND b.event_id > ?
            AND a.capv2_score IS NOT NULL
            AND b.t78_score IS NOT NULL
    )

    SELECT
        COUNT(*) AS tokens,

        SUM(
            CASE
            WHEN binary_label=1
            THEN 1 ELSE 0
            END
        ) AS run,

        SUM(
            CASE
            WHEN binary_label=0
            THEN 1 ELSE 0
            END
        ) AS dump,

        SUM(
            CASE
            WHEN status='NEUTRAL'
            THEN 1 ELSE 0
            END
        ) AS neutral,

        SUM(
            CASE
            WHEN status='WAIT'
            THEN 1 ELSE 0
            END
        ) AS wait

    FROM common

    WHERE rn=1
    """, (
        T78_BOUNDARY,
        T78_BOUNDARY,
    )).fetchone()


    # ========================================================
    # FREEZE INTEGRITY
    # ========================================================

    freeze59 = db.execute(f"""
    SELECT
        COUNT(DISTINCT freeze_sha256) AS hashes,
        COUNT(DISTINCT boundary_id) AS boundaries,
        MIN(boundary_id) AS boundary
    FROM {T59}
    """).fetchone()


    freeze78 = db.execute(f"""
    SELECT
        COUNT(DISTINCT freeze_sha256) AS hashes,
        COUNT(DISTINCT boundary_id) AS boundaries,
        MIN(boundary_id) AS boundary
    FROM {T78}
    """).fetchone()


    db.close()

    return {
        "t59": t59,
        "t78": t78,
        "common": common,
        "first_common": first_common,
        "freeze59": freeze59,
        "freeze78": freeze78,
    }


def display(s):

    clear()

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    t59 = s["t59"]
    t78 = s["t78"]
    common = s["common"]
    first = s["first_common"]


    print("=" * 150)
    print(
        "MEMECOIN LAB — T79 DUAL PROSPECTIVE MONITOR"
    )
    print("=" * 150)

    print(f"UPDATED          : {now}")
    print(f"REFRESH          : {REFRESH_SEC}s")
    print("MODE             : READ-ONLY")
    print("T59              : CAP-v2 frozen")
    print("T78              : CAP-v2 + BuyerVel10 frozen")
    print(f"COMPARE FROM ID  : > {T78_BOUNDARY}")


    # ========================================================
    # A) T59
    # ========================================================

    print()
    print("=" * 150)
    print("A) T59 — CAP-v2")
    print("=" * 150)

    print(f"ROWS             : {nz(t59['rows'])}")
    print(f"TOKENS           : {nz(t59['tokens'])}")
    print(f"SCORED ROWS      : {nz(t59['scored'])}")
    print()
    print(f"RUN              : {nz(t59['run'])}")
    print(f"DUMP             : {nz(t59['dump'])}")
    print(f"NEUTRAL          : {nz(t59['neutral'])}")
    print(f"WAIT             : {nz(t59['wait'])}")


    # ========================================================
    # B) T78
    # ========================================================

    print()
    print("=" * 150)
    print("B) T78 — CAP-v2 + BUYER VELOCITY 10")
    print("=" * 150)

    print(f"ROWS             : {nz(t78['rows'])}")
    print(f"TOKENS           : {nz(t78['tokens'])}")
    print(f"SCORED TOKENS    : {nz(t78['scored_tokens'])}")
    print(f"SCORED ROWS      : {nz(t78['scored_rows'])}")
    print(f"UNSCORED ROWS    : {nz(t78['unscored_rows'])}")

    if nz(t78["rows"]) > 0:
        coverage = (
            100
            * nz(t78["scored_rows"])
            / nz(t78["rows"])
        )

        print(
            f"SCORING COVERAGE : {coverage:.1f}%"
        )

    print()
    print(f"RUN              : {nz(t78['run'])}")
    print(f"DUMP             : {nz(t78['dump'])}")
    print(f"NEUTRAL          : {nz(t78['neutral'])}")
    print(f"WAIT             : {nz(t78['wait'])}")


    # ========================================================
    # C) COMMON COHORT
    # ========================================================

    print()
    print("=" * 150)
    print(
        "C) COMMON POST-1049 SCORABLE COHORT"
    )
    print("=" * 150)

    print(
        f"COMMON EVENTS    : {nz(common['events'])}"
    )

    print(
        f"COMMON TOKENS    : {nz(common['tokens'])}"
    )

    print(
        f"RUN              : {nz(common['run'])}"
    )

    print(
        f"DUMP             : {nz(common['dump'])}"
    )

    print(
        f"NEUTRAL          : {nz(common['neutral'])}"
    )

    print(
        f"WAIT             : {nz(common['wait'])}"
    )

    print()
    print(
        "This is the ONLY cohort that will be used "
        "for direct T59 vs T78 comparison."
    )


    # ========================================================
    # D) FIRST TOKEN COMMON
    # ========================================================

    print()
    print("=" * 150)
    print(
        "D) COMMON FIRST-EVENT/TOKEN"
    )
    print("=" * 150)

    print(
        f"TOKENS           : {nz(first['tokens'])}"
    )

    print(
        f"RUN              : {nz(first['run'])}"
    )

    print(
        f"DUMP             : {nz(first['dump'])}"
    )

    print(
        f"NEUTRAL          : {nz(first['neutral'])}"
    )

    print(
        f"WAIT             : {nz(first['wait'])}"
    )


    # ========================================================
    # E) T78 READINESS
    # ========================================================

    print()
    print("=" * 150)
    print("E) T78 PROSPECTIVE READINESS")
    print("=" * 150)

    tokens = nz(
        t78["scored_tokens"]
    )

    run = nz(
        t78["run"]
    )

    dump = nz(
        t78["dump"]
    )


    first_ready = (
        tokens >= FIRST_TOKENS
        and run >= FIRST_RUN
        and dump >= FIRST_DUMP
    )


    serious_ready = (
        tokens >= SERIOUS_TOKENS
        and run >= SERIOUS_RUN
        and dump >= SERIOUS_DUMP
    )


    print("FIRST AUDIT")

    progress(
        "SCORED TOKENS",
        tokens,
        FIRST_TOKENS
    )

    progress(
        "RUN",
        run,
        FIRST_RUN
    )

    progress(
        "DUMP",
        dump,
        FIRST_DUMP
    )

    print(
        f"READY                    : "
        f"{yesno(first_ready)}"
    )


    print()
    print("SERIOUS AUDIT")

    progress(
        "SCORED TOKENS",
        tokens,
        SERIOUS_TOKENS
    )

    progress(
        "RUN",
        run,
        SERIOUS_RUN
    )

    progress(
        "DUMP",
        dump,
        SERIOUS_DUMP
    )

    print(
        f"READY                    : "
        f"{yesno(serious_ready)}"
    )


    # ========================================================
    # F) FREEZE
    # ========================================================

    print()
    print("=" * 150)
    print("F) FREEZE INTEGRITY")
    print("=" * 150)

    f59 = s["freeze59"]
    f78 = s["freeze78"]

    ok59 = (
        nz(f59["hashes"]) == 1
        and nz(f59["boundaries"]) == 1
    )

    ok78 = (
        nz(f78["hashes"]) == 1
        and nz(f78["boundaries"]) == 1
        and f78["boundary"] == T78_BOUNDARY
    )

    print(
        f"T59 HASHES       : {nz(f59['hashes'])}"
    )

    print(
        f"T59 BOUNDARIES   : {nz(f59['boundaries'])}"
    )

    print(
        f"T59 BOUNDARY     : {f59['boundary']}"
    )

    print(
        f"T59 STATUS       : "
        f"{'✅ CONSISTENT' if ok59 else '❌ REVIEW'}"
    )

    print()

    print(
        f"T78 HASHES       : {nz(f78['hashes'])}"
    )

    print(
        f"T78 BOUNDARIES   : {nz(f78['boundaries'])}"
    )

    print(
        f"T78 BOUNDARY     : {f78['boundary']}"
    )

    print(
        f"T78 STATUS       : "
        f"{'✅ CONSISTENT' if ok78 else '❌ REVIEW'}"
    )


    # ========================================================
    # G) STATUS
    # ========================================================

    print()
    print("=" * 150)
    print("G) RESEARCH STATUS")
    print("=" * 150)

    if serious_ready:

        print(
            "🟢 T78 SERIOUS PROSPECTIVE AUDIT READY."
        )

    elif first_ready:

        print(
            "🟡 T78 FIRST PROSPECTIVE AUDIT READY."
        )

    else:

        print(
            "🔵 COLLECTION / OBSERVATION."
        )

        print(
            f"Need for first audit: "
            f"+{max(0,FIRST_TOKENS-tokens)} scored tokens, "
            f"+{max(0,FIRST_RUN-run)} RUN, "
            f"+{max(0,FIRST_DUMP-dump)} DUMP."
        )

    print()
    print(
        "NO PERFORMANCE COMPARISON IS RUN BY T79."
    )

    print(
        "NO REFIT / NO THRESHOLD CHANGE / NO DB WRITE."
    )

    print(
        f"Next refresh in {REFRESH_SEC}s."
    )


running = True


def stop_handler(sig, frame):
    global running
    running = False


signal.signal(
    signal.SIGINT,
    stop_handler
)

signal.signal(
    signal.SIGTERM,
    stop_handler
)


while running:

    try:
        s = snapshot()
        display(s)

    except Exception as exc:

        clear()

        print(
            "T79 ERROR:"
        )

        print(
            repr(exc)
        )


    for _ in range(
        REFRESH_SEC*10
    ):
        if not running:
            break

        time.sleep(
            0.1
        )


print()
print(
    "T79 stopped safely."
)
