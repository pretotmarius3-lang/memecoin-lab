#!/usr/bin/env python3

import os
import sys
import time
import signal
import sqlite3
from datetime import datetime

DB = "validation_v090.db"
TABLE = "t59_capv2_prospective"

REFRESH_SECONDS = 10

TOKEN_OBSERVATION = 15
TOKEN_FIRST_AUDIT = 30
TOKEN_CONFIRMATION = 50

MIN_FIRST_RUN = 5
MIN_FIRST_DUMP = 5

MIN_SERIOUS_RUN = 10
MIN_SERIOUS_DUMP = 10

MIN_CONFIRM_RUN = 15
MIN_CONFIRM_DUMP = 15


# ============================================================
# HELPERS
# ============================================================

def clear():
    os.system(
        "cls" if os.name == "nt"
        else "clear"
    )


def bar(value, target, width=26):

    if target <= 0:
        return "[" + "?"*width + "]"

    ratio = min(
        max(value/target, 0),
        1
    )

    filled = int(
        ratio*width
    )

    return (
        "["
        + "█"*filled
        + "·"*(width-filled)
        + "]"
    )


def yesno(x):
    return "✅ YES" if x else "❌ NO"


def progress_line(
    label,
    value,
    target
):

    pct = (
        min(
            100,
            100*value/target
        )
        if target > 0
        else 0
    )

    print(
        f"{label:24} "
        f"{value:3d}/{target:<3d} "
        f"{bar(value,target)} "
        f"{pct:5.1f}%"
    )


# ============================================================
# DATA
# ============================================================

def snapshot():

    db = sqlite3.connect(
        f"file:{DB}?mode=ro",
        uri=True,
        timeout=10
    )

    db.row_factory = sqlite3.Row


    exists = db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE
        type='table'
        AND name=?
    """, (
        TABLE,
    )).fetchone()


    if not exists:

        db.close()

        return {
            "error":
                f"Missing table {TABLE}"
        }


    row = db.execute(f"""
    SELECT
        COUNT(*) AS events,

        COUNT(
            DISTINCT token_mint
        ) AS tokens,

        SUM(
            CASE
            WHEN binary_label=1
            THEN 1
            ELSE 0
            END
        ) AS run_events,

        SUM(
            CASE
            WHEN binary_label=0
            THEN 1
            ELSE 0
            END
        ) AS dump_events,

        SUM(
            CASE
            WHEN status='NEUTRAL'
            THEN 1
            ELSE 0
            END
        ) AS neutral_events,

        SUM(
            CASE
            WHEN status='WAIT'
            THEN 1
            ELSE 0
            END
        ) AS wait_events

    FROM {TABLE}
    """).fetchone()


    # ========================================================
    # FIRST-EVENT/TOKEN CLASS COUNTS
    # ========================================================

    first_rows = db.execute(f"""
    WITH ranked AS (
        SELECT
            token_mint,
            event_id,
            event_timestamp,
            binary_label,
            status,

            ROW_NUMBER() OVER (
                PARTITION BY token_mint
                ORDER BY
                    event_timestamp,
                    event_id
            ) AS rn

        FROM {TABLE}

        WHERE
            token_mint IS NOT NULL
    )

    SELECT
        COUNT(*) AS first_tokens,

        SUM(
            CASE
            WHEN binary_label=1
            THEN 1
            ELSE 0
            END
        ) AS first_run,

        SUM(
            CASE
            WHEN binary_label=0
            THEN 1
            ELSE 0
            END
        ) AS first_dump,

        SUM(
            CASE
            WHEN status='NEUTRAL'
            THEN 1
            ELSE 0
            END
        ) AS first_neutral,

        SUM(
            CASE
            WHEN status='WAIT'
            THEN 1
            ELSE 0
            END
        ) AS first_wait

    FROM ranked

    WHERE rn=1
    """).fetchone()


    # ========================================================
    # FREEZE
    # ========================================================

    freeze = db.execute(f"""
    SELECT
        COUNT(
            DISTINCT freeze_sha256
        ) AS hashes,

        COUNT(
            DISTINCT boundary_id
        ) AS boundaries,

        MIN(boundary_id) AS boundary_id

    FROM {TABLE}
    """).fetchone()


    # ========================================================
    # BINARY TOKEN COVERAGE
    # ========================================================

    binary_token_row = db.execute(f"""
    SELECT
        COUNT(
            DISTINCT token_mint
        ) AS binary_tokens

    FROM {TABLE}

    WHERE
        binary_label IS NOT NULL
    """).fetchone()


    db.close()


    def nz(x):
        return 0 if x is None else int(x)


    return {
        "events":
            nz(row["events"]),

        "tokens":
            nz(row["tokens"]),

        "run_events":
            nz(row["run_events"]),

        "dump_events":
            nz(row["dump_events"]),

        "neutral_events":
            nz(row["neutral_events"]),

        "wait_events":
            nz(row["wait_events"]),

        "first_tokens":
            nz(first_rows["first_tokens"]),

        "first_run":
            nz(first_rows["first_run"]),

        "first_dump":
            nz(first_rows["first_dump"]),

        "first_neutral":
            nz(first_rows["first_neutral"]),

        "first_wait":
            nz(first_rows["first_wait"]),

        "binary_tokens":
            nz(binary_token_row[
                "binary_tokens"
            ]),

        "freeze_hashes":
            nz(freeze["hashes"]),

        "freeze_boundaries":
            nz(freeze["boundaries"]),

        "boundary_id":
            freeze["boundary_id"],
    }


# ============================================================
# READINESS
# ============================================================

def readiness(s):

    tokens = s[
        "tokens"
    ]

    runs = s[
        "run_events"
    ]

    dumps = s[
        "dump_events"
    ]


    observation = (
        tokens >= TOKEN_OBSERVATION
    )


    first_audit = (
        tokens >= TOKEN_FIRST_AUDIT
        and runs >= MIN_FIRST_RUN
        and dumps >= MIN_FIRST_DUMP
    )


    serious = (
        tokens >= TOKEN_FIRST_AUDIT
        and runs >= MIN_SERIOUS_RUN
        and dumps >= MIN_SERIOUS_DUMP
    )


    confirmation = (
        tokens >= TOKEN_CONFIRMATION
        and runs >= MIN_CONFIRM_RUN
        and dumps >= MIN_CONFIRM_DUMP
    )


    return {
        "observation":
            observation,

        "first_audit":
            first_audit,

        "serious":
            serious,

        "confirmation":
            confirmation,
    }


# ============================================================
# DISPLAY
# ============================================================

def display(s):

    clear()

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


    print("=" * 150)
    print(
        "MEMECOIN LAB — T73 T59 PROSPECTIVE READINESS MONITOR"
    )
    print("=" * 150)

    print(
        f"UPDATED          : {now}"
    )

    print(
        f"REFRESH          : {REFRESH_SECONDS}s"
    )

    print(
        "MODE             : READ-ONLY"
    )

    print(
        "RUN / DUMP       : frozen ±10% labels"
    )

    print(
        "CTRL+C           : stop monitor only"
    )


    if "error" in s:

        print()
        print(
            f"❌ {s['error']}"
        )

        return


    r = readiness(
        s
    )


    # ========================================================
    # A) COLLECTION
    # ========================================================

    print()
    print("=" * 150)
    print("A) COLLECTION")
    print("=" * 150)

    print(
        f"EVENTS                : {s['events']}"
    )

    print(
        f"UNIQUE TOKENS         : {s['tokens']}"
    )

    print(
        f"BINARY TOKENS         : {s['binary_tokens']}"
    )

    print()

    progress_line(
        "TOKENS TO 30",
        s["tokens"],
        TOKEN_FIRST_AUDIT
    )

    progress_line(
        "TOKENS TO 50",
        s["tokens"],
        TOKEN_CONFIRMATION
    )


    # ========================================================
    # B) EVENT LABELS
    # ========================================================

    print()
    print("=" * 150)
    print("B) EVENT-LEVEL LABEL BALANCE")
    print("=" * 150)

    print(
        f"RUN                   : {s['run_events']}"
    )

    print(
        f"DUMP                  : {s['dump_events']}"
    )

    print(
        f"NEUTRAL               : {s['neutral_events']}"
    )

    print(
        f"WAIT                  : {s['wait_events']}"
    )


    binary_total = (
        s["run_events"]
        + s["dump_events"]
    )


    print(
        f"BINARY TOTAL          : {binary_total}"
    )


    if binary_total > 0:

        print(
            f"RUN SHARE             : "
            f"{100*s['run_events']/binary_total:.1f}%"
        )

        print(
            f"DUMP SHARE            : "
            f"{100*s['dump_events']/binary_total:.1f}%"
        )

    else:

        print(
            "CLASS BALANCE         : no binary outcomes yet"
        )


    # ========================================================
    # C) FIRST TOKEN
    # ========================================================

    print()
    print("=" * 150)
    print("C) FIRST-EVENT/TOKEN LABEL BALANCE")
    print("=" * 150)

    print(
        f"FIRST TOKENS          : {s['first_tokens']}"
    )

    print(
        f"FIRST RUN             : {s['first_run']}"
    )

    print(
        f"FIRST DUMP            : {s['first_dump']}"
    )

    print(
        f"FIRST NEUTRAL         : {s['first_neutral']}"
    )

    print(
        f"FIRST WAIT            : {s['first_wait']}"
    )


    # ========================================================
    # D) READINESS REQUIREMENTS
    # ========================================================

    print()
    print("=" * 150)
    print("D) PRE-DECLARED READINESS GATES")
    print("=" * 150)

    print("OBSERVATION")
    progress_line(
        "TOKENS",
        s["tokens"],
        TOKEN_OBSERVATION
    )

    print(
        f"READY                 : "
        f"{yesno(r['observation'])}"
    )


    print()
    print("FIRST AUDIT")

    progress_line(
        "TOKENS",
        s["tokens"],
        TOKEN_FIRST_AUDIT
    )

    progress_line(
        "RUN",
        s["run_events"],
        MIN_FIRST_RUN
    )

    progress_line(
        "DUMP",
        s["dump_events"],
        MIN_FIRST_DUMP
    )

    print(
        f"READY                 : "
        f"{yesno(r['first_audit'])}"
    )


    print()
    print("SERIOUS AUDIT")

    progress_line(
        "TOKENS",
        s["tokens"],
        TOKEN_FIRST_AUDIT
    )

    progress_line(
        "RUN",
        s["run_events"],
        MIN_SERIOUS_RUN
    )

    progress_line(
        "DUMP",
        s["dump_events"],
        MIN_SERIOUS_DUMP
    )

    print(
        f"READY                 : "
        f"{yesno(r['serious'])}"
    )


    print()
    print("CONFIRMATION")

    progress_line(
        "TOKENS",
        s["tokens"],
        TOKEN_CONFIRMATION
    )

    progress_line(
        "RUN",
        s["run_events"],
        MIN_CONFIRM_RUN
    )

    progress_line(
        "DUMP",
        s["dump_events"],
        MIN_CONFIRM_DUMP
    )

    print(
        f"READY                 : "
        f"{yesno(r['confirmation'])}"
    )


    # ========================================================
    # E) CURRENT STATUS
    # ========================================================

    print()
    print("=" * 150)
    print("E) CURRENT RESEARCH STATUS")
    print("=" * 150)


    if r[
        "confirmation"
    ]:

        print(
            "🟢 CONFIRMATION AUDIT READY"
        )

        print(
            "Frozen T59 can undergo the full "
            "prospective confirmation audit."
        )


    elif r[
        "serious"
    ]:

        print(
            "🟠 SERIOUS PROSPECTIVE AUDIT READY"
        )

        print(
            "Enough binary outcomes for a more "
            "meaningful frozen audit."
        )


    elif r[
        "first_audit"
    ]:

        print(
            "🟡 FIRST PROSPECTIVE AUDIT READY"
        )

        print(
            "Exploratory forward audit allowed; "
            "no model changes."
        )


    elif r[
        "observation"
    ]:

        missing_tokens = max(
            0,
            TOKEN_FIRST_AUDIT
            - s["tokens"]
        )

        missing_runs = max(
            0,
            MIN_FIRST_RUN
            - s["run_events"]
        )

        missing_dumps = max(
            0,
            MIN_FIRST_DUMP
            - s["dump_events"]
        )


        print(
            "🔵 OBSERVATION / COLLECTION"
        )

        print(
            f"Need for first audit: "
            f"+{missing_tokens} tokens, "
            f"+{missing_runs} RUN, "
            f"+{missing_dumps} DUMP."
        )


    else:

        print(
            "⚪ EARLY COLLECTION"
        )


    # ========================================================
    # F) FREEZE
    # ========================================================

    print()
    print("=" * 150)
    print("F) FREEZE INTEGRITY")
    print("=" * 150)

    print(
        f"FREEZE HASH COUNT     : "
        f"{s['freeze_hashes']}"
    )

    print(
        f"BOUNDARY COUNT        : "
        f"{s['freeze_boundaries']}"
    )

    print(
        f"BOUNDARY ID           : "
        f"{s['boundary_id']}"
    )


    freeze_ok = (
        s["freeze_hashes"] == 1
        and s["freeze_boundaries"] == 1
    )


    print(
        f"FREEZE STATUS         : "
        f"{'✅ CONSISTENT' if freeze_ok else '❌ REVIEW'}"
    )


    # ========================================================
    # FOOTER
    # ========================================================

    print()
    print("=" * 150)

    print(
        "T73 DOES NOT evaluate model performance."
    )

    print(
        "T73 DOES NOT change RUN/DUMP thresholds."
    )

    print(
        "T73 DOES NOT refit CAP-v2."
    )

    print(
        "T73 DOES NOT write to DB."
    )

    print(
        "T59 remains frozen."
    )

    print(
        f"Next refresh in {REFRESH_SECONDS}s."
    )

    print(
        "CTRL+C stops T73 only."
    )


# ============================================================
# EXIT HANDLING
# ============================================================

running = True


def stop_handler(
    sig,
    frame
):

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


# ============================================================
# PREFLIGHT
# ============================================================

if not os.path.exists(
    DB
):

    print(
        f"❌ Missing DB: {DB}"
    )

    sys.exit(1)


# ============================================================
# LOOP
# ============================================================

while running:

    try:

        s = snapshot()

        display(
            s
        )

    except Exception as exc:

        clear()

        print(
            "T73 monitor error:"
        )

        print(
            repr(exc)
        )


    for _ in range(
        REFRESH_SECONDS*10
    ):

        if not running:
            break

        time.sleep(
            0.1
        )


print()
print(
    "T73 stopped safely."
)
