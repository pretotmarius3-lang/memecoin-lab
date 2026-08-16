#!/usr/bin/env python3

import os
import sys
import time
import signal
import sqlite3
import subprocess
from datetime import datetime

DB = "validation_v090.db"
MONITOR = "t71_master_prospective_research_monitor.py"

REFRESH_SECONDS = 10

CHECKPOINT_1 = 15
CHECKPOINT_2 = 30
CHECKPOINT_3 = 50


# ============================================================
# HELPERS
# ============================================================

def clear():
    os.system(
        "cls" if os.name == "nt"
        else "clear"
    )


def get_t59_tokens():
    """
    Independent read-only checkpoint counter.
    T71 remains the authoritative detailed monitor.
    """

    if not os.path.exists(DB):
        return None

    try:
        db = sqlite3.connect(
            f"file:{DB}?mode=ro",
            uri=True,
            timeout=5
        )

        row = db.execute("""
            SELECT COUNT(DISTINCT token_mint)
            FROM t59_capv2_prospective
            WHERE token_mint IS NOT NULL
        """).fetchone()

        db.close()

        if row is None:
            return None

        return row[0]

    except Exception:
        return None


def progress_bar(n, target, width=30):

    if n is None:
        return "[" + "?" * width + "]"

    ratio = min(
        max(n / target, 0),
        1
    )

    filled = int(
        ratio * width
    )

    return (
        "["
        + "█" * filled
        + "·" * (width - filled)
        + "]"
    )


def gate(n):

    if n is None:
        return (
            "⚪ T59 STATUS UNKNOWN — "
            "DO NOT INFER PROGRESS"
        )

    if n < CHECKPOINT_1:
        return (
            f"🔵 COLLECT — {n}/{CHECKPOINT_1}"
        )

    if n < CHECKPOINT_2:
        return (
            f"🟡 OBSERVE ONLY — "
            f"{n}/{CHECKPOINT_2}"
        )

    if n < CHECKPOINT_3:
        return (
            f"🟠 30-TOKEN AUDIT READY — "
            f"{n}/{CHECKPOINT_3}"
        )

    return (
        f"🟢 50+ CONFIRMATION AUDIT READY — "
        f"{n} TOKENS"
    )


def checkpoint_banner(n):

    if n is None:
        return

    print()
    print("=" * 78)

    if n >= CHECKPOINT_3:

        print(
            "🟢🟢🟢 T59 50+ TOKEN CHECKPOINT REACHED 🟢🟢🟢"
        )

        print(
            "CONFIRMATION AUDIT IS READY."
        )

        print(
            "KEEP MODEL FROZEN UNTIL AUDIT."
        )

    elif n >= CHECKPOINT_2:

        print(
            "🟠🟠🟠 T59 30 TOKEN CHECKPOINT REACHED 🟠🟠🟠"
        )

        print(
            "FIRST PROSPECTIVE DECISION AUDIT IS READY."
        )

        print(
            "NO REFIT / NO THRESHOLD CHANGE BEFORE AUDIT."
        )

    elif n >= CHECKPOINT_1:

        print(
            "🟡 T59 OBSERVATION PHASE"
        )

        print(
            "15 reached. Continue untouched until 30."
        )

    else:

        print(
            "🔵 T59 COLLECTION PHASE"
        )

    print("=" * 78)


def run_t71():

    try:

        p = subprocess.run(
            [
                sys.executable,
                MONITOR
            ],
            capture_output=True,
            text=True,
            timeout=20
        )

        if p.stdout:
            print(
                p.stdout.rstrip()
            )

        if p.stderr:
            print()
            print(
                "T71 STDERR:"
            )
            print(
                p.stderr.rstrip()
            )

    except subprocess.TimeoutExpired:

        print(
            "⚠️ T71 snapshot timed out."
        )

    except Exception as exc:

        print(
            f"⚠️ T71 snapshot failed: {exc}"
        )


# ============================================================
# CLEAN EXIT
# ============================================================

running = True


def stop_handler(sig, frame):

    global running

    running = False

    print()
    print()
    print(
        "T71.1 monitor stopped."
    )


signal.signal(
    signal.SIGINT,
    stop_handler
)

signal.signal(
    signal.SIGTERM,
    stop_handler
)


# ============================================================
# PRE-FLIGHT
# ============================================================

if not os.path.exists(MONITOR):

    print(
        f"❌ Missing {MONITOR}"
    )

    sys.exit(1)


if not os.path.exists(DB):

    print(
        f"❌ Missing {DB}"
    )

    sys.exit(1)


# ============================================================
# LIVE LOOP
# ============================================================

previous_tokens = None

while running:

    clear()

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    n = get_t59_tokens()

    print("=" * 150)
    print(
        "MEMECOIN LAB — T71.1 LIVE PROSPECTIVE RESEARCH MONITOR"
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
        "CTRL+C           : STOP MONITOR ONLY"
    )

    print()

    print(
        f"T59 TOKENS       : "
        f"{n if n is not None else 'NA'}"
    )

    if n is not None and n < CHECKPOINT_2:

        print(
            "TO FIRST AUDIT   : "
            f"{max(0, CHECKPOINT_2-n)} tokens"
        )

        print(
            "30 PROGRESS      : "
            f"{progress_bar(n, CHECKPOINT_2)} "
            f"{min(100, n/CHECKPOINT_2*100):.1f}%"
        )

    elif n is not None:

        print(
            "30 PROGRESS      : "
            f"{progress_bar(n, CHECKPOINT_2)} 100.0%"
        )

    if n is not None:

        print(
            "50 PROGRESS      : "
            f"{progress_bar(n, CHECKPOINT_3)} "
            f"{min(100, n/CHECKPOINT_3*100):.1f}%"
        )

    print()
    print(
        f"RESEARCH GATE    : {gate(n)}"
    )

    # Terminal bell only when a checkpoint
    # is crossed for the first time during
    # this monitor session.
    if (
        n is not None
        and previous_tokens is not None
    ):

        crossed_30 = (
            previous_tokens < CHECKPOINT_2
            <= n
        )

        crossed_50 = (
            previous_tokens < CHECKPOINT_3
            <= n
        )

        if crossed_30 or crossed_50:
            print("\a", end="")

    checkpoint_banner(n)

    print()
    print(
        "DETAILED T71 SNAPSHOT"
    )
    print("-" * 150)

    run_t71()

    print()
    print("-" * 150)

    if n is not None:

        if n < CHECKPOINT_2:

            print(
                f"NEXT ACTION: collect "
                f"{CHECKPOINT_2-n} more unique "
                f"T59 tokens."
            )

        elif n < CHECKPOINT_3:

            print(
                "NEXT ACTION: T59 30-token "
                "prospective audit is ready."
            )

        else:

            print(
                "NEXT ACTION: T59 confirmation "
                "audit is ready."
            )

    else:

        print(
            "NEXT ACTION: inspect T59 storage; "
            "do not guess progress."
        )

    print(
        f"Next refresh in {REFRESH_SECONDS}s — "
        "Ctrl+C stops T71.1 only."
    )

    previous_tokens = n

    for _ in range(
        REFRESH_SECONDS * 10
    ):

        if not running:
            break

        time.sleep(0.1)
