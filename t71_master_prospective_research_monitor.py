#!/usr/bin/env python3

import os
import re
import sys
import sqlite3
import subprocess
from collections import defaultdict

DB = "validation_v090.db"

CHECKPOINT_1 = 15
CHECKPOINT_2 = 30
CHECKPOINT_3 = 50

MODULES = {
    "T23": ["t23", "v2_frozen_prospective"],
    "T31": ["t31", "frozen_base_prospective"],
    "T32": ["t32", "prospective_shadow"],
    "T47": ["t47", "fastflip_prospective"],
    "T59": ["t59", "capv2", "cap_v2"],
}

EXPECTED_PROCESSES = {
    "CORE EVENT TRACKER": "event_tracker_v090.py",
    "CORE PRICE TRACKER": "price_tracker_v100.py",
    "SEQUENCE ENGINE": "event_sequence_v340.py",
    "REGIME V6.2": "frozen_regime_forward_v620.py",
    "T23": "v2_frozen_prospective_t23.py",
    "T31": "t31_frozen_base_prospective_execution.py",
    "T32": "t32_prospective_shadow_recorder.py",
    "T47": "t47_fastflip_prospective_shadow.py",
    "T59": "t59_capv2_prospective_shadow.py",
}


# ============================================================
# HELPERS
# ============================================================

def section(title):
    print()
    print("=" * 150)
    print(title)
    print("=" * 150)


def table_columns(db, table):
    try:
        return [
            r[1]
            for r in db.execute(
                f'PRAGMA table_info("{table}")'
            ).fetchall()
        ]
    except Exception:
        return []


def all_tables(db):
    return [
        r[0]
        for r in db.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        """).fetchall()
    ]


def find_tables(tables, patterns):
    out = []

    for t in tables:
        tl = t.lower()

        if any(
            p.lower() in tl
            for p in patterns
        ):
            out.append(t)

    return out


def safe_count(db, table):
    try:
        return db.execute(
            f'SELECT COUNT(*) FROM "{table}"'
        ).fetchone()[0]
    except Exception:
        return None


def count_distinct(db, table, column):
    try:
        return db.execute(
            f'''
            SELECT COUNT(DISTINCT "{column}")
            FROM "{table}"
            WHERE "{column}" IS NOT NULL
            '''
        ).fetchone()[0]
    except Exception:
        return None


def first_existing(columns, candidates):
    low = {
        c.lower(): c
        for c in columns
    }

    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]

    return None


def process_snapshot():
    try:
        return subprocess.check_output(
            ["ps", "aux"],
            text=True,
            stderr=subprocess.DEVNULL
        )
    except Exception:
        return ""


def process_running(snapshot, script):
    return script in snapshot


def checkpoint_status(n):
    if n is None:
        return "UNKNOWN"

    if n < CHECKPOINT_1:
        return f"COLLECT — {n}/{CHECKPOINT_1}"

    if n < CHECKPOINT_2:
        return (
            f"15 REACHED — KEEP COLLECTING "
            f"({n}/{CHECKPOINT_2})"
        )

    if n < CHECKPOINT_3:
        return (
            f"30 REACHED — AUDIT READY "
            f"({n}/{CHECKPOINT_3})"
        )

    return (
        f"50+ REACHED — CONFIRMATION AUDIT READY "
        f"({n})"
    )


# ============================================================
# READ-ONLY DB
# ============================================================

if not os.path.exists(DB):
    print(f"❌ Missing DB: {DB}")
    sys.exit(1)

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=10
)

tables = all_tables(db)


# ============================================================
# GLOBAL
# ============================================================

section("MEMECOIN LAB — T71 MASTER PROSPECTIVE RESEARCH MONITOR")

print("MODE             : READ-ONLY")
print("MODEL REFITTING  : DISABLED")
print("THRESHOLD TUNING : DISABLED")
print("DB WRITES        : DISABLED")
print()
print(
    "CHECKPOINTS      : "
    "15 = observation | "
    "30 = first decision audit | "
    "50+ = confirmation"
)


# ============================================================
# CORE DATABASE
# ============================================================

section("A) CORE DATABASE")

if "events" in tables:

    cols = table_columns(
        db,
        "events"
    )

    total = safe_count(
        db,
        "events"
    )

    token_col = first_existing(
        cols,
        [
            "token_mint",
            "mint",
            "token"
        ]
    )

    if token_col:
        tokens = count_distinct(
            db,
            "events",
            token_col
        )
    else:
        tokens = None

    print(
        f"EVENTS = {total if total is not None else 'NA'}"
    )

    print(
        f"TOKENS = {tokens if tokens is not None else 'NA'}"
    )

else:
    print("events table not found")


# ============================================================
# MODULE DISCOVERY
# ============================================================

section("B) PROSPECTIVE MODULE STORAGE")

module_info = {}

for module, patterns in MODULES.items():

    matches = find_tables(
        tables,
        patterns
    )

    module_info[module] = []

    print()
    print(module)

    if not matches:
        print("  TABLES : none detected")
        continue

    for table in matches:

        cols = table_columns(
            db,
            table
        )

        rows = safe_count(
            db,
            table
        )

        token_col = first_existing(
            cols,
            [
                "token_mint",
                "mint",
                "token",
                "token_address"
            ]
        )

        label_col = first_existing(
            cols,
            [
                "label",
                "y",
                "outcome",
                "class",
                "binary_label"
            ]
        )

        return_col = first_existing(
            cols,
            [
                "dex_return_60s",
                "return_60s",
                "r60",
                "ret60"
            ]
        )

        tokens = (
            count_distinct(
                db,
                table,
                token_col
            )
            if token_col
            else None
        )

        info = {
            "table": table,
            "rows": rows,
            "tokens": tokens,
            "token_col": token_col,
            "label_col": label_col,
            "return_col": return_col,
            "columns": cols,
        }

        module_info[
            module
        ].append(info)

        print(
            f"  {table}"
        )

        print(
            f"    ROWS   = "
            f"{rows if rows is not None else 'NA'}"
        )

        print(
            f"    TOKENS = "
            f"{tokens if tokens is not None else 'NA'}"
        )

        print(
            f"    TOKEN  = "
            f"{token_col or 'not detected'}"
        )

        print(
            f"    LABEL  = "
            f"{label_col or 'not detected'}"
        )

        print(
            f"    R60    = "
            f"{return_col or 'not detected'}"
        )


# ============================================================
# T59 PRIMARY STATUS
# ============================================================

section("C) T59 CAP-v2 — PRIMARY FORWARD STATUS")

t59_tables = module_info.get(
    "T59",
    []
)

best_t59 = None

if t59_tables:

    ranked = sorted(
        t59_tables,
        key=lambda x: (
            x["tokens"] or 0,
            x["rows"] or 0
        ),
        reverse=True
    )

    best_t59 = ranked[0]

    n_tokens = best_t59[
        "tokens"
    ]

    print(
        f"TABLE       : {best_t59['table']}"
    )

    print(
        f"ROWS        : {best_t59['rows']}"
    )

    print(
        f"TOKENS      : "
        f"{n_tokens if n_tokens is not None else 'NA'}"
    )

    print(
        f"STATUS      : {checkpoint_status(n_tokens)}"
    )

else:

    print(
        "No T59/CAP-v2 table automatically detected."
    )

    print(
        "This does NOT mean T59 is stopped; "
        "its storage may use a generic table name."
    )


# ============================================================
# T59 LABEL / PREDICTION INSPECTION
# ============================================================

section("D) T59 AVAILABLE FORWARD INFORMATION")

if best_t59:

    table = best_t59[
        "table"
    ]

    cols = best_t59[
        "columns"
    ]

    label_col = best_t59[
        "label_col"
    ]

    return_col = best_t59[
        "return_col"
    ]

    if label_col:

        try:
            dist = db.execute(
                f'''
                SELECT "{label_col}", COUNT(*)
                FROM "{table}"
                WHERE "{label_col}" IS NOT NULL
                GROUP BY "{label_col}"
                ORDER BY "{label_col}"
                '''
            ).fetchall()

            print(
                f"LABEL COLUMN : {label_col}"
            )

            for value, n in dist:
                print(
                    f"  {value} -> {n}"
                )

        except Exception as exc:
            print(
                f"Label inspection unavailable: {exc}"
            )

    elif return_col:

        try:
            vals = db.execute(
                f'''
                SELECT "{return_col}"
                FROM "{table}"
                WHERE "{return_col}" IS NOT NULL
                '''
            ).fetchall()

            rr = [
                x[0]
                for x in vals
                if x[0] is not None
            ]

            runs = sum(
                x >= 10
                for x in rr
            )

            dumps = sum(
                x <= -10
                for x in rr
            )

            unresolved = len(rr) - runs - dumps

            print(
                f"R60 COLUMN   : {return_col}"
            )

            print(
                f"RUN >= +10%  : {runs}"
            )

            print(
                f"DUMP <= -10% : {dumps}"
            )

            print(
                f"OTHER        : {unresolved}"
            )

        except Exception as exc:
            print(
                f"R60 inspection unavailable: {exc}"
            )

    else:

        print(
            "No explicit label/R60 column detected."
        )

        print(
            "Forward rows are counted without "
            "inventing outcome semantics."
        )

else:
    print("T59 storage unresolved.")


# ============================================================
# FIRST EVENT / TOKEN
# ============================================================

section("E) T59 FIRST-EVENT/TOKEN COVERAGE")

if best_t59 and best_t59["token_col"]:

    table = best_t59[
        "table"
    ]

    token_col = best_t59[
        "token_col"
    ]

    cols = best_t59[
        "columns"
    ]

    time_col = first_existing(
        cols,
        [
            "timestamp",
            "created_at",
            "event_timestamp",
            "ts"
        ]
    )

    id_col = first_existing(
        cols,
        [
            "event_id",
            "id"
        ]
    )

    if time_col:
        order_expr = (
            f'"{time_col}"'
            + (
                f', "{id_col}"'
                if id_col
                else ""
            )
        )

        try:
            n = db.execute(
                f'''
                SELECT COUNT(*)
                FROM (
                    SELECT
                        "{token_col}",
                        ROW_NUMBER() OVER (
                            PARTITION BY "{token_col}"
                            ORDER BY {order_expr}
                        ) AS rn
                    FROM "{table}"
                    WHERE "{token_col}" IS NOT NULL
                )
                WHERE rn=1
                '''
            ).fetchone()[0]

            print(
                f"FIRST EVENTS = {n}"
            )

        except Exception as exc:
            print(
                f"First-event calculation unavailable: {exc}"
            )

    else:
        print(
            "No timestamp column detected; "
            "unique-token count used as coverage proxy."
        )

        print(
            f"UNIQUE TOKENS = {best_t59['tokens']}"
        )

else:
    print(
        "First-event/token coverage unavailable."
    )


# ============================================================
# PROCESSES
# ============================================================

section("F) PROCESS / TERMINAL STATUS")

snapshot = process_snapshot()

running_count = 0

for name, script in EXPECTED_PROCESSES.items():

    running = process_running(
        snapshot,
        script
    )

    if running:
        running_count += 1

    print(
        f"{name:24} | "
        f"{'✅ RUNNING' if running else '⚪ NOT DETECTED':15} "
        f"| {script}"
    )


# ============================================================
# RESEARCH GATE
# ============================================================

section("G) RESEARCH GATE")

if best_t59 and best_t59["tokens"] is not None:

    n = best_t59[
        "tokens"
    ]

    if n < 15:

        print("STATUS : COLLECT")
        print(
            f"T59 has {n}/15 prospective tokens."
        )
        print(
            "ACTION : do nothing. Keep frozen."
        )

    elif n < 30:

        print("STATUS : OBSERVATION CHECKPOINT REACHED")
        print(
            f"T59 has {n}/30 prospective tokens."
        )
        print(
            "ACTION : inspect only. "
            "No promotion/rejection/refit."
        )

    elif n < 50:

        print("STATUS : FIRST DECISION AUDIT READY")
        print(
            f"T59 has {n} prospective tokens."
        )
        print(
            "ACTION : run frozen prospective audit."
        )

    else:

        print("STATUS : CONFIRMATION AUDIT READY")
        print(
            f"T59 has {n} prospective tokens."
        )
        print(
            "ACTION : evaluate promotion using "
            "the pre-declared frozen criteria."
        )

else:

    print("STATUS : STORAGE DISCOVERY INCOMPLETE")
    print(
        "ACTION : do not infer T59 progress."
    )


# ============================================================
# SAFETY / FREEZE
# ============================================================

section("H) FREEZE INTEGRITY")

print("T59 MODEL              : UNCHANGED")
print("CAP-v2 FEATURE         : early_div")
print(
    "early_div             : "
    "early_price_return - early_net_sol"
)
print("HISTORICAL DISCOVERY   : PAUSED")
print("THRESHOLD OPTIMIZATION : FORBIDDEN")
print("MODEL REFIT            : FORBIDDEN")
print("DATABASE WRITE         : NONE")
print()
print(
    "T71 is an observer only."
)

db.close()
