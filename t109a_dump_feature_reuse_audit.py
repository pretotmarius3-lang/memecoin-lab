#!/usr/bin/env python3

import sqlite3
import os

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# HELPERS
# ============================================================

def table_exists(name):

    return db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table'
          AND name=?
    """, (name,)).fetchone() is not None


def columns(name):

    if not table_exists(name):
        return set()

    return {
        r["name"]
        for r in db.execute(
            f"PRAGMA table_info({name})"
        ).fetchall()
    }


def count_window(table, mint_col, time_col, mint, start, end, extra=""):

    try:
        return db.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {mint_col}=?
              AND {time_col} >= ?
              AND {time_col} < ?
              {extra}
        """, (
            mint,
            start,
            end
        )).fetchone()[0]

    except Exception:
        return None


# ============================================================
# REQUIRED SOURCES
# ============================================================

if not table_exists("t108_dump_events"):

    raise RuntimeError(
        "t108_dump_events absent. "
        "T108 doit être lancé avant T109A."
    )


dex_ok = table_exists("dex_prices")
swaps_ok = table_exists("swaps")
holders_ok = table_exists(
    "t101_migrated_holder_snapshots"
)

t107_ok = table_exists(
    "t107_targeted_pumpswap_signatures"
)


# ============================================================
# DISCOVER OTHER REUSABLE TABLES
# ============================================================

all_tables = [
    r["name"]
    for r in db.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name
    """).fetchall()
]

candidate_tables = []

for table in all_tables:

    if table.startswith("sqlite_"):
        continue

    c = columns(table)

    has_mint = (
        "token_mint" in c
        or "mint" in c
    )

    has_time = any(
        x in c
        for x in (
            "timestamp",
            "created_at",
            "detected_at",
            "received_at",
            "checked_at",
            "block_time",
            "crash_timestamp",
            "trigger_timestamp",
        )
    )

    if has_mint and has_time:

        candidate_tables.append(
            (
                table,
                len(c)
            )
        )


# ============================================================
# DUMPS
# ============================================================

dumps = db.execute("""
    SELECT *
    FROM t108_dump_events
    ORDER BY trigger_timestamp DESC
""").fetchall()


print()
print("=" * 170)
print("MEMECOIN LAB — T109A DUMP FEATURE REUSE AUDIT")
print("=" * 170)

print(f"DUMP EVENTS          : {len(dumps)}")
print(f"DEX_PRICES           : {'YES' if dex_ok else 'NO'}")
print(f"SWAPS                : {'YES' if swaps_ok else 'NO'}")
print(f"HOLDER SNAPSHOTS     : {'YES' if holders_ok else 'NO'}")
print(f"T107 TARGETED FLOW   : {'YES' if t107_ok else 'NO'}")
print(f"REUSABLE TABLES SEEN : {len(candidate_tables)}")

print()
print("=" * 170)
print("KNOWN SOURCE COVERAGE AROUND EACH DUMP")
print("=" * 170)


if not dumps:

    print()
    print("No T108 dump event yet.")
    print("T109A is ready; let T108 collect prospective dumps.")

else:

    for d in dumps:

        mint = d["token_mint"]
        t = d["trigger_timestamp"]

        dex_pre60 = None
        dex_post60 = None
        dex_post300 = None

        swaps_pre60 = None
        swaps_post30 = None
        swaps_post300 = None

        holders_pre = None
        holders_post = None

        t107_pre60 = None
        t107_post30 = None


        # ----------------------------------------------------
        # DEX
        # ----------------------------------------------------

        if dex_ok:

            dex_pre60 = count_window(
                "dex_prices",
                "token_mint",
                "timestamp",
                mint,
                t - 60,
                t
            )

            dex_post60 = count_window(
                "dex_prices",
                "token_mint",
                "timestamp",
                mint,
                t,
                t + 60
            )

            dex_post300 = count_window(
                "dex_prices",
                "token_mint",
                "timestamp",
                mint,
                t,
                t + 300
            )


        # ----------------------------------------------------
        # PARSED PUMPSWAP FLOW
        # ----------------------------------------------------

        if swaps_ok:

            swaps_pre60 = count_window(
                "swaps",
                "token_mint",
                "timestamp",
                mint,
                t - 60,
                t,
                "AND program='PUMPSWAP'"
            )

            swaps_post30 = count_window(
                "swaps",
                "token_mint",
                "timestamp",
                mint,
                t,
                t + 30,
                "AND program='PUMPSWAP'"
            )

            swaps_post300 = count_window(
                "swaps",
                "token_mint",
                "timestamp",
                mint,
                t,
                t + 300,
                "AND program='PUMPSWAP'"
            )


        # ----------------------------------------------------
        # HOLDERS
        # ----------------------------------------------------

        if holders_ok:

            hc = columns(
                "t101_migrated_holder_snapshots"
            )

            if (
                "token_mint" in hc
                and "checked_at" in hc
            ):

                holders_pre = count_window(
                    "t101_migrated_holder_snapshots",
                    "token_mint",
                    "checked_at",
                    mint,
                    t - 300,
                    t
                )

                holders_post = count_window(
                    "t101_migrated_holder_snapshots",
                    "token_mint",
                    "checked_at",
                    mint,
                    t,
                    t + 300
                )


        # ----------------------------------------------------
        # RAW TARGETED T107 COVERAGE
        # ----------------------------------------------------

        if t107_ok:

            t107_pre60 = count_window(
                "t107_targeted_pumpswap_signatures",
                "token_mint",
                "received_at",
                mint,
                t - 60,
                t
            )

            t107_post30 = count_window(
                "t107_targeted_pumpswap_signatures",
                "token_mint",
                "received_at",
                mint,
                t,
                t + 30
            )


        def v(x):
            return "NA" if x is None else str(x)


        print()
        print(
            f"{mint[:22]:22} "
            f"| DUMP=-{d['dump_level']:2d}%"
        )

        print(
            f"   DEX      "
            f"| pre60={v(dex_pre60):>5} "
            f"| post60={v(dex_post60):>5} "
            f"| post300={v(dex_post300):>5}"
        )

        print(
            f"   FLOW     "
            f"| pre60={v(swaps_pre60):>5} "
            f"| post30={v(swaps_post30):>5} "
            f"| post300={v(swaps_post300):>5}"
        )

        print(
            f"   T107 RAW "
            f"| pre60={v(t107_pre60):>5} "
            f"| post30={v(t107_post30):>5}"
        )

        print(
            f"   HOLDERS  "
            f"| pre300={v(holders_pre):>5} "
            f"| post300={v(holders_post):>5}"
        )


# ============================================================
# REUSABLE TABLE INVENTORY
# ============================================================

print()
print("=" * 170)
print("DATABASE FEATURE INVENTORY")
print("=" * 170)

for table, ncols in candidate_tables:

    c = columns(table)

    interesting = [
        x
        for x in c
        if any(
            key in x.lower()
            for key in (
                "wallet",
                "holder",
                "buy",
                "sell",
                "volume",
                "liquid",
                "market",
                "price",
                "flow",
                "concentr",
                "accel",
                "return",
                "drawdown",
                "narrative",
                "sequence",
                "sol",
            )
        )
    ]

    preview = ", ".join(
        interesting[:12]
    )

    if len(interesting) > 12:
        preview += ", ..."

    print(
        f"{table:<42} "
        f"| cols={ncols:<3} "
        f"| {preview}"
    )


print()
print("=" * 170)
print("INTERPRETATION")
print("=" * 170)

print("""
T109A does NOT fit a model and does NOT select thresholds.

Its only job is to answer:

1. Which raw sources existed around each T108 dump?
2. Can PRE-DUMP features be reconstructed without future leakage?
3. Which old feature families already exist in the database?
4. Which features genuinely require new prospective collection?

NEXT:
When enough T108 dumps exist, build T109B only from feature
families whose temporal coverage is valid.
""")

db.close()
