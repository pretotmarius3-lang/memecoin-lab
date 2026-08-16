#!/usr/bin/env python3

import json
import os
import sqlite3
import time
import urllib.request
from dotenv import load_dotenv

load_dotenv(".env")

DB = "validation_v090.db"

SOURCE_TABLE = "t101_migrations"
TABLE = "t101_migrated_holder_snapshots"

REFRESH = 60
RECHECK_SECONDS = 300

RPC_URL = (
    os.getenv("SOLANA_RPC_URL")
    or os.getenv("SOLANA_HTTP_URL")
    or os.getenv("HELIUS_RPC_URL")
    or os.getenv("RPC_URL")
)

WS_URL = os.getenv("SOLANA_WS_URL")

if not RPC_URL and WS_URL:
    if WS_URL.startswith("wss://"):
        RPC_URL = "https://" + WS_URL[len("wss://"):]
    elif WS_URL.startswith("ws://"):
        RPC_URL = "http://" + WS_URL[len("ws://"):]

if not RPC_URL:
    raise RuntimeError("Aucune URL RPC HTTP trouvée dans .env")


# ============================================================
# RPC
# ============================================================

def rpc_call(method, params):

    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": "t101c",
        "method": method,
        "params": params,
    }).encode()

    req = urllib.request.Request(
        RPC_URL,
        data=payload,
        headers={
            "Content-Type": "application/json"
        },
        method="POST",
    )

    with urllib.request.urlopen(
        req,
        timeout=30
    ) as response:
        data = json.loads(
            response.read()
        )

    if "error" in data:
        raise RuntimeError(
            f"{method}: {data['error']}"
        )

    return data.get("result")


# ============================================================
# HOLDER COUNT
# ============================================================

def get_holder_count(mint):

    cursor = None
    owners = set()
    accounts_positive = 0
    pages = 0
    indexed_slot = None

    while True:

        params = {
            "mint": mint,
            "limit": 1000,
            "options": {
                "showZeroBalance": False
            }
        }

        if cursor:
            params["cursor"] = cursor

        result = rpc_call(
            "getTokenAccounts",
            params
        )

        if not result:
            break

        pages += 1

        if indexed_slot is None:
            indexed_slot = result.get(
                "last_indexed_slot"
            )

        accounts = result.get(
            "token_accounts",
            []
        )

        for acc in accounts:

            amount = acc.get(
                "amount"
            )

            owner = acc.get(
                "owner"
            )

            try:
                positive = (
                    amount is not None
                    and float(amount) > 0
                )
            except Exception:
                positive = False

            if not positive:
                continue

            accounts_positive += 1

            if owner:
                owners.add(owner)

        cursor = result.get(
            "cursor"
        )

        if not cursor:
            break

        # Safety against accidental runaway pagination.
        if pages >= 100:
            raise RuntimeError(
                "Pagination exceeded 100 pages"
            )

    return {
        "holders": len(owners),
        "positive_accounts": accounts_positive,
        "pages": pages,
        "indexed_slot": indexed_slot,
    }


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute(
    "PRAGMA busy_timeout=5000"
)

db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    token_mint TEXT NOT NULL,
    migration_signature TEXT NOT NULL,

    migrated_at REAL,
    checked_at REAL NOT NULL,

    holder_count INTEGER,
    positive_token_accounts INTEGER,

    pages INTEGER,
    indexed_slot INTEGER,

    status TEXT NOT NULL,
    error TEXT,

    UNIQUE(
        token_mint,
        checked_at
    )
)
""")

db.execute(f"""
CREATE INDEX IF NOT EXISTS
idx_t101c_mint_checked
ON {TABLE}(
    token_mint,
    checked_at
)
""")

db.commit()


# ============================================================
# SELECT MIGRATED TOKENS
# ============================================================

def migrated_tokens():

    return db.execute(f"""
    SELECT
        signature,
        token_mint,
        block_time

    FROM {SOURCE_TABLE}

    WHERE
        status='OK'
        AND confirmed=1
        AND migrate_v2=1
        AND create_pool=1
        AND token_mint IS NOT NULL

    ORDER BY
        COALESCE(block_time, detected_at),
        signature
    """).fetchall()


def due(token):

    row = db.execute(f"""
    SELECT checked_at
    FROM {TABLE}
    WHERE
        token_mint=?
        AND status='OK'
    ORDER BY checked_at DESC
    LIMIT 1
    """, (
        token,
    )).fetchone()

    if row is None:
        return True

    return (
        time.time()
        - row["checked_at"]
        >= RECHECK_SECONDS
    )


# ============================================================
# STORE
# ============================================================

def store_ok(
    migration,
    info,
):

    db.execute(f"""
    INSERT INTO {TABLE} (

        token_mint,
        migration_signature,

        migrated_at,
        checked_at,

        holder_count,
        positive_token_accounts,

        pages,
        indexed_slot,

        status,
        error
    )

    VALUES (
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        'OK',
        NULL
    )
    """, (

        migration["token_mint"],
        migration["signature"],

        migration["block_time"],
        time.time(),

        info["holders"],
        info["positive_accounts"],

        info["pages"],
        info["indexed_slot"],
    ))

    db.commit()


def store_error(
    migration,
    error,
):

    db.execute(f"""
    INSERT INTO {TABLE} (

        token_mint,
        migration_signature,

        migrated_at,
        checked_at,

        status,
        error
    )

    VALUES (
        ?, ?,
        ?, ?,
        'ERROR',
        ?
    )
    """, (

        migration["token_mint"],
        migration["signature"],

        migration["block_time"],
        time.time(),

        str(error),
    ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    migrations = migrated_tokens()

    latest = db.execute(f"""
    SELECT h.*

    FROM {TABLE} h

    JOIN (
        SELECT
            token_mint,
            MAX(checked_at) AS mx
        FROM {TABLE}
        WHERE status='OK'
        GROUP BY token_mint
    ) z

    ON
        z.token_mint=h.token_mint
        AND z.mx=h.checked_at

    ORDER BY
        h.checked_at DESC
    """).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )

    print("=" * 145)
    print(
        "MEMECOIN LAB — T101C MIGRATED TOKEN HOLDER RECORDER"
    )
    print("=" * 145)

    print(
        f"MIGRATED TOKENS    : {len(migrations)}"
    )

    print(
        f"HOLDER SNAPSHOTS   : "
        f"{db.execute(f'SELECT COUNT(*) FROM {TABLE}').fetchone()[0]}"
    )

    print(
        f"TOKENS RESOLVED    : {len(latest)}"
    )

    print()

    if latest:

        counts = [
            r["holder_count"]
            for r in latest
            if r["holder_count"] is not None
        ]

        print(
            f">= 20 HOLDERS      : "
            f"{sum(x >= 20 for x in counts)}"
        )

        print(
            f">= 50 HOLDERS      : "
            f"{sum(x >= 50 for x in counts)}"
        )

        print(
            f">= 100 HOLDERS     : "
            f"{sum(x >= 100 for x in counts)}"
        )

        print(
            f">= 200 HOLDERS     : "
            f"{sum(x >= 200 for x in counts)}"
        )

    print()
    print("=" * 145)
    print("LATEST")
    print("=" * 145)

    for r in latest[:20]:

        print(
            f"{r['token_mint'][:28]:28} "
            f"| HOLDERS={r['holder_count']:6d} "
            f"| ACCOUNTS={r['positive_token_accounts']:6d} "
            f"| PAGES={r['pages']:3d} "
            f"| SLOT={r['indexed_slot']}"
        )

    print()
    print(
        "FILTER >=50       : NOT ACTIVE"
    )

    print(
        "PURPOSE           : COLLECTION / DISCOVERY ONLY"
    )

    print(
        f"RECHECK           : every ~{RECHECK_SECONDS}s/token"
    )

    print(
        "CTRL+C stops T101C only."
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        migrations = migrated_tokens()

        for migration in migrations:

            mint = migration[
                "token_mint"
            ]

            if not due(mint):
                continue

            try:

                info = get_holder_count(
                    mint
                )

                store_ok(
                    migration,
                    info
                )

            except Exception as exc:

                store_error(
                    migration,
                    exc
                )

        show()

        time.sleep(
            REFRESH
        )

except KeyboardInterrupt:

    print()
    print(
        "T101C stopped safely."
    )

finally:

    db.close()
