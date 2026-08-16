#!/usr/bin/env python3

import os
import json
import time
import sqlite3
import urllib.request
import urllib.error

DB = "validation_v090.db"
TABLE = "t74_token_metadata"

REFRESH_SEC = 15

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")

if not HELIUS_API_KEY:
    raise RuntimeError(
        "Missing HELIUS_API_KEY environment variable."
    )

HELIUS_URL = (
    "https://mainnet.helius-rpc.com/"
    f"?api-key={HELIUS_API_KEY}"
)


# ============================================================
# HELPERS
# ============================================================

def now():
    return time.time()


def clean_text(x):
    if x is None:
        return None

    x = str(x).replace("\x00", "").strip()

    return x or None


def helius_get_asset(mint):
    """
    Resolve one Solana mint through Helius DAS getAsset.
    """

    payload = {
        "jsonrpc": "2.0",
        "id": mint,
        "method": "getAsset",
        "params": {
            "id": mint
        }
    }

    req = urllib.request.Request(
        HELIUS_URL,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(
            req,
            timeout=15
        ) as response:

            body = json.loads(
                response.read().decode()
            )

    except urllib.error.HTTPError as exc:

        return {
            "ok": False,
            "error": f"HTTP_{exc.code}"
        }

    except Exception as exc:

        return {
            "ok": False,
            "error": type(exc).__name__
        }


    if body.get("error"):

        return {
            "ok": False,
            "error": json.dumps(
                body["error"],
                ensure_ascii=False
            )[:500]
        }


    asset = body.get("result")

    if not asset:

        return {
            "ok": False,
            "error": "NO_RESULT"
        }


    content = (
        asset.get("content")
        or {}
    )

    metadata = (
        content.get("metadata")
        or {}
    )


    # Helius asset responses may expose metadata
    # through content.metadata.
    name = clean_text(
        metadata.get("name")
    )

    symbol = clean_text(
        metadata.get("symbol")
    )

    description = clean_text(
        metadata.get("description")
    )

    json_uri = clean_text(
        content.get("json_uri")
    )


    token_info = (
        asset.get("token_info")
        or {}
    )


    return {
        "ok": True,

        "name":
            name,

        "symbol":
            symbol,

        "uri":
            json_uri,

        "description":
            description,

        "interface":
            clean_text(
                asset.get("interface")
            ),

        "decimals":
            token_info.get("decimals"),

        "supply":
            str(token_info.get("supply")) if token_info.get("supply") is not None else None,

        "raw_json":
            json.dumps(
                asset,
                ensure_ascii=False
            )
    }


# ============================================================
# DATABASE
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

    token_mint TEXT PRIMARY KEY,

    name TEXT,
    symbol TEXT,
    uri TEXT,
    description TEXT,

    interface TEXT,
    decimals INTEGER,
    supply TEXT,

    source TEXT NOT NULL,

    first_seen_event_id INTEGER,
    first_seen_at REAL,

    captured_at REAL,
    last_attempt_at REAL,

    status TEXT NOT NULL,
    error TEXT,

    raw_json TEXT
)
""")


db.commit()


# ============================================================
# DISCOVER NEW MINTS
# ============================================================

def unresolved_mints():

    return db.execute(f"""
    SELECT
        e.token_mint,
        MIN(e.id) AS first_event_id,
        MIN(e.timestamp) AS first_seen_at

    FROM events e

    LEFT JOIN {TABLE} m
        ON m.token_mint=e.token_mint

    WHERE
        e.token_mint IS NOT NULL
        AND m.token_mint IS NULL

    GROUP BY
        e.token_mint

    ORDER BY
        MIN(e.id)
    """).fetchall()


# ============================================================
# STORE
# ============================================================

def store_success(
    mint,
    first_event_id,
    first_seen_at,
    result
):

    db.execute(f"""
    INSERT OR IGNORE INTO {TABLE} (

        token_mint,

        name,
        symbol,
        uri,
        description,

        interface,
        decimals,
        supply,

        source,

        first_seen_event_id,
        first_seen_at,

        captured_at,
        last_attempt_at,

        status,
        error,

        raw_json
    )

    VALUES (
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?
    )
    """, (

        mint,

        result.get("name"),
        result.get("symbol"),
        result.get("uri"),
        result.get("description"),

        result.get("interface"),
        result.get("decimals"),
        result.get("supply"),

        "helius_das_getAsset",

        first_event_id,
        first_seen_at,

        now(),
        now(),

        "OK",
        None,

        result.get("raw_json")
    ))

    db.commit()


def store_failure(
    mint,
    first_event_id,
    first_seen_at,
    error
):

    db.execute(f"""
    INSERT OR IGNORE INTO {TABLE} (

        token_mint,

        source,

        first_seen_event_id,
        first_seen_at,

        captured_at,
        last_attempt_at,

        status,
        error
    )

    VALUES (
        ?,
        ?,
        ?, ?,
        ?, ?,
        ?, ?
    )
    """, (

        mint,

        "helius_das_getAsset",

        first_event_id,
        first_seen_at,

        now(),
        now(),

        "ERROR",
        error
    ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    total = db.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        """
    ).fetchone()[0]


    ok = db.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE status='OK'
        """
    ).fetchone()[0]


    errors = db.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE status='ERROR'
        """
    ).fetchone()[0]


    names = db.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE
            status='OK'
            AND name IS NOT NULL
        """
    ).fetchone()[0]


    symbols = db.execute(
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE
            status='OK'
            AND symbol IS NOT NULL
        """
    ).fetchone()[0]


    latest = db.execute(
        f"""
        SELECT
            token_mint,
            name,
            symbol,
            status,
            error

        FROM {TABLE}

        ORDER BY
            captured_at DESC

        LIMIT 12
        """
    ).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 150)

    print(
        "MEMECOIN LAB — T74A TOKEN METADATA RECORDER"
    )

    print("=" * 150)

    print(
        f"RESOLVED ROWS   : {total}"
    )

    print(
        f"OK              : {ok}"
    )

    print(
        f"ERROR           : {errors}"
    )

    print(
        f"WITH NAME       : {names}"
    )

    print(
        f"WITH SYMBOL     : {symbols}"
    )

    print()

    print(
        "SOURCE          : HELIUS DAS getAsset"
    )

    print(
        "MODE            : METADATA COLLECTION ONLY"
    )

    print(
        "T59             : UNTOUCHED"
    )


    print()
    print("=" * 150)
    print("LATEST")
    print("=" * 150)


    for r in latest:

        name = (
            r["name"]
            or "-"
        )

        symbol = (
            r["symbol"]
            or "-"
        )

        extra = (
            ""
            if r["status"] == "OK"
            else f" | {r['error']}"
        )


        print(
            f"{r['token_mint'][:24]:24} "
            f"| {symbol[:12]:12} "
            f"| {name[:36]:36} "
            f"| {r['status']}{extra}"
        )


    print()
    print(
        f"Refresh every {REFRESH_SEC}s."
    )

    print(
        "CTRL+C stops T74A only."
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        pending = unresolved_mints()


        for r in pending:

            mint = r[
                "token_mint"
            ]

            result = helius_get_asset(
                mint
            )


            if result.get(
                "ok"
            ):

                store_success(
                    mint,
                    r["first_event_id"],
                    r["first_seen_at"],
                    result
                )

            else:

                store_failure(
                    mint,
                    r["first_event_id"],
                    r["first_seen_at"],
                    result.get(
                        "error",
                        "UNKNOWN"
                    )
                )


            # gentle rate limiting
            time.sleep(
                0.15
            )


        show()

        time.sleep(
            REFRESH_SEC
        )


except KeyboardInterrupt:

    print()
    print(
        "T74A stopped safely."
    )


finally:

    db.close()
