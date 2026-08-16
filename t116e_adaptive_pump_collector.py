#!/usr/bin/env python3

import asyncio
import hashlib
import json
import os
import sqlite3
import time

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv(".env")

RPC_URL = os.getenv("SOLANA_RPC_URL")
WS_URL = os.getenv("SOLANA_WS_URL")

if not RPC_URL or not WS_URL:
    raise RuntimeError(
        "SOLANA_RPC_URL / SOLANA_WS_URL absent du .env"
    )

DB = os.path.expanduser(
    "~/memecoin_lab/validation_v090.db"
)

PUMP_PROGRAM = (
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

SOL_MINT = (
    "So11111111111111111111111111111111111111112"
)

EXCLUDED_MINTS = {
    SOL_MINT,
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}

RPC_RPS = 4.0
FETCH_DELAY = 1.0 / RPC_RPS

MAX_RETRIES = 5
MONITOR_SECONDS = 10

# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")

db.execute("""
CREATE TABLE IF NOT EXISTS t116_pump_signatures (

    signature TEXT PRIMARY KEY,

    slot INTEGER,

    received_at REAL NOT NULL,

    attempts INTEGER NOT NULL DEFAULT 0,

    status TEXT NOT NULL DEFAULT 'WAITING',

    error TEXT
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS t116_pump_swaps (

    signature TEXT PRIMARY KEY,

    timestamp REAL NOT NULL,

    slot INTEGER,

    token_mint TEXT NOT NULL,

    wallet TEXT,

    side TEXT NOT NULL,

    token_delta REAL,

    sol_delta REAL,

    raw_price_sol REAL,

    sampled_percent REAL NOT NULL,

    created_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# STATE
# ============================================================

queue = asyncio.Queue()

stats = {
    "ws": 0,
    "sampled": 0,
    "done": 0,
    "not_swap": 0,
    "failed": 0,
    "rpc": 0,
}

CURRENT_SAMPLE = 2


# ============================================================
# ADAPTIVE SAMPLING
# ============================================================

def waiting_status():

    row = db.execute("""
    SELECT
        COUNT(*) AS waiting,

        MIN(received_at) AS oldest_received

    FROM t116_pump_signatures

    WHERE status IN (
        'WAITING',
        'RETRY'
    )
    """).fetchone()

    waiting = (
        row["waiting"]
        or 0
    )

    oldest_age = 0.0

    if row["oldest_received"] is not None:

        oldest_age = (
            time.time()
            - row["oldest_received"]
        )

    return (
        waiting,
        oldest_age
    )


def adaptive_sample_percent():

    waiting, oldest_age = waiting_status()

    # Emergency throttle
    if oldest_age > 120:
        return 1

    if waiting > 150:
        return 2

    if waiting >= 50:
        return 3

    return 5


def take_signature(
    signature,
    sample_percent
):

    h = hashlib.sha256(
        signature.encode()
    ).digest()

    x = int.from_bytes(
        h[:8],
        "big"
    )

    return (
        x % 100
        < sample_percent
    )


# ============================================================
# RPC
# ============================================================

async def rpc_tx(
    session,
    signature
):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,

        "method": "getTransaction",

        "params": [
            signature,
            {
                "encoding":
                    "jsonParsed",

                "commitment":
                    "confirmed",

                "maxSupportedTransactionVersion":
                    0,
            }
        ]
    }

    try:

        async with session.post(
            RPC_URL,
            json=payload,
            timeout=aiohttp.ClientTimeout(
                total=15
            )
        ) as response:

            data = await response.json()

        stats["rpc"] += 1

        if data.get("error"):
            return None

        return data.get("result")

    except Exception:
        return None


# ============================================================
# TX HELPERS
# ============================================================

def account_keys(tx):

    try:
        return (
            tx["transaction"]
            ["message"]
            ["accountKeys"]
        )

    except Exception:
        return []


def pubkey(x):

    if isinstance(x, dict):
        return x.get("pubkey")

    return x


def signer_wallet(tx):

    keys = account_keys(tx)

    for x in keys:

        if (
            isinstance(x, dict)
            and x.get("signer")
        ):

            return x.get(
                "pubkey"
            )

    if keys:
        return pubkey(
            keys[0]
        )

    return None


# ============================================================
# TOKEN BALANCE DELTAS
# ============================================================

def token_balances(
    tx,
    wallet,
    field
):

    out = {}

    try:

        rows = tx["meta"].get(
            field,
            []
        )

    except Exception:
        return out


    for r in rows:

        try:

            owner = r.get(
                "owner"
            )

            if (
                wallet is not None
                and owner is not None
                and owner != wallet
            ):
                continue

            mint = r.get(
                "mint"
            )

            ui = r.get(
                "uiTokenAmount",
                {}
            )

            raw = ui.get(
                "amount"
            )

            decimals = ui.get(
                "decimals",
                0
            )

            if (
                mint is None
                or raw is None
            ):
                continue

            amount = (
                int(raw)
                / (
                    10
                    ** int(decimals)
                )
            )

            out[mint] = (
                out.get(
                    mint,
                    0.0
                )
                + amount
            )

        except Exception:
            continue

    return out


def token_changes(
    tx,
    wallet
):

    pre = token_balances(
        tx,
        wallet,
        "preTokenBalances"
    )

    post = token_balances(
        tx,
        wallet,
        "postTokenBalances"
    )

    mints = (
        set(pre)
        | set(post)
    )

    changes = {}

    for mint in mints:

        delta = (
            post.get(
                mint,
                0.0
            )
            - pre.get(
                mint,
                0.0
            )
        )

        if abs(delta) > 1e-12:

            changes[
                mint
            ] = delta

    return changes


# ============================================================
# SOL DELTA
# ============================================================

def native_sol_delta(
    tx,
    wallet
):

    keys = account_keys(tx)

    index = None

    for i, x in enumerate(keys):

        if pubkey(x) == wallet:

            index = i
            break

    if index is None:
        return 0.0

    try:

        meta = tx["meta"]

        delta = (
            meta["postBalances"][index]
            - meta["preBalances"][index]
        ) / 1_000_000_000

        if index == 0:

            delta += (
                meta.get(
                    "fee",
                    0
                )
                / 1_000_000_000
            )

        return delta

    except Exception:
        return 0.0


# ============================================================
# CLASSIFIER
# ============================================================

def classify(tx):

    wallet = signer_wallet(tx)

    if wallet is None:
        return None

    changes = token_changes(
        tx,
        wallet
    )

    sol_delta = native_sol_delta(
        tx,
        wallet
    )

    wrapped = changes.pop(
        SOL_MINT,
        0.0
    )

    if abs(wrapped) > 1e-12:
        sol_delta += wrapped

    candidates = [
        (
            mint,
            delta
        )

        for mint, delta
        in changes.items()

        if (
            mint not in EXCLUDED_MINTS
            and abs(delta) > 1e-12
        )
    ]

    if not candidates:
        return None

    mint, token_delta = max(
        candidates,
        key=lambda x:
            abs(x[1])
    )

    if (
        token_delta > 0
        and sol_delta < 0
    ):
        side = "BUY"

    elif (
        token_delta < 0
        and sol_delta > 0
    ):
        side = "SELL"

    else:
        return None

    if (
        abs(token_delta) <= 0
        or abs(sol_delta) <= 0
    ):
        return None

    price = (
        abs(sol_delta)
        / abs(token_delta)
    )

    return {
        "wallet":
            wallet,

        "mint":
            mint,

        "side":
            side,

        "token_delta":
            token_delta,

        "sol_delta":
            sol_delta,

        "price":
            price,
    }


# ============================================================
# WORKER
# ============================================================

async def worker(
    session
):

    while True:

        signature = await queue.get()

        row = db.execute("""
        SELECT *
        FROM t116_pump_signatures
        WHERE signature=?
        """, (
            signature,
        )).fetchone()

        if not row:

            queue.task_done()
            continue

        if row["status"] not in (
            "WAITING",
            "RETRY"
        ):

            queue.task_done()
            continue

        await asyncio.sleep(
            FETCH_DELAY
        )

        tx = await rpc_tx(
            session,
            signature
        )

        if tx is None:

            attempts = (
                row["attempts"]
                + 1
            )

            if attempts >= MAX_RETRIES:

                db.execute("""
                UPDATE t116_pump_signatures

                SET
                    status='FAILED',
                    attempts=?,
                    error='RPC_TX_NONE'

                WHERE signature=?
                """, (
                    attempts,
                    signature
                ))

                stats["failed"] += 1

            else:

                db.execute("""
                UPDATE t116_pump_signatures

                SET
                    status='RETRY',
                    attempts=?

                WHERE signature=?
                """, (
                    attempts,
                    signature
                ))

                db.commit()

                await asyncio.sleep(
                    min(
                        2 ** attempts,
                        15
                    )
                )

                await queue.put(
                    signature
                )

            db.commit()

            queue.task_done()
            continue

        if (
            tx.get(
                "meta",
                {}
            ).get("err")
            is not None
        ):

            db.execute("""
            UPDATE t116_pump_signatures

            SET status='NOT_SWAP'

            WHERE signature=?
            """, (
                signature,
            ))

            db.commit()

            stats[
                "not_swap"
            ] += 1

            queue.task_done()
            continue

        trade = classify(tx)

        if not trade:

            db.execute("""
            UPDATE t116_pump_signatures

            SET status='NOT_SWAP'

            WHERE signature=?
            """, (
                signature,
            ))

            db.commit()

            stats[
                "not_swap"
            ] += 1

            queue.task_done()
            continue

        block_time = tx.get(
            "blockTime"
        )

        timestamp = (
            float(block_time)
            if block_time is not None
            else time.time()
        )

        db.execute("""
        INSERT OR IGNORE INTO t116_pump_swaps (

            signature,

            timestamp,

            slot,

            token_mint,

            wallet,

            side,

            token_delta,

            sol_delta,

            raw_price_sol,

            sampled_percent,

            created_at
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?
        )
        """, (

            signature,

            timestamp,

            row["slot"],

            trade["mint"],

            trade["wallet"],

            trade["side"],

            trade["token_delta"],

            trade["sol_delta"],

            trade["price"],

            CURRENT_SAMPLE,

            time.time(),
        ))

        db.execute("""
        UPDATE t116_pump_signatures

        SET
            status='DONE',
            error=NULL

        WHERE signature=?
        """, (
            signature,
        ))

        db.commit()

        stats["done"] += 1

        print(
            f"💱 {trade['side']:<4} "
            f"| {trade['mint'][:16]}... "
            f"| SOL={abs(trade['sol_delta']):.4f} "
            f"| P={trade['price']:.12g} "
            f"| SAMPLE={CURRENT_SAMPLE}%"
        )

        queue.task_done()


# ============================================================
# LISTENER
# ============================================================

async def listener():

    global CURRENT_SAMPLE

    while True:

        try:

            print()
            print(
                "🔌 T116E connecting to Pump..."
            )

            async with websockets.connect(

                WS_URL,

                ping_interval=20,

                ping_timeout=20,

                max_size=None

            ) as ws:

                await ws.send(
                    json.dumps({
                        "jsonrpc":
                            "2.0",

                        "id":
                            116,

                        "method":
                            "logsSubscribe",

                        "params": [

                            {
                                "mentions": [
                                    PUMP_PROGRAM
                                ]
                            },

                            {
                                "commitment":
                                    "processed"
                            }
                        ]
                    })
                )

                hello = json.loads(
                    await ws.recv()
                )

                print(
                    "✅ PUMP CONNECTED "
                    f"| subscription={hello.get('result')}"
                )

                while True:

                    msg = json.loads(
                        await ws.recv()
                    )

                    params = msg.get(
                        "params"
                    )

                    if not params:
                        continue

                    result = params.get(
                        "result",
                        {}
                    )

                    value = result.get(
                        "value",
                        {}
                    )

                    if value.get(
                        "err"
                    ) is not None:
                        continue

                    signature = value.get(
                        "signature"
                    )

                    if not signature:
                        continue

                    stats["ws"] += 1

                    CURRENT_SAMPLE = (
                        adaptive_sample_percent()
                    )

                    if not take_signature(
                        signature,
                        CURRENT_SAMPLE
                    ):
                        continue

                    stats["sampled"] += 1

                    cur = db.execute("""
                    INSERT OR IGNORE INTO
                    t116_pump_signatures (

                        signature,

                        slot,

                        received_at,

                        status
                    )

                    VALUES (
                        ?,?,?,?
                    )
                    """, (

                        signature,

                        result.get(
                            "context",
                            {}
                        ).get(
                            "slot"
                        ),

                        time.time(),

                        "WAITING",
                    ))

                    db.commit()

                    if cur.rowcount:

                        await queue.put(
                            signature
                        )

        except Exception as e:

            print(
                "⚠️ T116E WS error:",
                repr(e)
            )

            print(
                "↻ reconnect in 3s"
            )

            await asyncio.sleep(
                3
            )


# ============================================================
# MONITOR
# ============================================================

async def monitor():

    global CURRENT_SAMPLE

    while True:

        await asyncio.sleep(
            MONITOR_SECONDS
        )

        CURRENT_SAMPLE = (
            adaptive_sample_percent()
        )

        counts = db.execute("""
        SELECT
            COUNT(*) AS total,

            SUM(status='DONE') AS done,

            SUM(status='WAITING') AS waiting,

            SUM(status='RETRY') AS retry,

            SUM(status='NOT_SWAP') AS not_swap,

            SUM(status='FAILED') AS failed

        FROM t116_pump_signatures
        """).fetchone()

        swaps = db.execute("""
        SELECT
            COUNT(*) AS swaps,

            COUNT(DISTINCT token_mint) AS tokens,

            SUM(side='BUY') AS buys,

            SUM(side='SELL') AS sells

        FROM t116_pump_swaps
        """).fetchone()

        recent = db.execute("""
        SELECT
            COUNT(*) AS swaps,

            COUNT(DISTINCT token_mint) AS tokens

        FROM t116_pump_swaps

        WHERE timestamp >=
              strftime('%s','now') - 120
        """).fetchone()

        waiting, oldest = (
            waiting_status()
        )

        print()
        print("─" * 130)
        print(
            "T116E ADAPTIVE PUMP COLLECTOR"
        )

        print(
            f"WS={stats['ws']:,} "
            f"| SAMPLE={CURRENT_SAMPLE}% "
            f"| STORED_SIG={counts['total'] or 0:,} "
            f"| Q={queue.qsize():,}"
        )

        print(
            f"DONE={counts['done'] or 0:,} "
            f"| WAIT={counts['waiting'] or 0:,} "
            f"| RETRY={counts['retry'] or 0:,} "
            f"| NOT_SWAP={counts['not_swap'] or 0:,} "
            f"| FAILED={counts['failed'] or 0:,}"
        )

        print(
            f"WAIT DB={waiting:,} "
            f"| OLDEST={oldest:.1f}s"
        )

        print(
            f"SWAPS={swaps['swaps'] or 0:,} "
            f"| TOKENS={swaps['tokens'] or 0:,} "
            f"| BUY={swaps['buys'] or 0:,} "
            f"| SELL={swaps['sells'] or 0:,}"
        )

        print(
            f"LAST120s "
            f"| SWAPS={recent['swaps'] or 0:,} "
            f"| TOKENS={recent['tokens'] or 0:,}"
        )

        if CURRENT_SAMPLE == 5:

            print(
                "🟢 LOW LOAD → sampling increased to 5%"
            )

        elif CURRENT_SAMPLE == 3:

            print(
                "🟡 MODERATE LOAD → sampling 3%"
            )

        elif CURRENT_SAMPLE == 2:

            print(
                "🟠 HIGH LOAD → sampling 2%"
            )

        else:

            print(
                "🔴 BACKLOG PROTECTION → sampling 1%"
            )


# ============================================================
# RESUME
# ============================================================

async def resume_pending():

    rows = db.execute("""
    SELECT signature

    FROM t116_pump_signatures

    WHERE status IN (
        'WAITING',
        'RETRY'
    )

    ORDER BY received_at ASC

    LIMIT 10000
    """).fetchall()

    for r in rows:

        await queue.put(
            r["signature"]
        )

    if rows:

        print(
            f"♻️ resumed {len(rows)} pending signatures"
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    print()
    print("=" * 130)

    print(
        "MEMECOIN LAB — "
        "T116E ADAPTIVE PRE-MIGRATION COLLECTOR"
    )

    print("=" * 130)

    print(
        f"PUMP PROGRAM : {PUMP_PROGRAM}"
    )

    print(
        "SAMPLE       : adaptive 1% / 2% / 3% / 5%"
    )

    print(
        f"RPC LIMIT    : {RPC_RPS:.1f} tx/s"
    )

    print(
        "CONTROL      : queue + oldest waiting age"
    )

    print(
        "TABLES       : same T116 raw tables"
    )

    print(
        "MODEL        : NONE"
    )

    print(
        "TRADING      : NONE"
    )

    print("=" * 130)

    connector = aiohttp.TCPConnector(
        limit=10
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        await resume_pending()

        tasks = [

            asyncio.create_task(
                listener()
            ),

            asyncio.create_task(
                worker(
                    session
                )
            ),

            asyncio.create_task(
                monitor()
            ),
        ]

        await asyncio.gather(
            *tasks
        )


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "T116E stopped safely."
        )

    finally:

        db.close()
