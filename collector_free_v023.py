import asyncio
import json
import os
import sqlite3
import time
import hashlib
from collections import defaultdict

import aiohttp
import websockets
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================

load_dotenv(".env")

RPC_URL = os.getenv("SOLANA_RPC_URL")
WS_URL = os.getenv("SOLANA_WS_URL")

if not RPC_URL or not WS_URL:
    raise RuntimeError("RPC/WS absent du .env")

PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
}

SOL_MINT = "So11111111111111111111111111111111111111112"

DB_FILE = "memecoin_lab_sampler.db"

# ------------------------------------------------------------
# SAFE FREE SETTINGS
# ------------------------------------------------------------

RPC_RPS = 4.0

FETCH_DELAY = 2.0

MAX_RETRIES = 4

# 25% deterministic sample
SAMPLE_PERCENT = 25

stats = defaultdict(int)

queue = asyncio.PriorityQueue()

db = sqlite3.connect(
    DB_FILE,
    timeout=30,
    check_same_thread=False
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")

# ============================================================
# DATABASE
# ============================================================

db.execute("""
CREATE TABLE IF NOT EXISTS signatures (
    signature TEXT PRIMARY KEY,
    slot INTEGER,
    program TEXT,
    received_at REAL,
    eligible INTEGER,
    status TEXT,
    attempts INTEGER DEFAULT 0
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS swaps (
    signature TEXT PRIMARY KEY,
    timestamp REAL,
    slot INTEGER,
    program TEXT,
    wallet TEXT,
    side TEXT,
    token_mint TEXT,
    token_delta REAL,
    sol_delta REAL,
    price_sol REAL
)
""")

db.commit()

# ============================================================
# DETERMINISTIC SAMPLER
# ============================================================

def is_eligible(signature):

    h = hashlib.sha256(
        signature.encode()
    ).digest()

    value = int.from_bytes(
        h[:8],
        "big"
    )

    return (
        value % 100
    ) < SAMPLE_PERCENT

# ============================================================
# SAVE SIGNATURE
# ============================================================

def save_signature(
    signature,
    slot,
    program,
    eligible
):

    try:

        cur = db.execute(
            """
            INSERT OR IGNORE INTO signatures
            (
                signature,
                slot,
                program,
                received_at,
                eligible,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                signature,
                slot,
                program,
                time.time(),
                int(eligible),
                "WAITING" if eligible else "REJECTED"
            )
        )

        db.commit()

        return cur.rowcount > 0

    except Exception:

        stats["db_error"] += 1

        return False

# ============================================================
# WALLET
# ============================================================

def extract_wallet(tx):

    try:

        accounts = (
            tx["transaction"]
            ["message"]
            ["accountKeys"]
        )

        for account in accounts:

            if (
                isinstance(account, dict)
                and account.get("signer")
                and account.get("writable")
            ):

                return account["pubkey"]

        for account in accounts:

            if (
                isinstance(account, dict)
                and account.get("signer")
            ):

                return account["pubkey"]

    except Exception:

        pass

    return None

# ============================================================
# TOKEN DELTAS
# ============================================================

def wallet_token_changes(
    tx,
    wallet
):

    meta = tx.get(
        "meta",
        {}
    )

    pre = {}
    post = {}

    for item in meta.get(
        "preTokenBalances",
        []
    ):

        if item.get("owner") != wallet:
            continue

        mint = item.get("mint")

        try:

            amount = float(
                item["uiTokenAmount"]
                ["uiAmountString"]
            )

        except Exception:

            amount = 0.0

        pre[mint] = (
            pre.get(mint, 0.0)
            + amount
        )

    for item in meta.get(
        "postTokenBalances",
        []
    ):

        if item.get("owner") != wallet:
            continue

        mint = item.get("mint")

        try:

            amount = float(
                item["uiTokenAmount"]
                ["uiAmountString"]
            )

        except Exception:

            amount = 0.0

        post[mint] = (
            post.get(mint, 0.0)
            + amount
        )

    changes = {}

    for mint in set(pre) | set(post):

        delta = (
            post.get(mint, 0.0)
            - pre.get(mint, 0.0)
        )

        if abs(delta) > 1e-12:

            changes[mint] = delta

    return changes

# ============================================================
# SOL DELTA
# ============================================================

def native_sol_delta(
    tx,
    wallet
):

    try:

        accounts = (
            tx["transaction"]
            ["message"]
            ["accountKeys"]
        )

        index = None

        for i, account in enumerate(
            accounts
        ):

            pubkey = (
                account.get("pubkey")
                if isinstance(account, dict)
                else account
            )

            if pubkey == wallet:

                index = i
                break

        if index is None:

            return 0.0

        meta = tx["meta"]

        before = (
            meta["preBalances"][index]
        )

        after = (
            meta["postBalances"][index]
        )

        delta = (
            after - before
        ) / 1_000_000_000

        if index == 0:

            delta += (
                meta.get("fee", 0)
                / 1_000_000_000
            )

        return delta

    except Exception:

        return 0.0

# ============================================================
# CLASSIFIER
# ============================================================

def classify(tx):

    wallet = extract_wallet(tx)

    if not wallet:
        return None

    changes = wallet_token_changes(
        tx,
        wallet
    )

    native_sol = native_sol_delta(
        tx,
        wallet
    )

    wrapped_sol = changes.pop(
        SOL_MINT,
        0.0
    )

    sol_delta = (
        wrapped_sol
        if abs(wrapped_sol) > 1e-9
        else native_sol
    )

    candidates = [
        (mint, delta)
        for mint, delta in changes.items()
        if abs(delta) > 1e-12
    ]

    if not candidates:
        return None

    mint, token_delta = max(
        candidates,
        key=lambda x: abs(x[1])
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

    token_abs = abs(
        token_delta
    )

    sol_abs = abs(
        sol_delta
    )

    if (
        token_abs == 0
        or sol_abs == 0
    ):
        return None

    return {
        "wallet": wallet,
        "side": side,
        "mint": mint,
        "token_delta": token_delta,
        "sol_delta": sol_delta,
        "price": sol_abs / token_abs
    }

# ============================================================
# RPC
# ============================================================

async def get_transaction(
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
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    }

    try:

        async with session.post(
            RPC_URL,
            json=payload,
            timeout=15
        ) as response:

            stats["rpc"] += 1

            if response.status == 429:

                stats["429"] += 1

                return "RETRY", None

            if response.status != 200:

                stats["http_error"] += 1

                return "RETRY", None

            data = await response.json()

            if data.get("error"):

                stats["rpc_error"] += 1

                return "RETRY", None

            tx = data.get("result")

            if tx is None:

                stats["null"] += 1

                return "RETRY", None

            stats["fetched"] += 1

            return "OK", tx

    except Exception:

        stats["exception"] += 1

        return "RETRY", None

# ============================================================
# FETCHER
# ============================================================

async def fetcher(session):

    interval = 1 / RPC_RPS

    last_request = 0

    while True:

        ready_at, signature = (
            await queue.get()
        )

        now = time.time()

        if ready_at > now:

            await asyncio.sleep(
                ready_at - now
            )

        row = db.execute(
            """
            SELECT *
            FROM signatures
            WHERE signature = ?
            """,
            (signature,)
        ).fetchone()

        if not row:

            queue.task_done()
            continue

        if row["status"] in (
            "DONE",
            "NOT_SWAP",
            "FAILED"
        ):

            queue.task_done()
            continue

        attempts = row["attempts"]

        # Global RPS limiter
        elapsed = (
            time.monotonic()
            - last_request
        )

        wait = (
            interval - elapsed
        )

        if wait > 0:

            await asyncio.sleep(
                wait
            )

        last_request = (
            time.monotonic()
        )

        status, tx = (
            await get_transaction(
                session,
                signature
            )
        )

        if status != "OK":

            attempts += 1

            if attempts >= MAX_RETRIES:

                db.execute(
                    """
                    UPDATE signatures
                    SET
                        status='FAILED',
                        attempts=?
                    WHERE signature=?
                    """,
                    (
                        attempts,
                        signature
                    )
                )

                stats["failed"] += 1

            else:

                db.execute(
                    """
                    UPDATE signatures
                    SET attempts=?
                    WHERE signature=?
                    """,
                    (
                        attempts,
                        signature
                    )
                )

                delay = min(
                    2 ** attempts,
                    15
                )

                await queue.put(
                    (
                        time.time()
                        + delay,

                        signature
                    )
                )

            db.commit()

            queue.task_done()

            continue

        meta = tx.get(
            "meta",
            {}
        )

        if meta.get("err") is not None:

            db.execute(
                """
                UPDATE signatures
                SET status='NOT_SWAP'
                WHERE signature=?
                """,
                (signature,)
            )

            db.commit()

            stats["not_swap"] += 1

            queue.task_done()

            continue

        swap = classify(tx)

        if not swap:

            db.execute(
                """
                UPDATE signatures
                SET status='NOT_SWAP'
                WHERE signature=?
                """,
                (signature,)
            )

            db.commit()

            stats["not_swap"] += 1

            queue.task_done()

            continue

        db.execute(
            """
            INSERT OR IGNORE INTO swaps
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signature,
                time.time(),
                row["slot"],
                row["program"],
                swap["wallet"],
                swap["side"],
                swap["mint"],
                swap["token_delta"],
                swap["sol_delta"],
                swap["price"]
            )
        )

        db.execute(
            """
            UPDATE signatures
            SET status='DONE'
            WHERE signature=?
            """,
            (signature,)
        )

        db.commit()

        stats["saved"] += 1

        if swap["side"] == "BUY":
            stats["buy"] += 1
        else:
            stats["sell"] += 1

        queue.task_done()

# ============================================================
# WEBSOCKET
# ============================================================

async def listener(
    name,
    program
):

    while True:

        try:

            print(
                f"[{name}] connecting..."
            )

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=None
            ) as ws:

                request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {
                            "mentions": [
                                program
                            ]
                        },
                        {
                            "commitment":
                                "processed"
                        }
                    ]
                }

                await ws.send(
                    json.dumps(
                        request
                    )
                )

                response = json.loads(
                    await ws.recv()
                )

                print(
                    f"[{name}] connected | "
                    f"subscription="
                    f"{response.get('result')}"
                )

                while True:

                    message = json.loads(
                        await ws.recv()
                    )

                    params = message.get(
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

                    context = result.get(
                        "context",
                        {}
                    )

                    stats["ws"] += 1

                    if value.get("err") is not None:

                        continue

                    signature = value.get(
                        "signature"
                    )

                    if not signature:
                        continue

                    eligible = (
                        is_eligible(
                            signature
                        )
                    )

                    inserted = (
                        save_signature(
                            signature,
                            context.get(
                                "slot"
                            ),
                            name,
                            eligible
                        )
                    )

                    if not inserted:
                        continue

                    if eligible:

                        stats[
                            "eligible"
                        ] += 1

                        await queue.put(
                            (
                                time.time()
                                + FETCH_DELAY,

                                signature
                            )
                        )

                    else:

                        stats[
                            "rejected"
                        ] += 1

        except Exception:

            stats["ws_error"] += 1

            await asyncio.sleep(2)

# ============================================================
# MONITOR
# ============================================================

async def monitor():

    while True:

        await asyncio.sleep(5)

        total = db.execute(
            """
            SELECT COUNT(*)
            FROM signatures
            """
        ).fetchone()[0]

        eligible = db.execute(
            """
            SELECT COUNT(*)
            FROM signatures
            WHERE eligible=1
            """
        ).fetchone()[0]

        done = db.execute(
            """
            SELECT COUNT(*)
            FROM signatures
            WHERE status='DONE'
            """
        ).fetchone()[0]

        not_swap = db.execute(
            """
            SELECT COUNT(*)
            FROM signatures
            WHERE status='NOT_SWAP'
            """
        ).fetchone()[0]

        failed = db.execute(
            """
            SELECT COUNT(*)
            FROM signatures
            WHERE status='FAILED'
            """
        ).fetchone()[0]

        swaps = db.execute(
            """
            SELECT COUNT(*)
            FROM swaps
            """
        ).fetchone()[0]

        tokens = db.execute(
            """
            SELECT COUNT(
                DISTINCT token_mint
            )
            FROM swaps
            """
        ).fetchone()[0]

        wallets = db.execute(
            """
            SELECT COUNT(
                DISTINCT wallet
            )
            FROM swaps
            """
        ).fetchone()[0]

        processed = (
            done
            + not_swap
            + failed
        )

        coverage = (
            processed / eligible * 100
            if eligible
            else 0
        )

        print()
        print(
            "─" * 85
        )

        print(
            f"WS {stats['ws']:,}"
            f" | SAMPLE {eligible:,}/{total:,}"
            f" | QUEUE {queue.qsize():,}"
        )

        print(
            f"RPC {stats['rpc']:,}"
            f" | FETCHED {stats['fetched']:,}"
            f" | 429 {stats['429']:,}"
            f" | NULL {stats['null']:,}"
        )

        print(
            f"SWAPS {swaps:,}"
            f" | TOKENS {tokens:,}"
            f" | WALLETS {wallets:,}"
            f" | BUY {stats['buy']:,}"
            f" | SELL {stats['sell']:,}"
        )

        print(
            f"PROCESSED {processed:,}/{eligible:,}"
            f" | COVERAGE {coverage:.1f}%"
            f" | FAILED {failed:,}"
        )

# ============================================================
# RECOVER
# ============================================================

async def recover_queue():

    rows = db.execute(
        """
        SELECT signature, received_at
        FROM signatures
        WHERE
            eligible=1
            AND status='WAITING'
        """
    ).fetchall()

    for row in rows:

        await queue.put(
            (
                time.time(),
                row["signature"]
            )
        )

    print(
        f"Recovered queue: "
        f"{len(rows):,}"
    )

# ============================================================
# MAIN
# ============================================================

async def main():

    print()
    print("=" * 85)
    print(
        "MEMECOIN LAB — FREE SAMPLER V0.2.3"
    )
    print("=" * 85)

    print(
        f"Sample       : {SAMPLE_PERCENT}%"
    )

    print(
        f"RPC target   : {RPC_RPS} req/s"
    )

    print(
        f"Fetch delay  : {FETCH_DELAY}s"
    )

    print(
        f"Database     : {DB_FILE}"
    )

    print("=" * 85)

    await recover_queue()

    connector = aiohttp.TCPConnector(
        limit=2
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = [
            asyncio.create_task(
                listener(
                    name,
                    program
                )
            )
            for name, program
            in PROGRAMS.items()
        ]

        tasks.append(
            asyncio.create_task(
                fetcher(
                    session
                )
            )
        )

        tasks.append(
            asyncio.create_task(
                monitor()
            )
        )

        await asyncio.gather(
            *tasks
        )


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "\nCollector stopped."
        )

    finally:

        db.commit()
        db.close()
