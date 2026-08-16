import os
from dotenv import load_dotenv
import asyncio
import json
import sqlite3
import time
from collections import defaultdict

import aiohttp
import websockets

load_dotenv()

WS_URL = os.getenv("SOLANA_WS_URL")
RPC_URL = os.getenv("SOLANA_RPC_URL")

if not WS_URL or not RPC_URL:
    raise RuntimeError(
        "SOLANA_WS_URL ou SOLANA_RPC_URL absent du fichier .env"
    )

SOL_MINT = "So11111111111111111111111111111111111111112"

PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
}

WORKERS = 4
FETCH_DELAY = 1.0
MAX_RETRIES = 8

queue = asyncio.Queue(maxsize=50_000)
seen = set()

stats = defaultdict(int)

db = sqlite3.connect("memecoin_lab_v021.db")

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
    sol_delta REAL
)
""")

db.commit()


def extract_wallet(tx):
    try:
        accounts = tx["transaction"]["message"]["accountKeys"]

        for account in accounts:
            if (
                isinstance(account, dict)
                and account.get("signer")
                and account.get("writable")
            ):
                return account["pubkey"]

        for account in accounts:
            if isinstance(account, dict) and account.get("signer"):
                return account["pubkey"]

    except Exception:
        pass

    return None


def wallet_token_changes(tx, wallet):
    meta = tx.get("meta", {})

    pre = {}
    post = {}

    for item in meta.get("preTokenBalances", []):
        if item.get("owner") != wallet:
            continue

        mint = item.get("mint")

        try:
            amount = float(
                item["uiTokenAmount"]["uiAmountString"]
            )
        except Exception:
            amount = 0.0

        pre[mint] = pre.get(mint, 0.0) + amount

    for item in meta.get("postTokenBalances", []):
        if item.get("owner") != wallet:
            continue

        mint = item.get("mint")

        try:
            amount = float(
                item["uiTokenAmount"]["uiAmountString"]
            )
        except Exception:
            amount = 0.0

        post[mint] = post.get(mint, 0.0) + amount

    changes = {}

    for mint in set(pre) | set(post):
        delta = post.get(mint, 0) - pre.get(mint, 0)

        if abs(delta) > 1e-12:
            changes[mint] = delta

    return changes


def native_sol_delta(tx, wallet):
    try:
        accounts = tx["transaction"]["message"]["accountKeys"]

        index = None

        for i, account in enumerate(accounts):
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

        before = meta["preBalances"][index]
        after = meta["postBalances"][index]

        delta = (after - before) / 1_000_000_000

        # Remove network fee from economic SOL movement
        if index == 0:
            delta += meta.get("fee", 0) / 1_000_000_000

        return delta

    except Exception:
        return 0.0


def classify(tx):
    wallet = extract_wallet(tx)

    if not wallet:
        return None

    changes = wallet_token_changes(tx, wallet)

    native_sol = native_sol_delta(tx, wallet)

    wrapped_sol = changes.pop(SOL_MINT, 0.0)

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

    if token_delta > 0 and sol_delta < 0:
        side = "BUY"

    elif token_delta < 0 and sol_delta > 0:
        side = "SELL"

    else:
        return None

    return {
        "wallet": wallet,
        "side": side,
        "mint": mint,
        "token_delta": token_delta,
        "sol_delta": sol_delta,
    }


async def fetch_transaction(session, signature):
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

    delay = 0.5

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                RPC_URL,
                json=payload,
                timeout=15
            ) as response:

                if response.status == 429:
                    stats["rate_limited"] += 1

                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 8)

                    continue

                data = await response.json()

                result = data.get("result")

                if result:
                    stats["fetched"] += 1
                    return result

                stats["null_result"] += 1

        except Exception:
            stats["http_errors"] += 1

        await asyncio.sleep(delay)

        delay = min(delay * 1.5, 5)

    stats["failed"] += 1

    return None


async def worker(worker_id, session):
    while True:
        program, slot, signature = await queue.get()

        try:
            await asyncio.sleep(FETCH_DELAY)

            tx = await fetch_transaction(
                session,
                signature
            )

            if not tx:
                continue

            meta = tx.get("meta", {})

            if meta.get("err") is not None:
                stats["tx_failed"] += 1
                continue

            swap = classify(tx)

            if not swap:
                stats["not_swap"] += 1
                continue

            try:
                db.execute(
                    """
                    INSERT OR IGNORE INTO swaps
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signature,
                        time.time(),
                        slot,
                        program,
                        swap["wallet"],
                        swap["side"],
                        swap["mint"],
                        swap["token_delta"],
                        swap["sol_delta"],
                    )
                )

                db.commit()

                stats["saved"] += 1

            except Exception:
                stats["db_errors"] += 1

        finally:
            queue.task_done()


async def listener(name, program_id):
    while True:
        try:
            print(f"[{name}] connecting...")

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
                            "mentions": [program_id]
                        },
                        {
                            "commitment": "processed"
                        }
                    ]
                }

                await ws.send(json.dumps(request))

                confirmation = json.loads(
                    await ws.recv()
                )

                print(
                    f"[{name}] connected | "
                    f"subscription="
                    f"{confirmation.get('result')}"
                )

                while True:
                    message = json.loads(
                        await ws.recv()
                    )

                    params = message.get("params")

                    if not params:
                        continue

                    result = params.get("result", {})
                    context = result.get("context", {})
                    value = result.get("value", {})

                    stats["ws_received"] += 1

                    if value.get("err") is not None:
                        stats["ws_failed"] += 1
                        continue

                    signature = value.get("signature")

                    if not signature:
                        continue

                    if signature in seen:
                        stats["duplicates"] += 1
                        continue

                    seen.add(signature)

                    try:
                        queue.put_nowait(
                            (
                                name,
                                context.get("slot"),
                                signature
                            )
                        )

                        stats["queued"] += 1

                    except asyncio.QueueFull:
                        stats["queue_dropped"] += 1

        except Exception:
            stats["ws_errors"] += 1
            await asyncio.sleep(2)


async def monitor():
    while True:
        await asyncio.sleep(5)

        print(
            "\r"
            f"WS {stats['ws_received']:,} | "
            f"QUEUE {queue.qsize():,} | "
            f"FETCHED {stats['fetched']:,} | "
            f"SAVED {stats['saved']:,} | "
            f"429 {stats['rate_limited']:,} | "
            f"NULL {stats['null_result']:,} | "
            f"NOT_SWAP {stats['not_swap']:,} | "
            f"FAILED {stats['failed']:,}",
            end="",
            flush=True
        )


async def main():
    print("=" * 80)
    print("MEMECOIN LAB — ROBUST COLLECTOR V0.2.1")
    print("=" * 80)

    connector = aiohttp.TCPConnector(
        limit=WORKERS
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = []

        for name, program_id in PROGRAMS.items():
            tasks.append(
                asyncio.create_task(
                    listener(name, program_id)
                )
            )

        for worker_id in range(WORKERS):
            tasks.append(
                asyncio.create_task(
                    worker(worker_id, session)
                )
            )

        tasks.append(
            asyncio.create_task(
                monitor()
            )
        )

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\n\nCollector stopped.")

        print("\nFINAL STATS")
        print("-" * 40)

        for key in sorted(stats):
            print(
                f"{key:20} : "
                f"{stats[key]:,}"
            )

        db.close()
