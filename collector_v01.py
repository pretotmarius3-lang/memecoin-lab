import asyncio
import json
import sqlite3
from datetime import datetime, timezone

import aiohttp
import websockets

WS_URL = "wss://api.mainnet-beta.solana.com/"
RPC_URL = "https://api.mainnet-beta.solana.com"

PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
}

# -----------------------------
# DATABASE
# -----------------------------

db = sqlite3.connect("memecoin_lab.db")

db.execute("""
CREATE TABLE IF NOT EXISTS transactions (
    signature TEXT PRIMARY KEY,
    timestamp TEXT,
    slot INTEGER,
    program TEXT,
    wallet TEXT,
    success INTEGER
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS token_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signature TEXT,
    wallet TEXT,
    mint TEXT,
    pre_amount REAL,
    post_amount REAL,
    delta REAL
)
""")

db.commit()


# -----------------------------
# RPC
# -----------------------------

async def get_transaction(session, signature):

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

    for attempt in range(5):

        try:

            async with session.post(
                RPC_URL,
                json=payload,
                timeout=10
            ) as response:

                data = await response.json()

                result = data.get("result")

                if result:
                    return result

        except Exception:
            pass

        await asyncio.sleep(0.5)

    return None


# -----------------------------
# PARSER
# -----------------------------

def extract_wallet(tx):

    try:

        keys = tx["transaction"]["message"]["accountKeys"]

        for account in keys:

            if isinstance(account, dict) and account.get("signer"):
                return account.get("pubkey")

    except Exception:
        pass

    return None


def balance_map(balance_list):

    result = {}

    for item in balance_list or []:

        key = (
            item.get("accountIndex"),
            item.get("mint")
        )

        amount = (
            item
            .get("uiTokenAmount", {})
            .get("uiAmount")
        )

        if amount is None:

            raw = (
                item
                .get("uiTokenAmount", {})
                .get("uiAmountString")
            )

            try:
                amount = float(raw)
            except Exception:
                amount = 0.0

        result[key] = float(amount or 0)

    return result


def extract_token_changes(tx):

    meta = tx.get("meta", {})

    pre = balance_map(
        meta.get("preTokenBalances", [])
    )

    post = balance_map(
        meta.get("postTokenBalances", [])
    )

    keys = set(pre) | set(post)

    changes = []

    for key in keys:

        before = pre.get(key, 0)
        after = post.get(key, 0)

        delta = after - before

        if abs(delta) < 1e-12:
            continue

        account_index, mint = key

        changes.append({
            "mint": mint,
            "pre": before,
            "post": after,
            "delta": delta,
            "account_index": account_index
        })

    return changes


# -----------------------------
# PROCESS TRANSACTION
# -----------------------------

async def process_transaction(
    session,
    program,
    slot,
    signature
):

    tx = await get_transaction(
        session,
        signature
    )

    if not tx:
        return

    meta = tx.get("meta", {})

    # Ignore failed transactions
    if meta.get("err") is not None:
        return

    wallet = extract_wallet(tx)

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    try:

        db.execute(
            """
            INSERT OR IGNORE INTO transactions
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                signature,
                timestamp,
                slot,
                program,
                wallet,
                1
            )
        )

        changes = extract_token_changes(tx)

        for change in changes:

            db.execute(
                """
                INSERT INTO token_changes
                (
                    signature,
                    wallet,
                    mint,
                    pre_amount,
                    post_amount,
                    delta
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    signature,
                    wallet,
                    change["mint"],
                    change["pre"],
                    change["post"],
                    change["delta"]
                )
            )

        db.commit()

        if changes:

            print()
            print("=" * 80)

            print(
                f"{program} | "
                f"slot={slot}"
            )

            print(
                f"WALLET: {wallet}"
            )

            print(
                f"TX: {signature}"
            )

            for change in changes:

                delta = change["delta"]

                direction = (
                    "RECEIVED"
                    if delta > 0
                    else "SENT"
                )

                print(
                    f"{direction:8} "
                    f"{abs(delta):,.6f} "
                    f"{change['mint']}"
                )

    except Exception as e:

        print(
            "DB ERROR:",
            e
        )


# -----------------------------
# WEBSOCKET
# -----------------------------

async def listen_program(
    session,
    name,
    program_id
):

    while True:

        try:

            print(
                f"[{name}] connexion..."
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
                                program_id
                            ]
                        },
                        {
                            "commitment":
                            "processed"
                        }
                    ]
                }

                await ws.send(
                    json.dumps(request)
                )

                response = json.loads(
                    await ws.recv()
                )

                print(
                    f"[{name}] connecté | "
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

                    context = result.get(
                        "context",
                        {}
                    )

                    value = result.get(
                        "value",
                        {}
                    )

                    # Ignore failures immediately
                    if value.get("err") is not None:
                        continue

                    signature = value.get(
                        "signature"
                    )

                    slot = context.get(
                        "slot"
                    )

                    if not signature:
                        continue

                    asyncio.create_task(
                        process_transaction(
                            session,
                            name,
                            slot,
                            signature
                        )
                    )

        except Exception as e:

            print(
                f"[{name}] ERROR:",
                e
            )

            print(
                "Reconnexion dans 3s..."
            )

            await asyncio.sleep(3)


# -----------------------------
# MAIN
# -----------------------------

async def main():

    print("=" * 80)
    print(
        "MEMECOIN LAB — "
        "TRANSACTION PARSER V0.1"
    )
    print("=" * 80)

    async with aiohttp.ClientSession() as session:

        tasks = []

        for name, program_id in PROGRAMS.items():

            tasks.append(
                asyncio.create_task(
                    listen_program(
                        session,
                        name,
                        program_id
                    )
                )
            )

        await asyncio.gather(*tasks)


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        print(
            "\nCollector arrêté."
        )

        db.close()
