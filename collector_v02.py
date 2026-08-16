import asyncio
import json
import sqlite3
from datetime import datetime, timezone

import aiohttp
import websockets

WS_URL = "wss://api.mainnet-beta.solana.com/"
RPC_URL = "https://api.mainnet-beta.solana.com"

SOL_MINT = "So11111111111111111111111111111111111111112"

PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
}

db = sqlite3.connect("memecoin_lab_v02.db")

db.execute("""
CREATE TABLE IF NOT EXISTS swaps (
    signature TEXT PRIMARY KEY,
    timestamp TEXT,
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

    for _ in range(6):

        try:
            async with session.post(
                RPC_URL,
                json=payload,
                timeout=10
            ) as response:

                data = await response.json()

                if data.get("result"):
                    return data["result"]

        except Exception:
            pass

        await asyncio.sleep(0.4)

    return None


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

            if (
                isinstance(account, dict)
                and account.get("signer")
            ):
                return account["pubkey"]

    except Exception:
        pass

    return None


def get_wallet_token_balances(tx, wallet):

    meta = tx.get("meta", {})

    pre = {}
    post = {}

    for item in meta.get("preTokenBalances", []):

        if item.get("owner") != wallet:
            continue

        mint = item.get("mint")

        raw = item.get(
            "uiTokenAmount", {}
        ).get("uiAmountString", "0")

        try:
            amount = float(raw)
        except Exception:
            amount = 0.0

        pre[mint] = pre.get(mint, 0.0) + amount


    for item in meta.get("postTokenBalances", []):

        if item.get("owner") != wallet:
            continue

        mint = item.get("mint")

        raw = item.get(
            "uiTokenAmount", {}
        ).get("uiAmountString", "0")

        try:
            amount = float(raw)
        except Exception:
            amount = 0.0

        post[mint] = post.get(mint, 0.0) + amount


    changes = {}

    for mint in set(pre) | set(post):

        delta = (
            post.get(mint, 0.0)
            - pre.get(mint, 0.0)
        )

        if abs(delta) > 1e-12:
            changes[mint] = delta

    return changes


def get_native_sol_delta(tx, wallet):

    try:

        accounts = tx["transaction"]["message"]["accountKeys"]

        wallet_index = None

        for i, account in enumerate(accounts):

            pubkey = (
                account.get("pubkey")
                if isinstance(account, dict)
                else account
            )

            if pubkey == wallet:
                wallet_index = i
                break

        if wallet_index is None:
            return 0.0

        meta = tx["meta"]

        before = meta["preBalances"][wallet_index]
        after = meta["postBalances"][wallet_index]

        return (after - before) / 1_000_000_000

    except Exception:
        return 0.0


def classify_swap(tx):

    wallet = extract_wallet(tx)

    if not wallet:
        return None

    token_changes = get_wallet_token_balances(
        tx,
        wallet
    )

    native_sol_delta = get_native_sol_delta(
        tx,
        wallet
    )

    wrapped_sol_delta = token_changes.pop(
        SOL_MINT,
        0.0
    )

    # Native SOL can include network fees.
    # Wrapped SOL movement is generally cleaner for swaps.
    if abs(wrapped_sol_delta) > 1e-9:
        sol_delta = wrapped_sol_delta
    else:
        sol_delta = native_sol_delta

    candidates = [
        (mint, delta)
        for mint, delta in token_changes.items()
        if abs(delta) > 1e-12
    ]

    if not candidates:
        return None

    # Largest absolute wallet token movement
    token_mint, token_delta = max(
        candidates,
        key=lambda x: abs(x[1])
    )

    # BUY:
    # wallet receives token and loses SOL
    if token_delta > 0 and sol_delta < 0:

        side = "BUY"

    # SELL:
    # wallet loses token and receives SOL
    elif token_delta < 0 and sol_delta > 0:

        side = "SELL"

    else:

        side = "UNKNOWN"

    return {
        "wallet": wallet,
        "side": side,
        "token_mint": token_mint,
        "token_delta": token_delta,
        "sol_delta": sol_delta
    }


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

    if meta.get("err") is not None:
        return

    swap = classify_swap(tx)

    if not swap:
        return

    # For now don't save ambiguous transactions.
    if swap["side"] == "UNKNOWN":
        return

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    try:

        db.execute(
            """
            INSERT OR IGNORE INTO swaps
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signature,
                timestamp,
                slot,
                program,
                swap["wallet"],
                swap["side"],
                swap["token_mint"],
                swap["token_delta"],
                swap["sol_delta"]
            )
        )

        db.commit()

    except Exception as e:

        print("DB ERROR:", e)
        return


    print()
    print("=" * 76)

    emoji = (
        "🟢"
        if swap["side"] == "BUY"
        else "🔴"
    )

    print(
        f"{emoji} {swap['side']} | {program}"
    )

    print(
        f"WALLET : {swap['wallet']}"
    )

    print(
        f"TOKEN  : {swap['token_mint']}"
    )

    print(
        f"TOKENS : {abs(swap['token_delta']):,.6f}"
    )

    print(
        f"SOL    : {abs(swap['sol_delta']):,.6f}"
    )

    print(
        f"TX     : {signature}"
    )


async def listen_program(
    session,
    name,
    program_id
):

    while True:

        try:

            print(f"[{name}] connexion...")

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
                            "commitment": "processed"
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

                    params = message.get("params")

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


async def main():

    print("=" * 76)
    print(
        "MEMECOIN LAB — "
        "SWAP CLASSIFIER V0.2"
    )
    print("=" * 76)

    async with aiohttp.ClientSession() as session:

        tasks = [
            asyncio.create_task(
                listen_program(
                    session,
                    name,
                    program_id
                )
            )
            for name, program_id
            in PROGRAMS.items()
        ]

        await asyncio.gather(*tasks)


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        print("\nCollector arrêté.")

        db.close()
