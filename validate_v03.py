import asyncio
import sqlite3
import aiohttp

RPC_URL = "https://api.mainnet-beta.solana.com"

db = sqlite3.connect("memecoin_lab_v02.db")
db.row_factory = sqlite3.Row


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

    async with session.post(RPC_URL, json=payload, timeout=15) as r:
        data = await r.json()
        return data.get("result")


def wallet_native_sol(tx, wallet):
    keys = tx["transaction"]["message"]["accountKeys"]

    idx = None

    for i, account in enumerate(keys):
        pubkey = account.get("pubkey") if isinstance(account, dict) else account

        if pubkey == wallet:
            idx = i
            break

    if idx is None:
        return None

    meta = tx["meta"]

    pre = meta["preBalances"][idx]
    post = meta["postBalances"][idx]

    raw_delta = (post - pre) / 1_000_000_000

    # Network fee is paid by the transaction fee payer.
    fee_sol = meta.get("fee", 0) / 1_000_000_000

    signer_is_fee_payer = idx == 0

    if signer_is_fee_payer:
        economic_delta = raw_delta + fee_sol
    else:
        economic_delta = raw_delta

    return {
        "raw_delta": raw_delta,
        "fee": fee_sol,
        "economic_delta": economic_delta
    }


async def main():

    rows = db.execute("""
        SELECT *
        FROM swaps
        ORDER BY timestamp DESC
        LIMIT 30
    """).fetchall()

    print("=" * 100)
    print("MEMECOIN LAB — V0.3 SWAP VALIDATION")
    print("=" * 100)

    async with aiohttp.ClientSession() as session:

        for row in rows:

            tx = await get_transaction(
                session,
                row["signature"]
            )

            if not tx:
                print("TX unavailable:", row["signature"])
                continue

            info = wallet_native_sol(
                tx,
                row["wallet"]
            )

            if not info:
                continue

            token_amount = abs(row["token_delta"])

            sol_amount = abs(info["economic_delta"])

            price_sol = (
                sol_amount / token_amount
                if token_amount > 0
                else 0
            )

            print()
            print("-" * 100)

            print(
                f"{row['side']:4} | "
                f"{row['program']:7} | "
                f"{row['token_mint']}"
            )

            print(
                f"WALLET       : {row['wallet']}"
            )

            print(
                f"TOKENS       : {token_amount:,.9f}"
            )

            print(
                f"SOL raw      : {info['raw_delta']:+.9f}"
            )

            print(
                f"NETWORK FEE  : {info['fee']:.9f}"
            )

            print(
                f"SOL economic : {info['economic_delta']:+.9f}"
            )

            print(
                f"PRICE        : {price_sol:.14f} SOL/token"
            )

            print(
                f"TX           : {row['signature']}"
            )

            # Simple anomaly flags
            if sol_amount <= 0:
                print("⚠️  ANOMALY: zero SOL movement")

            if token_amount <= 0:
                print("⚠️  ANOMALY: zero token movement")

            await asyncio.sleep(0.12)


if __name__ == "__main__":
    asyncio.run(main())
