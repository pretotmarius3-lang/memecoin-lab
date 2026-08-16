import asyncio
import json
import os

import websockets
from dotenv import load_dotenv

load_dotenv(".env")

WS_URL = os.getenv("SOLANA_WS_URL")

RAYDIUM = [
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # AMM v4
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # CPMM
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # CLMM
]

async def main():

    if not WS_URL:
        raise RuntimeError("SOLANA_WS_URL absent du fichier .env")

    print("=" * 70)
    print("MEMECOIN LAB — TRANSACTION STREAM TEST")
    print("=" * 70)

    async with websockets.connect(
        WS_URL,
        ping_interval=30,
        ping_timeout=20,
        max_size=None
    ) as ws:

        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "transactionSubscribe",
            "params": [
                {
                    "vote": False,
                    "failed": False,
                    "accountInclude": RAYDIUM
                },
                {
                    "commitment": "processed",
                    "encoding": "jsonParsed",
                    "transactionDetails": "full",
                    "showRewards": False,
                    "maxSupportedTransactionVersion": 0
                }
            ]
        }

        await ws.send(json.dumps(request))

        first = json.loads(await ws.recv())

        print()
        print("Réponse Helius :")
        print(json.dumps(first, indent=2)[:3000])

        if "error" in first:
            print()
            print("❌ transactionSubscribe refusé.")
            print("Ton plan Helius actuel ne donne probablement pas accès")
            print("aux Enhanced WebSockets.")
            return

        print()
        print("✅ transactionSubscribe accepté")
        print("Subscription ID :", first.get("result"))
        print()
        print("Attente des transactions Raydium...")
        print()

        count = 0

        while True:

            raw = await ws.recv()
            msg = json.loads(raw)

            params = msg.get("params")
            if not params:
                continue

            result = params.get("result", {})

            count += 1

            print(
                f"\rFULL TX RECEIVED : {count:,}",
                end="",
                flush=True
            )

            if count >= 20:
                print()
                print()
                print("✅ TEST RÉUSSI : 20 transactions complètes reçues.")
                break


if __name__ == "__main__":
    asyncio.run(main())
