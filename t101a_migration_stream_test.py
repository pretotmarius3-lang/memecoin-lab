import asyncio
import json
import os

import websockets
from dotenv import load_dotenv

load_dotenv(".env")

WS_URL = os.getenv("SOLANA_WS_URL")

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

PROGRAMS = [
    PUMP_PROGRAM,
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
]


async def main():

    if not WS_URL:
        raise RuntimeError("SOLANA_WS_URL absent du fichier .env")

    print("=" * 80)
    print("MEMECOIN LAB — T101A PUMP MIGRATION STREAM TEST")
    print("=" * 80)

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
                    "accountInclude": PROGRAMS
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
        print("Réponse subscription :")
        print(json.dumps(first, indent=2)[:3000])

        if "error" in first:
            print()
            print("❌ transactionSubscribe refusé.")
            return

        print()
        print("✅ Subscription acceptée")
        print("Subscription ID :", first.get("result"))
        print()
        print("Attente des transactions touchant Pump.fun / Raydium...")
        print()

        count = 0
        pump_count = 0

        while True:

            raw = await ws.recv()
            msg = json.loads(raw)

            params = msg.get("params")
            if not params:
                continue

            result = params.get("result", {})

            count += 1

            blob = json.dumps(result)

            touches_pump = PUMP_PROGRAM in blob

            if touches_pump:
                pump_count += 1

                print()
                print("=" * 80)
                print(f"PUMP TX #{pump_count} | TOTAL TX={count}")
                print("=" * 80)

                print(
                    json.dumps(
                        result,
                        indent=2
                    )[:12000]
                )

                print()

            else:
                print(
                    f"\rTOTAL={count:,} | PUMP={pump_count:,}",
                    end="",
                    flush=True
                )


if __name__ == "__main__":
    asyncio.run(main())
