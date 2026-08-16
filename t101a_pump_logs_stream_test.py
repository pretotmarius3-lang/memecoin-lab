import asyncio
import json
import os

import websockets
from dotenv import load_dotenv

load_dotenv(".env")

WS_URL = os.getenv("SOLANA_WS_URL")

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


async def main():

    if not WS_URL:
        raise RuntimeError(
            "SOLANA_WS_URL absent du fichier .env"
        )

    print("=" * 90)
    print("MEMECOIN LAB — T101A PUMP.FUN LOG STREAM TEST")
    print("=" * 90)

    async with websockets.connect(
        WS_URL,
        ping_interval=30,
        ping_timeout=20,
        max_size=None
    ) as ws:

        request = {
            "jsonrpc": "2.0",
            "id": 101,
            "method": "logsSubscribe",
            "params": [
                {
                    "mentions": [
                        PUMP_PROGRAM
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

        first = json.loads(
            await ws.recv()
        )

        print()
        print("Réponse subscription :")
        print(
            json.dumps(
                first,
                indent=2
            )
        )

        if "error" in first:

            print()
            print(
                "❌ logsSubscribe refusé."
            )

            return

        print()
        print("✅ logsSubscribe accepté")
        print(
            "Subscription ID :",
            first.get("result")
        )
        print()
        print(
            "Attente des transactions Pump.fun..."
        )
        print()

        count = 0

        while True:

            raw = await ws.recv()

            msg = json.loads(raw)

            params = msg.get(
                "params"
            )

            if not params:
                continue

            result = (
                params
                .get("result", {})
                .get("value", {})
            )

            signature = result.get(
                "signature"
            )

            err = result.get(
                "err"
            )

            logs = result.get(
                "logs", []
            )

            if err is not None:
                continue

            count += 1

            text = "\n".join(
                str(x)
                for x in logs
            )

            lower = text.lower()

            looks_migration = (
                "migrate" in lower
                or "migration" in lower
            )

            if looks_migration:

                print()
                print("=" * 90)
                print(
                    f"🚨 POSSIBLE MIGRATION #{count}"
                )
                print("=" * 90)

                print(
                    "SIGNATURE:",
                    signature
                )

                print()

                for log in logs:
                    print(log)

                print()

            else:

                print(
                    f"\rPUMP TX={count:,} "
                    f"| waiting for migration log...",
                    end="",
                    flush=True
                )


if __name__ == "__main__":
    asyncio.run(main())
