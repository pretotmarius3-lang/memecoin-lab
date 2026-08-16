import asyncio
import json
from datetime import datetime, timezone

import websockets

WS_URL = "wss://api.mainnet-beta.solana.com/"

RAYDIUM_PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "CLMM": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
}

async def listen_program(name, program_id):
    while True:
        try:
            print(f"[{name}] connexion à Solana...")

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as ws:

                request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [program_id]},
                        {"commitment": "processed"}
                    ],
                }

                await ws.send(json.dumps(request))

                response = json.loads(await ws.recv())

                print(
                    f"[{name}] connecté | "
                    f"subscription={response.get('result')}"
                )

                while True:
                    raw = await ws.recv()
                    message = json.loads(raw)

                    params = message.get("params")

                    if not params:
                        continue

                    result = params.get("result", {})
                    context = result.get("context", {})
                    value = result.get("value", {})

                    signature = value.get("signature")
                    slot = context.get("slot")
                    err = value.get("err")
                    logs = value.get("logs", [])

                    now = datetime.now(timezone.utc).isoformat()

                    print(
                        f"{now} | "
                        f"{name:<7} | "
                        f"slot={slot} | "
                        f"tx={signature} | "
                        f"err={err} | "
                        f"logs={len(logs)}"
                    )

        except Exception as e:
            print(f"[{name}] erreur: {e}")
            print(f"[{name}] reconnexion dans 3 secondes...")
            await asyncio.sleep(3)


async def main():
    print("=" * 75)
    print("MEMECOIN LAB — SOLANA / RAYDIUM COLLECTOR V0")
    print("=" * 75)

    tasks = [
        asyncio.create_task(listen_program(name, program_id))
        for name, program_id in RAYDIUM_PROGRAMS.items()
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCollector arrêté.")
