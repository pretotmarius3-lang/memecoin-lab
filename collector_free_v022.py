import asyncio
import json
import os
import sqlite3
import time
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
    raise RuntimeError(
        "SOLANA_RPC_URL / SOLANA_WS_URL manquant dans .env"
    )

SOL_MINT = "So11111111111111111111111111111111111111112"

# On commence volontairement avec les 2 programmes principaux.
# CLMM pourra être réactivé ensuite.
PROGRAMS = {
    "AMM_V4":
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",

    "CPMM":
        "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
}

# Free Helius = 10 RPC/s.
# On reste sous la limite pour absorber jitter/bursts.
RPC_RPS = 8.0

REQUEST_INTERVAL = 1.0 / RPC_RPS

MAX_RETRIES = 8

DB_FILE = "memecoin_lab_free.db"

stats = defaultdict(int)

shutdown_event = asyncio.Event()


# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(
    DB_FILE,
    timeout=30,
    check_same_thread=False
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")


db.execute("""
CREATE TABLE IF NOT EXISTS signatures (

    signature TEXT PRIMARY KEY,

    slot INTEGER,

    program TEXT,

    received_at REAL,

    status TEXT DEFAULT 'PENDING',

    attempts INTEGER DEFAULT 0,

    next_retry REAL DEFAULT 0,

    last_error TEXT

)
""")


db.execute("""
CREATE INDEX IF NOT EXISTS idx_signatures_status

ON signatures (
    status,
    next_retry,
    received_at
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

    price_sol REAL,

    FOREIGN KEY(signature)
        REFERENCES signatures(signature)

)
""")


db.commit()


# ============================================================
# DATABASE HELPERS
# ============================================================

def save_signature(
    signature,
    slot,
    program
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
                status,
                attempts,
                next_retry
            )
            VALUES (?, ?, ?, ?, 'PENDING', 0, 0)
            """,
            (
                signature,
                slot,
                program,
                time.time()
            )
        )

        db.commit()

        if cur.rowcount:

            stats["signatures_saved"] += 1

            return True

        stats["duplicates"] += 1

        return False

    except Exception:

        stats["db_errors"] += 1

        return False


def get_next_signature():

    now = time.time()

    # Priority = newest transaction currently eligible.
    row = db.execute(
        """
        SELECT
            signature,
            slot,
            program,
            attempts,
            received_at

        FROM signatures

        WHERE
            status = 'PENDING'
            AND next_retry <= ?

        ORDER BY received_at DESC

        LIMIT 1
        """,
        (now,)
    ).fetchone()

    return row


def mark_processing(signature):

    db.execute(
        """
        UPDATE signatures
        SET status = 'PROCESSING'
        WHERE signature = ?
        """,
        (signature,)
    )

    db.commit()


def mark_done(signature):

    db.execute(
        """
        UPDATE signatures
        SET status = 'DONE'
        WHERE signature = ?
        """,
        (signature,)
    )

    db.commit()


def mark_not_swap(signature):

    db.execute(
        """
        UPDATE signatures
        SET status = 'NOT_SWAP'
        WHERE signature = ?
        """,
        (signature,)
    )

    db.commit()


def retry_signature(
    signature,
    attempts,
    reason
):

    attempts += 1

    if attempts >= MAX_RETRIES:

        db.execute(
            """
            UPDATE signatures

            SET
                status = 'FAILED',
                attempts = ?,
                last_error = ?

            WHERE signature = ?
            """,
            (
                attempts,
                reason,
                signature
            )
        )

        db.commit()

        stats["failed"] += 1

        return


    # Progressive retry:
    # 1s, 2s, 4s, 8s ... max 60s
    delay = min(
        2 ** max(0, attempts - 1),
        60
    )

    db.execute(
        """
        UPDATE signatures

        SET
            status = 'PENDING',
            attempts = ?,
            next_retry = ?,
            last_error = ?

        WHERE signature = ?
        """,
        (
            attempts,
            time.time() + delay,
            reason,
            signature
        )
    )

    db.commit()


def recover_interrupted():

    cur = db.execute(
        """
        UPDATE signatures
        SET status = 'PENDING'
        WHERE status = 'PROCESSING'
        """
    )

    db.commit()

    return cur.rowcount


# ============================================================
# WALLET PARSING
# ============================================================

def extract_wallet(tx):

    try:

        accounts = (
            tx["transaction"]
            ["message"]
            ["accountKeys"]
        )

        # Prefer writable signer / fee payer
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
# TOKEN BALANCE CHANGES
# ============================================================

def wallet_token_changes(
    tx,
    wallet
):

    meta = tx.get("meta", {})

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
# NATIVE SOL CHANGE
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

        before = meta[
            "preBalances"
        ][index]

        after = meta[
            "postBalances"
        ][index]


        delta = (
            after - before
        ) / 1_000_000_000


        # Fee payer = first account.
        # Add fee back to estimate economic swap movement.
        if index == 0:

            fee_sol = (
                meta.get("fee", 0)
                / 1_000_000_000
            )

            delta += fee_sol


        return delta


    except Exception:

        return 0.0


# ============================================================
# SWAP CLASSIFIER
# ============================================================

def classify_swap(tx):

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

        for mint, delta
        in changes.items()

        if abs(delta) > 1e-12
    ]


    if not candidates:

        return None


    token_mint, token_delta = max(
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
        token_abs <= 0
        or sol_abs <= 0
    ):

        return None


    price_sol = (
        sol_abs
        / token_abs
    )


    return {

        "wallet":
            wallet,

        "side":
            side,

        "token_mint":
            token_mint,

        "token_delta":
            token_delta,

        "sol_delta":
            sol_delta,

        "price_sol":
            price_sol,
    }


# ============================================================
# RPC
# ============================================================

async def fetch_transaction(
    session,
    signature
):

    payload = {

        "jsonrpc": "2.0",

        "id": 1,

        "method":
            "getTransaction",

        "params": [

            signature,

            {
                "encoding":
                    "jsonParsed",

                "commitment":
                    "confirmed",

                "maxSupportedTransactionVersion":
                    0
            }
        ]
    }


    try:

        async with session.post(
            RPC_URL,
            json=payload,
            timeout=15
        ) as response:


            if response.status == 429:

                stats["rate_limited"] += 1

                return (
                    "RETRY",
                    None,
                    "HTTP_429"
                )


            if response.status != 200:

                stats["http_errors"] += 1

                return (
                    "RETRY",
                    None,
                    f"HTTP_{response.status}"
                )


            data = await response.json()


            if data.get("error"):

                stats["rpc_errors"] += 1

                return (
                    "RETRY",
                    None,
                    str(
                        data["error"].get(
                            "code",
                            "RPC_ERROR"
                        )
                    )
                )


            result = data.get(
                "result"
            )


            if result is None:

                stats["null_result"] += 1

                return (
                    "RETRY",
                    None,
                    "NULL_RESULT"
                )


            stats["fetched"] += 1


            return (
                "OK",
                result,
                None
            )


    except asyncio.TimeoutError:

        stats["timeouts"] += 1

        return (
            "RETRY",
            None,
            "TIMEOUT"
        )


    except Exception:

        stats["http_errors"] += 1

        return (
            "RETRY",
            None,
            "HTTP_EXCEPTION"
        )


# ============================================================
# RATE-LIMITED FETCHER
# ============================================================

async def fetcher(session):

    print(
        f"[RPC] rate limiter = "
        f"{RPC_RPS:.1f} req/s"
    )


    last_request = 0.0


    while not shutdown_event.is_set():

        row = get_next_signature()


        if row is None:

            await asyncio.sleep(
                0.05
            )

            continue


        signature = row[
            "signature"
        ]

        attempts = row[
            "attempts"
        ]


        mark_processing(
            signature
        )


        # ------------------------------------------------
        # GLOBAL RATE LIMIT
        # ------------------------------------------------

        now = time.monotonic()

        elapsed = (
            now - last_request
        )

        wait = (
            REQUEST_INTERVAL
            - elapsed
        )


        if wait > 0:

            await asyncio.sleep(
                wait
            )


        last_request = (
            time.monotonic()
        )


        result_type, tx, error = (
            await fetch_transaction(
                session,
                signature
            )
        )


        stats["rpc_requests"] += 1


        if result_type != "OK":

            retry_signature(
                signature,
                attempts,
                error
            )

            continue


        meta = tx.get(
            "meta",
            {}
        )


        if meta.get("err") is not None:

            mark_not_swap(
                signature
            )

            stats["failed_chain_tx"] += 1

            continue


        swap = classify_swap(
            tx
        )


        if not swap:

            mark_not_swap(
                signature
            )

            stats["not_swap"] += 1

            continue


        try:

            db.execute(
                """
                INSERT OR IGNORE INTO swaps
                (
                    signature,
                    timestamp,
                    slot,
                    program,
                    wallet,
                    side,
                    token_mint,
                    token_delta,
                    sol_delta,
                    price_sol
                )

                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signature,

                    time.time(),

                    row["slot"],

                    row["program"],

                    swap["wallet"],

                    swap["side"],

                    swap["token_mint"],

                    swap["token_delta"],

                    swap["sol_delta"],

                    swap["price_sol"]
                )
            )


            db.commit()


            mark_done(
                signature
            )


            stats["saved"] += 1


            if (
                swap["side"]
                == "BUY"
            ):

                stats["buys"] += 1

            else:

                stats["sells"] += 1


        except Exception:

            stats["db_errors"] += 1

            retry_signature(
                signature,
                attempts,
                "DB_ERROR"
            )


# ============================================================
# WEBSOCKET
# ============================================================

async def listener(
    name,
    program_id
):

    while not shutdown_event.is_set():

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

                    "method":
                        "logsSubscribe",

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
                    json.dumps(
                        request
                    )
                )


                confirmation = json.loads(
                    await ws.recv()
                )


                if confirmation.get(
                    "error"
                ):

                    raise RuntimeError(
                        confirmation[
                            "error"
                        ]
                    )


                print(
                    f"[{name}] connected | "
                    f"subscription="
                    f"{confirmation.get('result')}"
                )


                while not shutdown_event.is_set():

                    raw = (
                        await ws.recv()
                    )


                    message = (
                        json.loads(raw)
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


                    stats["ws_received"] += 1


                    if (
                        value.get("err")
                        is not None
                    ):

                        stats["ws_failed"] += 1

                        continue


                    signature = value.get(
                        "signature"
                    )


                    if not signature:

                        continue


                    save_signature(

                        signature,

                        context.get(
                            "slot"
                        ),

                        name
                    )


        except Exception as e:

            stats["ws_errors"] += 1


            print(
                f"\n[{name}] "
                f"WebSocket reconnecting "
                f"({type(e).__name__})"
            )


            await asyncio.sleep(
                2
            )


# ============================================================
# LIVE METRICS
# ============================================================

def database_metrics():

    result = {}


    for status in [
        "PENDING",
        "PROCESSING",
        "DONE",
        "NOT_SWAP",
        "FAILED",
    ]:

        result[status] = (
            db.execute(
                """
                SELECT COUNT(*)

                FROM signatures

                WHERE status = ?
                """,
                (status,)
            ).fetchone()[0]
        )


    result["TOTAL"] = (
        db.execute(
            """
            SELECT COUNT(*)
            FROM signatures
            """
        ).fetchone()[0]
    )


    result["SWAPS"] = (
        db.execute(
            """
            SELECT COUNT(*)
            FROM swaps
            """
        ).fetchone()[0]
    )


    result["TOKENS"] = (
        db.execute(
            """
            SELECT COUNT(
                DISTINCT token_mint
            )
            FROM swaps
            """
        ).fetchone()[0]
    )


    result["WALLETS"] = (
        db.execute(
            """
            SELECT COUNT(
                DISTINCT wallet
            )
            FROM swaps
            """
        ).fetchone()[0]
    )


    return result


async def monitor():

    last_ws = 0
    last_rpc = 0

    last_time = (
        time.monotonic()
    )


    while not shutdown_event.is_set():

        await asyncio.sleep(
            5
        )


        metrics = (
            database_metrics()
        )


        now = time.monotonic()

        dt = (
            now - last_time
        )


        ws_now = (
            stats["ws_received"]
        )


        rpc_now = (
            stats["rpc_requests"]
        )


        ws_rate = (
            (ws_now - last_ws)
            / dt
        )


        rpc_rate = (
            (rpc_now - last_rpc)
            / dt
        )


        last_ws = ws_now
        last_rpc = rpc_now
        last_time = now


        processed = (
            metrics["DONE"]
            + metrics["NOT_SWAP"]
            + metrics["FAILED"]
        )


        total = metrics[
            "TOTAL"
        ]


        coverage = (

            100.0
            * processed
            / total

            if total
            else 0.0
        )


        print()
        print(
            "─" * 88
        )


        print(
            f"WS {ws_now:,} "
            f"({ws_rate:.1f}/s)"
            f" | RPC {rpc_now:,} "
            f"({rpc_rate:.2f}/s)"
            f" | 429 {stats['rate_limited']:,}"
        )


        print(
            f"SIG {total:,}"
            f" | PENDING {metrics['PENDING']:,}"
            f" | DONE {metrics['DONE']:,}"
            f" | NOT_SWAP {metrics['NOT_SWAP']:,}"
            f" | FAILED {metrics['FAILED']:,}"
        )


        print(
            f"SWAPS {metrics['SWAPS']:,}"
            f" | TOKENS {metrics['TOKENS']:,}"
            f" | WALLETS {metrics['WALLETS']:,}"
            f" | BUY {stats['buys']:,}"
            f" | SELL {stats['sells']:,}"
        )


        print(
            f"COVERAGE {coverage:.2f}%"
            f" | NULL {stats['null_result']:,}"
            f" | RPC_ERR {stats['rpc_errors']:,}"
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    recovered = (
        recover_interrupted()
    )


    print()
    print("=" * 88)
    print(
        "MEMECOIN LAB — "
        "FREE COLLECTOR V0.2.2"
    )
    print("=" * 88)

    print(
        f"Database   : {DB_FILE}"
    )

    print(
        f"RPC budget : {RPC_RPS:.1f} req/s"
    )

    print(
        f"Programs   : "
        f"{', '.join(PROGRAMS.keys())}"
    )

    print(
        f"Recovered  : "
        f"{recovered} interrupted jobs"
    )

    print("=" * 88)
    print()


    connector = aiohttp.TCPConnector(
        limit=4
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


        try:

            await asyncio.gather(
                *tasks
            )

        finally:

            shutdown_event.set()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print()
        print(
            "Collector arrêté proprement."
        )

    finally:

        db.commit()
        db.close()
