import asyncio
import json
import os
import sqlite3
import time
import hashlib
from collections import defaultdict, deque

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

DB_FILE = "validation_v070.db"

PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
}

SOL_MINT = "So11111111111111111111111111111111111111112"

EXCLUDED_MINTS = {
    SOL_MINT,
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD7iBFGYpVbR9U4kXQfM8g",
}

# Free Helius
RPC_RPS = 4.0
SAMPLE_PERCENT = 25
FETCH_DELAY = 2.0
MAX_RETRIES = 4

# ============================================================
# FROZEN SIGNALS FROM V0.6
# DO NOT CHANGE DURING VALIDATION
# ============================================================

FA_P90 = 0.17644751
FA_P95 = 0.28944273
NF_P75 = 1.57757355

# Previous P75 was exactly 1.0
IMB_P75 = 1.0

# Independent-event cooldown
SIGNAL_COOLDOWN = 60

# Price sanity
MAX_JUMP_RATIO = 5.0
MIN_JUMP_RATIO = 0.20
MIN_PRICE_HISTORY = 3

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
db.execute("PRAGMA busy_timeout=5000")

# ============================================================
# TABLES
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
    raw_price REAL,
    clean_price REAL,
    price_valid INTEGER,
    reject_reason TEXT
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    timestamp REAL,
    token_mint TEXT,

    signal_type TEXT,

    price REAL,

    flow_accel_fast REAL,
    net_flow_30 REAL,
    imbalance_30 REAL,
    price_change_30 REAL,

    buyers_5 INTEGER,
    buyers_10 INTEGER,
    buyers_30 INTEGER,

    return_10s REAL,
    return_30s REAL,
    return_60s REAL,
    return_300s REAL,

    done_10 INTEGER DEFAULT 0,
    done_30 INTEGER DEFAULT 0,
    done_60 INTEGER DEFAULT 0,
    done_300 INTEGER DEFAULT 0
)
""")

db.execute("""
CREATE INDEX IF NOT EXISTS idx_swap_token_time
ON swaps(token_mint, timestamp)
""")

db.execute("""
CREATE INDEX IF NOT EXISTS idx_signal_token_time
ON signals(token_mint, timestamp)
""")

db.commit()

# ============================================================
# MEMORY
# ============================================================

price_history = defaultdict(
    lambda: deque(maxlen=15)
)

last_signal = {}

# Recover price history if restarted
for row in db.execute("""
SELECT token_mint, clean_price
FROM swaps
WHERE price_valid=1
ORDER BY timestamp ASC
"""):

    price_history[
        row["token_mint"]
    ].append(
        row["clean_price"]
    )

# Recover cooldown
for row in db.execute("""
SELECT
    token_mint,
    signal_type,
    MAX(timestamp) AS t
FROM signals
GROUP BY token_mint, signal_type
"""):

    last_signal[
        (
            row["token_mint"],
            row["signal_type"]
        )
    ] = row["t"]

# ============================================================
# SAMPLER
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
# PARSER
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


def wallet_token_changes(tx, wallet):

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


def native_sol_delta(tx, wallet):

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

        before = meta["preBalances"][index]
        after = meta["postBalances"][index]

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
        if (
            abs(delta) > 1e-12
            and mint not in EXCLUDED_MINTS
        )
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

    token_abs = abs(token_delta)
    sol_abs = abs(sol_delta)

    if token_abs == 0 or sol_abs == 0:
        return None

    return {
        "wallet": wallet,
        "side": side,
        "mint": mint,
        "token_delta": token_delta,
        "sol_delta": sol_delta,
        "raw_price": sol_abs / token_abs
    }

# ============================================================
# PRICE VALIDATION
# ============================================================

def validate_price(mint, raw_price):

    if mint in EXCLUDED_MINTS:
        return False, None, "EXCLUDED"

    if (
        raw_price is None
        or raw_price <= 0
    ):
        return False, None, "INVALID"

    hist = price_history[mint]

    if len(hist) >= MIN_PRICE_HISTORY:

        sorted_hist = sorted(hist)

        reference = (
            sorted_hist[
                len(sorted_hist) // 2
            ]
        )

        if reference <= 0:
            return (
                False,
                None,
                "REFERENCE"
            )

        ratio = (
            raw_price / reference
        )

        if ratio > MAX_JUMP_RATIO:
            return (
                False,
                None,
                "SPIKE"
            )

        if ratio < MIN_JUMP_RATIO:
            return (
                False,
                None,
                "CRASH"
            )

    hist.append(raw_price)

    return True, raw_price, None

# ============================================================
# FEATURE HELPERS
# ============================================================

def recent_token_swaps(
    mint,
    now,
    seconds=60
):

    return db.execute("""
        SELECT
            timestamp,
            wallet,
            side,
            ABS(sol_delta) AS sol,
            clean_price

        FROM swaps

        WHERE
            token_mint=?
            AND price_valid=1
            AND timestamp BETWEEN ? AND ?

        ORDER BY timestamp ASC
    """, (
        mint,
        now - seconds,
        now
    )).fetchall()


def calc_window(
    events,
    now,
    seconds
):

    cutoff = now - seconds

    data = [
        x for x in events
        if x["timestamp"] >= cutoff
    ]

    buys = [
        x for x in data
        if x["side"] == "BUY"
    ]

    sells = [
        x for x in data
        if x["side"] == "SELL"
    ]

    buyers = len(set(
        x["wallet"]
        for x in buys
    ))

    sellers = len(set(
        x["wallet"]
        for x in sells
    ))

    bv = sum(
        x["sol"]
        for x in buys
    )

    sv = sum(
        x["sol"]
        for x in sells
    )

    nf = bv - sv

    total = bv + sv

    imbalance = (
        nf / total
        if total > 0
        else 0.0
    )

    return {
        "buyers": buyers,
        "sellers": sellers,
        "buy_vol": bv,
        "sell_vol": sv,
        "net_flow": nf,
        "imbalance": imbalance,
    }


def historical_price(
    mint,
    target
):

    row = db.execute("""
        SELECT
            timestamp,
            clean_price

        FROM swaps

        WHERE
            token_mint=?
            AND price_valid=1
            AND timestamp <= ?

        ORDER BY timestamp DESC

        LIMIT 1
    """, (
        mint,
        target
    )).fetchone()

    if not row:
        return None

    # Don't use ancient observations
    if target - row["timestamp"] > 15:
        return None

    return row["clean_price"]


def price_change_30(
    mint,
    now,
    current
):

    old = historical_price(
        mint,
        now - 30
    )

    if (
        old is None
        or old <= 0
        or current <= 0
    ):
        return None

    return (
        current / old
        - 1
    ) * 100

# ============================================================
# SIGNAL ENGINE
# ============================================================

def cooldown_ok(
    mint,
    signal,
    now
):

    previous = last_signal.get(
        (mint, signal)
    )

    if previous is None:
        return True

    return (
        now - previous
        >= SIGNAL_COOLDOWN
    )


def save_signal(
    mint,
    signal,
    now,
    price,
    fa,
    nf30,
    imb30,
    pc30,
    s5,
    s10,
    s30
):

    if not cooldown_ok(
        mint,
        signal,
        now
    ):
        return False

    db.execute("""
        INSERT INTO signals (
            timestamp,
            token_mint,
            signal_type,
            price,

            flow_accel_fast,
            net_flow_30,
            imbalance_30,
            price_change_30,

            buyers_5,
            buyers_10,
            buyers_30
        )

        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        now,
        mint,
        signal,
        price,

        fa,
        nf30,
        imb30,
        pc30,

        s5["buyers"],
        s10["buyers"],
        s30["buyers"]
    ))

    db.commit()

    last_signal[
        (mint, signal)
    ] = now

    stats[
        f"signal_{signal}"
    ] += 1

    print(
        f"\n🔥 SIGNAL {signal}"
        f" | {mint[:12]}..."
        f" | FA={fa:+.4f}"
        f" | NF30={nf30:+.3f}"
        f" | IMB={imb30:+.3f}"
    )

    return True


def evaluate_signals(
    mint,
    now,
    current_price
):

    events = recent_token_swaps(
        mint,
        now,
        60
    )

    if len(events) < 2:
        return

    s5 = calc_window(
        events,
        now,
        5
    )

    s10 = calc_window(
        events,
        now,
        10
    )

    s30 = calc_window(
        events,
        now,
        30
    )

    fv5 = (
        s5["net_flow"] / 5
    )

    fv10 = (
        s10["net_flow"] / 10
    )

    fa = fv5 - fv10

    pc30 = price_change_30(
        mint,
        now,
        current_price
    )

    # --------------------------------------------------------
    # FROZEN SIGNAL 1 — FA P90
    # --------------------------------------------------------

    if fa >= FA_P90:

        save_signal(
            mint,
            "FA_P90",
            now,
            current_price,
            fa,
            s30["net_flow"],
            s30["imbalance"],
            pc30,
            s5,
            s10,
            s30
        )

    # --------------------------------------------------------
    # FROZEN SIGNAL 2 — FA P95
    # --------------------------------------------------------

    if fa >= FA_P95:

        save_signal(
            mint,
            "FA_P95",
            now,
            current_price,
            fa,
            s30["net_flow"],
            s30["imbalance"],
            pc30,
            s5,
            s10,
            s30
        )

    # --------------------------------------------------------
    # FROZEN SIGNAL 3 — EXTREME BUY PRESSURE
    # --------------------------------------------------------

    if (
        s30["net_flow"] >= NF_P75
        and s30["imbalance"] >= IMB_P75
    ):

        save_signal(
            mint,
            "EXTREME_BUY",
            now,
            current_price,
            fa,
            s30["net_flow"],
            s30["imbalance"],
            pc30,
            s5,
            s10,
            s30
        )

    # --------------------------------------------------------
    # FROZEN SIGNAL 4 — FPA
    # --------------------------------------------------------

    if (
        fa >= FA_P90
        and s30["net_flow"] > 0
        and s30["imbalance"] > 0
        and (
            pc30 is None
            or pc30 < 10
        )
    ):

        save_signal(
            mint,
            "FPA",
            now,
            current_price,
            fa,
            s30["net_flow"],
            s30["imbalance"],
            pc30,
            s5,
            s10,
            s30
        )

# ============================================================
# OUTCOMES
# ============================================================

def future_price(
    mint,
    target
):

    row = db.execute("""
        SELECT
            timestamp,
            clean_price

        FROM swaps

        WHERE
            token_mint=?
            AND price_valid=1
            AND timestamp >= ?

        ORDER BY timestamp ASC

        LIMIT 1
    """, (
        mint,
        target
    )).fetchone()

    if not row:
        return None

    if (
        row["timestamp"]
        - target
        > 15
    ):
        return None

    return row["clean_price"]


def calc_return(
    start,
    future
):

    if (
        start is None
        or future is None
        or start <= 0
        or future <= 0
    ):
        return None

    return (
        future / start
        - 1
    ) * 100


def update_outcomes():

    now = time.time()

    configs = [
        (
            10,
            "return_10s",
            "done_10"
        ),
        (
            30,
            "return_30s",
            "done_30"
        ),
        (
            60,
            "return_60s",
            "done_60"
        ),
        (
            300,
            "return_300s",
            "done_300"
        ),
    ]

    for (
        seconds,
        return_col,
        done_col
    ) in configs:

        rows = db.execute(
            f"""
            SELECT
                id,
                timestamp,
                token_mint,
                price

            FROM signals

            WHERE
                {done_col}=0
                AND timestamp <= ?

            LIMIT 1000
            """,
            (
                now - seconds,
            )
        ).fetchall()

        for row in rows:

            fp = future_price(
                row["token_mint"],
                row["timestamp"]
                + seconds
            )

            result = calc_return(
                row["price"],
                fp
            )

            db.execute(
                f"""
                UPDATE signals
                SET
                    {return_col}=?,
                    {done_col}=1
                WHERE id=?
                """,
                (
                    result,
                    row["id"]
                )
            )

        db.commit()

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
                return None

            if response.status != 200:
                return None

            data = await response.json()

            return data.get("result")

    except Exception:
        return None

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

        wait = (
            ready_at - time.time()
        )

        if wait > 0:
            await asyncio.sleep(wait)

        row = db.execute("""
            SELECT *
            FROM signatures
            WHERE signature=?
        """, (
            signature,
        )).fetchone()

        if not row:
            queue.task_done()
            continue

        if row["status"] != "WAITING":
            queue.task_done()
            continue

        elapsed = (
            time.monotonic()
            - last_request
        )

        if elapsed < interval:
            await asyncio.sleep(
                interval - elapsed
            )

        last_request = (
            time.monotonic()
        )

        tx = await get_transaction(
            session,
            signature
        )

        if tx is None:

            attempts = (
                row["attempts"] + 1
            )

            if attempts >= MAX_RETRIES:

                db.execute("""
                    UPDATE signatures
                    SET
                        status='FAILED',
                        attempts=?
                    WHERE signature=?
                """, (
                    attempts,
                    signature
                ))

                stats["failed"] += 1

            else:

                db.execute("""
                    UPDATE signatures
                    SET attempts=?
                    WHERE signature=?
                """, (
                    attempts,
                    signature
                ))

                await queue.put(
                    (
                        time.time()
                        + min(
                            2 ** attempts,
                            15
                        ),
                        signature
                    )
                )

            db.commit()
            queue.task_done()
            continue

        if tx.get(
            "meta",
            {}
        ).get("err") is not None:

            db.execute("""
                UPDATE signatures
                SET status='NOT_SWAP'
                WHERE signature=?
            """, (
                signature,
            ))

            db.commit()

            queue.task_done()
            continue

        swap = classify(tx)

        if not swap:

            db.execute("""
                UPDATE signatures
                SET status='NOT_SWAP'
                WHERE signature=?
            """, (
                signature,
            ))

            db.commit()

            queue.task_done()
            continue

        valid, clean, reason = (
            validate_price(
                swap["mint"],
                swap["raw_price"]
            )
        )

        now = time.time()

        db.execute("""
            INSERT OR IGNORE INTO swaps
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signature,
            now,
            row["slot"],
            row["program"],

            swap["wallet"],
            swap["side"],
            swap["mint"],

            swap["token_delta"],
            swap["sol_delta"],

            swap["raw_price"],
            clean,
            int(valid),
            reason
        ))

        db.execute("""
            UPDATE signatures
            SET status='DONE'
            WHERE signature=?
        """, (
            signature,
        ))

        db.commit()

        stats["swaps"] += 1

        if valid:

            stats["clean"] += 1

            evaluate_signals(
                swap["mint"],
                now,
                clean
            )

        else:

            stats["rejected_price"] += 1

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

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=None
            ) as ws:

                await ws.send(
                    json.dumps({
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
                    })
                )

                response = json.loads(
                    await ws.recv()
                )

                print(
                    f"[{name}] connected "
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

                    if value.get("err") is not None:
                        continue

                    signature = value.get(
                        "signature"
                    )

                    if not signature:
                        continue

                    stats["ws"] += 1

                    eligible = (
                        is_eligible(
                            signature
                        )
                    )

                    cur = db.execute("""
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
                    """, (
                        signature,
                        result.get(
                            "context",
                            {}
                        ).get("slot"),
                        name,
                        time.time(),
                        int(eligible),
                        (
                            "WAITING"
                            if eligible
                            else "REJECTED"
                        )
                    ))

                    db.commit()

                    if (
                        cur.rowcount
                        and eligible
                    ):

                        stats[
                            "sample"
                        ] += 1

                        await queue.put(
                            (
                                time.time()
                                + FETCH_DELAY,
                                signature
                            )
                        )

        except Exception:

            stats["ws_error"] += 1

            await asyncio.sleep(2)

# ============================================================
# OUTCOME LOOP
# ============================================================

async def outcome_loop():

    while True:

        await asyncio.sleep(5)

        update_outcomes()

# ============================================================
# MONITOR
# ============================================================

async def monitor():

    while True:

        await asyncio.sleep(10)

        swaps = db.execute("""
            SELECT COUNT(*)
            FROM swaps
        """).fetchone()[0]

        clean = db.execute("""
            SELECT COUNT(*)
            FROM swaps
            WHERE price_valid=1
        """).fetchone()[0]

        signals = db.execute("""
            SELECT COUNT(*)
            FROM signals
        """).fetchone()[0]

        fa90 = db.execute("""
            SELECT COUNT(*)
            FROM signals
            WHERE signal_type='FA_P90'
        """).fetchone()[0]

        fa95 = db.execute("""
            SELECT COUNT(*)
            FROM signals
            WHERE signal_type='FA_P95'
        """).fetchone()[0]

        extreme = db.execute("""
            SELECT COUNT(*)
            FROM signals
            WHERE signal_type='EXTREME_BUY'
        """).fetchone()[0]

        fpa = db.execute("""
            SELECT COUNT(*)
            FROM signals
            WHERE signal_type='FPA'
        """).fetchone()[0]

        r60 = db.execute("""
            SELECT COUNT(*)
            FROM signals
            WHERE return_60s IS NOT NULL
        """).fetchone()[0]

        r300 = db.execute("""
            SELECT COUNT(*)
            FROM signals
            WHERE return_300s IS NOT NULL
        """).fetchone()[0]

        print()
        print("─" * 90)

        print(
            f"WS {stats['ws']:,}"
            f" | SAMPLE {stats['sample']:,}"
            f" | QUEUE {queue.qsize():,}"
            f" | RPC {stats['rpc']:,}"
            f" | 429 {stats['429']:,}"
        )

        print(
            f"SWAPS {swaps:,}"
            f" | CLEAN {clean:,}"
            f" | PRICE REJECT {stats['rejected_price']:,}"
        )

        print(
            f"SIGNALS {signals:,}"
            f" | FA90 {fa90:,}"
            f" | FA95 {fa95:,}"
            f" | EXTREME {extreme:,}"
            f" | FPA {fpa:,}"
        )

        print(
            f"OUTCOMES 60s={r60:,}"
            f" | 300s={r300:,}"
        )

# ============================================================
# RECOVERY
# ============================================================

async def recover():

    rows = db.execute("""
        SELECT signature
        FROM signatures
        WHERE
            eligible=1
            AND status='WAITING'
    """).fetchall()

    for row in rows:

        await queue.put(
            (
                time.time(),
                row["signature"]
            )
        )

    print(
        f"Recovered: {len(rows):,}"
    )

# ============================================================
# MAIN
# ============================================================

async def main():

    print()
    print("=" * 90)
    print(
        "MEMECOIN LAB — "
        "OUT-OF-SAMPLE VALIDATION V0.7"
    )
    print("=" * 90)

    print(
        "FROZEN:"
        f" FA90={FA_P90}"
        f" | FA95={FA_P95}"
        f" | NF75={NF_P75}"
        f" | IMB75={IMB_P75}"
    )

    print(
        f"SAMPLE={SAMPLE_PERCENT}%"
        f" | RPC={RPC_RPS}/s"
        f" | COOLDOWN={SIGNAL_COOLDOWN}s"
    )

    print(
        f"DATABASE={DB_FILE}"
    )

    print("=" * 90)

    await recover()

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

        tasks += [
            asyncio.create_task(
                fetcher(session)
            ),
            asyncio.create_task(
                outcome_loop()
            ),
            asyncio.create_task(
                monitor()
            ),
        ]

        await asyncio.gather(
            *tasks
        )


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print(
            "\nValidation stopped."
        )

    finally:
        db.commit()
        db.close()
