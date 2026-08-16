import asyncio
import json
import os
import sqlite3
import time
import hashlib
import statistics
from collections import defaultdict, deque

import aiohttp
import websockets
from dotenv import load_dotenv

load_dotenv(".env")

RPC_URL = os.getenv("SOLANA_RPC_URL")
WS_URL = os.getenv("SOLANA_WS_URL")

if not RPC_URL or not WS_URL:
    raise RuntimeError("RPC / WS absent du .env")

DB = "validation_v090.db"

PROGRAMS = {
    "AMM_V4": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CPMM": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "PUMPSWAP": "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
}

SOL_MINT = "So11111111111111111111111111111111111111112"

EXCLUDED = {
    SOL_MINT,
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERmJfrF4H2FYD7iBFGYpVbR9U4kXQfM8g",
}

# ============================================================
# FROZEN V0.6 SIGNALS
# ============================================================

FA90 = 0.17644751
FA95 = 0.28944273
NF75 = 1.57757355
IMB75 = 1.0

SAMPLE_PERCENT = 25

# Free RPC
RPC_RPS = 4.0
FETCH_DELAY = 1.5
MAX_RETRIES = 4

EVENT_COOLDOWN = 60

# Horizons we actually care about now
HORIZONS = [5, 10, 20, 30, 60, 300]

# Max temporal error accepted for each outcome
TOLERANCE = {
    5: 15,
    10: 15,
    20: 20,
    30: 20,
    60: 30,
    300: 60,
}

# Don't mark an outcome impossible until this window expires
TRACK_SECONDS = 390

MAX_JUMP = 5.0
MIN_JUMP = 0.20
MIN_HISTORY = 3

stats = defaultdict(int)
queue = asyncio.PriorityQueue()

db = sqlite3.connect(
    DB,
    timeout=30,
    check_same_thread=False
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA busy_timeout=5000")

# ============================================================
# DATABASE
# ============================================================

db.executescript("""
CREATE TABLE IF NOT EXISTS signatures (
    signature TEXT PRIMARY KEY,
    slot INTEGER,
    program TEXT,
    received_at REAL,
    status TEXT,
    attempts INTEGER DEFAULT 0
);

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
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    timestamp REAL,
    token_mint TEXT,
    entry_price REAL,

    fa REAL,
    nf30 REAL,
    imbalance30 REAL,
    price_change30 REAL,

    fa90 INTEGER,
    fa95 INTEGER,
    fpa INTEGER,
    extreme INTEGER,

    flow_regime TEXT,

    swaps5 INTEGER,
    swaps10 INTEGER,
    swaps30 INTEGER,
    swaps60 INTEGER,

    buyers5 INTEGER,
    buyers10 INTEGER,
    buyers30 INTEGER,
    buyers60 INTEGER,

    sellers5 INTEGER,
    sellers10 INTEGER,
    sellers30 INTEGER,
    sellers60 INTEGER,

    wallets30 INTEGER,
    wallets60 INTEGER,

    new_wallets10 INTEGER,
    new_wallets30 INTEGER,

    buyer_growth REAL,
    wallet_growth REAL,

    buy_volume30 REAL,
    sell_volume30 REAL,

    largest_buy30 REAL,
    buy_concentration30 REAL,

    return_5s REAL,
    return_10s REAL,
    return_20s REAL,
    return_30s REAL,
    return_60s REAL,
    return_300s REAL,

    delay_5s REAL,
    delay_10s REAL,
    delay_20s REAL,
    delay_30s REAL,
    delay_60s REAL,
    delay_300s REAL,

    done_5s INTEGER DEFAULT 0,
    done_10s INTEGER DEFAULT 0,
    done_20s INTEGER DEFAULT 0,
    done_30s INTEGER DEFAULT 0,
    done_60s INTEGER DEFAULT 0,
    done_300s INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_swap_token_time
ON swaps(token_mint,timestamp);

CREATE INDEX IF NOT EXISTS idx_event_token_time
ON events(token_mint,timestamp);
""")

db.commit()

# ============================================================
# MEMORY
# ============================================================

price_history = defaultdict(
    lambda: deque(maxlen=15)
)

last_event = {}

for r in db.execute("""
SELECT token_mint, clean_price
FROM swaps
WHERE price_valid=1
ORDER BY timestamp
"""):
    price_history[
        r["token_mint"]
    ].append(
        r["clean_price"]
    )

for r in db.execute("""
SELECT token_mint, MAX(timestamp) AS t
FROM events
GROUP BY token_mint
"""):
    last_event[
        r["token_mint"]
    ] = r["t"]

# ============================================================
# SAMPLING
# ============================================================

def take_signature(signature):
    digest = hashlib.sha256(
        signature.encode()
    ).digest()

    n = int.from_bytes(
        digest[:8],
        "big"
    )

    return (
        n % 100
    ) < SAMPLE_PERCENT

# ============================================================
# PARSER
# ============================================================

def get_wallet(tx):
    try:
        keys = (
            tx["transaction"]
            ["message"]
            ["accountKeys"]
        )

        for x in keys:
            if (
                isinstance(x, dict)
                and x.get("signer")
                and x.get("writable")
            ):
                return x["pubkey"]

        for x in keys:
            if (
                isinstance(x, dict)
                and x.get("signer")
            ):
                return x["pubkey"]

    except Exception:
        pass

    return None


def get_token_changes(tx, wallet):
    meta = tx.get("meta", {})

    pre = defaultdict(float)
    post = defaultdict(float)

    for x in meta.get(
        "preTokenBalances", []
    ):
        if x.get("owner") != wallet:
            continue

        try:
            amount = float(
                x["uiTokenAmount"]
                ["uiAmountString"]
            )
        except Exception:
            amount = 0.0

        pre[x["mint"]] += amount

    for x in meta.get(
        "postTokenBalances", []
    ):
        if x.get("owner") != wallet:
            continue

        try:
            amount = float(
                x["uiTokenAmount"]
                ["uiAmountString"]
            )
        except Exception:
            amount = 0.0

        post[x["mint"]] += amount

    changes = {}

    for mint in set(pre) | set(post):
        d = post[mint] - pre[mint]

        if abs(d) > 1e-12:
            changes[mint] = d

    return changes


def get_native_sol_delta(tx, wallet):
    try:
        keys = (
            tx["transaction"]
            ["message"]
            ["accountKeys"]
        )

        index = None

        for i, x in enumerate(keys):
            pubkey = (
                x.get("pubkey")
                if isinstance(x, dict)
                else x
            )

            if pubkey == wallet:
                index = i
                break

        if index is None:
            return 0.0

        meta = tx["meta"]

        delta = (
            meta["postBalances"][index]
            - meta["preBalances"][index]
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
    wallet = get_wallet(tx)

    if not wallet:
        return None

    changes = get_token_changes(
        tx,
        wallet
    )

    native_sol = get_native_sol_delta(
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
            mint not in EXCLUDED
            and abs(delta) > 1e-12
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

    if (
        abs(token_delta) == 0
        or abs(sol_delta) == 0
    ):
        return None

    return {
        "wallet": wallet,
        "side": side,
        "mint": mint,
        "token_delta": token_delta,
        "sol_delta": sol_delta,
        "price":
            abs(sol_delta)
            / abs(token_delta)
    }

# ============================================================
# PRICE CLEANING
# ============================================================

def clean_price(mint, price):
    if (
        price is None
        or price <= 0
    ):
        return False, None, "INVALID"

    hist = price_history[mint]

    if len(hist) >= MIN_HISTORY:
        reference = statistics.median(
            hist
        )

        if reference <= 0:
            return (
                False,
                None,
                "REFERENCE"
            )

        ratio = price / reference

        if ratio > MAX_JUMP:
            return (
                False,
                None,
                "SPIKE"
            )

        if ratio < MIN_JUMP:
            return (
                False,
                None,
                "CRASH"
            )

    hist.append(price)

    return True, price, None

# ============================================================
# FEATURE DATA
# ============================================================

def token_swaps(
    mint,
    now,
    seconds
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
        now-seconds,
        now
    )).fetchall()


def window(events, now, seconds):
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

    buyer_wallets = set(
        x["wallet"] for x in buys
    )

    seller_wallets = set(
        x["wallet"] for x in sells
    )

    wallets = set(
        x["wallet"] for x in data
    )

    bv = sum(
        x["sol"] for x in buys
    )

    sv = sum(
        x["sol"] for x in sells
    )

    nf = bv - sv

    total = bv + sv

    imb = (
        nf / total
        if total
        else 0.0
    )

    largest_buy = max(
        (
            x["sol"]
            for x in buys
        ),
        default=0.0
    )

    concentration = (
        largest_buy / bv
        if bv > 0
        else 0.0
    )

    return {
        "swaps": len(data),

        "buyers": len(
            buyer_wallets
        ),

        "sellers": len(
            seller_wallets
        ),

        "wallets": len(
            wallets
        ),

        "buyer_wallet_set":
            buyer_wallets,

        "wallet_set":
            wallets,

        "bv": bv,
        "sv": sv,

        "nf": nf,
        "imb": imb,

        "largest_buy":
            largest_buy,

        "concentration":
            concentration,
    }


def price_before(
    mint,
    target
):
    row = db.execute("""
        SELECT timestamp, clean_price
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

    if (
        target - row["timestamp"]
        > 20
    ):
        return None

    return row["clean_price"]


def price_change(
    mint,
    now,
    current,
    seconds
):
    old = price_before(
        mint,
        now-seconds
    )

    if (
        old is None
        or old <= 0
    ):
        return None

    return (
        current / old
        - 1
    ) * 100

# ============================================================
# ADOPTION
# ============================================================

def new_wallet_count(
    mint,
    now,
    seconds
):
    """
    Wallets active in the recent window that have
    never appeared for this token before that window.
    """

    cutoff = now - seconds

    recent = db.execute("""
        SELECT DISTINCT wallet
        FROM swaps

        WHERE
            token_mint=?
            AND price_valid=1
            AND timestamp BETWEEN ? AND ?
    """, (
        mint,
        cutoff,
        now
    )).fetchall()

    count = 0

    for row in recent:
        wallet = row["wallet"]

        previous = db.execute("""
            SELECT 1
            FROM swaps

            WHERE
                token_mint=?
                AND wallet=?
                AND price_valid=1
                AND timestamp < ?

            LIMIT 1
        """, (
            mint,
            wallet,
            cutoff
        )).fetchone()

        if previous is None:
            count += 1

    return count

# ============================================================
# EVENT CREATION
# ============================================================

def maybe_event(
    mint,
    now,
    price
):
    previous = last_event.get(mint)

    if (
        previous is not None
        and now - previous
        < EVENT_COOLDOWN
    ):
        return

    events = token_swaps(
        mint,
        now,
        60
    )

    if len(events) < 2:
        return

    w5 = window(
        events, now, 5
    )

    w10 = window(
        events, now, 10
    )

    w30 = window(
        events, now, 30
    )

    w60 = window(
        events, now, 60
    )

    # Same FA definition as frozen discovery
    fv5 = w5["nf"] / 5
    fv10 = w10["nf"] / 10

    fa = fv5 - fv10

    nf = w30["nf"]
    imb = w30["imb"]

    pc30 = price_change(
        mint,
        now,
        price,
        30
    )

    flag90 = int(
        fa >= FA90
    )

    flag95 = int(
        fa >= FA95
    )

    extreme = int(
        nf >= NF75
        and imb >= IMB75
    )

    fpa = int(
        fa >= FA90
        and nf > 0
        and imb > 0
        and (
            pc30 is None
            or pc30 < 10
        )
    )

    if not any([
        flag90,
        flag95,
        extreme,
        fpa
    ]):
        return

    new10 = new_wallet_count(
        mint,
        now,
        10
    )

    new30 = new_wallet_count(
        mint,
        now,
        30
    )

    buyer_growth = (
        w5["buyers"]
        - (
            w30["buyers"]
            / 6
        )
    )

    wallet_growth = (
        new10
        - (
            new30 / 3
        )
    )

    if fa > 0 and nf > 0:
        regime = "FA+_NF+"

    elif fa > 0 and nf < 0:
        regime = "FA+_NF-"

    elif fa < 0 and nf > 0:
        regime = "FA-_NF+"

    else:
        regime = "OTHER"

    db.execute("""
        INSERT INTO events (
            timestamp,
            token_mint,
            entry_price,

            fa,
            nf30,
            imbalance30,
            price_change30,

            fa90,
            fa95,
            fpa,
            extreme,

            flow_regime,

            swaps5,
            swaps10,
            swaps30,
            swaps60,

            buyers5,
            buyers10,
            buyers30,
            buyers60,

            sellers5,
            sellers10,
            sellers30,
            sellers60,

            wallets30,
            wallets60,

            new_wallets10,
            new_wallets30,

            buyer_growth,
            wallet_growth,

            buy_volume30,
            sell_volume30,

            largest_buy30,
            buy_concentration30
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        now,
        mint,
        price,

        fa,
        nf,
        imb,
        pc30,

        flag90,
        flag95,
        fpa,
        extreme,

        regime,

        w5["swaps"],
        w10["swaps"],
        w30["swaps"],
        w60["swaps"],

        w5["buyers"],
        w10["buyers"],
        w30["buyers"],
        w60["buyers"],

        w5["sellers"],
        w10["sellers"],
        w30["sellers"],
        w60["sellers"],

        w30["wallets"],
        w60["wallets"],

        new10,
        new30,

        buyer_growth,
        wallet_growth,

        w30["bv"],
        w30["sv"],

        w30["largest_buy"],
        w30["concentration"]
    ))

    db.commit()

    last_event[mint] = now

    stats["events"] += 1

    print()
    print(
        "🔥 EVENT "
        f"{mint[:12]}..."
        f" | FA={fa:+.3f}"
        f" NF={nf:+.2f}"
        f" NEW10={new10}"
        f" NEW30={new30}"
        f" CONC={w30['concentration']:.2f}"
        f" | "
        f"90={flag90}"
        f" 95={flag95}"
        f" FPA={fpa}"
        f" EXT={extreme}"
    )

# ============================================================
# OUTCOMES
# ============================================================

def nearest_future_price(
    mint,
    target,
    tolerance
):
    """
    We prefer the first valid observed price AFTER target.
    We explicitly record how late that observation is.
    """

    row = db.execute("""
        SELECT
            timestamp,
            clean_price

        FROM swaps

        WHERE
            token_mint=?
            AND price_valid=1
            AND clean_price IS NOT NULL
            AND timestamp >= ?

        ORDER BY timestamp ASC

        LIMIT 1
    """, (
        mint,
        target
    )).fetchone()

    if not row:
        return None, None

    delay = (
        row["timestamp"]
        - target
    )

    if delay > tolerance:
        return None, delay

    return (
        row["clean_price"],
        delay
    )


def get_return(a, b):
    if (
        a is None
        or b is None
        or a <= 0
        or b <= 0
    ):
        return None

    return (
        b / a - 1
    ) * 100


def update_outcomes():
    now = time.time()

    for h in HORIZONS:
        return_col = (
            f"return_{h}s"
        )

        delay_col = (
            f"delay_{h}s"
        )

        done_col = (
            f"done_{h}s"
        )

        rows = db.execute(
            f"""
            SELECT
                id,
                timestamp,
                token_mint,
                entry_price

            FROM events

            WHERE
                {done_col}=0
                AND timestamp <= ?
            """,
            (
                now-h,
            )
        ).fetchall()

        for row in rows:
            price, delay = (
                nearest_future_price(
                    row["token_mint"],
                    row["timestamp"] + h,
                    TOLERANCE[h]
                )
            )

            result = get_return(
                row["entry_price"],
                price
            )

            # We wait until tolerance window is finished
            # before permanently declaring outcome missing.
            expired = (
                now
                >= row["timestamp"]
                + h
                + TOLERANCE[h]
            )

            if (
                result is None
                and not expired
            ):
                continue

            db.execute(
                f"""
                UPDATE events

                SET
                    {return_col}=?,
                    {delay_col}=?,
                    {done_col}=1

                WHERE id=?
                """,
                (
                    result,
                    delay,
                    row["id"]
                )
            )

    db.commit()

# ============================================================
# RPC
# ============================================================

async def rpc_tx(
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

            stats["rpc"] += 1

            if response.status == 429:
                stats["429"] += 1
                return None

            if response.status != 200:
                return None

            body = await response.json()

            return body.get(
                "result"
            )

    except Exception:
        return None

# ============================================================
# FETCHER
# ============================================================

async def fetcher(session):
    interval = 1 / RPC_RPS
    last_request = 0.0

    while True:
        ready, signature = (
            await queue.get()
        )

        wait = ready - time.time()

        if wait > 0:
            await asyncio.sleep(wait)

        row = db.execute("""
            SELECT *
            FROM signatures
            WHERE signature=?
        """, (
            signature,
        )).fetchone()

        if (
            not row
            or row["status"]
            != "WAITING"
        ):
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

        tx = await rpc_tx(
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

                await queue.put((
                    time.time()
                    + min(
                        2**attempts,
                        15
                    ),
                    signature
                ))

            db.commit()
            queue.task_done()
            continue

        if tx.get(
            "meta", {}
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

        valid, price, reason = (
            clean_price(
                swap["mint"],
                swap["price"]
            )
        )

        block_time = tx.get(
            "blockTime"
        )

        timestamp = (
            float(block_time)
            if block_time is not None
            else time.time()
        )

        db.execute("""
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

                raw_price,
                clean_price,

                price_valid,
                reject_reason
            )

            VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            signature,
            timestamp,
            row["slot"],
            row["program"],

            swap["wallet"],
            swap["side"],
            swap["mint"],

            swap["token_delta"],
            swap["sol_delta"],

            swap["price"],
            price,

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

            maybe_event(
                swap["mint"],
                timestamp,
                price
            )

        else:
            stats["reject"] += 1

        queue.task_done()

# ============================================================
# WS
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
                        "method":
                            "logsSubscribe",
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

                hello = json.loads(
                    await ws.recv()
                )

                print(
                    f"[{name}] connected "
                    f"{hello.get('result')}"
                )

                while True:
                    msg = json.loads(
                        await ws.recv()
                    )

                    params = msg.get(
                        "params"
                    )

                    if not params:
                        continue

                    result = params.get(
                        "result", {}
                    )

                    value = result.get(
                        "value", {}
                    )

                    if value.get(
                        "err"
                    ) is not None:
                        continue

                    signature = value.get(
                        "signature"
                    )

                    if not signature:
                        continue

                    stats["ws"] += 1

                    if not take_signature(
                        signature
                    ):
                        continue

                    cur = db.execute("""
                        INSERT OR IGNORE INTO signatures
                        (
                            signature,
                            slot,
                            program,
                            received_at,
                            status
                        )

                        VALUES (
                            ?,?,?,?,?
                        )
                    """, (
                        signature,

                        result.get(
                            "context", {}
                        ).get("slot"),

                        name,

                        time.time(),

                        "WAITING"
                    ))

                    db.commit()

                    if cur.rowcount:
                        stats["sample"] += 1

                        await queue.put((
                            time.time()
                            + FETCH_DELAY,
                            signature
                        ))

        except Exception:
            stats["ws_error"] += 1

            await asyncio.sleep(2)

# ============================================================
# MONITOR
# ============================================================

async def outcome_loop():
    while True:
        await asyncio.sleep(2)
        update_outcomes()


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

        events = db.execute("""
            SELECT COUNT(*)
            FROM events
        """).fetchone()[0]

        fa90 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE fa90=1
        """).fetchone()[0]

        fa95 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE fa95=1
        """).fetchone()[0]

        fpa = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE fpa=1
        """).fetchone()[0]

        ext = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE extreme=1
        """).fetchone()[0]

        outcome_counts = {}

        for h in HORIZONS:
            outcome_counts[h] = (
                db.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM events
                    WHERE return_{h}s
                    IS NOT NULL
                    """
                ).fetchone()[0]
            )

        print()
        print("─" * 100)

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
            f" | REJECT {stats['reject']:,}"
        )

        print(
            f"EVENTS {events:,}"
            f" | FA90 {fa90:,}"
            f" | FA95 {fa95:,}"
            f" | FPA {fpa:,}"
            f" | EXT {ext:,}"
        )

        print(
            "OUTCOMES "
            + " | ".join(
                f"{h}s="
                f"{outcome_counts[h]:,}"
                for h in HORIZONS
            )
        )

# ============================================================
# RECOVERY
# ============================================================

async def recover():
    # Prospective mode:
    # Do NOT inject the historical WAITING backlog into the live queue.
    #
    # Old rows remain untouched in SQLite and can be recovered separately.
    # The live worker therefore starts with an empty queue and processes
    # newly observed signatures instead of chasing an hours-old backlog.

    rows = db.execute("""
        SELECT COUNT(*)
        FROM signatures
        WHERE status='WAITING'
    """).fetchone()[0]

    print(
        f"Historical WAITING preserved: "
        f"{rows:,} | not queued"
    )

# ============================================================
# MAIN
# ============================================================

async def main():
    print()
    print("=" * 100)
    print(
        "MEMECOIN LAB — "
        "V0.9 OUTCOME + ADOPTION"
    )
    print("=" * 100)

    print(
        f"FROZEN "
        f"FA90={FA90}"
        f" | FA95={FA95}"
        f" | NF75={NF75}"
    )

    print(
        f"SAMPLE={SAMPLE_PERCENT}%"
        f" | RPC={RPC_RPS}/s"
    )

    print(
        f"HORIZONS={HORIZONS}"
    )

    print(
        f"DB={DB}"
    )

    print("=" * 100)

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
            )
        ]

        await asyncio.gather(
            *tasks
        )


if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        print(
            "\nV0.9 stopped."
        )

    finally:
        db.commit()
        db.close()
