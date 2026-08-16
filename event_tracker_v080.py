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

# ============================================================
# CONFIG
# ============================================================

load_dotenv(".env")

RPC_URL = os.getenv("SOLANA_RPC_URL")
WS_URL = os.getenv("SOLANA_WS_URL")

if not RPC_URL or not WS_URL:
    raise RuntimeError("SOLANA_RPC_URL / SOLANA_WS_URL absents du .env")

DB_FILE = "validation_v080.db"

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

# ------------------------------------------------------------
# FROZEN V0.6 THRESHOLDS — DO NOT OPTIMIZE DURING OOS
# ------------------------------------------------------------

FA_P90 = 0.17644751
FA_P95 = 0.28944273
NF_P75 = 1.57757355
IMB_P75 = 1.0

# Global discovery sampler
SAMPLE_PERCENT = 25

# Free-plan RPC protection
RPC_RPS = 4.0
FETCH_DELAY = 2.0
MAX_RETRIES = 4

# One independent event / token / 60 seconds
EVENT_COOLDOWN = 60

# Keep active tokens for outcome tracking
TRACK_SECONDS = 390

# Price sanity
MAX_JUMP_RATIO = 5.0
MIN_JUMP_RATIO = 0.20
MIN_PRICE_HISTORY = 3

# Outcome horizon -> max accepted timing error
HORIZONS = {
    10: 15,
    30: 15,
    60: 20,
    300: 30,
}

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
# DATABASE
# ============================================================

db.executescript("""
CREATE TABLE IF NOT EXISTS signatures (
    signature TEXT PRIMARY KEY,
    slot INTEGER,
    program TEXT,
    received_at REAL,
    sampled INTEGER,
    tracked INTEGER,
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

    flag_fa90 INTEGER DEFAULT 0,
    flag_fa95 INTEGER DEFAULT 0,
    flag_fpa INTEGER DEFAULT 0,
    flag_extreme INTEGER DEFAULT 0,

    flow_accel_fast REAL,
    net_flow_30 REAL,
    imbalance_30 REAL,
    price_change_30 REAL,

    flow_regime TEXT,

    buyers_5 INTEGER,
    buyers_10 INTEGER,
    buyers_30 INTEGER,

    return_10s REAL,
    return_30s REAL,
    return_60s REAL,
    return_300s REAL,

    delay_10s REAL,
    delay_30s REAL,
    delay_60s REAL,
    delay_300s REAL,

    done_10s INTEGER DEFAULT 0,
    done_30s INTEGER DEFAULT 0,
    done_60s INTEGER DEFAULT 0,
    done_300s INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_swaps_token_time
ON swaps(token_mint, timestamp);

CREATE INDEX IF NOT EXISTS idx_events_token_time
ON events(token_mint, timestamp);
""")

db.commit()

# ============================================================
# MEMORY
# ============================================================

price_history = defaultdict(
    lambda: deque(maxlen=15)
)

active_tokens = {}
last_event = {}

for r in db.execute("""
SELECT token_mint, clean_price
FROM swaps
WHERE price_valid=1
ORDER BY timestamp ASC
"""):
    price_history[r["token_mint"]].append(
        r["clean_price"]
    )

for r in db.execute("""
SELECT token_mint, MAX(timestamp) AS t
FROM events
GROUP BY token_mint
"""):
    last_event[r["token_mint"]] = r["t"]

for r in db.execute("""
SELECT token_mint, MAX(timestamp) AS t
FROM events
GROUP BY token_mint
"""):
    active_tokens[r["token_mint"]] = (
        r["t"] + TRACK_SECONDS
    )

# ============================================================
# DETERMINISTIC SAMPLING
# ============================================================

def sampled(signature):
    h = hashlib.sha256(
        signature.encode()
    ).digest()

    n = int.from_bytes(
        h[:8],
        "big"
    )

    return (
        n % 100
    ) < SAMPLE_PERCENT

# ============================================================
# TX PARSER
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


def token_changes(tx, wallet):
    meta = tx.get("meta", {})

    pre = defaultdict(float)
    post = defaultdict(float)

    for x in meta.get(
        "preTokenBalances", []
    ):
        if x.get("owner") != wallet:
            continue

        try:
            value = float(
                x["uiTokenAmount"]
                ["uiAmountString"]
            )
        except Exception:
            value = 0.0

        pre[x.get("mint")] += value

    for x in meta.get(
        "postTokenBalances", []
    ):
        if x.get("owner") != wallet:
            continue

        try:
            value = float(
                x["uiTokenAmount"]
                ["uiAmountString"]
            )
        except Exception:
            value = 0.0

        post[x.get("mint")] += value

    result = {}

    for mint in set(pre) | set(post):
        d = post[mint] - pre[mint]

        if abs(d) > 1e-12:
            result[mint] = d

    return result


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

        delta = (
            meta["postBalances"][index]
            - meta["preBalances"][index]
        ) / 1_000_000_000

        # Remove network fee from trader SOL delta
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

    changes = token_changes(
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
            mint
            and mint not in EXCLUDED_MINTS
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
        abs(token_delta) <= 0
        or abs(sol_delta) <= 0
    ):
        return None

    return {
        "wallet": wallet,
        "side": side,
        "mint": mint,
        "token_delta": token_delta,
        "sol_delta": sol_delta,
        "raw_price":
            abs(sol_delta)
            / abs(token_delta)
    }

# ============================================================
# PRICE FILTER
# ============================================================

def validate_price(mint, price):
    if (
        price is None
        or price <= 0
    ):
        return False, None, "INVALID"

    hist = price_history[mint]

    if len(hist) >= MIN_PRICE_HISTORY:
        ref = statistics.median(hist)

        if ref <= 0:
            return False, None, "REFERENCE"

        ratio = price / ref

        if ratio > MAX_JUMP_RATIO:
            return False, None, "SPIKE"

        if ratio < MIN_JUMP_RATIO:
            return False, None, "CRASH"

    hist.append(price)

    return True, price, None

# ============================================================
# FEATURES
# ============================================================

def recent_swaps(
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
        now-seconds,
        now
    )).fetchall()


def calc_window(events, now, seconds):
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
        x["wallet"] for x in buys
    ))

    sellers = len(set(
        x["wallet"] for x in sells
    ))

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

    return {
        "buyers": buyers,
        "sellers": sellers,
        "buy_vol": bv,
        "sell_vol": sv,
        "net_flow": nf,
        "imbalance": imb,
    }


def historical_price(
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

    if target - row["timestamp"] > 15:
        return None

    return row["clean_price"]


def get_pc30(
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
        current / old - 1
    ) * 100

# ============================================================
# EVENT ENGINE
# ============================================================

def create_event(
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

    events = recent_swaps(
        mint,
        now,
        60
    )

    if len(events) < 2:
        return

    s5 = calc_window(
        events, now, 5
    )

    s10 = calc_window(
        events, now, 10
    )

    s30 = calc_window(
        events, now, 30
    )

    fv5 = s5["net_flow"] / 5
    fv10 = s10["net_flow"] / 10

    fa = fv5 - fv10
    nf = s30["net_flow"]
    imb = s30["imbalance"]

    pc30 = get_pc30(
        mint,
        now,
        price
    )

    flag_fa90 = int(
        fa >= FA_P90
    )

    flag_fa95 = int(
        fa >= FA_P95
    )

    flag_extreme = int(
        nf >= NF_P75
        and imb >= IMB_P75
    )

    flag_fpa = int(
        fa >= FA_P90
        and nf > 0
        and imb > 0
        and (
            pc30 is None
            or pc30 < 10
        )
    )

    # No signal = no event
    if not any([
        flag_fa90,
        flag_fa95,
        flag_extreme,
        flag_fpa
    ]):
        return

    if fa > 0 and nf > 0:
        regime = "FA_POS_NF_POS"

    elif fa > 0 and nf < 0:
        regime = "FA_POS_NF_NEG"

    elif fa < 0 and nf > 0:
        regime = "FA_NEG_NF_POS"

    else:
        regime = "OTHER"

    db.execute("""
        INSERT INTO events (
            timestamp,
            token_mint,
            entry_price,

            flag_fa90,
            flag_fa95,
            flag_fpa,
            flag_extreme,

            flow_accel_fast,
            net_flow_30,
            imbalance_30,
            price_change_30,

            flow_regime,

            buyers_5,
            buyers_10,
            buyers_30
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        now,
        mint,
        price,

        flag_fa90,
        flag_fa95,
        flag_fpa,
        flag_extreme,

        fa,
        nf,
        imb,
        pc30,

        regime,

        s5["buyers"],
        s10["buyers"],
        s30["buyers"]
    ))

    db.commit()

    last_event[mint] = now

    # This token is now tracked for the next ~6.5 min
    active_tokens[mint] = (
        now + TRACK_SECONDS
    )

    stats["events"] += 1

    print()
    print(
        "🔥 EVENT"
        f" | {mint[:12]}..."
        f" | FA={fa:+.4f}"
        f" | NF={nf:+.3f}"
        f" | IMB={imb:+.3f}"
        f" | "
        f"FA90={flag_fa90}"
        f" FA95={flag_fa95}"
        f" FPA={flag_fpa}"
        f" EXT={flag_extreme}"
    )

# ============================================================
# OUTCOME ENGINE
# ============================================================

def outcome_price(
    mint,
    target,
    tolerance
):
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


def calc_return(a, b):
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

    for seconds, tolerance in HORIZONS.items():
        rcol = f"return_{seconds}s"
        dcol = f"delay_{seconds}s"
        done = f"done_{seconds}s"

        rows = db.execute(
            f"""
            SELECT
                id,
                timestamp,
                token_mint,
                entry_price

            FROM events

            WHERE
                {done}=0
                AND timestamp <= ?
            """,
            (
                now - seconds,
            )
        ).fetchall()

        for row in rows:
            fp, delay = outcome_price(
                row["token_mint"],
                row["timestamp"] + seconds,
                tolerance
            )

            result = calc_return(
                row["entry_price"],
                fp
            )

            # Don't mark missing outcome as permanently done
            # until tracking window has elapsed.
            expired = (
                now
                >= row["timestamp"]
                + seconds
                + tolerance
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
                    {rcol}=?,
                    {dcol}=?,
                    {done}=1
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
# QUEUE / FETCH
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
            or row["status"] != "WAITING"
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

        last_request = time.monotonic()

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

                await queue.put((
                    time.time()
                    + min(
                        2 ** attempts,
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

        valid, clean, reason = (
            validate_price(
                swap["mint"],
                swap["raw_price"]
            )
        )

        # Use approximate observed-chain time.
        block_time = tx.get("blockTime")

        observed_time = (
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
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            signature,
            observed_time,
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

            create_event(
                swap["mint"],
                observed_time,
                clean
            )

        else:
            stats["price_reject"] += 1

        queue.task_done()

# ============================================================
# WEBSOCKET
# ============================================================

async def listener(name, program):
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

                    if value.get("err") is not None:
                        continue

                    signature = value.get(
                        "signature"
                    )

                    if not signature:
                        continue

                    stats["ws"] += 1

                    # ------------------------------------------------
                    # 25% global discovery
                    #
                    # NOTE:
                    # logsSubscribe gives program-level transactions.
                    # We cannot know token mint before fetching TX.
                    #
                    # Therefore "tracked" below means signatures already
                    # selected globally. True 100%-token tracking would
                    # require token/account-specific subscription logic.
                    # ------------------------------------------------

                    take = sampled(
                        signature
                    )

                    if not take:
                        continue

                    cur = db.execute("""
                        INSERT OR IGNORE INTO signatures
                        (
                            signature,
                            slot,
                            program,
                            received_at,
                            sampled,
                            tracked,
                            status
                        )
                        VALUES (?,?,?,?,?,?,?)
                    """, (
                        signature,
                        result.get(
                            "context", {}
                        ).get("slot"),
                        name,
                        time.time(),
                        1,
                        0,
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
        await asyncio.sleep(3)
        update_outcomes()


async def monitor():
    while True:
        await asyncio.sleep(10)

        now = time.time()

        # Remove expired tracking markers
        expired = [
            mint
            for mint, until
            in active_tokens.items()
            if until < now
        ]

        for mint in expired:
            active_tokens.pop(
                mint,
                None
            )

        swaps = db.execute("""
            SELECT COUNT(*) FROM swaps
        """).fetchone()[0]

        clean = db.execute("""
            SELECT COUNT(*)
            FROM swaps
            WHERE price_valid=1
        """).fetchone()[0]

        events = db.execute("""
            SELECT COUNT(*) FROM events
        """).fetchone()[0]

        fa90 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE flag_fa90=1
        """).fetchone()[0]

        fa95 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE flag_fa95=1
        """).fetchone()[0]

        fpa = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE flag_fpa=1
        """).fetchone()[0]

        extreme = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE flag_extreme=1
        """).fetchone()[0]

        o10 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE return_10s IS NOT NULL
        """).fetchone()[0]

        o30 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE return_30s IS NOT NULL
        """).fetchone()[0]

        o60 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE return_60s IS NOT NULL
        """).fetchone()[0]

        o300 = db.execute("""
            SELECT COUNT(*)
            FROM events
            WHERE return_300s IS NOT NULL
        """).fetchone()[0]

        print()
        print("─" * 96)

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
            f" | PRICE_REJECT "
            f"{stats['price_reject']:,}"
        )

        print(
            f"EVENTS {events:,}"
            f" | FA90 {fa90:,}"
            f" | FA95 {fa95:,}"
            f" | FPA {fpa:,}"
            f" | EXT {extreme:,}"
        )

        print(
            f"OUTCOMES "
            f"10s={o10:,}"
            f" | 30s={o30:,}"
            f" | 60s={o60:,}"
            f" | 300s={o300:,}"
        )

        print(
            f"ACTIVE TOKENS "
            f"{len(active_tokens):,}"
        )

# ============================================================
# RECOVERY
# ============================================================

async def recover():
    rows = db.execute("""
        SELECT signature
        FROM signatures
        WHERE status='WAITING'
    """).fetchall()

    for row in rows:
        await queue.put((
            time.time(),
            row["signature"]
        ))

    print(
        f"Recovered queue: "
        f"{len(rows):,}"
    )

# ============================================================
# MAIN
# ============================================================

async def main():
    print()
    print("=" * 96)
    print(
        "MEMECOIN LAB — "
        "EVENT TRACKER V0.8"
    )
    print("=" * 96)

    print(
        f"FA90={FA_P90}"
        f" | FA95={FA_P95}"
        f" | NF75={NF_P75}"
        f" | IMB75={IMB_P75}"
    )

    print(
        f"DISCOVERY={SAMPLE_PERCENT}%"
        f" | EVENT COOLDOWN="
        f"{EVENT_COOLDOWN}s"
    )

    print(
        f"DB={DB_FILE}"
    )

    print("=" * 96)

    await recover()

    connector = aiohttp.TCPConnector(
        limit=2
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = [
            asyncio.create_task(
                listener(name, program)
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
            "\nV0.8 stopped."
        )

    finally:
        db.commit()
        db.close()
