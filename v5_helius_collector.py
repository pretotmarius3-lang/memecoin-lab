#!/usr/bin/env python3
"""Memecoin Lab V5 raw data factory — Helius WebSocket collector.

Purpose
-------
Capture the *raw* real-time transaction stream for Pump.fun bonding-curve and
PumpSwap programs without pretending that a lightweight parser has reconstructed
perfect executions. Raw payloads are compressed and retained so later IDL-aware
parsers can be upgraded without losing the original observations.

Research-only: this process never signs or submits a transaction and needs only a
Helius API key from the HELIUS_API_KEY environment variable.

Sources
-------
Pump program:     6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
Pump AMM program: pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA

The collector uses Helius transactionSubscribe with full jsonParsed details,
confirmed commitment, automatic WebSocket pinging, reconnect backoff, batched WAL
writes, signature deduplication, and a configurable disk safety limit.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
import zlib
from pathlib import Path
from typing import Any
from urllib.parse import quote

try:
    import websockets
except ImportError:
    raise SystemExit("Missing dependency. Run: pip install -r requirements-v5.txt")

ROOT = Path.home() / "memecoin_lab"
DB_PATH = Path(os.environ.get("MEMECOIN_V5_DB", ROOT / "v5_raw_events.db"))
API_KEY = os.environ.get("HELIUS_API_KEY", "").strip()
WS_BASE = os.environ.get("HELIUS_WS_BASE", "wss://mainnet.helius-rpc.com/")
COMMITMENT = os.environ.get("MEMECOIN_V5_COMMITMENT", "confirmed")
MAX_DB_GB = float(os.environ.get("MEMECOIN_V5_MAX_DB_GB", "20"))
BATCH_SIZE = int(os.environ.get("MEMECOIN_V5_BATCH_SIZE", "250"))
BATCH_MAX_WAIT = float(os.environ.get("MEMECOIN_V5_BATCH_MAX_WAIT", "0.25"))
QUEUE_MAX = int(os.environ.get("MEMECOIN_V5_QUEUE_MAX", "20000"))

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PROGRAMS = {
    101: ("PUMP", PUMP_PROGRAM),
    102: ("PUMPSWAP", PUMP_AMM_PROGRAM),
}

STOP = asyncio.Event()


def open_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def initialize() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = open_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS v5_raw_transactions (
            signature TEXT PRIMARY KEY,
            source_program TEXT NOT NULL,
            source_program_id TEXT NOT NULL,
            subscription_id INTEGER,
            slot INTEGER,
            transaction_index INTEGER,
            observed_at REAL NOT NULL,
            event_hint TEXT,
            token_hint TEXT,
            creator_hint TEXT,
            payload_zlib BLOB NOT NULL,
            payload_bytes INTEGER NOT NULL,
            compressed_bytes INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_v5_raw_slot ON v5_raw_transactions(slot);
        CREATE INDEX IF NOT EXISTS idx_v5_raw_observed ON v5_raw_transactions(observed_at);
        CREATE INDEX IF NOT EXISTS idx_v5_raw_event ON v5_raw_transactions(event_hint);
        CREATE INDEX IF NOT EXISTS idx_v5_raw_token ON v5_raw_transactions(token_hint);

        CREATE TABLE IF NOT EXISTS v5_collector_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS v5_sessions (
            session_id TEXT PRIMARY KEY,
            started_at REAL NOT NULL,
            stopped_at REAL,
            reconnects INTEGER NOT NULL DEFAULT 0,
            received INTEGER NOT NULL DEFAULT 0,
            inserted INTEGER NOT NULL DEFAULT 0,
            duplicates INTEGER NOT NULL DEFAULT 0,
            last_error TEXT
        );
        """
    )
    db.commit()
    db.close()


def set_state(db: sqlite3.Connection, key: str, value: Any) -> None:
    db.execute(
        """INSERT INTO v5_collector_state(key,value,updated_at) VALUES(?,?,?)
           ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
        (key, json.dumps(value, separators=(",", ":"), default=str), time.time()),
    )


def db_size_bytes() -> int:
    total = 0
    for p in (DB_PATH, Path(str(DB_PATH) + "-wal"), Path(str(DB_PATH) + "-shm")):
        try:
            total += p.stat().st_size
        except OSError:
            pass
    return total


def account_keys(result: dict) -> list[str]:
    try:
        keys = result["transaction"]["transaction"]["message"]["accountKeys"]
    except (KeyError, TypeError):
        return []
    out = []
    for k in keys or []:
        if isinstance(k, str):
            out.append(k)
        elif isinstance(k, dict) and k.get("pubkey"):
            out.append(str(k["pubkey"]))
    return out


def log_messages(result: dict) -> list[str]:
    try:
        logs = result["transaction"]["meta"].get("logMessages") or []
        return [str(x) for x in logs]
    except (KeyError, TypeError, AttributeError):
        return []


def infer_event_hint(logs: list[str]) -> str:
    """Conservative hint only; authoritative decoding comes from the future IDL parser."""
    text = "\n".join(logs).lower()
    tests = (
        ("MIGRATE", ("instruction: migrate",)),
        ("CREATE", ("instruction: create", "instruction: initializemint2")),
        ("BUY", ("instruction: buy", "instruction: buy_v2", "instruction: buyexactsolin")),
        ("SELL", ("instruction: sell", "instruction: sell_v2")),
    )
    for label, needles in tests:
        if any(n in text for n in needles):
            return label
    return "OTHER"


def token_balance_mints(result: dict) -> list[str]:
    try:
        meta = result["transaction"]["meta"] or {}
    except (KeyError, TypeError):
        return []
    mints = []
    for name in ("preTokenBalances", "postTokenBalances"):
        for b in meta.get(name) or []:
            mint = b.get("mint") if isinstance(b, dict) else None
            if mint and mint not in mints:
                mints.append(str(mint))
    return mints


def hints(result: dict, source: str) -> tuple[str, str | None, str | None]:
    logs = log_messages(result)
    event = infer_event_hint(logs)
    keys = account_keys(result)
    token = None
    creator = keys[0] if keys else None

    # Helius' Pump monitoring guide identifies account key 1 as the mint for
    # creation transactions. For all other transactions we only publish a token
    # hint when token balances reveal exactly one non-WSOL mint.
    if source == "PUMP" and event == "CREATE" and len(keys) > 1:
        token = keys[1]
    else:
        ignored = {"So11111111111111111111111111111111111111112"}
        mints = [m for m in token_balance_mints(result) if m not in ignored]
        if len(mints) == 1:
            token = mints[0]
    return event, token, creator


def pack_notification(source: str, program_id: str, sub_id: int | None, result: dict) -> tuple | None:
    signature = result.get("signature")
    if not signature:
        return None
    event, token, creator = hints(result, source)
    raw = json.dumps(result, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed = zlib.compress(raw, level=3)
    return (
        str(signature), source, program_id, sub_id,
        result.get("slot"), result.get("transactionIndex"), time.time(),
        event, token, creator, sqlite3.Binary(compressed), len(raw), len(compressed),
    )


async def writer(queue: asyncio.Queue, session_id: str, counters: dict[str, int]) -> None:
    db = open_db()
    sql = """INSERT OR IGNORE INTO v5_raw_transactions(
        signature,source_program,source_program_id,subscription_id,slot,transaction_index,
        observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    try:
        while not STOP.is_set() or not queue.empty():
            batch = []
            try:
                item = await asyncio.wait_for(queue.get(), timeout=BATCH_MAX_WAIT)
                batch.append(item)
            except asyncio.TimeoutError:
                pass
            deadline = time.monotonic() + BATCH_MAX_WAIT
            while len(batch) < BATCH_SIZE and time.monotonic() < deadline:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if not batch:
                continue
            before = db.total_changes
            db.executemany(sql, batch)
            inserted = db.total_changes - before
            counters["inserted"] += inserted
            counters["duplicates"] += len(batch) - inserted
            now = time.time()
            set_state(db, "last_write_at", now)
            set_state(db, "last_signature", batch[-1][0])
            set_state(db, "rows", counters["inserted"])
            set_state(db, "queue_depth", queue.qsize())
            db.execute(
                "UPDATE v5_sessions SET received=?,inserted=?,duplicates=? WHERE session_id=?",
                (counters["received"], counters["inserted"], counters["duplicates"], session_id),
            )
            db.commit()
            for _ in batch:
                queue.task_done()

            if db_size_bytes() >= MAX_DB_GB * 1024**3:
                set_state(db, "disk_guard", {"triggered": True, "max_db_gb": MAX_DB_GB})
                db.commit()
                print(f"\nDISK GUARD: V5 database reached {MAX_DB_GB:.1f} GB; stopping safely.")
                STOP.set()
    finally:
        db.commit()
        db.close()


async def subscribe_once(queue: asyncio.Queue, session_id: str, counters: dict[str, int], reconnect_no: int) -> None:
    url = f"{WS_BASE}?api-key={quote(API_KEY)}"
    subscriptions: dict[int, tuple[str, str]] = {}
    request_ids = {rid: pair for rid, pair in PROGRAMS.items()}

    async with websockets.connect(
        url,
        ping_interval=30,
        ping_timeout=20,
        close_timeout=10,
        max_size=None,
        max_queue=4096,
    ) as ws:
        print("Helius WebSocket connected")
        for rid, (source, program_id) in PROGRAMS.items():
            req = {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "transactionSubscribe",
                "params": [
                    {"failed": False, "vote": False, "accountInclude": [program_id]},
                    {
                        "commitment": COMMITMENT,
                        "encoding": "jsonParsed",
                        "transactionDetails": "full",
                        "showRewards": False,
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
            }
            await ws.send(json.dumps(req, separators=(",", ":")))

        db = open_db()
        set_state(db, "connection", "CONNECTED")
        set_state(db, "connected_at", time.time())
        set_state(db, "commitment", COMMITMENT)
        set_state(db, "programs", {v[0]: v[1] for v in PROGRAMS.values()})
        db.commit(); db.close()

        async for raw in ws:
            if STOP.is_set():
                break
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            # Subscription acknowledgement maps Helius' server subscription ID
            # back to the requested Pump/PumpSwap source.
            if "id" in msg and "result" in msg and msg.get("id") in request_ids:
                source, program_id = request_ids[msg["id"]]
                sub_id = int(msg["result"])
                subscriptions[sub_id] = (source, program_id)
                print(f"Subscribed {source:<8} id={sub_id} program={program_id}")
                continue
            if msg.get("error"):
                raise RuntimeError(f"Helius subscription error: {msg['error']}")
            if msg.get("method") != "transactionNotification":
                continue

            params = msg.get("params") or {}
            result = params.get("result") or {}
            sub_id = params.get("subscription")
            source_program = subscriptions.get(int(sub_id)) if sub_id is not None else None
            if source_program is None:
                # Do not discard observations solely because an acknowledgement
                # race or provider behavior prevented mapping the source.
                keys = set(account_keys(result))
                if PUMP_PROGRAM in keys:
                    source_program = ("PUMP", PUMP_PROGRAM)
                elif PUMP_AMM_PROGRAM in keys:
                    source_program = ("PUMPSWAP", PUMP_AMM_PROGRAM)
                else:
                    source_program = ("UNKNOWN", "UNKNOWN")

            packed = pack_notification(source_program[0], source_program[1], sub_id, result)
            if packed is None:
                continue
            counters["received"] += 1
            await queue.put(packed)

            if counters["received"] % 1000 == 0:
                mb = db_size_bytes() / 1024**2
                print(
                    f"received={counters['received']:,} inserted={counters['inserted']:,} "
                    f"dupes={counters['duplicates']:,} queue={queue.qsize():,} db={mb:,.1f}MB"
                )


async def main_async() -> None:
    if not API_KEY:
        raise SystemExit(
            "HELIUS_API_KEY is not set. Keep the key local and export it in this terminal; "
            "do not commit it to GitHub."
        )
    initialize()
    session_id = f"V5-{int(time.time())}-{os.getpid()}"
    counters = {"received": 0, "inserted": 0, "duplicates": 0}
    db = open_db()
    db.execute("INSERT INTO v5_sessions(session_id,started_at) VALUES(?,?)", (session_id, time.time()))
    set_state(db, "session_id", session_id)
    set_state(db, "connection", "STARTING")
    db.commit(); db.close()

    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
    writer_task = asyncio.create_task(writer(queue, session_id, counters))
    reconnects = 0
    backoff = 1.0

    try:
        while not STOP.is_set():
            try:
                await subscribe_once(queue, session_id, counters, reconnects)
                if not STOP.is_set():
                    raise RuntimeError("WebSocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnects += 1
                db = open_db()
                set_state(db, "connection", "RECONNECTING")
                set_state(db, "last_error", repr(exc))
                db.execute(
                    "UPDATE v5_sessions SET reconnects=?,last_error=? WHERE session_id=?",
                    (reconnects, repr(exc)[-4000:], session_id),
                )
                db.commit(); db.close()
                print(f"WebSocket error: {exc!r} | reconnect in {backoff:.0f}s")
                try:
                    await asyncio.wait_for(STOP.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(30.0, backoff * 2)
            else:
                backoff = 1.0
    finally:
        STOP.set()
        await queue.join()
        await writer_task
        db = open_db()
        set_state(db, "connection", "STOPPED")
        db.execute(
            "UPDATE v5_sessions SET stopped_at=?,reconnects=?,received=?,inserted=?,duplicates=? WHERE session_id=?",
            (time.time(), reconnects, counters["received"], counters["inserted"], counters["duplicates"], session_id),
        )
        db.commit(); db.close()
        print(
            f"V5 stopped cleanly | received={counters['received']:,} "
            f"inserted={counters['inserted']:,} duplicates={counters['duplicates']:,}"
        )


def main() -> None:
    if sys.platform != "win32":
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, STOP.set)
        try:
            loop.run_until_complete(main_async())
        finally:
            loop.close()
    else:
        asyncio.run(main_async())


if __name__ == "__main__":
    main()
