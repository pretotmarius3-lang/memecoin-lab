#!/usr/bin/env python3
"""Memecoin Lab V5 — Helius FREE-plan collector.

Uses only standard Solana RPC methods available on Helius Free:
  1) logsSubscribe for Pump + PumpSwap signatures/logs
  2) getTransaction over HTTP to enrich each successful signature

Research-only. Never signs or submits transactions.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.request
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
HTTP_BASE = os.environ.get("HELIUS_HTTP_BASE", "https://mainnet.helius-rpc.com/")
COMMITMENT = os.environ.get("MEMECOIN_V5_COMMITMENT", "confirmed")
MAX_DB_GB = float(os.environ.get("MEMECOIN_V5_MAX_DB_GB", "20"))
QUEUE_MAX = int(os.environ.get("MEMECOIN_V5_QUEUE_MAX", "50000"))
FETCH_WORKERS = int(os.environ.get("MEMECOIN_V5_FETCH_WORKERS", "4"))
HTTP_TIMEOUT = float(os.environ.get("MEMECOIN_V5_HTTP_TIMEOUT", "20"))
MAX_FETCH_RETRIES = int(os.environ.get("MEMECOIN_V5_FETCH_RETRIES", "7"))

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PROGRAMS = {101: ("PUMP", PUMP_PROGRAM), 102: ("PUMPSWAP", PUMP_AMM_PROGRAM)}
STOP = asyncio.Event()


def open_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def initialize():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = open_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v5_raw_transactions (
      signature TEXT PRIMARY KEY, source_program TEXT NOT NULL, source_program_id TEXT NOT NULL,
      subscription_id INTEGER, slot INTEGER, transaction_index INTEGER, observed_at REAL NOT NULL,
      event_hint TEXT, token_hint TEXT, creator_hint TEXT, payload_zlib BLOB NOT NULL,
      payload_bytes INTEGER NOT NULL, compressed_bytes INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v5_raw_slot ON v5_raw_transactions(slot);
    CREATE INDEX IF NOT EXISTS idx_v5_raw_observed ON v5_raw_transactions(observed_at);
    CREATE INDEX IF NOT EXISTS idx_v5_raw_event ON v5_raw_transactions(event_hint);
    CREATE INDEX IF NOT EXISTS idx_v5_raw_token ON v5_raw_transactions(token_hint);
    CREATE TABLE IF NOT EXISTS v5_collector_state (key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v5_sessions (
      session_id TEXT PRIMARY KEY, started_at REAL NOT NULL, stopped_at REAL, reconnects INTEGER NOT NULL DEFAULT 0,
      received INTEGER NOT NULL DEFAULT 0, inserted INTEGER NOT NULL DEFAULT 0, duplicates INTEGER NOT NULL DEFAULT 0, last_error TEXT);
    CREATE TABLE IF NOT EXISTS v5_fetch_failures (
      signature TEXT PRIMARY KEY, source_program TEXT, slot INTEGER, attempts INTEGER NOT NULL, last_error TEXT, updated_at REAL NOT NULL);
    """)
    db.commit(); db.close()


def set_state(db,key,value):
    db.execute("""INSERT INTO v5_collector_state(key,value,updated_at) VALUES(?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
               (key,json.dumps(value,separators=(",",":"),default=str),time.time()))


def db_size_bytes():
    total=0
    for p in (DB_PATH,Path(str(DB_PATH)+"-wal"),Path(str(DB_PATH)+"-shm")):
        try: total += p.stat().st_size
        except OSError: pass
    return total


def infer_event_hint(logs):
    text="\n".join(logs or []).lower()
    for label,needles in (("MIGRATE",("instruction: migrate",)),("CREATE",("instruction: create","instruction: initializemint2")),
                          ("BUY",("instruction: buy","instruction: buy_v2","instruction: buyexactsolin")),
                          ("SELL",("instruction: sell","instruction: sell_v2"))):
        if any(n in text for n in needles): return label
    return "OTHER"


def account_keys(tx):
    try: keys=tx["transaction"]["message"]["accountKeys"]
    except Exception: return []
    out=[]
    for k in keys or []:
        if isinstance(k,str): out.append(k)
        elif isinstance(k,dict) and k.get("pubkey"): out.append(str(k["pubkey"]))
    return out


def token_balance_mints(tx):
    meta=tx.get("meta") or {}; out=[]
    for name in ("preTokenBalances","postTokenBalances"):
        for b in meta.get(name) or []:
            if isinstance(b,dict) and b.get("mint") and b["mint"] not in out: out.append(str(b["mint"]))
    return out


def hints(tx,source,logs):
    event=infer_event_hint(logs); keys=account_keys(tx); creator=keys[0] if keys else None; token=None
    mints=[m for m in token_balance_mints(tx) if m!="So11111111111111111111111111111111111111112"]
    if len(mints)==1: token=mints[0]
    elif source=="PUMP" and event=="CREATE" and len(keys)>1: token=keys[1]
    return event,token,creator


def http_get_transaction_sync(signature):
    url=f"{HTTP_BASE}?api-key={quote(API_KEY)}"
    payload=json.dumps({"jsonrpc":"2.0","id":signature[-12:],"method":"getTransaction","params":[signature,
        {"encoding":"jsonParsed","commitment":COMMITMENT,"maxSupportedTransactionVersion":0}]}).encode()
    req=urllib.request.Request(url,data=payload,headers={"Content-Type":"application/json"},method="POST")
    with urllib.request.urlopen(req,timeout=HTTP_TIMEOUT) as resp: body=json.loads(resp.read().decode())
    if body.get("error"): raise RuntimeError(f"RPC error: {body['error']}")
    return body.get("result")


async def get_transaction(signature): return await asyncio.to_thread(http_get_transaction_sync,signature)


def pack(source,pid,sub_id,signature,slot,logs,tx):
    event,token,creator=hints(tx,source)
    raw=json.dumps({"signature":signature,"slot":slot or tx.get("slot"),"logs":logs,"rpc_transaction":tx},separators=(",",":"),ensure_ascii=False).encode()
    comp=zlib.compress(raw,3)
    return (signature,source,pid,sub_id,slot or tx.get("slot"),None,time.time(),event,token,creator,sqlite3.Binary(comp),len(raw),len(comp))


async def store_row(row,counters,session_id):
    db=open_db(); before=db.total_changes
    db.execute("""INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,
                observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",row)
    inserted=db.total_changes-before; counters["inserted"]+=inserted; counters["duplicates"]+=1-inserted
    set_state(db,"last_write_at",time.time()); set_state(db,"last_signature",row[0]); set_state(db,"rows",counters["inserted"])
    db.execute("UPDATE v5_sessions SET received=?,inserted=?,duplicates=? WHERE session_id=?",
               (counters["received"],counters["inserted"],counters["duplicates"],session_id)); db.commit(); db.close()


async def fetch_worker(index,q,counters,session_id):
    while not STOP.is_set() or not q.empty():
        try: source,pid,sub_id,signature,slot,logs=await asyncio.wait_for(q.get(),timeout=.5)
        except asyncio.TimeoutError: continue
        try:
            tx=None; last_err=None
            for attempt in range(1,MAX_FETCH_RETRIES+1):
                try:
                    tx=await get_transaction(signature)
                    if tx is not None: break
                    last_err="getTransaction returned null"
                except urllib.error.HTTPError as exc:
                    last_err=f"HTTP {exc.code}: {exc.reason}"
                    if exc.code==401: raise RuntimeError("Helius API key rejected (HTTP 401)") from exc
                except Exception as exc: last_err=repr(exc)
                await asyncio.sleep(min(5.0,.25*(2**(attempt-1))))
            if tx is None:
                db=open_db(); db.execute("""INSERT INTO v5_fetch_failures(signature,source_program,slot,attempts,last_error,updated_at) VALUES(?,?,?,?,?,?)
                    ON CONFLICT(signature) DO UPDATE SET attempts=excluded.attempts,last_error=excluded.last_error,updated_at=excluded.updated_at""",
                    (signature,source,slot,MAX_FETCH_RETRIES,last_err,time.time())); set_state(db,"last_fetch_error",last_err); db.commit(); db.close()
                counters["fetch_failed"]+=1; continue
            await store_row(pack(source,pid,sub_id,signature,slot,logs,tx),counters,session_id); counters["fetched"]+=1
            if counters["fetched"]%100==0:
                print(f"fetched={counters['fetched']:,} inserted={counters['inserted']:,} failed={counters['fetch_failed']:,} backlog={q.qsize():,} db={db_size_bytes()/1024**2:,.1f}MB")
        finally: q.task_done()


async def subscribe_logs(q,session_id,counters):
    url=f"{WS_BASE}?api-key={quote(API_KEY)}"; subscriptions={}; request_ids={rid:pair for rid,pair in PROGRAMS.items()}
    async with websockets.connect(url,ping_interval=30,ping_timeout=20,close_timeout=10,max_size=None,max_queue=8192) as ws:
        print("Helius Standard WebSocket connected (FREE-compatible)")
        for rid,(source,pid) in PROGRAMS.items():
            await ws.send(json.dumps({"jsonrpc":"2.0","id":rid,"method":"logsSubscribe","params":[{"mentions":[pid]},{"commitment":COMMITMENT}]},separators=(",",":")))
        db=open_db(); set_state(db,"connection","CONNECTED_FREE"); set_state(db,"transport","logsSubscribe+getTransaction"); db.commit(); db.close()
        async for raw in ws:
            if STOP.is_set(): break
            msg=json.loads(raw)
            if "id" in msg and msg.get("id") in request_ids:
                if msg.get("error"): raise RuntimeError(f"Helius logsSubscribe error: {msg['error']}")
                source,pid=request_ids[msg["id"]]; sub_id=int(msg["result"]); subscriptions[sub_id]=(source,pid)
                print(f"Subscribed {source:<8} id={sub_id} program={pid}"); continue
            if msg.get("error"): raise RuntimeError(f"Helius websocket error: {msg['error']}")
            if msg.get("method")!="logsNotification": continue
            params=msg.get("params") or {}; sub_id=params.get("subscription"); result=params.get("result") or {}; value=result.get("value") or {}
            signature=value.get("signature"); err=value.get("err"); logs=value.get("logs") or []; slot=(result.get("context") or {}).get("slot")
            if not signature or err is not None: continue
            source,pid=subscriptions.get(int(sub_id),("UNKNOWN","UNKNOWN")) if sub_id is not None else ("UNKNOWN","UNKNOWN")
            counters["received"]+=1; await q.put((source,pid,sub_id,signature,slot,logs))
            if counters["received"]%500==0: print(f"logs={counters['received']:,} fetched={counters['fetched']:,} backlog={q.qsize():,}")
            if db_size_bytes()>=MAX_DB_GB*1024**3: print(f"DISK GUARD reached {MAX_DB_GB:.1f} GB; stopping safely"); STOP.set(); break


async def main_async():
    if not API_KEY: raise SystemExit("HELIUS_API_KEY is not set in this terminal.")
    initialize(); session_id=f"V5FREE-{int(time.time())}-{os.getpid()}"; counters={"received":0,"fetched":0,"inserted":0,"duplicates":0,"fetch_failed":0}
    db=open_db(); db.execute("INSERT INTO v5_sessions(session_id,started_at) VALUES(?,?)",(session_id,time.time())); set_state(db,"session_id",session_id); set_state(db,"connection","STARTING_FREE"); db.commit(); db.close()
    q=asyncio.Queue(maxsize=QUEUE_MAX); workers=[asyncio.create_task(fetch_worker(i+1,q,counters,session_id)) for i in range(FETCH_WORKERS)]
    reconnects=0; backoff=1.0
    try:
        while not STOP.is_set():
            try:
                await subscribe_logs(q,session_id,counters)
                if not STOP.is_set(): raise RuntimeError("WebSocket closed")
            except asyncio.CancelledError: raise
            except Exception as exc:
                reconnects+=1; db=open_db(); set_state(db,"connection","RECONNECTING_FREE"); set_state(db,"last_error",repr(exc)); db.execute("UPDATE v5_sessions SET reconnects=?,last_error=? WHERE session_id=?",(reconnects,repr(exc)[-4000:],session_id)); db.commit(); db.close()
                print(f"WebSocket error: {exc!r} | reconnect in {backoff:.0f}s")
                try: await asyncio.wait_for(STOP.wait(),timeout=backoff)
                except asyncio.TimeoutError: pass
                backoff=min(30.0,backoff*2)
            else: backoff=1.0
    finally:
        STOP.set(); await q.join()
        for w in workers: w.cancel()
        await asyncio.gather(*workers,return_exceptions=True)
        db=open_db(); set_state(db,"connection","STOPPED"); db.execute("UPDATE v5_sessions SET stopped_at=?,reconnects=?,received=?,inserted=?,duplicates=? WHERE session_id=?",(time.time(),reconnects,counters['received'],counters['inserted'],counters['duplicates'],session_id)); db.commit(); db.close()
        print(f"V5 FREE stopped | logs={counters['received']:,} fetched={counters['fetched']:,} inserted={counters['inserted']:,} failed={counters['fetch_failed']:,}")


def main():
    if sys.platform!="win32":
        loop=asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(sig,STOP.set)
        try: loop.run_until_complete(main_async())
        finally: loop.close()
    else: asyncio.run(main_async())


if __name__=="__main__": main()
