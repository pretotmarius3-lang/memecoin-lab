#!/usr/bin/env python3
"""Memecoin Lab V5.1 durable Helius Free collector.

Key difference from V5 Free collector:
- every successful Pump/PumpSwap log signature is persisted immediately to SQLite
- HTTP enrichment drains the durable spool at a controlled rate
- restart-safe: pending signatures survive process crashes/restarts
- bounded in-memory work only; no 50k RAM queue requirement
- 429-aware global pacing

Research-only. Never signs or submits transactions.
"""
from __future__ import annotations

import asyncio, json, os, signal, sqlite3, sys, time, urllib.error, urllib.request, zlib
from pathlib import Path
from urllib.parse import quote

try:
    import websockets
except ImportError:
    raise SystemExit("Missing dependency. Run: pip install -r requirements-v5.txt")

ROOT=Path.home()/"memecoin_lab"
DB_PATH=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
API_KEY=os.environ.get("HELIUS_API_KEY","").strip()
WS_BASE=os.environ.get("HELIUS_WS_BASE","wss://mainnet.helius-rpc.com/")
HTTP_BASE=os.environ.get("HELIUS_HTTP_BASE","https://mainnet.helius-rpc.com/")
COMMITMENT=os.environ.get("MEMECOIN_V5_COMMITMENT","confirmed")
FETCH_WORKERS=int(os.environ.get("MEMECOIN_V51_FETCH_WORKERS","3"))
TARGET_RPS=float(os.environ.get("MEMECOIN_V51_TARGET_RPS","7.0"))
HTTP_TIMEOUT=float(os.environ.get("MEMECOIN_V5_HTTP_TIMEOUT","20"))
MAX_RETRIES=int(os.environ.get("MEMECOIN_V51_MAX_RETRIES","12"))
CLAIM_LEASE_S=float(os.environ.get("MEMECOIN_V51_LEASE_S","60"))
MAX_DB_GB=float(os.environ.get("MEMECOIN_V5_MAX_DB_GB","20"))

PUMP_PROGRAM="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PROGRAMS={101:("PUMP",PUMP_PROGRAM),102:("PUMPSWAP",PUMP_AMM_PROGRAM)}
STOP=asyncio.Event()
RATE_LOCK=asyncio.Lock(); NEXT_HTTP_AT=0.0


def open_db():
    db=sqlite3.connect(DB_PATH,timeout=30)
    db.row_factory=sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def initialize():
    DB_PATH.parent.mkdir(parents=True,exist_ok=True)
    db=open_db(); db.executescript("""
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
    CREATE TABLE IF NOT EXISTS v51_signature_spool (
      signature TEXT PRIMARY KEY,
      source_program TEXT NOT NULL,
      source_program_id TEXT NOT NULL,
      subscription_id INTEGER,
      slot INTEGER,
      logs_json TEXT NOT NULL,
      event_hint TEXT NOT NULL,
      priority INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDING',
      attempts INTEGER NOT NULL DEFAULT 0,
      lease_until REAL,
      first_seen REAL NOT NULL,
      updated_at REAL NOT NULL,
      last_error TEXT);
    CREATE INDEX IF NOT EXISTS idx_v51_spool_claim ON v51_signature_spool(status,priority,first_seen);
    CREATE INDEX IF NOT EXISTS idx_v51_spool_lease ON v51_signature_spool(status,lease_until);
    """); db.commit(); db.close()


def set_state(db,key,value):
    db.execute("""INSERT INTO v5_collector_state(key,value,updated_at) VALUES(?,?,?)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
    (key,json.dumps(value,separators=(",",":"),default=str),time.time()))


def infer_event_hint(logs):
    text="\n".join(logs or []).lower()
    for label, needles in (("MIGRATE",("instruction: migrate",)),("CREATE",("instruction: create","instruction: initializemint2")),("BUY",("instruction: buy","instruction: buy_v2","instruction: buyexactsolin")),("SELL",("instruction: sell","instruction: sell_v2"))):
        if any(n in text for n in needles): return label
    return "OTHER"


def priority_for(event,source):
    return {"CREATE":1,"MIGRATE":2,"BUY":10,"SELL":10,"OTHER":40}.get(event,50)+(0 if source=="PUMP" else 2)


def spool_signature(source,pid,sub_id,signature,slot,logs):
    event=infer_event_hint(logs); now=time.time(); db=open_db()
    db.execute("""INSERT OR IGNORE INTO v51_signature_spool(
      signature,source_program,source_program_id,subscription_id,slot,logs_json,event_hint,priority,status,first_seen,updated_at)
      VALUES(?,?,?,?,?,?,?,?, 'PENDING',?,?)""",
      (signature,source,pid,sub_id,slot,json.dumps(logs,separators=(",",":")),event,priority_for(event,source),now,now))
    inserted=db.total_changes>0
    if inserted: set_state(db,"last_spooled_signature",signature); set_state(db,"last_spool_at",now)
    db.commit(); db.close(); return inserted


def claim_spool(worker):
    now=time.time(); db=open_db(); db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        row=db.execute("""SELECT * FROM v51_signature_spool WHERE status='PENDING' AND attempts<?
                          ORDER BY priority ASC,first_seen ASC LIMIT 1""",(MAX_RETRIES,)).fetchone()
        if row is None: db.commit(); db.close(); return None
        cur=db.execute("""UPDATE v51_signature_spool SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=?
                          WHERE signature=? AND status='PENDING'""",(now+CLAIM_LEASE_S,now,row['signature']))
        if cur.rowcount!=1: db.rollback(); db.close(); return None
        out=dict(row); out['attempts']=int(row['attempts'])+1; db.commit(); db.close(); return out
    except BaseException:
        db.rollback(); db.close(); raise


async def rate_limit():
    global NEXT_HTTP_AT
    async with RATE_LOCK:
        now=time.monotonic(); wait=max(0.0,NEXT_HTTP_AT-now)
        if wait: await asyncio.sleep(wait)
        NEXT_HTTP_AT=max(time.monotonic(),NEXT_HTTP_AT)+1.0/max(0.1,TARGET_RPS)


def http_tx_sync(sig):
    url=f"{HTTP_BASE}?api-key={quote(API_KEY)}"
    payload=json.dumps({"jsonrpc":"2.0","id":sig[-12:],"method":"getTransaction","params":[sig,{"encoding":"jsonParsed","commitment":COMMITMENT,"maxSupportedTransactionVersion":0}]}).encode()
    req=urllib.request.Request(url,data=payload,headers={"Content-Type":"application/json"},method="POST")
    with urllib.request.urlopen(req,timeout=HTTP_TIMEOUT) as resp: body=json.loads(resp.read().decode())
    if body.get('error'): raise RuntimeError(f"RPC error: {body['error']}")
    return body.get('result')


async def get_tx(sig):
    await rate_limit(); return await asyncio.to_thread(http_tx_sync,sig)


def account_keys(tx):
    try: keys=tx['transaction']['message']['accountKeys']
    except Exception: return []
    return [k if isinstance(k,str) else str(k.get('pubkey')) for k in keys or [] if isinstance(k,str) or (isinstance(k,dict) and k.get('pubkey'))]


def token_mints(tx):
    meta=tx.get('meta') or {}; out=[]
    for name in ('preTokenBalances','postTokenBalances'):
        for b in meta.get(name) or []:
            if isinstance(b,dict) and b.get('mint') and b['mint'] not in out: out.append(str(b['mint']))
    return out


def hints(tx,source,event):
    keys=account_keys(tx); creator=keys[0] if keys else None
    mints=[m for m in token_mints(tx) if m!='So11111111111111111111111111111111111111112']
    token=mints[0] if len(mints)==1 else (keys[1] if source=='PUMP' and event=='CREATE' and len(keys)>1 else None)
    return token,creator


def store_success(row,tx):
    logs=json.loads(row['logs_json']); event=row['event_hint']; token,creator=hints(tx,row['source_program'],event)
    raw=json.dumps({'signature':row['signature'],'slot':row['slot'],'logs':logs,'rpc_transaction':tx},separators=(",",":"),ensure_ascii=False).encode(); comp=zlib.compress(raw,3)
    db=open_db(); before=db.total_changes
    db.execute("""INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,
      observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (row['signature'],row['source_program'],row['source_program_id'],row['subscription_id'],row['slot'],None,time.time(),event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
    inserted=db.total_changes-before
    db.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),row['signature']))
    set_state(db,'last_write_at',time.time()); set_state(db,'last_signature',row['signature']); db.commit(); db.close(); return inserted


def mark_retry(row,error,terminal=False):
    db=open_db(); status='FAILED' if terminal or row['attempts']>=MAX_RETRIES else 'PENDING'
    db.execute("UPDATE v51_signature_spool SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",(status,error[-2000:],time.time(),row['signature']))
    set_state(db,'last_fetch_error',error[-1000:]); db.commit(); db.close()


async def fetch_worker(index,counters):
    while not STOP.is_set():
        row=claim_spool(index)
        if row is None: await asyncio.sleep(.15); continue
        try:
            try: tx=await get_tx(row['signature'])
            except urllib.error.HTTPError as exc:
                if exc.code==429:
                    counters['429']+=1; mark_retry(row,f"HTTP 429 {exc.reason}"); await asyncio.sleep(1.0); continue
                if exc.code in (401,403): mark_retry(row,f"HTTP {exc.code} {exc.reason}",True); await asyncio.sleep(.5); continue
                raise
            if tx is None:
                mark_retry(row,'getTransaction returned null'); await asyncio.sleep(.15); continue
            counters['fetched']+=1; counters['inserted']+=store_success(row,tx)
            if counters['fetched']%100==0:
                db=open_db(); s={r['status']:r['n'] for r in db.execute("SELECT status,COUNT(*) n FROM v51_signature_spool GROUP BY status")}; db.close()
                print(f"fetched={counters['fetched']:,} inserted={counters['inserted']:,} 429={counters['429']:,} pending={s.get('PENDING',0):,} done={s.get('DONE',0):,}")
        except Exception as exc:
            counters['failed']+=1; mark_retry(row,repr(exc)); await asyncio.sleep(.25)


async def subscribe(counters):
    url=f"{WS_BASE}?api-key={quote(API_KEY)}"; subs={}; request_ids={rid:pair for rid,pair in PROGRAMS.items()}
    async with websockets.connect(url,ping_interval=20,ping_timeout=20,close_timeout=10,max_size=None,max_queue=16384) as ws:
        print('Helius Standard WebSocket connected — V5.1 durable spool')
        for rid,(source,pid) in PROGRAMS.items():
            await ws.send(json.dumps({'jsonrpc':'2.0','id':rid,'method':'logsSubscribe','params':[{'mentions':[pid]},{'commitment':COMMITMENT}]},separators=(",",":")))
        db=open_db(); set_state(db,'connection','CONNECTED_V51'); set_state(db,'transport','durable_spool+getTransaction'); db.commit(); db.close()
        async for raw in ws:
            if STOP.is_set(): break
            msg=json.loads(raw)
            if 'id' in msg and msg.get('id') in request_ids:
                if msg.get('error'): raise RuntimeError(msg['error'])
                source,pid=request_ids[msg['id']]; sid=int(msg['result']); subs[sid]=(source,pid); print(f"Subscribed {source:<8} id={sid} program={pid}"); continue
            if msg.get('method')!='logsNotification': continue
            params=msg.get('params') or {}; result=params.get('result') or {}; value=result.get('value') or {}; sid=params.get('subscription')
            sig=value.get('signature'); err=value.get('err'); logs=value.get('logs') or []; slot=(result.get('context') or {}).get('slot')
            if not sig or err is not None: continue
            source,pid=subs.get(int(sid),("UNKNOWN","UNKNOWN")) if sid is not None else ("UNKNOWN","UNKNOWN")
            counters['logs']+=1; counters['spooled']+=int(spool_signature(source,pid,sid,sig,slot,logs))
            if counters['logs']%1000==0:
                db=open_db(); pending=db.execute("SELECT COUNT(*) FROM v51_signature_spool WHERE status='PENDING'").fetchone()[0]; db.close()
                print(f"logs={counters['logs']:,} spooled={counters['spooled']:,} fetched={counters['fetched']:,} pending={pending:,}")


async def main_async():
    if not API_KEY: raise SystemExit('HELIUS_API_KEY is not set in this terminal.')
    initialize(); counters={'logs':0,'spooled':0,'fetched':0,'inserted':0,'429':0,'failed':0}
    workers=[asyncio.create_task(fetch_worker(i+1,counters)) for i in range(FETCH_WORKERS)]
    backoff=1.0
    try:
        while not STOP.is_set():
            try:
                await subscribe(counters)
                if not STOP.is_set(): raise RuntimeError('WebSocket closed')
            except asyncio.CancelledError: raise
            except Exception as exc:
                db=open_db(); set_state(db,'connection','RECONNECTING_V51'); set_state(db,'last_error',repr(exc)); db.commit(); db.close()
                print(f"WebSocket error: {exc!r} | reconnect in {backoff:.0f}s")
                try: await asyncio.wait_for(STOP.wait(),timeout=backoff)
                except asyncio.TimeoutError: pass
                backoff=min(30.0,backoff*2)
    finally:
        STOP.set()
        for w in workers: w.cancel()
        await asyncio.gather(*workers,return_exceptions=True)
        db=open_db(); set_state(db,'connection','STOPPED_V51'); db.commit(); db.close()


def main():
    if sys.platform!='win32':
        loop=asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        for sig in (signal.SIGINT,signal.SIGTERM): loop.add_signal_handler(sig,STOP.set)
        try: loop.run_until_complete(main_async())
        finally: loop.close()
    else: asyncio.run(main_async())

if __name__=='__main__': main()
